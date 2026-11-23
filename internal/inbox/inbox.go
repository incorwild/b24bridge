package inbox

import (
	"context"
	"fmt"
	"html"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"b24bridge/internal/bitrix"
)

// Portal is what the inbox needs from the Bitrix24 client.
type Portal interface {
	API
	SendMessage(ctx context.Context, dialogID, text string) (int64, error)
	DialogHistory(ctx context.Context, dialogID string, limit int) (bitrix.History, error)
	MyActiveTasks(ctx context.Context, userID int64, since time.Time) ([]bitrix.Task, error)
	UnreadDialogs(ctx context.Context) ([]bitrix.RecentItem, error)
	MessageByID(ctx context.Context, dialogID string, id int64) (bitrix.HistoryMessage, bool, error)
	ReplyRefs(ctx context.Context, dialogID string, limit int) (map[int64]bitrix.ReplyRef, error)
}

type Inbox struct {
	Store    *Store
	Portal   Portal
	Resolver *Resolver
	Me       func() int64
	Filter   Filter     // what never reaches the agent
	OnNew    func(Item) // e.g. webhook "inbox.new"
	Log      *slog.Logger

	queue chan bitrix.Event
	batch *Batcher
}

func New(store *Store, portal Portal, me func() int64) *Inbox {
	return &Inbox{
		Store:    store,
		Portal:   portal,
		Resolver: &Resolver{API: portal, Me: me},
		Me:       me,
		Log:      slog.Default(),
		queue:    make(chan bitrix.Event, 1000),
	}
}

// Enqueue never blocks the push listener; classification needs REST calls.
func (in *Inbox) Enqueue(ev bitrix.Event) {
	if ev.Event != bitrix.EvMessage {
		return
	}
	select {
	case in.queue <- ev:
	default:
		in.Log.Error("inbox: queue full, message dropped", "message_id", ev.Payload.MessageID)
	}
}

func (in *Inbox) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-in.queue:
			if err := in.Handle(ctx, ev); err != nil {
				in.Log.Warn("inbox: handle", "message_id", ev.Payload.MessageID, "err", err)
			}
		}
	}
}

// Handle processes one "message" event.
func (in *Inbox) Handle(ctx context.Context, ev bitrix.Event) error {
	p := ev.Payload
	me := in.Me()
	self := strconv.FormatInt(me, 10)
	if p.SenderID == me {
		if p.DialogID == self {
			return in.command(ctx, p.Text)
		}
		// The user answered by hand: whatever waited in that dialog is handled.
		if in.batch != nil {
			in.batch.Cancel(p.DialogID)
		}
		return in.Store.DoneDialog(ctx, p.DialogID)
	}
	meta, err := in.Resolver.Meta(ctx, p.DialogID)
	if err != nil {
		in.Log.Warn("inbox: chat info", "dialog", p.DialogID, "err", err)
		meta = ChatMeta{Private: IsPrivateDialog(p.DialogID)}
	}
	bot := p.Sender != nil && p.Sender.Bot
	if reason := in.Filter.Skip(p.SenderID, bot, p.Text); reason != "" {
		in.Log.Debug("inbox: filtered", "reason", reason, "message_id", p.MessageID)
		return nil
	}
	reply, replyToMe := in.resolveReply(ctx, p.DialogID, p.ReplyTo)
	labels := Labels(p.DialogID, p.SenderID, p.Text, p.System, replyToMe, meta, me, in.Filter.IncludeSystem)
	if len(labels) == 0 {
		return nil
	}
	it := Item{
		MessageID: p.MessageID, DialogID: p.DialogID, ChatID: p.ChatID, SenderID: p.SenderID,
		Text: withAttachment(PlainText(p.Text), p.Files != nil), Date: p.Date, Labels: labels, ChatTitle: meta.Title,
		TaskID: meta.TaskID, TaskTitle: meta.TaskTitle,
	}
	if p.Sender != nil {
		it.SenderName = p.Sender.Name
	}
	it.setReply(reply)
	_, err = in.add(ctx, &it)
	return err
}

// resolveReply completes a reply reference and reports whether it answers the
// user. If the target cannot be loaded, the reply counts as addressed to the
// user: an extra notification is better than a missed answer.
func (in *Inbox) resolveReply(ctx context.Context, dialogID string, ref *bitrix.ReplyRef) (*bitrix.ReplyRef, bool) {
	if ref == nil || ref.ID == 0 {
		return nil, false
	}
	r := *ref
	if r.AuthorID == 0 {
		m, ok, err := in.Portal.MessageByID(ctx, dialogID, r.ID)
		if err != nil {
			in.Log.Warn("inbox: reply target", "dialog", dialogID, "message_id", r.ID, "err", err)
			return &r, true
		}
		if ok {
			r.AuthorID, r.Text = m.AuthorID, m.Text
		}
	}
	return &r, r.AuthorID == in.Me()
}

// add stores it and notifies; added=false for a message already in the inbox.
func (in *Inbox) add(ctx context.Context, it *Item) (bool, error) {
	added, err := in.Store.Add(ctx, it)
	if err != nil || !added {
		return false, err
	}
	in.Log.Info("inbox: new", "id", it.ID, "labels", strings.Join(it.Labels, ","), "dialog", it.DialogID)
	if in.batch != nil {
		in.batch.Add(it.DialogID)
	}
	if in.OnNew != nil {
		in.OnNew(*it)
	}
	return true, nil
}

// Backfill picks up what arrived while the bridge was down: unread dialogs and
// chats of my tasks with activity since. Messages older than my latest reply
// in the same chat count as handled.
func (in *Inbox) Backfill(ctx context.Context, since time.Time) error {
	me := in.Me()
	dialogs := map[string]bool{}
	unread, err := in.Portal.UnreadDialogs(ctx)
	if err != nil {
		return fmt.Errorf("recent list: %w", err)
	}
	for _, r := range unread {
		dialogs[string(r.ID)] = true
	}
	tasks, err := in.Portal.MyActiveTasks(ctx, me, since)
	if err != nil {
		in.Log.Warn("inbox: task list", "err", err)
	}
	for _, t := range tasks {
		if t.ChatID.Int() == 0 {
			continue
		}
		d := "chat" + string(t.ChatID)
		in.Resolver.Put(d, ChatMeta{Title: t.Title, TaskID: t.ID.Int(), TaskTitle: t.Title, TaskRole: t.Role(me)})
		dialogs[d] = true
	}
	n := 0
	for d := range dialogs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		h, err := in.Portal.DialogHistory(ctx, d, 30)
		if err != nil {
			in.Log.Warn("inbox: history", "dialog", d, "err", err)
			continue
		}
		meta, err := in.Resolver.Meta(ctx, d)
		if err != nil {
			meta = ChatMeta{Private: IsPrivateDialog(d)}
		}
		var lastMine int64
		for _, m := range h.Messages {
			if m.AuthorID == me && m.ID > lastMine {
				lastMine = m.ID
			}
		}
		refs, err := in.Portal.ReplyRefs(ctx, d, 50)
		if err != nil {
			in.Log.Warn("inbox: replies", "dialog", d, "err", err)
		}
		for _, m := range h.Messages {
			if m.ID <= lastMine {
				continue
			}
			if t, err := time.Parse(time.RFC3339, m.Date); err == nil && t.Before(since) {
				continue
			}
			if in.Filter.Skip(m.AuthorID, h.IsBot(m.AuthorID), m.Text) != "" {
				continue
			}
			var ref *bitrix.ReplyRef
			if r, ok := refs[m.ID]; ok {
				ref = &r
			}
			reply, replyToMe := in.resolveReply(ctx, d, ref)
			labels := Labels(d, m.AuthorID, m.Text, m.AuthorID == 0, replyToMe, meta, me, in.Filter.IncludeSystem)
			if len(labels) == 0 {
				continue
			}
			it := Item{
				MessageID: m.ID, DialogID: d, ChatID: h.ChatID.Int(), SenderID: m.AuthorID,
				SenderName: h.UserName(m.AuthorID), Text: withAttachment(PlainText(m.Text), hasFiles(m.Params)), Date: m.Date,
				Labels: labels, ChatTitle: meta.Title, TaskID: meta.TaskID, TaskTitle: meta.TaskTitle,
			}
			it.setReply(reply)
			added, err := in.add(ctx, &it)
			if err != nil {
				return err
			}
			if added {
				n++
			}
		}
	}
	in.Log.Info("inbox: backfill done", "dialogs", len(dialogs), "items", n)
	return nil
}

// --- reply drafts approved from the self chat ---

var (
	reOK   = regexp.MustCompile(`(?i)^\s*(ок|ok|да|yes|отправ\S*)\s*#?(\d+)\s*$`)
	reNo   = regexp.MustCompile(`(?i)^\s*(нет|no|отмена|cancel)\s*#?(\d+)\s*$`)
	reEdit = regexp.MustCompile(`(?s)^\s*#?(\d+)\s*:\s*(.+?)\s*$`)
)

func (in *Inbox) selfDialog() string { return strconv.FormatInt(in.Me(), 10) }

func (in *Inbox) notify(ctx context.Context, text string) {
	if _, err := in.Portal.SendMessage(ctx, in.selfDialog(), text); err != nil {
		in.Log.Error("inbox: notify self chat", "err", err)
	}
}

// command handles the user's own messages in the self chat.
func (in *Inbox) command(ctx context.Context, raw string) error {
	text := html.UnescapeString(PlainText(raw))
	// For "N: text" keep the user's own formatting (BB-codes) in the reply.
	rawText := html.UnescapeString(raw)
	if m := reOK.FindStringSubmatch(text); m != nil {
		id, _ := strconv.ParseInt(m[2], 10, 64)
		return in.Approve(ctx, id, "")
	}
	if m := reNo.FindStringSubmatch(text); m != nil {
		id, _ := strconv.ParseInt(m[2], 10, 64)
		return in.Reject(ctx, id)
	}
	if m := reEdit.FindStringSubmatch(rawText); m != nil {
		id, _ := strconv.ParseInt(m[1], 10, 64)
		return in.Approve(ctx, id, m[2])
	}
	return nil
}

// ProposeReply stores a draft and asks the user in the self chat.
func (in *Inbox) ProposeReply(ctx context.Context, dialogID, text string, inboxID int64) (Draft, error) {
	text = strings.TrimSpace(text)
	if dialogID == "" || text == "" {
		return Draft{}, fmt.Errorf("dialog_id and text are required")
	}
	if dialogID == in.selfDialog() {
		return Draft{}, fmt.Errorf("cannot draft a reply to the self chat")
	}
	d := Draft{DialogID: dialogID, Text: text, InboxID: inboxID}
	if err := in.Store.AddDraft(ctx, &d); err != nil {
		return d, err
	}
	to, quote := "диалог "+dialogID, ""
	if inboxID > 0 {
		if it, err := in.Store.Get(ctx, inboxID); err == nil {
			to = describe(it)
			quote = "\nНа сообщение: «" + excerpt(it.Text, 300) + "»\n"
		}
	} else if meta, err := in.Resolver.Meta(ctx, dialogID); err == nil && meta.Title != "" {
		to = meta.Title
	}
	in.notify(ctx, fmt.Sprintf("🤖 Черновик #%d → %s%s\n\n%s\n\nОтветьте: «ок %d» — отправить, «нет %d» — отменить, «%d: другой текст» — отправить с правкой.",
		d.ID, to, quote, text, d.ID, d.ID, d.ID))
	return d, nil
}

// Approve sends a pending draft (with newText if given) and confirms it.
func (in *Inbox) Approve(ctx context.Context, id int64, newText string) error {
	ok, err := in.Store.MoveDraft(ctx, id, "pending", "sending", newText, "", 0)
	if err != nil || !ok {
		if err == nil {
			in.notify(ctx, fmt.Sprintf("⚠️ Черновик #%d не найден или уже обработан.", id))
		}
		return err
	}
	d, err := in.Store.GetDraft(ctx, id)
	if err != nil {
		return err
	}
	msgID, err := in.Portal.SendMessage(ctx, d.DialogID, d.Text)
	if err != nil {
		_, _ = in.Store.MoveDraft(ctx, id, "sending", "failed", "", err.Error(), 0)
		in.notify(ctx, fmt.Sprintf("⚠️ #%d не отправлен: %v", id, err))
		return err
	}
	_, _ = in.Store.MoveDraft(ctx, id, "sending", "sent", "", "", msgID)
	if d.InboxID > 0 {
		_, _ = in.Store.Done(ctx, []int64{d.InboxID})
	}
	_ = in.Store.DoneDialog(ctx, d.DialogID)
	in.notify(ctx, fmt.Sprintf("✅ #%d отправлен.", id))
	return nil
}

// RecoverSending settles drafts left in "sending" by a crash between the
// send and its bookkeeping: if the dialog shows my message with the draft's
// text after the draft was created, it was sent; otherwise mark it failed
// and tell the user, who can resend by hand. Never resends automatically.
func (in *Inbox) RecoverSending(ctx context.Context) error {
	drafts, err := in.Store.ListDrafts(ctx, "sending", 100)
	if err != nil {
		return err
	}
	me := in.Me()
	for _, d := range drafts {
		created, _ := time.Parse(time.RFC3339, d.CreatedAt)
		var sentID int64
		if h, err := in.Portal.DialogHistory(ctx, d.DialogID, 30); err == nil {
			for _, m := range h.Messages {
				t, _ := time.Parse(time.RFC3339, m.Date)
				if m.AuthorID == me && !t.Before(created.Add(-time.Minute)) &&
					sameText(m.Text, d.Text) {
					sentID = m.ID
					break
				}
			}
		} else {
			in.Log.Warn("inbox: recover draft history", "draft", d.ID, "err", err)
			continue // try again on the next start
		}
		if sentID > 0 {
			_, _ = in.Store.MoveDraft(ctx, d.ID, "sending", "sent", "", "recovered after restart", sentID)
			if d.InboxID > 0 {
				_, _ = in.Store.Done(ctx, []int64{d.InboxID})
			}
			continue
		}
		_, _ = in.Store.MoveDraft(ctx, d.ID, "sending", "failed", "", "interrupted, not found in the dialog", 0)
		in.notify(ctx, fmt.Sprintf("⚠️ #%d: отправка прервалась, в диалоге сообщения нет. Отправьте вручную, если нужно.", d.ID))
	}
	return nil
}

func (in *Inbox) Reject(ctx context.Context, id int64) error {
	ok, err := in.Store.MoveDraft(ctx, id, "pending", "rejected", "", "rejected by user", 0)
	if err != nil {
		return err
	}
	if ok {
		in.notify(ctx, fmt.Sprintf("✖ #%d отменён.", id))
	}
	return nil
}

// SendNow sends without approval (AGENT_SEND_MODE=direct) and closes the dialog's items.
func (in *Inbox) SendNow(ctx context.Context, dialogID, text string) (int64, error) {
	id, err := in.Portal.SendMessage(ctx, dialogID, text)
	if err == nil {
		_ = in.Store.DoneDialog(ctx, dialogID)
	}
	return id, err
}

func describe(it Item) string {
	switch {
	case it.TaskTitle != "":
		return fmt.Sprintf("задача «%s» (%s)", it.TaskTitle, nonEmpty(it.SenderName, "участник"))
	case it.ChatTitle != "":
		return fmt.Sprintf("%s в «%s»", nonEmpty(it.SenderName, "участник"), it.ChatTitle)
	default:
		return nonEmpty(it.SenderName, "диалог "+it.DialogID)
	}
}

func nonEmpty(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func excerpt(s string, n int) string {
	r := []rune(strings.Join(strings.Fields(s), " "))
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "…"
}

var reFileID = regexp.MustCompile(`"FILE_ID"\s*:\s*\[\s*"?\d`)

// MessageText is the agent-facing text of a stored message: BB-codes
// stripped, attachments marked.
func MessageText(text string, params []byte) string {
	return withAttachment(PlainText(text), hasFiles(params))
}

// hasFiles reports whether history params carry attachments (FILE_ID: [..]).
func hasFiles(params []byte) bool { return reFileID.Match(params) }

// withAttachment keeps attachment-only messages from looking empty.
func withAttachment(text string, files bool) string {
	switch {
	case !files:
		return text
	case text == "":
		return "[вложение]"
	default:
		return text + " [+вложение]"
	}
}

// sameText compares a sent draft with a message from history, ignoring the
// formatting and whitespace Bitrix24 may add (auto-links, BB-codes, entities).
func sameText(fromHistory, draft string) bool {
	norm := func(t string) string { return strings.Join(strings.Fields(PlainText(html.UnescapeString(t))), " ") }
	return norm(fromHistory) == norm(draft)
}
