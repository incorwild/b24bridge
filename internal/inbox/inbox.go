package inbox

import (
	"context"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"b24bridge/internal/bitrix"
)

// Portal is what the inbox needs from the Bitrix24 client.
type Portal interface {
	API
	SendMessage(ctx context.Context, dialogID, text string) (int64, error)
	DialogHistory(ctx context.Context, dialogID string, limit int) (bitrix.History, error)
	MyActiveTasks(ctx context.Context, userID int64, since time.Time) ([]bitrix.Task, error)
	RecentChats(ctx context.Context, limit int) ([]bitrix.RecentItem, error)
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

	fillMu     sync.Mutex    // one backfill at a time
	retryDelay time.Duration // first pause between reply lookups
}

func New(store *Store, portal Portal, me func() int64) *Inbox {
	return &Inbox{
		Store:    store,
		Portal:   portal,
		Resolver: &Resolver{API: portal, Me: me},
		Me:       me,
		Log:      slog.Default(),
		queue:    make(chan bitrix.Event, 1000),

		retryDelay: time.Second,
	}
}

// Enqueue never blocks the push listener; classification needs REST calls.
func (in *Inbox) Enqueue(ev bitrix.Event) {
	switch ev.Event {
	case bitrix.EvMessage, bitrix.EvMessageEdited, bitrix.EvMessageDeleted:
	default:
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

// Handle processes one message event: new, edited or deleted.
func (in *Inbox) Handle(ctx context.Context, ev bitrix.Event) error {
	p := ev.Payload
	switch ev.Event {
	case bitrix.EvMessageEdited:
		return in.edited(ctx, p)
	case bitrix.EvMessageDeleted:
		return in.Store.DoneMessages(ctx, p.MessageIDs...)
	}
	me := in.Me()
	self := strconv.FormatInt(me, 10)
	if p.SenderID == me {
		if p.DialogID == self {
			return in.command(ctx, p.Text)
		}
		// The user answered by hand.
		if IsPrivateDialog(p.DialogID) {
			if in.batch != nil {
				in.batch.Cancel(p.DialogID)
			}
			return in.Store.DoneDialog(ctx, p.DialogID)
		}
		// In a group only the quoted message is certainly answered; the batch
		// keeps running and skips whatever got closed.
		if p.ReplyTo != nil {
			if err := in.Store.DoneMessages(ctx, p.ReplyTo.ID); err != nil {
				return err
			}
		}
		return in.Store.DoneConversation(ctx, p.DialogID)
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

// edited updates the text of a waiting item and hands it to the agent again.
// Bitrix24 also sends messageUpdate when only a link preview was added.
func (in *Inbox) edited(ctx context.Context, p bitrix.Payload) error {
	it, ok, err := in.Store.ByMessage(ctx, p.MessageID)
	if err != nil || !ok || it.Status != "new" {
		return err
	}
	text := withAttachment(PlainText(p.Text), strings.HasSuffix(it.Text, "вложение]"))
	if text == "" || text == it.Text {
		return nil
	}
	if err := in.Store.Edit(ctx, it.ID, text); err != nil {
		return err
	}
	it.Text, it.Edited = text, true
	in.notifyNew(it)
	return nil
}

// lookup loads a message, retrying temporary failures.
func (in *Inbox) lookup(ctx context.Context, dialogID string, id int64) (m bitrix.HistoryMessage, ok bool, err error) {
	wait := in.retryDelay
	for try := 0; ; try++ {
		m, ok, err = in.Portal.MessageByID(ctx, dialogID, id)
		if err == nil || !transient(err) || try == 2 {
			return m, ok, err
		}
		select {
		case <-ctx.Done():
			return m, ok, ctx.Err()
		case <-time.After(wait):
		}
		wait *= 3
	}
}

// transient tells a temporary failure from a refusal (no access, no such chat).
func transient(err error) bool {
	var ae *bitrix.APIError
	if errors.As(err, &ae) {
		return ae.Status >= 500 || strings.Contains(ae.Code, "LIMIT") || strings.Contains(ae.Code, "INTERNAL")
	}
	return true
}

// resolveReply completes a reply reference and reports whether it answers the
// user. If the target cannot be loaded for a temporary reason even after
// retries, the reply counts as addressed to the user: an extra notification
// is better than a missed answer.
func (in *Inbox) resolveReply(ctx context.Context, dialogID string, ref *bitrix.ReplyRef) (*bitrix.ReplyRef, bool) {
	if ref == nil || ref.ID == 0 {
		return nil, false
	}
	r := *ref
	if r.AuthorID == 0 {
		m, ok, err := in.lookup(ctx, dialogID, r.ID)
		if err != nil {
			addressed := transient(err)
			in.Log.Warn("inbox: reply target", "dialog", dialogID, "message_id", r.ID, "notify", addressed, "err", err)
			return &r, addressed
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
	in.notifyNew(*it)
	return true, nil
}

func (in *Inbox) notifyNew(it Item) {
	if in.batch != nil {
		in.batch.Add(it.DialogID)
	}
	if in.OnNew != nil {
		in.OnNew(it)
	}
}

// Backfill picks up what arrived while the bridge was not listening: unread
// dialogs, chats of my tasks with activity since, and - for messages addressed
// to the user personally - chats with messages since that were already read.
func (in *Inbox) Backfill(ctx context.Context, since time.Time) error {
	in.fillMu.Lock()
	defer in.fillMu.Unlock()
	me := in.Me()
	dialogs := map[string]bool{} // dialog → personal messages only
	recent, err := in.Portal.RecentChats(ctx, 200)
	if err != nil {
		return fmt.Errorf("recent list: %w", err)
	}
	for _, r := range recent {
		t, err := time.Parse(time.RFC3339, r.Message.Date)
		switch {
		case r.Counter > 0:
			dialogs[string(r.ID)] = false
		case err == nil && !t.Before(since) && !IsPrivateDialog(string(r.ID)):
			dialogs[string(r.ID)] = true
		}
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
		dialogs[d] = false
	}
	n := 0
	for d, personalOnly := range dialogs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		added, err := in.backfillDialog(ctx, d, since, personalOnly)
		n += added
		if err != nil {
			return err
		}
	}
	in.Log.Info("inbox: backfill done", "dialogs", len(dialogs), "items", n)
	return nil
}

func (in *Inbox) backfillDialog(ctx context.Context, d string, since time.Time, personalOnly bool) (int, error) {
	me := in.Me()
	h, err := in.Portal.DialogHistory(ctx, d, 50)
	if err != nil {
		in.Log.Warn("inbox: history", "dialog", d, "err", err)
		return 0, nil
	}
	private := IsPrivateDialog(d)
	var lastMine int64
	for _, m := range h.Messages {
		if m.AuthorID == me && m.ID > lastMine {
			lastMine = m.ID
		}
	}
	var todo []bitrix.HistoryMessage
	for _, m := range h.Messages {
		if m.AuthorID == me || (private && m.ID <= lastMine) {
			continue
		}
		if t, err := time.Parse(time.RFC3339, m.Date); err == nil && t.Before(since) {
			continue
		}
		if m.AuthorID == 0 && !in.Filter.IncludeSystem {
			continue
		}
		if in.Filter.Skip(m.AuthorID, h.IsBot(m.AuthorID), m.Text) != "" {
			continue
		}
		if _, known, err := in.Store.ByMessage(ctx, m.ID); err != nil {
			return 0, err
		} else if known {
			continue
		}
		todo = append(todo, m)
	}
	if len(todo) == 0 {
		return 0, nil // no further requests for this dialog
	}
	meta, err := in.Resolver.Meta(ctx, d)
	if err != nil {
		meta = ChatMeta{Private: private}
	}
	refs, err := in.Portal.ReplyRefs(ctx, d, 50)
	if err != nil {
		in.Log.Warn("inbox: replies", "dialog", d, "err", err)
	}
	answered := map[int64]bool{} // messages I quoted in an answer
	for _, m := range h.Messages {
		if r, ok := refs[m.ID]; ok && m.AuthorID == me {
			answered[r.ID] = true
		}
	}
	n := 0
	for _, m := range todo {
		var ref *bitrix.ReplyRef
		if r, ok := refs[m.ID]; ok {
			ref = &r
		}
		reply, replyToMe := in.resolveReply(ctx, d, ref)
		labels := Labels(d, m.AuthorID, m.Text, m.AuthorID == 0, replyToMe, meta, me, in.Filter.IncludeSystem)
		personal := slices.Contains(labels, LabelMention) || slices.Contains(labels, LabelReply)
		switch {
		case len(labels) == 0, personalOnly && !personal:
			continue
		case !private && personal && answered[m.ID], !private && !personal && m.ID <= lastMine:
			continue // already answered
		}
		it := Item{
			MessageID: m.ID, DialogID: d, ChatID: h.ChatID.Int(), SenderID: m.AuthorID,
			SenderName: h.UserName(m.AuthorID), Text: withAttachment(PlainText(m.Text), hasFiles(m.Params)), Date: m.Date,
			Labels: labels, ChatTitle: meta.Title, TaskID: meta.TaskID, TaskTitle: meta.TaskTitle,
		}
		it.setReply(reply)
		added, err := in.add(ctx, &it)
		if err != nil {
			return n, err
		}
		if added {
			n++
		}
	}
	return n, nil
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
	in.closeAnswered(ctx, d.DialogID, d.InboxID)
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
			in.closeAnswered(ctx, d.DialogID, d.InboxID)
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

// closeAnswered closes what a sent message answers: everything in a private
// chat; in a group the given item with its sender's earlier messages.
func (in *Inbox) closeAnswered(ctx context.Context, dialogID string, inboxID int64) {
	var err error
	if IsPrivateDialog(dialogID) {
		err = in.Store.DoneDialog(ctx, dialogID)
	}
	if inboxID > 0 {
		it, e := in.Store.Get(ctx, inboxID)
		if e == nil && it.DialogID == dialogID {
			e = in.Store.DoneBurst(ctx, it)
		}
		err = errors.Join(err, e)
	}
	if err != nil {
		in.Log.Warn("inbox: close answered", "dialog", dialogID, "inbox_id", inboxID, "err", err)
	}
}

// SendNow sends without approval (AGENT_SEND_MODE=direct).
func (in *Inbox) SendNow(ctx context.Context, dialogID, text string, inboxID int64) (int64, error) {
	id, err := in.Portal.SendMessage(ctx, dialogID, text)
	if err == nil {
		in.closeAnswered(ctx, dialogID, inboxID)
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
