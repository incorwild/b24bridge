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
	labels := Labels(p.DialogID, p.SenderID, p.Text, p.System, meta, me, in.Filter.IncludeSystem)
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
	return in.add(ctx, &it)
}

func (in *Inbox) add(ctx context.Context, it *Item) error {
	added, err := in.Store.Add(ctx, it)
	if err != nil || !added {
		return err
	}
	in.Log.Info("inbox: new", "id", it.ID, "labels", strings.Join(it.Labels, ","), "dialog", it.DialogID)
	if in.OnNew != nil {
		in.OnNew(*it)
	}
	return nil
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
			labels := Labels(d, m.AuthorID, m.Text, m.AuthorID == 0, meta, me, in.Filter.IncludeSystem)
			if len(labels) == 0 {
				continue
			}
			it := Item{
				MessageID: m.ID, DialogID: d, ChatID: h.ChatID.Int(), SenderID: m.AuthorID,
				SenderName: h.UserName(m.AuthorID), Text: withAttachment(PlainText(m.Text), hasFiles(m.Params)), Date: m.Date,
				Labels: labels, ChatTitle: meta.Title, TaskID: meta.TaskID, TaskTitle: meta.TaskTitle,
			}
			if err := in.add(ctx, &it); err != nil {
				return err
			}
			n++
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
	if m := reOK.FindStringSubmatch(text); m != nil {
		id, _ := strconv.ParseInt(m[2], 10, 64)
		return in.Approve(ctx, id, "")
	}
	if m := reNo.FindStringSubmatch(text); m != nil {
		id, _ := strconv.ParseInt(m[2], 10, 64)
		return in.Reject(ctx, id)
	}
	if m := reEdit.FindStringSubmatch(text); m != nil {
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
