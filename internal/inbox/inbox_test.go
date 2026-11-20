package inbox

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"b24bridge/internal/bitrix"
)

const me = 54

type sent struct{ dialog, text string }

type fakePortal struct {
	mu      sync.Mutex
	sent    []sent
	dialogs map[string]bitrix.DialogInfo
	tasks   map[int64]bitrix.Task
	hist    map[string]bitrix.History
	unread  []bitrix.RecentItem
	failTo  string
}

func (f *fakePortal) DialogInfo(_ context.Context, d string) (bitrix.DialogInfo, error) {
	return f.dialogs[d], nil
}
func (f *fakePortal) TaskGet(_ context.Context, id int64) (bitrix.Task, error) {
	return f.tasks[id], nil
}
func (f *fakePortal) SendMessage(_ context.Context, d, t string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d == f.failTo {
		return 0, fmt.Errorf("boom")
	}
	f.sent = append(f.sent, sent{d, t})
	return int64(1000 + len(f.sent)), nil
}
func (f *fakePortal) DialogHistory(_ context.Context, d string, _ int) (bitrix.History, error) {
	return f.hist[d], nil
}
func (f *fakePortal) MyActiveTasks(_ context.Context, uid int64, _ time.Time) ([]bitrix.Task, error) {
	var out []bitrix.Task
	for _, t := range f.tasks {
		if t.Role(uid) != "" {
			out = append(out, t)
		}
	}
	return out, nil
}
func (f *fakePortal) UnreadDialogs(context.Context) ([]bitrix.RecentItem, error) {
	return f.unread, nil
}

func (f *fakePortal) sentTo(d string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, s := range f.sent {
		if s.dialog == d {
			out = append(out, s.text)
		}
	}
	return out
}

func newTest(t *testing.T) (*Inbox, *fakePortal) {
	t.Helper()
	st, err := Open(t.TempDir() + "/inbox.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	fp := &fakePortal{
		dialogs: map[string]bitrix.DialogInfo{
			"chat100":  {Name: "Общий", Type: "chat"},
			"chat5986": {Name: "Задача", Type: "tasksTask", EntityType: "TASKS_TASK", EntityID: "5314"},
			"chat7000": {Name: "Чужая задача", Type: "tasksTask", EntityType: "TASKS_TASK", EntityID: "7000"},
			"chat7100": {Name: "Соисполнитель", Type: "tasksTask", EntityType: "TASKS_TASK", EntityID: "7100"},
		},
		tasks: map[int64]bitrix.Task{
			5314: {ID: "5314", Title: "Отчёт", ResponsibleID: "54", Accomplices: []bitrix.ID{"20"}, ChatID: "5986"},
			7000: {ID: "7000", Title: "Не моя", ResponsibleID: "20", ChatID: "7000"},
			7100: {ID: "7100", Title: "Помогаю", ResponsibleID: "20", Accomplices: []bitrix.ID{"54"}, ChatID: "7100"},
		},
		hist: map[string]bitrix.History{},
	}
	in := New(st, fp, func() int64 { return me })
	return in, fp
}

func msg(id int64, dialog string, sender int64, text string) bitrix.Event {
	return bitrix.Event{Event: bitrix.EvMessage, Payload: bitrix.Payload{
		MessageID: id, DialogID: dialog, SenderID: sender, Text: text, IsFromMe: sender == me,
		Sender: &bitrix.UserBrief{ID: sender, Name: fmt.Sprintf("User%d", sender)},
	}}
}

func labelsOf(t *testing.T, in *Inbox) map[int64]string {
	items, err := in.Store.List(context.Background(), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	out := map[int64]string{}
	for _, it := range items {
		out[it.MessageID] = strings.Join(it.Labels, ",")
	}
	return out
}

func TestClassification(t *testing.T) {
	in, _ := newTest(t)
	ctx := context.Background()
	for _, ev := range []bitrix.Event{
		msg(1, "88", 88, "привет"),                                // direct
		msg(2, "chat100", 88, "всем привет"),                      // group noise
		msg(3, "chat100", 88, "[USER=54]Иван[/USER], глянь"),      // mention
		msg(4, "chat100", 88, "[USER=540]Другой[/USER]"),          // not me
		msg(5, "chat5986", 20, "готово?"),                         // my task (responsible)
		msg(6, "chat7000", 88, "в чужой задаче"),                  // not my task
		msg(7, "chat7100", 20, "нужна помощь"),                    // accomplice
		msg(8, "chat7000", 88, "[user=54]Иван[/user] подключись"), // mention in foreign task
		msg(9, "88", me, "мой ответ"),                             // mine
		msg(10, "54", 88, "self chat"),                            // self chat is never inbox
	} {
		if err := in.Handle(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	got := labelsOf(t, in)
	want := map[int64]string{1: "direct", 3: "mention", 5: "task_responsible", 7: "task_accomplice", 8: "mention"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("labels\n got %v\nwant %v", got, want)
	}
	// My reply in dialog 88 (message 9) closed item 1.
	items, _ := in.Store.List(ctx, "new", 100)
	for _, it := range items {
		if it.MessageID == 1 {
			t.Fatal("direct message should be done after my reply")
		}
	}
	// Duplicate push is ignored.
	_ = in.Handle(ctx, msg(5, "chat5986", 20, "готово?"))
	if len(labelsOf(t, in)) != 5 {
		t.Fatal("duplicate stored")
	}
}

func TestSystemMessagesInTaskChats(t *testing.T) {
	in, _ := newTest(t)
	ev := msg(1, "chat5986", 0, "Задача просрочена")
	ev.Payload.System = true
	_ = in.Handle(context.Background(), ev)
	if len(labelsOf(t, in)) != 0 {
		t.Fatal("system message stored by default")
	}
	in.Filter.IncludeSystem = true
	ev.Payload.MessageID = 2
	_ = in.Handle(context.Background(), ev)
	if labelsOf(t, in)[2] != "task_responsible" {
		t.Fatal("system message should be kept with IncludeSystem")
	}
}

func TestDraftApprovalFlow(t *testing.T) {
	in, fp := newTest(t)
	ctx := context.Background()
	_ = in.Handle(ctx, msg(1, "88", 88, "когда созвон?"))
	items, _ := in.Store.List(ctx, "new", 10)

	d, err := in.ProposeReply(ctx, "88", "Давай в 15:00", items[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	ask := fp.sentTo("54")
	if len(ask) != 1 || !strings.Contains(ask[0], fmt.Sprintf("Черновик #%d", d.ID)) || !strings.Contains(ask[0], "когда созвон?") || !strings.Contains(ask[0], "Давай в 15:00") {
		t.Fatalf("self-chat prompt: %q", ask)
	}
	// The bridge's own prompt comes back as my message in the self chat: no-op.
	_ = in.Handle(ctx, msg(500, "54", me, ask[0]))
	if len(fp.sentTo("88")) != 0 {
		t.Fatal("prompt echo must not send")
	}

	_ = in.Handle(ctx, msg(501, "54", me, fmt.Sprintf("ок %d", d.ID)))
	if got := fp.sentTo("88"); len(got) != 1 || got[0] != "Давай в 15:00" {
		t.Fatalf("sent to 88: %v", got)
	}
	_ = in.Handle(ctx, msg(502, "54", me, fmt.Sprintf("OK #%d", d.ID))) // double approval
	if len(fp.sentTo("88")) != 1 {
		t.Fatal("draft sent twice")
	}
	if n, _ := in.Store.CountNew(ctx); n != 0 {
		t.Fatalf("inbox item should be done, %d new", n)
	}
	got, _ := in.Store.GetDraft(ctx, d.ID)
	if got.Status != "sent" || got.SentMessageID == 0 {
		t.Fatalf("draft %+v", got)
	}
}

func TestDraftEditRejectAndFailure(t *testing.T) {
	in, fp := newTest(t)
	ctx := context.Background()
	d1, _ := in.ProposeReply(ctx, "88", "черновик", 0)
	_ = in.Handle(ctx, msg(1, "54", me, fmt.Sprintf("%d: исправленный\nтекст", d1.ID)))
	if got := fp.sentTo("88"); len(got) != 1 || got[0] != "исправленный\nтекст" {
		t.Fatalf("edited send: %v", got)
	}

	d2, _ := in.ProposeReply(ctx, "88", "не надо", 0)
	_ = in.Handle(ctx, msg(2, "54", me, fmt.Sprintf("нет %d", d2.ID)))
	if len(fp.sentTo("88")) != 1 {
		t.Fatal("rejected draft was sent")
	}
	if g, _ := in.Store.GetDraft(ctx, d2.ID); g.Status != "rejected" {
		t.Fatalf("status %s", g.Status)
	}

	fp.failTo = "99"
	d3, _ := in.ProposeReply(ctx, "99", "упадёт", 0)
	_ = in.Handle(ctx, msg(3, "54", me, fmt.Sprintf("ок %d", d3.ID)))
	if g, _ := in.Store.GetDraft(ctx, d3.ID); g.Status != "failed" || g.Note == "" {
		t.Fatalf("draft %+v", g)
	}

	if _, err := in.ProposeReply(ctx, "54", "self", 0); err == nil {
		t.Fatal("drafts to the self chat must be refused")
	}
}

func TestBackfill(t *testing.T) {
	in, fp := newTest(t)
	now := time.Now()
	ts := func(ago time.Duration) string { return now.Add(-ago).Format(time.RFC3339) }
	fp.unread = []bitrix.RecentItem{{ID: "88", Counter: 2}}
	fp.hist["88"] = bitrix.History{Users: []bitrix.HistoryUser{{ID: "88", Name: "Коллега"}}, Messages: []bitrix.HistoryMessage{
		{ID: 14, AuthorID: 88, Date: ts(time.Minute), Text: "ау"},
		{ID: 13, AuthorID: 88, Date: ts(2 * time.Minute), Text: "вопрос"},
		{ID: 12, AuthorID: me, Date: ts(time.Hour), Text: "мой ответ"},
		{ID: 11, AuthorID: 88, Date: ts(2 * time.Hour), Text: "старое, я уже ответил"},
	}}
	fp.hist["chat7100"] = bitrix.History{ChatID: "7100", Messages: []bitrix.HistoryMessage{
		{ID: 21, AuthorID: 20, Date: ts(time.Minute), Text: "нужна помощь"},
		{ID: 20, AuthorID: 20, Date: ts(72 * time.Hour), Text: "давно"},
	}}
	if err := in.Backfill(context.Background(), now.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	got := labelsOf(t, in)
	want := map[int64]string{13: "direct", 14: "direct", 21: "task_accomplice"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("backfill\n got %v\nwant %v", got, want)
	}
	items, _ := in.Store.List(context.Background(), "new", 10)
	for _, it := range items {
		if it.MessageID == 13 && it.SenderName != "Коллега" {
			t.Fatalf("sender name %q", it.SenderName)
		}
		if it.MessageID == 21 && it.TaskTitle != "Помогаю" {
			t.Fatalf("task title %q", it.TaskTitle)
		}
	}
}

func TestPlainText(t *testing.T) {
	got := PlainText("[USER=54]Иван Петров[/USER], [b]срочно[/b] см. [URL=https://x]ссылку[/URL]")
	if got != "@Иван Петров, срочно см. ссылку" {
		t.Fatalf("%q", got)
	}
}

func TestAttachments(t *testing.T) {
	if !hasFiles([]byte(`{"FILE_ID":["123"]}`)) || !hasFiles([]byte(`{"FILE_ID":[45]}`)) || hasFiles([]byte(`{"FILE_ID":[]}`)) {
		t.Fatal("hasFiles")
	}
	if withAttachment("", true) != "[вложение]" || withAttachment("см.", true) != "см. [+вложение]" || withAttachment("x", false) != "x" {
		t.Fatal("withAttachment")
	}
}

// Service messages as seen in real task chats (2026-11-20): author 0, and
// often a mention of the user. None of them may reach the agent by default.
func TestTaskServiceMessagesIgnored(t *testing.T) {
	in, _ := newTest(t)
	ctx := context.Background()
	for i, text := range []string{
		"[USER=20]Иван[/USER] создал [URL=/workgroups/group/30/tasks/task/view/5314/]задачу[/URL]",
		"[USER=20]Иван[/USER] изменил исполнителя на [USER=54]Пётр[/USER]",
		"[USER=20]Иван[/USER] добавил соисполнителя [USER=54]Пётр[/USER]",
		"[USER=54]Пётр[/USER], задача просрочена",
		"[USER=86]Анна[/USER] изменил стадию на ПАУЗА",
	} {
		ev := msg(int64(100+i), "chat5986", 0, text)
		ev.Payload.System = true
		if err := in.Handle(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := labelsOf(t, in); len(got) != 0 {
		t.Fatalf("service messages reached the inbox: %v", got)
	}
}

func TestFilterBotsSendersText(t *testing.T) {
	in, _ := newTest(t)
	ctx := context.Background()
	f, err := ParseFilter(false, true, "77, 78", `^напоминание:`)
	if err != nil {
		t.Fatal(err)
	}
	in.Filter = f

	bot := msg(1, "300", 300, "Ваш отчёт готов")
	bot.Payload.Sender.Bot = true
	_ = in.Handle(ctx, bot)                                       // bot in a private chat
	_ = in.Handle(ctx, msg(2, "77", 77, "привет"))                // ignored sender
	_ = in.Handle(ctx, msg(3, "88", 88, "Напоминание: планёрка")) // ignored text, any case
	_ = in.Handle(ctx, msg(4, "88", 88, "а напоминание где?"))    // regex anchored: kept
	got := labelsOf(t, in)
	if fmt.Sprint(got) != "map[4:direct]" {
		t.Fatalf("got %v", got)
	}

	if _, err := ParseFilter(false, true, "x", ""); err == nil {
		t.Fatal("bad sender id must fail")
	}
	if _, err := ParseFilter(false, true, "", "("); err == nil {
		t.Fatal("bad regexp must fail")
	}
}

func TestBackfillSkipsBots(t *testing.T) {
	in, fp := newTest(t)
	in.Filter.IgnoreBots = true
	now := time.Now().Format(time.RFC3339)
	fp.unread = []bitrix.RecentItem{{ID: "300", Counter: 1}}
	fp.hist["300"] = bitrix.History{
		Users:    []bitrix.HistoryUser{{ID: "300", Name: "CRM-бот", Bot: true}},
		Messages: []bitrix.HistoryMessage{{ID: 1, AuthorID: 300, Date: now, Text: "Новый лид"}},
	}
	_ = in.Backfill(context.Background(), time.Now().Add(-time.Hour))
	if got := labelsOf(t, in); len(got) != 0 {
		t.Fatalf("bot message in inbox: %v", got)
	}
}

func TestInboxDBIsPrivate(t *testing.T) {
	path := t.TempDir() + "/inbox.db"
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode().Perm())
	}
}

func TestRecoverSending(t *testing.T) {
	in, fp := newTest(t)
	ctx := context.Background()
	d1, _ := in.ProposeReply(ctx, "88", "Готово, проверь", 0)
	d2, _ := in.ProposeReply(ctx, "99", "Не дошло", 0)
	for _, id := range []int64{d1.ID, d2.ID} {
		if ok, _ := in.Store.MoveDraft(ctx, id, "pending", "sending", "", "", 0); !ok {
			t.Fatal("setup")
		}
	}
	// The crash happened after Bitrix accepted d1 but before bookkeeping.
	fp.hist["88"] = bitrix.History{Messages: []bitrix.HistoryMessage{
		{ID: 777, AuthorID: me, Date: time.Now().Format(time.RFC3339), Text: "Готово, проверь"},
	}}
	before := len(fp.sentTo("54"))
	if err := in.RecoverSending(ctx); err != nil {
		t.Fatal(err)
	}
	if g, _ := in.Store.GetDraft(ctx, d1.ID); g.Status != "sent" || g.SentMessageID != 777 {
		t.Fatalf("d1 %+v", g)
	}
	if g, _ := in.Store.GetDraft(ctx, d2.ID); g.Status != "failed" {
		t.Fatalf("d2 %+v", g)
	}
	if len(fp.sentTo("88"))+len(fp.sentTo("99")) != 0 {
		t.Fatal("recovery must never resend")
	}
	if len(fp.sentTo("54")) != before+1 {
		t.Fatal("user should be told about the failed draft")
	}
}

func TestEditKeepsFormatting(t *testing.T) {
	in, fp := newTest(t)
	ctx := context.Background()
	d, _ := in.ProposeReply(ctx, "88", "черновик", 0)
	_ = in.Handle(ctx, msg(1, "54", me, fmt.Sprintf("%d: [b]важно[/b] &amp; срочно", d.ID)))
	if got := fp.sentTo("88"); len(got) != 1 || got[0] != "[b]важно[/b] & срочно" {
		t.Fatalf("sent %q", got)
	}
}

func TestResolverCacheBounded(t *testing.T) {
	r := &Resolver{}
	for i := 0; i < maxCache+10; i++ {
		r.Put(fmt.Sprintf("chat%d", i), ChatMeta{})
	}
	if n := r.size(); n > maxCache {
		t.Fatalf("cache grew to %d", n)
	}
}
