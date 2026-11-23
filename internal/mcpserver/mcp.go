// Package mcpserver exposes the inbox and messenger to AI agents over MCP
// (streamable HTTP at /mcp), like ui/mcp in go-whatsapp-web-multidevice.
package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"b24bridge/internal/bitrix"
	"b24bridge/internal/inbox"
)

type Deps struct {
	B24   *bitrix.Client
	Inbox *inbox.Inbox
	// DirectSend registers send_message, which posts without the user's
	// approval. Off by default: agents get propose_reply only.
	DirectSend bool
}

func Handler(d Deps) http.Handler {
	s := server.NewMCPServer("b24bridge", "0.2.0",
		server.WithToolCapabilities(false),
		server.WithInstructions(instructions),
	)
	h := &handlers{d}
	s.AddTool(mcp.NewTool("inbox_list",
		mcp.WithDescription("Messages that need the user's attention: direct messages, mentions of the user, replies to the user's messages (reply_to_text is what was answered), and messages in chats of tasks where the user is responsible or accomplice. Oldest first."),
		mcp.WithString("status", mcp.Description("new (default), done or all"), mcp.Enum("new", "done", "all")),
		mcp.WithNumber("limit", mcp.Description("max items, default 30")),
		mcp.WithBoolean("include_settling", mcp.Description("also return dialogs where messages are still arriving (by default they are held back until the sender pauses)")),
		mcp.WithReadOnlyHintAnnotation(true),
	), h.inboxList)
	s.AddTool(mcp.NewTool("inbox_done",
		mcp.WithDescription("Mark inbox items as handled (no reply needed, or handled elsewhere). Replies sent through propose_reply close items automatically."),
		mcp.WithString("ids", mcp.Required(), mcp.Description("comma-separated inbox item ids, e.g. \"3,4\"")),
	), h.inboxDone)
	s.AddTool(mcp.NewTool("chat_history",
		mcp.WithDescription("Read a dialog's messages, oldest first. Returns the latest `limit` messages; to read further back, call again with before_id = next_before_id from the previous result (while has_more is true). Works for any dialog the user can see, not only inbox ones."),
		mcp.WithString("dialog_id", mcp.Required(), mcp.Description("user id (\"88\") for a private chat, \"chat5986\" for a group or task chat; from inbox_list or chats_list")),
		mcp.WithNumber("limit", mcp.Description("messages to return, default 30, max 200")),
		mcp.WithNumber("before_id", mcp.Description("return messages older than this message id (paging)")),
		mcp.WithReadOnlyHintAnnotation(true),
	), h.chatHistory)
	s.AddTool(mcp.NewTool("chats_list",
		mcp.WithDescription("Find dialogs to read: the user's recent chats (private, group) plus chats of tasks where the user is responsible or accomplice. Use it to get a dialog_id for chat_history when the chat is not in the inbox."),
		mcp.WithString("query", mcp.Description("case-insensitive part of the chat, person or task name")),
		mcp.WithNumber("limit", mcp.Description("default 30, max 200")),
		mcp.WithBoolean("unread_only", mcp.Description("only chats with unread messages")),
		mcp.WithReadOnlyHintAnnotation(true),
	), h.chatsList)
	s.AddTool(mcp.NewTool("task_get",
		mcp.WithDescription("Task title, status, responsible and accomplices."),
		mcp.WithNumber("task_id", mcp.Required()),
		mcp.WithReadOnlyHintAnnotation(true),
	), h.taskGet)
	s.AddTool(mcp.NewTool("propose_reply",
		mcp.WithDescription("Draft a reply on the user's behalf. The draft is sent to the user's own Bitrix24 chat; it goes to the recipient only after the user answers \"ок N\" there (or edits it with \"N: text\"). Returns the draft id."),
		mcp.WithString("dialog_id", mcp.Required(), mcp.Description("where the reply goes, usually the inbox item's dialog_id")),
		mcp.WithString("text", mcp.Required(), mcp.Description("reply text in the user's voice")),
		mcp.WithNumber("inbox_id", mcp.Description("inbox item this answers; it is closed once the reply is sent")),
	), h.proposeReply)
	s.AddTool(mcp.NewTool("drafts_list",
		mcp.WithDescription("Reply drafts and their status: pending (waiting for the user), sent, rejected, failed."),
		mcp.WithString("status", mcp.Description("filter: pending, sent, rejected, failed; empty = all")),
		mcp.WithReadOnlyHintAnnotation(true),
	), h.draftsList)
	s.AddTool(mcp.NewTool("mark_read",
		mcp.WithDescription("Mark a dialog as read in Bitrix24 for the user."),
		mcp.WithString("dialog_id", mcp.Required()),
	), h.markRead)
	if d.DirectSend {
		s.AddTool(mcp.NewTool("send_message",
			mcp.WithDescription("Send a message immediately on the user's behalf, without approval."),
			mcp.WithString("dialog_id", mcp.Required()),
			mcp.WithString("text", mcp.Required()),
			mcp.WithDestructiveHintAnnotation(true),
		), h.sendMessage)
	}
	return server.NewStreamableHTTPServer(s, server.WithStateLess(true), server.WithDisableStreaming(true))
}

const instructions = `You work through the user's Bitrix24 messenger.
Start with inbox_list. People often split one thought into several messages; handle all new items of a dialog together and answer once. For each dialog, read chat_history (and task_get for task chats) before deciding; if the context is not enough, page back with before_id. To look into another chat (e.g. an earlier discussion with the same person), find it with chats_list.
Reply with propose_reply: the user approves every reply in their own chat, so write the final text, in Russian unless the conversation is in another language, in the user's voice, without mentioning that you are an AI.
Close items that need no answer (thanks, FYI, service notices) with inbox_done.
Never invent facts, deadlines or promises; if the answer needs information you do not have, draft a short reply that asks for it or leave the item for the user.`

type handlers struct{ d Deps }

func errResult(err error) (*mcp.CallToolResult, error) {
	return mcp.NewToolResultError(err.Error()), nil
}

func (h *handlers) inboxList(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	st := r.GetString("status", "new")
	if st == "all" {
		st = ""
	}
	items, err := h.d.Inbox.List(ctx, st, r.GetInt("limit", 30), r.GetBool("include_settling", false))
	if err != nil {
		return errResult(err)
	}
	return mcp.NewToolResultStructured(map[string]any{"items": items, "count": len(items)},
		fmt.Sprintf("%d item(s)", len(items))), nil
}

func (h *handlers) inboxDone(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	raw, err := r.RequireString("ids")
	if err != nil {
		return errResult(err)
	}
	var ids []int64
	for _, p := range strings.FieldsFunc(raw, func(c rune) bool { return c == ',' || c == ' ' }) {
		id, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return errResult(fmt.Errorf("bad id %q", p))
		}
		ids = append(ids, id)
	}
	n, err := h.d.Inbox.Store.Done(ctx, ids)
	if err != nil {
		return errResult(err)
	}
	return mcp.NewToolResultText(fmt.Sprintf("%d item(s) marked done", n)), nil
}

type histMsg struct {
	ID     int64  `json:"id"`
	Author string `json:"author"`
	FromMe bool   `json:"from_me,omitempty"`
	System bool   `json:"system,omitempty"`
	Date   string `json:"date"`
	Text   string `json:"text"`
}

func (h *handlers) chatHistory(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dialog, err := dialogArg(r)
	if err != nil {
		return errResult(err)
	}
	limit := r.GetInt("limit", 30)
	if limit <= 0 {
		limit = 30
	}
	if limit > 200 {
		limit = 200
	}
	before := int64(r.GetInt("before_id", 0))
	const page = 50
	var (
		msgs  []bitrix.HistoryMessage
		users = map[int64]string{}
		more  = true
	)
	for len(msgs) < limit && more {
		hist, err := h.d.B24.DialogHistoryBefore(ctx, dialog, page, before)
		if err != nil {
			return errResult(err)
		}
		for _, u := range hist.Users {
			users[u.ID.Int()] = u.Name
		}
		if len(hist.Messages) == 0 {
			more = false
			break
		}
		oldest := before
		for _, m := range hist.Messages { // newest first
			if before > 0 && m.ID >= before {
				continue // never repeat what the caller already has
			}
			if len(msgs) < limit {
				msgs = append(msgs, m)
			}
			if oldest == 0 || m.ID < oldest {
				oldest = m.ID
			}
		}
		if oldest == before { // nothing older came back
			more = false
			break
		}
		before = oldest
	}
	me := h.d.B24.UserID()
	out := make([]histMsg, 0, len(msgs))
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		name := users[m.AuthorID]
		if m.AuthorID == 0 {
			name = "system"
		}
		out = append(out, histMsg{ID: m.ID, Author: name, FromMe: m.AuthorID == me, System: m.AuthorID == 0,
			Date: m.Date, Text: inbox.MessageText(m.Text, m.Params)})
	}
	res := map[string]any{"dialog_id": dialog, "messages": out, "has_more": more && len(out) > 0}
	if len(out) > 0 {
		res["next_before_id"] = out[0].ID
	}
	return mcp.NewToolResultStructured(res, fmt.Sprintf("%d message(s), has_more=%v", len(out), res["has_more"])), nil
}

type chatInfo struct {
	DialogID    string `json:"dialog_id"`
	Title       string `json:"title"`
	Type        string `json:"type"` // user | chat | task | ...
	Unread      int    `json:"unread,omitempty"`
	TaskID      int64  `json:"task_id,omitempty"`
	TaskRole    string `json:"task_role,omitempty"`
	LastDate    string `json:"last_date,omitempty"`
	LastMessage string `json:"last_message,omitempty"`
}

func (h *handlers) chatsList(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := strings.ToLower(strings.TrimSpace(r.GetString("query", "")))
	unreadOnly := r.GetBool("unread_only", false)
	limit := r.GetInt("limit", 30)
	if limit <= 0 || limit > 200 {
		limit = 30
	}
	recent, err := h.d.B24.RecentChats(ctx, 200)
	if err != nil {
		return errResult(err)
	}
	var out []chatInfo
	seen := map[string]bool{}
	match := func(c chatInfo) bool {
		return (query == "" || strings.Contains(strings.ToLower(c.Title), query)) && (!unreadOnly || c.Unread > 0)
	}
	for _, it := range recent {
		c := chatInfo{DialogID: string(it.ID), Title: it.Title, Type: it.Type, Unread: it.Counter,
			LastDate: it.Message.Date, LastMessage: excerpt(inbox.PlainText(it.Message.Text), 120)}
		seen[c.DialogID] = true
		if match(c) {
			out = append(out, c)
		}
	}
	// Task chats are not in the recent list.
	me := h.d.B24.UserID()
	var warning string
	tasks, err := h.d.B24.MyActiveTasks(ctx, me, time.Now().AddDate(0, 0, -30))
	if err != nil {
		warning = "task chats are missing: " + err.Error()
	} else {
		for _, t := range tasks {
			if t.ChatID.Int() == 0 {
				continue
			}
			c := chatInfo{DialogID: "chat" + string(t.ChatID), Title: t.Title, Type: "task",
				TaskID: t.ID.Int(), TaskRole: t.Role(me), LastDate: t.ActivityDate}
			if !seen[c.DialogID] && match(c) {
				out = append(out, c)
			}
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	if out == nil {
		out = []chatInfo{}
	}
	res := map[string]any{"chats": out}
	if warning != "" {
		res["warning"] = warning
	}
	return mcp.NewToolResultStructured(res, fmt.Sprintf("%d chat(s)", len(out))), nil
}

// dialogArg accepts dialog_id as a string ("88", "chat5986") or a number
// (88): models often send user ids as numbers.
func dialogArg(r mcp.CallToolRequest) (string, error) {
	switch v := r.GetArguments()["dialog_id"].(type) {
	case string:
		if v = strings.TrimSpace(v); v != "" {
			return v, nil
		}
	case float64:
		if v > 0 && v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10), nil
		}
	case json.Number:
		return v.String(), nil
	}
	return "", fmt.Errorf("dialog_id is required: a user id like \"88\" or a chat like \"chat5986\"")
}

func excerpt(s string, n int) string {
	r := []rune(strings.Join(strings.Fields(s), " "))
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "…"
}

func (h *handlers) taskGet(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id := r.GetInt("task_id", 0)
	if id <= 0 {
		return errResult(fmt.Errorf("task_id is required"))
	}
	t, err := h.d.B24.TaskGet(ctx, int64(id))
	if err != nil {
		return errResult(err)
	}
	return mcp.NewToolResultStructured(map[string]any{
		"task": t, "my_role": t.Role(h.d.B24.UserID()),
		"url": fmt.Sprintf("%s/company/personal/user/%d/tasks/task/view/%d/", h.d.B24.Portal(), h.d.B24.UserID(), id),
	}, t.Title), nil
}

func (h *handlers) proposeReply(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dialog, err := dialogArg(r)
	if err != nil {
		return errResult(err)
	}
	text, err := r.RequireString("text")
	if err != nil {
		return errResult(err)
	}
	d, err := h.d.Inbox.ProposeReply(ctx, dialog, text, int64(r.GetInt("inbox_id", 0)))
	if err != nil {
		return errResult(err)
	}
	return mcp.NewToolResultStructured(d, fmt.Sprintf("Draft #%d is waiting for the user's approval in their self chat", d.ID)), nil
}

func (h *handlers) draftsList(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ds, err := h.d.Inbox.Store.ListDrafts(ctx, r.GetString("status", ""), 50)
	if err != nil {
		return errResult(err)
	}
	return mcp.NewToolResultStructured(map[string]any{"drafts": ds}, fmt.Sprintf("%d draft(s)", len(ds))), nil
}

func (h *handlers) markRead(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dialog, err := dialogArg(r)
	if err != nil {
		return errResult(err)
	}
	res, err := h.d.B24.ReadDialog(ctx, dialog)
	if err != nil {
		return errResult(err)
	}
	return mcp.NewToolResultStructured(res, "read"), nil
}

func (h *handlers) sendMessage(ctx context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dialog, err := dialogArg(r)
	if err != nil {
		return errResult(err)
	}
	text, err := r.RequireString("text")
	if err != nil {
		return errResult(err)
	}
	id, err := h.d.Inbox.SendNow(ctx, dialog, text)
	if err != nil {
		return errResult(err)
	}
	return mcp.NewToolResultText(fmt.Sprintf("sent, message_id %d", id)), nil
}
