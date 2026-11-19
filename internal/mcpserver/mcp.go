// Package mcpserver exposes the inbox and messenger to AI agents over MCP
// (streamable HTTP at /mcp), like ui/mcp in go-whatsapp-web-multidevice.
package mcpserver

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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
		mcp.WithDescription("Messages that need the user's attention: direct messages, mentions of the user, and messages in chats of tasks where the user is responsible or accomplice. Oldest first."),
		mcp.WithString("status", mcp.Description("new (default), done or all"), mcp.Enum("new", "done", "all")),
		mcp.WithNumber("limit", mcp.Description("max items, default 30")),
		mcp.WithReadOnlyHintAnnotation(true),
	), h.inboxList)
	s.AddTool(mcp.NewTool("inbox_done",
		mcp.WithDescription("Mark inbox items as handled (no reply needed, or handled elsewhere). Replies sent through propose_reply close items automatically."),
		mcp.WithString("ids", mcp.Required(), mcp.Description("comma-separated inbox item ids, e.g. \"3,4\"")),
	), h.inboxDone)
	s.AddTool(mcp.NewTool("chat_history",
		mcp.WithDescription("Recent messages of a dialog for context, oldest first. dialog_id: user id (\"88\") or chat (\"chat5986\")."),
		mcp.WithString("dialog_id", mcp.Required()),
		mcp.WithNumber("limit", mcp.Description("default 20, max 100")),
		mcp.WithReadOnlyHintAnnotation(true),
	), h.chatHistory)
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
Start with inbox_list. For each item, read chat_history (and task_get for task chats) before deciding.
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
	items, err := h.d.Inbox.Store.List(ctx, st, r.GetInt("limit", 30))
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
	dialog, err := r.RequireString("dialog_id")
	if err != nil {
		return errResult(err)
	}
	limit := r.GetInt("limit", 20)
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	hist, err := h.d.B24.DialogHistory(ctx, dialog, limit)
	if err != nil {
		return errResult(err)
	}
	me := h.d.B24.UserID()
	out := make([]histMsg, 0, len(hist.Messages))
	for i := len(hist.Messages) - 1; i >= 0; i-- { // API returns newest first
		m := hist.Messages[i]
		name := hist.UserName(m.AuthorID)
		if m.AuthorID == 0 {
			name = "system"
		}
		out = append(out, histMsg{ID: m.ID, Author: name, FromMe: m.AuthorID == me, System: m.AuthorID == 0,
			Date: m.Date, Text: inbox.PlainText(m.Text)})
	}
	return mcp.NewToolResultStructured(map[string]any{"dialog_id": dialog, "messages": out},
		fmt.Sprintf("%d message(s)", len(out))), nil
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
	dialog, err := r.RequireString("dialog_id")
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
	dialog, err := r.RequireString("dialog_id")
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
	dialog, err := r.RequireString("dialog_id")
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
