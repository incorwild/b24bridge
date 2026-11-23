package bitrix

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
)

// ReplyRef is the message another message replies to. AuthorID is 0 when only
// the id is known.
type ReplyRef struct {
	ID       int64  `json:"id"`
	AuthorID int64  `json:"author_id,omitempty"`
	Text     string `json:"text,omitempty"`
}

// replyID reads REPLY_ID from message params. Params are [] when empty, and
// the value comes as "123" (im.v2.Chat.Message.list); numbers and one-element
// arrays are accepted too.
func replyID(params json.RawMessage) int64 {
	var m map[string]json.RawMessage
	if json.Unmarshal(params, &m) != nil {
		return 0
	}
	for _, k := range []string{"REPLY_ID", "replyId", "reply_id"} {
		raw, ok := m[k]
		if !ok {
			continue
		}
		var id ID
		if json.Unmarshal(raw, &id) == nil && id.Int() > 0 {
			return id.Int()
		}
		var ids []ID
		if json.Unmarshal(raw, &ids) == nil && len(ids) > 0 && ids[0].Int() > 0 {
			return ids[0].Int()
		}
	}
	return 0
}

// v2Message is a message as im.v2 returns it (both key styles are present).
type v2Message struct {
	ID       ID              `json:"id"`
	AuthorID ID              `json:"author_id"`
	AuthorV2 ID              `json:"authorId"`
	SenderID ID              `json:"senderId"`
	Text     string          `json:"text"`
	Params   json.RawMessage `json:"params"`
}

func (m v2Message) author() int64 {
	return firstNonZero(m.AuthorID.Int(), m.AuthorV2.Int(), m.SenderID.Int())
}

// pushReply finds the reply target of a pushed message. The web client reads
// the target from message.additionalEntities; when it is not there, the
// author stays 0 and the caller looks the message up.
func pushReply(params, additional json.RawMessage) *ReplyRef {
	id := replyID(params)
	if id == 0 {
		return nil
	}
	ref := &ReplyRef{ID: id}
	var e struct {
		Messages           json.RawMessage `json:"messages"`
		AdditionalMessages json.RawMessage `json:"additionalMessages"`
	}
	_ = json.Unmarshal(additional, &e)
	for _, raw := range []json.RawMessage{e.AdditionalMessages, e.Messages} {
		var ms []v2Message
		_ = json.Unmarshal(raw, &ms)
		for _, m := range ms {
			if m.ID.Int() == id {
				ref.AuthorID, ref.Text = m.author(), m.Text
				return ref
			}
		}
	}
	return ref
}

// MessageByID fetches one message of a dialog; ok=false if it is gone.
func (c *Client) MessageByID(ctx context.Context, dialogID string, id int64) (HistoryMessage, bool, error) {
	h, err := c.DialogHistoryBefore(ctx, dialogID, 1, id+1)
	if err != nil {
		return HistoryMessage{}, false, err
	}
	for _, m := range h.Messages {
		if m.ID == id {
			return m, true, nil
		}
	}
	return HistoryMessage{}, false, nil
}

// ReplyRefs maps recent message ids of a dialog to what they reply to.
// im.dialog.messages.get drops REPLY_ID, im.v2.Chat.Message.list keeps it.
// The call does not mark anything as read (checked on a portal).
func (c *Client) ReplyRefs(ctx context.Context, dialogID string, limit int) (map[int64]ReplyRef, error) {
	var r struct {
		Messages   []v2Message     `json:"messages"`
		Additional json.RawMessage `json:"additionalMessages"`
	}
	err := c.Call(ctx, "im.v2.Chat.Message.list",
		url.Values{"dialogId": {dialogID}, "limit": {strconv.Itoa(limit)}}, &r)
	if err != nil {
		return nil, err
	}
	var extra []v2Message
	_ = json.Unmarshal(r.Additional, &extra)
	byID := map[int64]v2Message{}
	for _, m := range append(extra, r.Messages...) {
		byID[m.ID.Int()] = m
	}
	out := map[int64]ReplyRef{}
	for _, m := range r.Messages {
		id := replyID(m.Params)
		if id == 0 {
			continue
		}
		ref := ReplyRef{ID: id}
		if t, ok := byID[id]; ok {
			ref.AuthorID, ref.Text = t.author(), t.Text
		}
		out[m.ID.Int()] = ref
	}
	return out, nil
}
