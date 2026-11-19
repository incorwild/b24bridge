package bitrix

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
)

// ID accepts both JSON numbers and strings: Bitrix24 sends dialogId as 54,
// "54" or "chat12250" depending on the method and event.
type ID string

func (id *ID) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		*id = ""
		return nil
	}
	if strings.HasPrefix(s, `"`) {
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			return err
		}
		*id = ID(str)
		return nil
	}
	*id = ID(s)
	return nil
}

func (id ID) Int() int64 {
	n, _ := strconv.ParseInt(string(id), 10, 64)
	return n
}

func vals(kv ...string) map[string][]string {
	m := map[string][]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] != "" {
			m[kv[i]] = []string{kv[i+1]}
		}
	}
	return m
}

// SendMessage posts text to a dialog: "54" for a user, "chat123" for a chat.
func (c *Client) SendMessage(ctx context.Context, dialogID, text string) (int64, error) {
	var id int64
	err := c.Call(ctx, "im.message.add", vals("DIALOG_ID", dialogID, "MESSAGE", text), &id)
	return id, err
}

func (c *Client) UpdateMessage(ctx context.Context, messageID int64, text string) error {
	return c.Call(ctx, "im.message.update", vals("MESSAGE_ID", strconv.FormatInt(messageID, 10), "MESSAGE", text), nil)
}

func (c *Client) DeleteMessage(ctx context.Context, messageID int64) error {
	return c.Call(ctx, "im.message.delete", vals("MESSAGE_ID", strconv.FormatInt(messageID, 10)), nil)
}

// RecentList returns the chat list as Bitrix24 sends it.
func (c *Client) RecentList(ctx context.Context, limit int) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.Call(ctx, "im.recent.list", vals("LIMIT", strconv.Itoa(limit)), &out)
	return out, err
}

// DialogMessages returns history; lastID > 0 pages backwards from that message.
func (c *Client) DialogMessages(ctx context.Context, dialogID string, limit int, lastID int64) (json.RawMessage, error) {
	p := vals("DIALOG_ID", dialogID, "LIMIT", strconv.Itoa(limit))
	if lastID > 0 {
		p["LAST_ID"] = []string{strconv.FormatInt(lastID, 10)}
	}
	var out json.RawMessage
	err := c.Call(ctx, "im.dialog.messages.get", p, &out)
	return out, err
}

type ReadResult struct {
	ChatID   ID    `json:"chatId"`
	DialogID ID    `json:"dialogId"`
	LastID   int64 `json:"lastId"`
	Counter  int   `json:"counter"`
}

// ReadDialog marks the dialog as read up to its last message.
func (c *Client) ReadDialog(ctx context.Context, dialogID string) (ReadResult, error) {
	var r ReadResult
	err := c.Call(ctx, "im.dialog.read", vals("DIALOG_ID", dialogID), &r)
	return r, err
}

type Reader struct {
	UserID    int64  `json:"user_id"`
	UserName  string `json:"user_name"`
	MessageID int64  `json:"message_id"`
	Date      any    `json:"date"`
}

type ReadStatus struct {
	DialogID  string   `json:"dialog_id"`
	MessageID int64    `json:"message_id"`
	Read      bool     `json:"read"`    // every other participant has read message_id
	ReadBy    []Reader `json:"read_by"` // participants whose read pointer reached message_id
	Pending   []Reader `json:"pending"` // participants who have not
}

// ReadStatus tells whether the other participants have read messageID
// (0 = the dialog's last message), using im.dialog.get's readed_list.
func (c *Client) ReadStatus(ctx context.Context, dialogID string, messageID int64) (ReadStatus, error) {
	var d struct {
		LastMessageID int64    `json:"last_message_id"`
		ReadedList    []Reader `json:"readed_list"`
	}
	if err := c.Call(ctx, "im.dialog.get", vals("DIALOG_ID", dialogID), &d); err != nil {
		return ReadStatus{}, err
	}
	if messageID == 0 {
		messageID = d.LastMessageID
	}
	me := c.UserID()
	st := ReadStatus{DialogID: dialogID, MessageID: messageID, ReadBy: []Reader{}, Pending: []Reader{}}
	for _, r := range d.ReadedList {
		if r.UserID == me {
			continue
		}
		if r.MessageID >= messageID {
			st.ReadBy = append(st.ReadBy, r)
		} else {
			st.Pending = append(st.Pending, r)
		}
	}
	st.Read = len(st.Pending) == 0 && len(st.ReadBy) > 0
	return st, nil
}
