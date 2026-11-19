package bitrix

import (
	"encoding/json"
	"regexp"
	"time"
)

// PullMessage is one message from the push server (server version 4,
// plain-text mode): #!NGINXNMS!#{...}#!NGINXNME!#.
type PullMessage struct {
	// Types vary between server builds (number or string), hence ID.
	ID   ID `json:"id"`
	Mid  ID `json:"mid"`
	Tag  ID `json:"tag"`
	Time ID `json:"time"`
	Text struct {
		ModuleID string          `json:"module_id"`
		Command  string          `json:"command"`
		Params   json.RawMessage `json:"params"`
		Extra    json.RawMessage `json:"extra"`
	} `json:"text"`
}

var reFrame = regexp.MustCompile(`(?s)#!NGINXNMS!#(.*?)#!NGINXNME!#`)

// ParseFrame splits one websocket text frame into push messages. Broken
// pieces are skipped, as the browser client does.
func ParseFrame(frame []byte) []PullMessage {
	var out []PullMessage
	for _, m := range reFrame.FindAllSubmatch(frame, -1) {
		if len(m[1]) == 0 {
			continue
		}
		var pm PullMessage
		if json.Unmarshal(m[1], &pm) == nil {
			out = append(out, pm)
		}
	}
	return out
}

// Event is the normalized payload sent to webhooks, close to the shape
// go-whatsapp-web-multidevice uses: {event, payload, ...}.
type Event struct {
	Event     string    `json:"event"`
	Timestamp time.Time `json:"timestamp"`
	Portal    string    `json:"portal"`
	UserID    int64     `json:"user_id"` // the account the bridge runs as
	Payload   Payload   `json:"payload"`
	// Raw is the original push message params, for fields not normalized yet.
	Raw json.RawMessage `json:"raw,omitempty"`
}

type Payload struct {
	DialogID  string `json:"dialog_id,omitempty"`
	ChatID    int64  `json:"chat_id,omitempty"`
	MessageID int64  `json:"message_id,omitempty"`
	SenderID  int64  `json:"sender_id,omitempty"`
	IsFromMe  bool   `json:"is_from_me"`
	System    bool   `json:"system,omitempty"`
	Text      string `json:"text,omitempty"`
	Date      string `json:"date,omitempty"`
	Reaction  string `json:"reaction,omitempty"`
	UserID    int64  `json:"user_id,omitempty"` // reactor / reader / typer
	UserName  string `json:"user_name,omitempty"`
	LastID    int64  `json:"last_id,omitempty"` // read pointer
	// Viewed lists the message ids this read marked as viewed.
	Viewed []int64 `json:"viewed_messages,omitempty"`
	// Status is Bitrix's chatMessageStatus on read receipts (e.g. "delivered").
	Status string     `json:"status,omitempty"`
	Files  any        `json:"files,omitempty"`
	Sender *UserBrief `json:"sender,omitempty"`
}

type UserBrief struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Event names. Commands marked "unverified" were not observed on a live
// portal yet; their field names follow the web client and may need fixes.
const (
	EvMessage        = "message"
	EvMessageEdited  = "message.edited"
	EvMessageDeleted = "message.deleted"  // unverified
	EvReaction       = "message.reaction" // addReaction / deleteReaction
	EvChatRead       = "chat.read"        // this account read a chat
	EvMessageAck     = "message.ack"      // someone else read your messages
	EvChatPresence   = "chat_presence"    // someone is typing
)

var commandEvents = map[string]string{
	"message":                 EvMessage,
	"messageChat":             EvMessage,
	"messageUpdate":           EvMessageEdited,
	"messageDelete":           EvMessageDeleted,
	"messageDeleteComplete":   EvMessageDeleted,
	"addReaction":             EvReaction,
	"deleteReaction":          EvReaction,
	"readMessage":             EvChatRead,
	"readMessageChat":         EvChatRead,
	"readMessageOpponent":     EvMessageAck,
	"readMessageChatOpponent": EvMessageAck,
	"startWriting":            EvChatPresence,
}

// Normalize maps an "im" push message to an Event. ok=false for commands the
// bridge does not forward (service events such as deleteChatAnchors).
func Normalize(pm PullMessage, me int64) (Event, bool) {
	if pm.Text.ModuleID != "im" {
		return Event{}, false
	}
	name, known := commandEvents[pm.Text.Command]
	if !known {
		return Event{}, false
	}
	var p struct {
		DialogID   ID              `json:"dialogId"`
		ChatID     ID              `json:"chatId"`
		ID         ID              `json:"id"`
		SenderID   ID              `json:"senderId"`
		FromUserID ID              `json:"fromUserId"`
		UserID     ID              `json:"userId"`
		UserName   string          `json:"userName"`
		Viewed     []ID            `json:"viewedMessages"`
		MsgStatus  string          `json:"chatMessageStatus"`
		Text       string          `json:"text"`
		Reaction   string          `json:"reaction"`
		LastID     ID              `json:"lastId"`
		Files      json.RawMessage `json:"files"`
		Message    *struct {
			ID       ID              `json:"id"`
			ChatID   ID              `json:"chatId"`
			SenderID ID              `json:"senderId"`
			System   json.RawMessage `json:"system"`
			Date     string          `json:"date"`
			Text     string          `json:"text"`
		} `json:"message"`
		Users map[string]struct {
			ID   ID     `json:"id"`
			Name string `json:"name"`
		} `json:"users"`
		ActualReactions *struct {
			Reaction struct {
				MessageID ID `json:"messageId"`
			} `json:"reaction"`
		} `json:"actualReactions"`
	}
	_ = json.Unmarshal(pm.Text.Params, &p)

	pl := Payload{DialogID: string(p.DialogID), ChatID: p.ChatID.Int()}
	switch name {
	case EvMessage:
		if p.Message == nil {
			return Event{}, false
		}
		pl.MessageID = p.Message.ID.Int()
		pl.SenderID = p.Message.SenderID.Int()
		pl.Text = p.Message.Text
		pl.Date = p.Message.Date
		sys := string(p.Message.System)
		pl.System = sys == `"Y"` || sys == "true" || pl.SenderID == 0
		if pl.ChatID == 0 {
			pl.ChatID = p.Message.ChatID.Int()
		}
		if u, ok := p.Users[string(p.Message.SenderID)]; ok {
			pl.Sender = &UserBrief{ID: pl.SenderID, Name: u.Name}
		}
		if len(p.Files) > 0 && string(p.Files) != "[]" && string(p.Files) != "{}" {
			pl.Files = p.Files
		}
	case EvMessageEdited, EvMessageDeleted:
		pl.MessageID = p.ID.Int()
		pl.SenderID = firstNonZero(p.SenderID.Int(), p.FromUserID.Int())
		pl.Text = p.Text
	case EvReaction:
		pl.UserID = p.UserID.Int()
		pl.Reaction = p.Reaction
		if p.ActualReactions != nil {
			pl.MessageID = p.ActualReactions.Reaction.MessageID.Int()
		}
		if pm.Text.Command == "deleteReaction" {
			pl.Reaction = "-" + pl.Reaction
		}
	case EvChatRead, EvMessageAck:
		// Verified on a live portal: readMessage*(Opponent) carry
		// {userId, userName, lastId, viewedMessages[], chatMessageStatus}.
		pl.UserID = firstNonZero(p.UserID.Int(), me)
		pl.UserName = p.UserName
		pl.LastID = p.LastID.Int()
		pl.MessageID = pl.LastID
		pl.Status = p.MsgStatus
		for _, v := range p.Viewed {
			if n := v.Int(); n > 0 {
				pl.Viewed = append(pl.Viewed, n)
			}
		}
	case EvChatPresence:
		pl.UserID = p.UserID.Int()
		pl.UserName = p.UserName
	}
	switch name {
	case EvReaction, EvChatRead, EvMessageAck, EvChatPresence:
		pl.IsFromMe = pl.UserID == me
	default:
		pl.IsFromMe = pl.SenderID != 0 && pl.SenderID == me
	}
	return Event{Event: name, Timestamp: time.Now().UTC(), UserID: me, Payload: pl, Raw: pm.Text.Params}, true
}

func firstNonZero(v ...int64) int64 {
	for _, x := range v {
		if x != 0 {
			return x
		}
	}
	return 0
}
