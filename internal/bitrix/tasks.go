package bitrix

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"
)

// Task is the subset of tasks.task.get/list the bridge needs. Shapes were
// checked on a cloud portal (2026-11-19): ids come as strings, chatId as a
// number in get and a string in list.
type Task struct {
	ID            ID     `json:"id"`
	Title         string `json:"title"`
	ResponsibleID ID     `json:"responsibleId"`
	Accomplices   []ID   `json:"accomplices"`
	ChatID        ID     `json:"chatId"`
	Status        ID     `json:"status"`
	ActivityDate  string `json:"activityDate"`
}

// Role returns "responsible", "accomplice" or "" for userID.
func (t Task) Role(userID int64) string {
	if t.ResponsibleID.Int() == userID {
		return "responsible"
	}
	for _, a := range t.Accomplices {
		if a.Int() == userID {
			return "accomplice"
		}
	}
	return ""
}

var taskSelect = []string{"ID", "TITLE", "RESPONSIBLE_ID", "ACCOMPLICES", "CHAT_ID", "STATUS", "ACTIVITY_DATE"}

func addSelect(v url.Values) {
	for i, f := range taskSelect {
		v.Set("select["+strconv.Itoa(i)+"]", f)
	}
}

func (c *Client) TaskGet(ctx context.Context, taskID int64) (Task, error) {
	v := url.Values{"taskId": {strconv.FormatInt(taskID, 10)}}
	addSelect(v)
	var out struct {
		Task Task `json:"task"`
	}
	err := c.Call(ctx, "tasks.task.get", v, &out)
	return out.Task, err
}

// MyActiveTasks lists tasks where userID is responsible or accomplice and
// that had activity after since, newest first (one page, up to 50).
func (c *Client) MyActiveTasks(ctx context.Context, userID int64, since time.Time) ([]Task, error) {
	uid := strconv.FormatInt(userID, 10)
	v := url.Values{
		"filter[>ACTIVITY_DATE]":                    {since.Format(time.RFC3339)},
		"filter[::SUBFILTER-ROLES][::LOGIC]":        {"OR"},
		"filter[::SUBFILTER-ROLES][RESPONSIBLE_ID]": {uid},
		"filter[::SUBFILTER-ROLES][ACCOMPLICE]":     {uid},
		"order[ACTIVITY_DATE]":                      {"desc"},
	}
	addSelect(v)
	var out struct {
		Tasks []Task `json:"tasks"`
	}
	err := c.Call(ctx, "tasks.task.list", v, &out)
	return out.Tasks, err
}

// DialogInfo is the part of im.dialog.get used to classify chats.
type DialogInfo struct {
	ID         ID     `json:"id"`
	Name       string `json:"name"`
	Type       string `json:"type"`        // "tasksTask", "chat", "collab", ...
	EntityType string `json:"entity_type"` // "TASKS_TASK" for task chats
	EntityID   ID     `json:"entity_id"`
}

func (c *Client) DialogInfo(ctx context.Context, dialogID string) (DialogInfo, error) {
	var d DialogInfo
	err := c.Call(ctx, "im.dialog.get", vals("DIALOG_ID", dialogID), &d)
	return d, err
}

type HistoryMessage struct {
	ID       int64           `json:"id"`
	ChatID   ID              `json:"chat_id"`
	AuthorID int64           `json:"author_id"` // 0 = system message
	Date     string          `json:"date"`
	Text     string          `json:"text"`
	Unread   bool            `json:"unread"`
	Params   json.RawMessage `json:"params"`
}

type HistoryUser struct {
	ID   ID     `json:"id"`
	Name string `json:"name"`
}

type History struct {
	ChatID   ID               `json:"chat_id"`
	Messages []HistoryMessage `json:"messages"`
	Users    []HistoryUser    `json:"users"`
}

// UserName returns the display name of id from the history's users list.
func (h History) UserName(id int64) string {
	for _, u := range h.Users {
		if u.ID.Int() == id {
			return u.Name
		}
	}
	return ""
}

// DialogHistory is DialogMessages decoded; newest messages first.
func (c *Client) DialogHistory(ctx context.Context, dialogID string, limit int) (History, error) {
	var h History
	err := c.Call(ctx, "im.dialog.messages.get", vals("DIALOG_ID", dialogID, "LIMIT", strconv.Itoa(limit)), &h)
	return h, err
}

type RecentItem struct {
	ID      ID     `json:"id"` // dialog id
	Type    string `json:"type"`
	Title   string `json:"title"`
	Counter int    `json:"counter"`
}

// UnreadDialogs returns dialogs from the recent list with unread messages.
func (c *Client) UnreadDialogs(ctx context.Context) ([]RecentItem, error) {
	var out struct {
		Items []RecentItem `json:"items"`
	}
	if err := c.Call(ctx, "im.recent.list", vals("LIMIT", "200"), &out); err != nil {
		return nil, err
	}
	var res []RecentItem
	for _, it := range out.Items {
		if it.Counter > 0 {
			res = append(res, it)
		}
	}
	return res, nil
}
