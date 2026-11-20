// Package inbox picks the messages that matter to the bridge account
// (direct messages, mentions, chats of tasks where the account is
// responsible or accomplice), stores them for an agent and handles reply
// drafts that the user approves from their own "self" chat.
package inbox

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"b24bridge/internal/bitrix"
)

// Labels attached to inbox items.
const (
	LabelDirect          = "direct"
	LabelMention         = "mention"
	LabelTaskResponsible = "task_responsible"
	LabelTaskAccomplice  = "task_accomplice"
)

// ChatMeta describes a dialog for classification.
type ChatMeta struct {
	Private   bool   `json:"private"`
	Title     string `json:"title,omitempty"`
	TaskID    int64  `json:"task_id,omitempty"`
	TaskTitle string `json:"task_title,omitempty"`
	TaskRole  string `json:"task_role,omitempty"` // responsible | accomplice | ""
}

// API is what the resolver needs from the Bitrix24 client.
type API interface {
	DialogInfo(ctx context.Context, dialogID string) (bitrix.DialogInfo, error)
	TaskGet(ctx context.Context, taskID int64) (bitrix.Task, error)
}

// Resolver caches dialog metadata; task roles change, so entries expire.
type Resolver struct {
	API API
	Me  func() int64
	TTL time.Duration

	mu    sync.Mutex
	cache map[string]cached
}

type cached struct {
	meta ChatMeta
	at   time.Time
}

func IsPrivateDialog(dialogID string) bool {
	_, err := strconv.ParseInt(dialogID, 10, 64)
	return err == nil
}

func (r *Resolver) Meta(ctx context.Context, dialogID string) (ChatMeta, error) {
	if IsPrivateDialog(dialogID) {
		return ChatMeta{Private: true}, nil
	}
	ttl := r.TTL
	if ttl == 0 {
		ttl = 10 * time.Minute
	}
	r.mu.Lock()
	if c, ok := r.cache[dialogID]; ok && time.Since(c.at) < ttl {
		r.mu.Unlock()
		return c.meta, nil
	}
	r.mu.Unlock()

	d, err := r.API.DialogInfo(ctx, dialogID)
	if err != nil {
		return ChatMeta{}, err
	}
	m := ChatMeta{Title: d.Name}
	if d.EntityType == "TASKS_TASK" && d.EntityID.Int() > 0 {
		t, err := r.API.TaskGet(ctx, d.EntityID.Int())
		if err != nil {
			return ChatMeta{}, err
		}
		m.TaskID, m.TaskTitle, m.TaskRole = t.ID.Int(), t.Title, t.Role(r.Me())
	}
	r.Put(dialogID, m)
	return m, nil
}

// maxCache bounds the dialog cache; expired entries are dropped when full.
const maxCache = 2000

// Put seeds the cache (e.g. from tasks.task.list during backfill).
func (r *Resolver) Put(dialogID string, m ChatMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil {
		r.cache = map[string]cached{}
	}
	if len(r.cache) >= maxCache {
		ttl := r.TTL
		if ttl == 0 {
			ttl = 10 * time.Minute
		}
		for k, c := range r.cache {
			if time.Since(c.at) >= ttl {
				delete(r.cache, k)
			}
		}
		if len(r.cache) >= maxCache { // all fresh: start over rather than grow
			r.cache = map[string]cached{}
		}
	}
	r.cache[dialogID] = cached{meta: m, at: time.Now()}
}

func (r *Resolver) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.cache)
}

var reMention = regexp.MustCompile(`(?i)\[user=(\d+)\]`)

// Mentions reports whether text mentions userID ([USER=54]Name[/USER]).
func Mentions(text string, userID int64) bool {
	want := strconv.FormatInt(userID, 10)
	for _, m := range reMention.FindAllStringSubmatch(text, -1) {
		if m[1] == want {
			return true
		}
	}
	return false
}

// Labels decides why a message matters. Empty means skip it. Own messages and
// the self chat never match.
func Labels(dialogID string, senderID int64, text string, system bool, meta ChatMeta, me int64, includeSystem bool) []string {
	if senderID == me || dialogID == strconv.FormatInt(me, 10) {
		return nil
	}
	var l []string
	if meta.Private && !system {
		l = append(l, LabelDirect)
	}
	if !system && Mentions(text, me) {
		l = append(l, LabelMention)
	}
	if meta.TaskRole != "" && (!system || includeSystem) {
		if meta.TaskRole == "responsible" {
			l = append(l, LabelTaskResponsible)
		} else {
			l = append(l, LabelTaskAccomplice)
		}
	}
	return l
}

// PlainText strips the BB-codes agents do not need ([USER=1]Name[/USER] → @Name).
var (
	reUserTag = regexp.MustCompile(`(?i)\[user=\d+\](.*?)\[/user\]`)
	reFormat  = regexp.MustCompile(`(?i)\[/?(b|i|u|s|url(=[^\]]*)?|quote|code|br)\]`)
)

func PlainText(s string) string {
	s = reUserTag.ReplaceAllString(s, "@$1")
	s = reFormat.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}
