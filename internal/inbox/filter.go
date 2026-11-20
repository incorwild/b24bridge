package inbox

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Filter drops messages before they are classified. System messages of task
// chats (author 0: "created the task", "changed the responsible person to…",
// "the task is overdue") are dropped unless IncludeSystem is set.
type Filter struct {
	IncludeSystem bool           // keep service messages in my task chats
	IgnoreBots    bool           // drop messages from chat bots (user.bot = true)
	IgnoreSenders map[int64]bool // user ids to ignore
	IgnoreText    *regexp.Regexp // drop messages whose plain text matches
}

// Skip returns the reason a message is dropped, or "".
func (f Filter) Skip(senderID int64, bot bool, text string) string {
	switch {
	case f.IgnoreBots && bot:
		return "bot"
	case f.IgnoreSenders[senderID]:
		return "ignored sender"
	case f.IgnoreText != nil && f.IgnoreText.MatchString(PlainText(text)):
		return "ignored text"
	}
	return ""
}

// ParseFilter builds a Filter from env-style strings:
// senders "12, 34", text a Go regexp (case-insensitive).
func ParseFilter(includeSystem, ignoreBots bool, senders, text string) (Filter, error) {
	f := Filter{IncludeSystem: includeSystem, IgnoreBots: ignoreBots, IgnoreSenders: map[int64]bool{}}
	for _, p := range strings.FieldsFunc(senders, func(r rune) bool { return r == ',' || r == ' ' }) {
		id, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return f, fmt.Errorf("INBOX_IGNORE_SENDERS: bad id %q", p)
		}
		f.IgnoreSenders[id] = true
	}
	if strings.TrimSpace(text) != "" {
		re, err := regexp.Compile("(?i)" + text)
		if err != nil {
			return f, fmt.Errorf("INBOX_IGNORE_TEXT: %w", err)
		}
		f.IgnoreText = re
	}
	return f, nil
}
