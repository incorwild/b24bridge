package inbox

import (
	"context"
	"sync"
	"time"
)

// Batcher groups bursts of messages per dialog: people often write "привет",
// then the question, then a detail, as separate messages. A dialog is flushed
// once it has been quiet for Window, but no later than Max after its first
// message, so a chatty dialog still reaches the agent.
type Batcher struct {
	Window time.Duration
	Max    time.Duration
	Flush  func(dialogID string)

	mu   sync.Mutex
	pend map[string]*pending
}

type pending struct {
	first time.Time
	timer *time.Timer
	gen   uint64 // bumped on every Add; a timer only fires for its own gen
}

// Add records a new message in dialogID and (re)starts its quiet timer.
func (b *Batcher) Add(dialogID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pend == nil {
		b.pend = map[string]*pending{}
	}
	now := time.Now()
	p, ok := b.pend[dialogID]
	if !ok {
		p = &pending{first: now}
		b.pend[dialogID] = p
	}
	wait := b.Window
	if b.Max > 0 {
		if left := p.first.Add(b.Max).Sub(now); left < wait {
			wait = left
		}
	}
	if wait < 0 {
		wait = 0
	}
	if p.timer != nil {
		p.timer.Stop() // may be too late if it already fired; gen covers that
	}
	p.gen++
	gen := p.gen
	p.timer = time.AfterFunc(wait, func() { b.fire(dialogID, p, gen) })
}

func (b *Batcher) fire(dialogID string, p *pending, gen uint64) {
	b.mu.Lock()
	if b.pend[dialogID] != p || p.gen != gen {
		b.mu.Unlock()
		return // superseded by a newer message, or cancelled
	}
	delete(b.pend, dialogID)
	b.mu.Unlock()
	if b.Flush != nil {
		b.Flush(dialogID)
	}
}

// Cancel forgets a pending dialog (e.g. the user answered it by hand).
func (b *Batcher) Cancel(dialogID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.pend[dialogID]; ok {
		p.timer.Stop()
		delete(b.pend, dialogID)
	}
}

// Pending reports whether dialogID is still collecting messages.
func (b *Batcher) Pending(dialogID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.pend[dialogID]
	return ok
}

// Stop cancels all timers without flushing; on restart the stored items are
// simply visible in the inbox.
func (b *Batcher) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for d, p := range b.pend {
		p.timer.Stop()
		delete(b.pend, d)
	}
}

// Batch is what an agent receives for one settled dialog.
type Batch struct {
	DialogID  string   `json:"dialog_id"`
	ChatTitle string   `json:"chat_title,omitempty"`
	TaskID    int64    `json:"task_id,omitempty"`
	TaskTitle string   `json:"task_title,omitempty"`
	Labels    []string `json:"labels"`
	// Items are the messages new since the previous batch of this dialog.
	Items []Item `json:"items"`
	// Text joins Items in order, one per line, prefixed by sender.
	Text string `json:"text"`
	// Earlier are still-unanswered items already sent in a previous batch:
	// context only, the agent has seen them (and may have a draft pending).
	Earlier []Item `json:"earlier,omitempty"`
}

// EnableBatching makes the inbox report settled dialogs through OnBatch
// instead of every message through OnNew.
func (in *Inbox) EnableBatching(ctx context.Context, window, max time.Duration, onBatch func(Batch)) {
	in.batch = &Batcher{Window: window, Max: max}
	in.batch.Flush = func(dialogID string) {
		items, err := in.Store.ListDialog(ctx, dialogID, "new")
		if err != nil {
			in.Log.Error("inbox: batch flush", "dialog", dialogID, "err", err)
			return
		}
		var fresh, earlier []Item
		for _, it := range items {
			if it.Batched {
				earlier = append(earlier, it)
			} else {
				fresh = append(fresh, it)
			}
		}
		if len(fresh) == 0 {
			return // answered, closed or already delivered meanwhile
		}
		ids := make([]int64, len(fresh))
		for i, it := range fresh {
			ids[i] = it.ID
		}
		if err := in.Store.MarkBatched(ctx, ids); err != nil {
			in.Log.Error("inbox: mark batched", "dialog", dialogID, "err", err)
			return
		}
		b := MakeBatch(fresh)
		b.Earlier = earlier
		items = fresh
		in.Log.Info("inbox: batch", "dialog", dialogID, "items", len(items))
		if onBatch != nil {
			onBatch(b)
		}
	}
	go func() {
		<-ctx.Done()
		in.batch.Stop()
	}()
}

// MakeBatch merges items of one dialog (oldest first).
func MakeBatch(items []Item) Batch {
	b := Batch{DialogID: items[0].DialogID, Items: items, Labels: []string{}}
	seen := map[string]bool{}
	lines := make([]string, 0, len(items))
	prev := ""
	for _, it := range items {
		if it.ChatTitle != "" {
			b.ChatTitle = it.ChatTitle
		}
		if it.TaskID != 0 {
			b.TaskID, b.TaskTitle = it.TaskID, it.TaskTitle
		}
		for _, l := range it.Labels {
			if !seen[l] {
				seen[l] = true
				b.Labels = append(b.Labels, l)
			}
		}
		who := nonEmpty(it.SenderName, "участник")
		if who == prev {
			lines = append(lines, it.Text)
		} else {
			lines = append(lines, who+": "+it.Text)
		}
		prev = who
	}
	for i, l := range lines {
		if i > 0 {
			b.Text += "\n"
		}
		b.Text += l
	}
	return b
}

// Settling reports whether dialogID is still collecting a burst.
func (in *Inbox) Settling(dialogID string) bool {
	return in.batch != nil && in.batch.Pending(dialogID)
}

// List is Store.List without dialogs that are still collecting a burst, so a
// polling agent does not answer "привет" before the actual question arrives.
func (in *Inbox) List(ctx context.Context, status string, limit int, includeSettling bool) ([]Item, error) {
	items, err := in.Store.List(ctx, status, limit)
	if err != nil || includeSettling || in.batch == nil {
		return items, err
	}
	out := items[:0]
	for _, it := range items {
		if !in.Settling(it.DialogID) {
			out = append(out, it)
		}
	}
	return out, nil
}
