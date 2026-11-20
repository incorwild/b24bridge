package inbox

import (
	"context"
	"sync"
	"testing"
	"time"
)

type flushLog struct {
	mu  sync.Mutex
	got []string
	at  []time.Time
}

func (f *flushLog) add(d string) {
	f.mu.Lock()
	f.got = append(f.got, d)
	f.at = append(f.at, time.Now())
	f.mu.Unlock()
}

func (f *flushLog) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.got...)
}

func TestBatcherQuietWindowAndMax(t *testing.T) {
	var fl flushLog
	b := &Batcher{Window: 60 * time.Millisecond, Max: 200 * time.Millisecond, Flush: fl.add}

	// Quiet window: three messages 20ms apart give one flush.
	start := time.Now()
	for i := 0; i < 3; i++ {
		b.Add("88")
		time.Sleep(20 * time.Millisecond)
	}
	if !b.Pending("88") {
		t.Fatal("should still be collecting")
	}
	time.Sleep(120 * time.Millisecond)
	if got := fl.snapshot(); len(got) != 1 || got[0] != "88" {
		t.Fatalf("flushes %v", got)
	}
	if d := fl.at[0].Sub(start); d < 100*time.Millisecond {
		t.Fatalf("flushed too early: %v", d)
	}

	// Max: a message every 30ms never leaves 60ms of quiet, Max still flushes.
	start = time.Now()
	stop := time.After(350 * time.Millisecond)
loop:
	for {
		select {
		case <-stop:
			break loop
		default:
			b.Add("chat1")
			time.Sleep(30 * time.Millisecond)
		}
	}
	time.Sleep(100 * time.Millisecond)
	n := 0
	for _, d := range fl.snapshot() {
		if d == "chat1" {
			n++
		}
	}
	if n < 1 {
		t.Fatal("max wait did not flush a busy dialog")
	}

	// Cancel: no flush.
	b.Add("77")
	b.Cancel("77")
	time.Sleep(100 * time.Millisecond)
	for _, d := range fl.snapshot() {
		if d == "77" {
			t.Fatal("cancelled dialog flushed")
		}
	}
}

func TestInboxBatchesBurst(t *testing.T) {
	in, _ := newTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan Batch, 4)
	in.EnableBatching(ctx, 80*time.Millisecond, time.Second, func(b Batch) { got <- b })

	for i, text := range []string{"привет", "есть минута?", "по отчёту вопрос"} {
		if err := in.Handle(ctx, msg(int64(10+i), "88", 88, text)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// While the burst is open a polling agent sees nothing from this dialog.
	if items, _ := in.List(ctx, "new", 10, false); len(items) != 0 {
		t.Fatalf("settling dialog visible: %d items", len(items))
	}
	if items, _ := in.List(ctx, "new", 10, true); len(items) != 3 {
		t.Fatalf("include_settling: %d items", len(items))
	}

	select {
	case b := <-got:
		if b.DialogID != "88" || len(b.Items) != 3 || b.Text != "User88: привет\nесть минута?\nпо отчёту вопрос" {
			t.Fatalf("batch %+v", b)
		}
		if len(b.Labels) != 1 || b.Labels[0] != LabelDirect {
			t.Fatalf("labels %v", b.Labels)
		}
	case <-time.After(time.Second):
		t.Fatal("no batch")
	}
	if items, _ := in.List(ctx, "new", 10, false); len(items) != 3 {
		t.Fatalf("settled dialog hidden: %d", len(items))
	}
}

func TestInboxBatchCancelledByOwnReply(t *testing.T) {
	in, _ := newTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan Batch, 4)
	in.EnableBatching(ctx, 60*time.Millisecond, time.Second, func(b Batch) { got <- b })

	_ = in.Handle(ctx, msg(1, "88", 88, "привет"))
	_ = in.Handle(ctx, msg(2, "88", me, "привет, слушаю")) // the user answers by hand
	select {
	case b := <-got:
		t.Fatalf("unexpected batch %+v", b)
	case <-time.After(200 * time.Millisecond):
	}
}

// A second burst while the first is still unanswered (draft pending) must not
// re-send the first burst as new.
func TestSecondBatchCarriesOnlyNewItems(t *testing.T) {
	in, _ := newTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan Batch, 4)
	in.EnableBatching(ctx, 50*time.Millisecond, time.Second, func(b Batch) { got <- b })

	_ = in.Handle(ctx, msg(1, "88", 88, "привет"))
	_ = in.Handle(ctx, msg(2, "88", 88, "вопрос 1"))
	b1 := <-got
	if len(b1.Items) != 2 || len(b1.Earlier) != 0 {
		t.Fatalf("batch 1: %d items, %d earlier", len(b1.Items), len(b1.Earlier))
	}
	_ = in.Handle(ctx, msg(3, "88", 88, "и ещё вопрос 2"))
	select {
	case b2 := <-got:
		if len(b2.Items) != 1 || b2.Items[0].MessageID != 3 || len(b2.Earlier) != 2 || b2.Text != "User88: и ещё вопрос 2" {
			t.Fatalf("batch 2: %+v", b2)
		}
	case <-time.After(time.Second):
		t.Fatal("no second batch")
	}
}
