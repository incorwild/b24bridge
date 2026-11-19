package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeliverSignsAndRetries(t *testing.T) {
	var hits atomic.Int32
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(500)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-Hub-Signature-256") != "sha256="+Sign(body, "secret") {
			t.Error("bad signature")
		}
		got <- string(body)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := &Sender{URLs: []string{srv.URL}, Secret: "secret", SkipFromMe: true}
	s.Start(ctx, 1)
	s.Enqueue("message", true, map[string]string{"skip": "me"}) // own message: filtered
	s.Enqueue("message", false, map[string]string{"a": "b"})

	select {
	case b := <-got:
		if b != `{"a":"b"}` {
			t.Fatalf("body %s", b)
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", hits.Load())
	}
}

func TestEventFilter(t *testing.T) {
	s := &Sender{Events: map[string]bool{"message": true}}
	if !s.Wants("message") || s.Wants("message.reaction") {
		t.Fatal("filter broken")
	}
	if !(&Sender{}).Wants("anything") {
		t.Fatal("empty filter must pass all")
	}
}
