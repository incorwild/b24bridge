// Package webhook delivers events to HTTP endpoints with the same contract as
// go-whatsapp-web-multidevice: JSON POST, X-Hub-Signature-256 HMAC, retries.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Sender struct {
	URLs        []string
	Secret      string
	Events      map[string]bool // empty = all
	SkipFromMe  bool            // drop events caused by the bridge account itself
	MaxAttempts int
	Client      *http.Client
	Log         *slog.Logger

	queue chan job
}

type job struct {
	url  string
	body []byte
}

// Sign returns the hex HMAC-SHA256 of body.
func Sign(body []byte, secret string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// Start runs delivery workers; Enqueue never blocks the push listener.
func (s *Sender) Start(ctx context.Context, workers int) {
	if s.Client == nil {
		s.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 5
	}
	s.queue = make(chan job, 1000)
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-s.queue:
					if err := s.deliver(ctx, j); err != nil {
						s.Log.Error("webhook: delivery failed", "url", j.url, "err", err)
					}
				}
			}
		}()
	}
}

// Wants reports whether an event name passes the filter.
func (s *Sender) Wants(event string) bool {
	return len(s.Events) == 0 || s.Events[event]
}

// Enqueue marshals v once and queues it for every URL. fromMe and event are
// used for filtering.
func (s *Sender) Enqueue(event string, fromMe bool, v any) {
	if len(s.URLs) == 0 || !s.Wants(event) || (s.SkipFromMe && fromMe) {
		return
	}
	body, err := json.Marshal(v)
	if err != nil {
		s.Log.Error("webhook: marshal", "err", err)
		return
	}
	for _, u := range s.URLs {
		select {
		case s.queue <- job{url: u, body: body}:
		default:
			s.Log.Error("webhook: queue full, event dropped", "event", event, "url", u)
		}
	}
}

func (s *Sender) deliver(ctx context.Context, j job) error {
	sig := "sha256=" + Sign(j.body, s.Secret)
	wait := time.Second
	var lastErr error
	for attempt := 1; attempt <= s.MaxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.url, bytes.NewReader(j.body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Hub-Signature-256", sig)
		resp, err := s.Client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		lastErr = err
		if attempt == s.MaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait *= 2
	}
	return fmt.Errorf("after %d attempts: %w", s.MaxAttempts, lastErr)
}
