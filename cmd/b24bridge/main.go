// Command b24bridge connects to a cloud Bitrix24 portal as a signed-in user,
// forwards messenger events to webhooks and exposes an HTTP API to reply,
// read history and check read status.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"b24bridge/internal/api"
	"b24bridge/internal/bitrix"
	"b24bridge/internal/webhook"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level()}))
	slog.SetDefault(log)

	cookie := os.Getenv("B24_COOKIE")
	if f := os.Getenv("B24_COOKIE_FILE"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			fatal("read B24_COOKIE_FILE", err)
		}
		cookie = strings.TrimSpace(string(b))
	}
	portal := os.Getenv("B24_PORTAL")
	if portal == "" || cookie == "" {
		fatal("config", errors.New("set B24_PORTAL and B24_COOKIE (or B24_COOKIE_FILE)"))
	}

	b24, err := bitrix.New(bitrix.Options{Portal: portal, Cookie: cookie, SessID: os.Getenv("B24_SESSID")})
	if err != nil {
		fatal("client", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	wh := &webhook.Sender{
		URLs:       splitList(os.Getenv("WEBHOOK_URLS")),
		Secret:     os.Getenv("WEBHOOK_SECRET"),
		Events:     toSet(splitList(os.Getenv("WEBHOOK_EVENTS"))),
		SkipFromMe: os.Getenv("WEBHOOK_INCLUDE_OWN") != "true",
	}
	wh.Start(ctx, 4)

	// Session lifecycle, like device.logged_out in go-whatsapp-web-multidevice.
	var (
		startedMu sync.Mutex
		started   bool
	)
	b24.OnSessionChange(func(st bitrix.SessionState) {
		startedMu.Lock()
		defer startedMu.Unlock()
		name := "session.restored"
		if !st.Authorized {
			name = "session.logged_out"
			log.Error("logged out: sign in in a browser and POST the new Cookie header to /session/cookies", "err", st.LastError)
		} else {
			log.Info("session ok", "portal", b24.Host(), "user_id", b24.UserID())
		}
		if !started && st.Authorized {
			started = true
			return // first login at startup is not an event
		}
		started = true
		wh.Enqueue(name, false, map[string]any{
			"event": name, "timestamp": time.Now().UTC(), "portal": b24.Host(),
			"user_id": b24.UserID(), "payload": st,
		})
	})
	if err := b24.RefreshSession(ctx); err != nil {
		fatal("login check (are the cookies fresh?)", err)
	}

	keepalive := 10 * time.Minute
	if d, err := time.ParseDuration(os.Getenv("SESSION_KEEPALIVE")); err == nil && d > 0 {
		keepalive = d
	}
	go b24.KeepAlive(ctx, keepalive, log)
	if f := os.Getenv("B24_COOKIE_FILE"); f != "" && os.Getenv("B24_COOKIE_READONLY") != "true" {
		go b24.PersistCookies(ctx, f, 30*time.Second, log)
	}

	listener := &bitrix.Listener{
		Client: b24,
		Handle: func(ev bitrix.Event) {
			log.Info("event", "event", ev.Event, "dialog", ev.Payload.DialogID, "message_id", ev.Payload.MessageID, "from_me", ev.Payload.IsFromMe)
			if ev.Event != bitrix.EvMessage && ev.Event != bitrix.EvMessageEdited {
				// Field names of read/typing commands are still being mapped.
				log.Debug("event raw params", "event", ev.Event, "raw", string(ev.Raw))
			}
			wh.Enqueue(ev.Event, ev.Payload.IsFromMe, ev)
		},
	}
	go func() { _ = listener.Run(ctx) }()

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:3000"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           (&api.Server{B24: b24, Listener: listener, Token: os.Getenv("API_TOKEN")}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("http api listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal("http", err)
	}
}

func fatal(what string, err error) {
	slog.Error(what, "err", err)
	os.Exit(1)
}

func level() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func toSet(xs []string) map[string]bool {
	m := map[string]bool{}
	for _, x := range xs {
		m[x] = true
	}
	return m
}
