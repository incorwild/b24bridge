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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"b24bridge/internal/api"
	"b24bridge/internal/bitrix"
	"b24bridge/internal/inbox"
	"b24bridge/internal/mcpserver"
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

	token := os.Getenv("API_TOKEN")
	if len(token) < 16 {
		fatal("config", errors.New("set API_TOKEN to a random string of 16+ characters (e.g. `openssl rand -hex 24`): the API can write to Bitrix24 as you"))
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
		Events:     webhookEvents(os.Getenv("WEBHOOK_EVENTS"), os.Getenv("INBOX_BATCH_WINDOW")),
		SkipFromMe: os.Getenv("WEBHOOK_INCLUDE_OWN") != "true",
	}
	if len(wh.URLs) > 0 && wh.Secret == "" {
		log.Warn("WEBHOOK_SECRET is empty: receivers cannot verify X-Hub-Signature-256, anyone can forge webhooks")
	}
	log.Info("webhooks", "urls", len(wh.URLs), "events", eventNames(wh.Events))
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

	// Inbox: what matters to the user (direct, mentions, my task chats).
	dbPath := os.Getenv("INBOX_DB")
	if dbPath == "" {
		dbPath = "inbox.db"
	}
	store, err := inbox.Open(dbPath)
	if err != nil {
		fatal("open inbox db", err)
	}
	defer store.Close()
	in := inbox.New(store, b24, b24.UserID)
	in.Filter, err = inbox.ParseFilter(
		os.Getenv("INBOX_INCLUDE_SYSTEM") == "true",
		os.Getenv("INBOX_IGNORE_BOTS") != "false",
		os.Getenv("INBOX_IGNORE_SENDERS"),
		os.Getenv("INBOX_IGNORE_TEXT"),
	)
	if err != nil {
		fatal("inbox filter", err)
	}
	// Bursts of messages in one dialog are reported once the sender pauses
	// (inbox.batch); with INBOX_BATCH_WINDOW=0 every item goes out as inbox.new.
	batchWindow, batchMax := 3*time.Minute, 10*time.Minute
	if d, err := time.ParseDuration(os.Getenv("INBOX_BATCH_WINDOW")); err == nil {
		batchWindow = d
	}
	if d, err := time.ParseDuration(os.Getenv("INBOX_BATCH_MAX")); err == nil && d > 0 {
		batchMax = d
	}
	if batchWindow <= 0 {
		in.OnNew = func(it inbox.Item) {
			wh.Enqueue("inbox.new", false, map[string]any{
				"event": "inbox.new", "timestamp": time.Now().UTC(), "portal": b24.Host(),
				"user_id": b24.UserID(), "payload": it,
			})
		}
	} else {
		in.EnableBatching(ctx, batchWindow, batchMax, func(b inbox.Batch) {
			wh.Enqueue("inbox.batch", false, map[string]any{
				"event": "inbox.batch", "timestamp": time.Now().UTC(), "portal": b24.Host(),
				"user_id": b24.UserID(), "payload": b,
			})
		})
	}
	go in.Run(ctx)
	if err := in.RecoverSending(ctx); err != nil {
		log.Warn("inbox: recover drafts", "err", err)
	}
	backfill := 24 * time.Hour
	if d, err := time.ParseDuration(os.Getenv("INBOX_BACKFILL")); err == nil {
		backfill = d
	}
	if backfill > 0 {
		go func() {
			if err := in.Backfill(ctx, time.Now().Add(-backfill)); err != nil {
				log.Warn("inbox backfill", "err", err)
			}
		}()
	}

	skipSystemWebhooks := os.Getenv("WEBHOOK_INCLUDE_SYSTEM") != "true"
	listener := &bitrix.Listener{
		Client: b24,
		Handle: func(ev bitrix.Event) {
			log.Debug("event", "event", ev.Event, "dialog", ev.Payload.DialogID, "message_id", ev.Payload.MessageID, "from_me", ev.Payload.IsFromMe)
			in.Enqueue(ev)
			if ev.Payload.System && skipSystemWebhooks {
				return // service notices ("task created", "deadline changed") are not for agents
			}
			wh.Enqueue(ev.Event, ev.Payload.IsFromMe, ev)
		},
	}
	if backfill > 0 {
		// Whatever the gap was (channel refresh, network), fetch what it hid.
		listener.OnReconnect = func(since time.Time) {
			if err := in.Backfill(ctx, since); err != nil {
				log.Warn("inbox catch-up", "err", err)
			}
		}
	}
	go func() { _ = listener.Run(ctx) }()

	directSend := os.Getenv("AGENT_SEND_MODE") == "direct"
	mcpHandler := mcpserver.Handler(mcpserver.Deps{B24: b24, Inbox: in, DirectSend: directSend})

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:3000"
	}
	srv := &http.Server{
		Addr: addr,
		Handler: (&api.Server{B24: b24, Listener: listener, Token: token, Inbox: in, MCP: mcpHandler,
			AllowedHosts: splitList(os.Getenv("API_ALLOWED_HOSTS"))}).Handler(),
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

// webhookEvents: empty = what an agent needs (inbox batches or items, and
// session changes); "*" = every messenger event; otherwise the given list.
func webhookEvents(env, batchWindow string) map[string]bool {
	switch strings.TrimSpace(env) {
	case "*":
		return nil
	case "":
		inboxEvent := "inbox.batch"
		if d, err := time.ParseDuration(batchWindow); err == nil && d <= 0 {
			inboxEvent = "inbox.new"
		}
		return map[string]bool{inboxEvent: true, "session.logged_out": true, "session.restored": true}
	}
	return toSet(splitList(env))
}

func eventNames(m map[string]bool) string {
	if len(m) == 0 {
		return "*"
	}
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
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
