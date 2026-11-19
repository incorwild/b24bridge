package bitrix

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type PullConfig struct {
	Server struct {
		Version         int    `json:"version"`
		Mode            string `json:"mode"`
		Hostname        string `json:"hostname"`
		WebsocketSecure string `json:"websocket_secure"`
		Websocket       string `json:"websocket"`
	} `json:"server"`
	API struct {
		RevisionWeb int `json:"revision_web"`
	} `json:"api"`
	Channels map[string]struct {
		ID    string    `json:"id"`
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	} `json:"channels"`
	ClientID string `json:"clientId"`
}

func (c *Client) PullConfig(ctx context.Context) (PullConfig, error) {
	var cfg PullConfig
	err := c.Call(ctx, "pull.config.get", url.Values{"CACHE": {"N"}}, &cfg)
	return cfg, err
}

// expires is the earliest channel end; the push server rejects the channel
// signature after it, so we reconnect with a fresh config before then.
func (cfg PullConfig) expires() time.Time {
	var t time.Time
	for _, ch := range cfg.Channels {
		if !ch.End.IsZero() && (t.IsZero() || ch.End.Before(t)) {
			t = ch.End
		}
	}
	return t
}

type session struct{ mid, tag, time string }

// ConnectionURL builds the websocket URL the same way the web client does
// for server version 4 without binary mode.
func (cfg PullConfig) ConnectionURL(s session) (string, error) {
	base := cfg.Server.WebsocketSecure
	if base == "" {
		base = cfg.Server.Websocket
	}
	if base == "" {
		return "", errors.New("pull config has no websocket URL")
	}
	if cfg.Server.Version >= 5 {
		return "", fmt.Errorf("push server version %d (JSON-RPC) is not supported yet", cfg.Server.Version)
	}
	var ids string
	for _, k := range []string{"private", "shared"} {
		if ch, ok := cfg.Channels[k]; ok && ch.ID != "" {
			if ids != "" {
				ids += "/"
			}
			ids += ch.ID
		}
	}
	if ids == "" {
		return "", errors.New("pull config has no channels")
	}
	q := url.Values{"CHANNEL_ID": {ids}, "revision": {strconv.Itoa(cfg.API.RevisionWeb)}}
	if cfg.Server.Hostname != "" {
		q.Set("hostname", cfg.Server.Hostname)
	}
	if cfg.Server.Mode == "shared" && cfg.ClientID != "" {
		q.Set("clientId", cfg.ClientID)
	}
	if s.mid != "" {
		q.Set("mid", s.mid)
	}
	if s.tag != "" {
		q.Set("tag", s.tag)
	}
	if s.time != "" {
		q.Set("time", s.time)
	}
	return base + "?" + q.Encode(), nil
}

// Listener keeps a push-server connection open and hands normalized events
// to Handle. Handle must not block for long.
type Listener struct {
	Client *Client
	Handle func(Event)
	Log    *slog.Logger

	connected atomic.Bool
	lastEvent atomic.Int64

	mu   sync.Mutex
	sess session
	seen map[string]struct{}
	ring []string
}

func (l *Listener) Connected() bool { return l.connected.Load() }
func (l *Listener) LastEventAt() time.Time {
	if v := l.lastEvent.Load(); v > 0 {
		return time.Unix(v, 0)
	}
	return time.Time{}
}

// Run blocks until ctx is cancelled, reconnecting with backoff.
func (l *Listener) Run(ctx context.Context) error {
	if l.Log == nil {
		l.Log = slog.Default()
	}
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := l.runOnce(ctx)
		l.connected.Store(false)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrUnauthorized) {
			// The client already tried the "remember me" cookies. Wait for
			// someone to supply new ones (POST /session/cookies).
			l.Log.Error("pull: logged out, waiting for new cookies", "err", err)
			if werr := l.Client.WaitAuthorized(ctx); werr != nil {
				return werr
			}
			backoff = time.Second
			continue
		} else {
			l.Log.Warn("pull: disconnected", "err", err)
			if time.Since(start) > time.Minute {
				backoff = time.Second
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
	return ctx.Err()
}

func (l *Listener) runOnce(ctx context.Context) error {
	cfg, err := l.Client.PullConfig(ctx)
	if err != nil {
		return fmt.Errorf("pull.config.get: %w", err)
	}
	l.mu.Lock()
	sess := l.sess
	l.mu.Unlock()
	u, err := cfg.ConnectionURL(sess)
	if err != nil {
		return err
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if exp := cfg.expires(); !exp.IsZero() {
		// Reconnect a minute before the channel signature expires.
		d := time.Until(exp) - time.Minute
		if d < time.Minute {
			d = time.Minute
		}
		t := time.AfterFunc(d, cancel)
		defer t.Stop()
	}

	hdr := http.Header{}
	hdr.Set("Origin", l.Client.Portal())
	hdr.Set("User-Agent", userAgent)
	conn, _, err := websocket.Dial(connCtx, u, &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(16 << 20)
	l.connected.Store(true)
	l.Log.Info("pull: connected", "server_version", cfg.Server.Version, "expires", cfg.expires())

	go func() {
		t := time.NewTicker(45 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-connCtx.Done():
				return
			case <-t.C:
				pctx, pc := context.WithTimeout(connCtx, 15*time.Second)
				err := conn.Ping(pctx)
				pc()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.Read(connCtx)
		if err != nil {
			if connCtx.Err() != nil && ctx.Err() == nil {
				return errors.New("channel refresh")
			}
			return err
		}
		for _, pm := range ParseFrame(data) {
			if !l.firstTime(pm) {
				continue
			}
			ev, ok := Normalize(pm, l.Client.UserID())
			if !ok {
				l.Log.Debug("pull: skipped", "module", pm.Text.ModuleID, "command", pm.Text.Command)
				continue
			}
			ev.Portal = l.Client.Host()
			l.lastEvent.Store(time.Now().Unix())
			if l.Handle != nil {
				l.Handle(ev)
			}
		}
	}
}

// firstTime records the session cursor and drops duplicates (the server can
// replay messages after a reconnect with mid/tag/time).
func (l *Listener) firstTime(pm PullMessage) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if pm.Mid != "" {
		l.sess.mid = string(pm.Mid)
	}
	if pm.Tag != "" {
		l.sess.tag = string(pm.Tag)
	}
	if pm.Time != "" {
		l.sess.time = string(pm.Time)
	}
	key := string(pm.Mid)
	if key == "" {
		key = string(pm.ID)
	}
	if key == "" {
		return true
	}
	if l.seen == nil {
		l.seen = map[string]struct{}{}
	}
	if _, dup := l.seen[key]; dup {
		return false
	}
	l.seen[key] = struct{}{}
	l.ring = append(l.ring, key)
	if len(l.ring) > 2000 {
		delete(l.seen, l.ring[0])
		l.ring = l.ring[1:]
	}
	return true
}
