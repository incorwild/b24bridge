package bitrix

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// swapJar lets ReplaceCookies swap the whole cookie set while requests run.
type swapJar struct {
	mu  sync.RWMutex
	jar *cookiejar.Jar
}

func (s *swapJar) swap(j *cookiejar.Jar) {
	s.mu.Lock()
	s.jar = j
	s.mu.Unlock()
}

func (s *swapJar) current() *cookiejar.Jar {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.jar
}

func (s *swapJar) SetCookies(u *url.URL, cs []*http.Cookie) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.jar.SetCookies(u, cs)
}

func (s *swapJar) Cookies(u *url.URL) []*http.Cookie {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.jar.Cookies(u)
}

// ErrOtherUser means new cookies sign in as a different Bitrix24 user than
// the one the bridge (and its inbox) belongs to.
var ErrOtherUser = errors.New("bitrix24: these cookies belong to another user")

// SwapCookies tries a new cookie set and keeps it only if it signs in as the
// same user. On failure the previous cookies, sessid and state stay as they
// were, so a wrong paste cannot log a working bridge out.
func (c *Client) SwapCookies(ctx context.Context, header string) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	oldJar := c.jar.current()
	c.mu.RLock()
	oldSess, oldUID := c.sessid, c.userID
	c.mu.RUnlock()
	restore := func() {
		c.jar.swap(oldJar)
		c.mu.Lock()
		c.sessid, c.userID = oldSess, oldUID
		c.mu.Unlock()
	}

	c.ReplaceCookies(header)
	err := c.loadSession(ctx)
	if errors.Is(err, ErrUnauthorized) {
		c.dropCookie("PHPSESSID")
		err = c.loadSession(ctx)
	}
	if err == nil && oldUID != 0 && c.UserID() != oldUID {
		err = ErrOtherUser
	}
	if err != nil {
		restore()
		return err
	}
	c.auth.set(nil)
	return nil
}

// SessionState is what /healthz and webhooks report about the login.
type SessionState struct {
	Authorized  bool      `json:"authorized"`
	Since       time.Time `json:"since"`                  // last change of Authorized
	LastRefresh time.Time `json:"last_refresh,omitempty"` // last successful refresh
	LastError   string    `json:"last_error,omitempty"`
}

type authState struct {
	mu       sync.Mutex
	st       SessionState
	known    bool          // set() was called at least once
	ok       chan struct{} // closed while authorized
	onChange []func(SessionState)
}

func (a *authState) init() {
	a.ok = make(chan struct{})
}

// set records the outcome of a refresh. Only an ErrUnauthorized flips the
// state to logged out; network errors leave it as it was.
func (a *authState) set(err error) {
	a.mu.Lock()
	prev := a.st
	now := time.Now()
	switch {
	case err == nil:
		a.st.Authorized = true
		a.st.LastRefresh = now
		a.st.LastError = ""
	case errors.Is(err, ErrUnauthorized):
		a.st.Authorized = false
		a.st.LastError = err.Error()
	default:
		a.st.LastError = err.Error()
		a.mu.Unlock()
		return
	}
	changed := !a.known || prev.Authorized != a.st.Authorized
	a.known = true
	if changed {
		a.st.Since = now
		if a.st.Authorized {
			close(a.ok)
		} else {
			a.ok = make(chan struct{})
		}
	}
	st, cbs := a.st, append([]func(SessionState){}, a.onChange...)
	a.mu.Unlock()
	if changed {
		for _, f := range cbs {
			f(st)
		}
	}
}

func (c *Client) Session() SessionState {
	c.auth.mu.Lock()
	defer c.auth.mu.Unlock()
	return c.auth.st
}

// OnSessionChange registers f for login/logout transitions (the first
// successful refresh counts as a transition).
func (c *Client) OnSessionChange(f func(SessionState)) {
	c.auth.mu.Lock()
	c.auth.onChange = append(c.auth.onChange, f)
	c.auth.mu.Unlock()
}

// WaitAuthorized blocks until the session is authorized again.
func (c *Client) WaitAuthorized(ctx context.Context) error {
	c.auth.mu.Lock()
	ch := c.auth.ok
	c.auth.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// KeepAlive pings the portal every interval so the PHP session does not
// expire from inactivity (the push websocket goes to another server and does
// not count), and refreshes it through the "remember me" cookies if it did.
// While logged out it stops pinging and waits for new cookies.
func (c *Client) KeepAlive(ctx context.Context, interval time.Duration, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !c.Session().Authorized {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := c.PullConfig(cctx) // cheap, and Call refreshes on auth errors
		cancel()
		if err != nil {
			log.Warn("session keepalive failed", "err", err)
		} else {
			c.auth.mu.Lock()
			c.auth.st.LastRefresh = time.Now()
			c.auth.mu.Unlock()
		}
	}
}

// PersistCookies writes the current cookie set to path whenever it changes
// (Bitrix24 rotates PHPSESSID and kernel cookies), so a restart picks up the
// fresh values. The write is atomic and the file is 0600.
func (c *Client) PersistCookies(ctx context.Context, path string, every time.Duration, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	last := c.CookieHeader()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cur := c.CookieHeader()
		if cur == last || cur == "" {
			continue
		}
		if err := writeFileAtomic(path, []byte(cur+"\n")); err != nil {
			log.Error("save cookies", "path", path, "err", err)
			continue
		}
		last = cur
		log.Info("cookies saved", "path", path)
	}
}

func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".cookies-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
