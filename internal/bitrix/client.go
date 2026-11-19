// Package bitrix talks to a cloud Bitrix24 portal the way its own web
// messenger does: browser session cookies plus the sessid CSRF token, calling
// /rest/<method>.json. No incoming webhook or REST application is needed.
package bitrix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrUnauthorized means the cookies no longer sign in, including the
// "remember me" ones: someone has to sign in in a browser and hand the bridge
// new cookies (POST /session/cookies or the cookie file).
var ErrUnauthorized = errors.New("bitrix24: session is not authorized (re-login and update cookies)")

// APIError is an error object returned by /rest/.
type APIError struct {
	Status      int
	Code        string `json:"error"`
	Description string `json:"error_description"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("bitrix24 %d %s: %s", e.Status, e.Code, e.Description)
}

type Client struct {
	base *url.URL
	http *http.Client

	jar *swapJar

	mu     sync.RWMutex
	sessid string
	userID int64

	refreshMu sync.Mutex // one session refresh at a time
	auth      authState

	limiter <-chan time.Time
}

type Options struct {
	Portal string // e.g. https://example.bitrix24.ru
	Cookie string // raw Cookie header copied from the browser
	SessID string // optional; fetched from the messenger page when empty
	// MinInterval between REST calls. Bitrix24 cloud throttles per portal;
	// 500ms (~2 rps) stays inside the default budget. Negative disables.
	MinInterval time.Duration
	HTTPClient  *http.Client
}

func New(opts Options) (*Client, error) {
	portal := strings.TrimRight(opts.Portal, "/")
	if !strings.Contains(portal, "://") {
		portal = "https://" + portal
	}
	base, err := url.Parse(portal)
	if err != nil || base.Host == "" {
		return nil, fmt.Errorf("bad portal URL %q", opts.Portal)
	}
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	// Do not follow redirects: an expired session redirects to the login page,
	// and we want to see that as 302 rather than parse a login form.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	interval := opts.MinInterval
	if interval == 0 {
		interval = 500 * time.Millisecond
	}
	c := &Client{base: base, http: hc, sessid: opts.SessID, jar: &swapJar{}}
	hc.Jar = c.jar
	c.auth.init()
	c.ReplaceCookies(opts.Cookie)
	if interval > 0 {
		c.limiter = time.Tick(interval)
	}
	return c, nil
}

func (c *Client) Portal() string { return c.base.String() }
func (c *Client) Host() string   { return c.base.Host }

func (c *Client) UserID() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userID
}

func (c *Client) SetUserID(id int64) {
	c.mu.Lock()
	c.userID = id
	c.mu.Unlock()
}

func (c *Client) sessID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessid
}

// ReplaceCookies swaps the whole cookie set (a raw Cookie header).
func (c *Client) ReplaceCookies(header string) {
	jar, _ := cookiejar.New(nil)
	jar.SetCookies(c.base, parseCookieHeader(header))
	c.jar.swap(jar)
}

// CookieHeader returns the current cookies for the portal as a Cookie header,
// including values Bitrix24 rotated via Set-Cookie.
func (c *Client) CookieHeader() string {
	var parts []string
	for _, ck := range c.jar.Cookies(c.base) {
		parts = append(parts, ck.Name+"="+ck.Value)
	}
	return strings.Join(parts, "; ")
}

func (c *Client) dropCookie(name string) {
	c.jar.SetCookies(c.base, []*http.Cookie{{Name: name, Value: "", Path: "/", MaxAge: -1}})
}

var (
	reSessID = regexp.MustCompile(`['"]bitrix_sessid['"]\s*:\s*['"]([A-Za-z0-9]+)['"]`)
	reUserID = regexp.MustCompile(`['"]USER_ID['"]\s*:\s*['"]?(\d+)`)
)

// RefreshSession makes sure the cookies sign in and reloads sessid/USER_ID.
// If the PHP session is dead it drops PHPSESSID and lets the "remember me"
// cookies (BITRIX_SM_UIDH/UIDL) create a new one, as a browser does after a
// restart. Verified on a cloud portal on 2026-11-19.
func (c *Client) RefreshSession(ctx context.Context) error {
	return c.refresh(ctx, "")
}

// refresh is serialized; staleSessid != "" skips the work when another
// goroutine already replaced that sessid.
func (c *Client) refresh(ctx context.Context, staleSessid string) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	if staleSessid != "" && c.sessID() != staleSessid {
		return nil
	}
	err := c.loadSession(ctx)
	if errors.Is(err, ErrUnauthorized) {
		c.dropCookie("PHPSESSID")
		err = c.loadSession(ctx)
	}
	c.auth.set(err)
	return err
}

func (c *Client) loadSession(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.base.String()+"/online/", nil)
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return ErrUnauthorized
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	sid, uid, ok := extractSession(body)
	if !ok {
		return ErrUnauthorized
	}
	c.mu.Lock()
	c.sessid = sid
	if uid > 0 {
		c.userID = uid
	}
	c.mu.Unlock()
	return nil
}

func extractSession(page []byte) (sessid string, userID int64, ok bool) {
	m := reSessID.FindSubmatch(page)
	if m == nil {
		return "", 0, false
	}
	if u := reUserID.FindSubmatch(page); u != nil {
		userID, _ = strconv.ParseInt(string(u[1]), 10, 64)
	}
	return string(m[1]), userID, true
}

const userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"

// Call invokes /rest/<method>.json and decodes "result" into out (may be nil).
// On an auth failure it refreshes the session once and retries.
func (c *Client) Call(ctx context.Context, method string, params url.Values, out any) error {
	used := c.sessID()
	err := c.call(ctx, method, params, out)
	if isAuthError(err) {
		if rerr := c.refresh(ctx, used); rerr != nil {
			return rerr
		}
		err = c.call(ctx, method, params, out)
	}
	return err
}

func isAuthError(err error) bool {
	if errors.Is(err, ErrUnauthorized) {
		return true
	}
	var ae *APIError
	if errors.As(err, &ae) {
		switch strings.ToUpper(ae.Code) {
		case "NO_AUTH_FOUND", "INVALID_CREDENTIALS", "EXPIRED_TOKEN", "INVALID_TOKEN", "WRONG_SESSID", "AUTHORIZE_ERROR":
			return true
		}
		return ae.Status == http.StatusUnauthorized
	}
	return false
}

func (c *Client) call(ctx context.Context, method string, params url.Values, out any) error {
	if c.limiter != nil {
		select {
		case <-c.limiter:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	form := url.Values{}
	for k, v := range params {
		form[k] = v
	}
	form.Set("sessid", c.sessID())

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		c.base.String()+"/rest/"+method+".json", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Origin", c.base.String())
	req.Header.Set("Referer", c.base.String()+"/online/")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return ErrUnauthorized
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	var env struct {
		Result json.RawMessage `json:"result"`
		APIError
	}
	if err := json.Unmarshal(body, &env); err != nil {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return ErrUnauthorized
		}
		return fmt.Errorf("bitrix24 %s: status %d, non-JSON body: %.200s", method, resp.StatusCode, body)
	}
	if env.Code != "" {
		env.APIError.Status = resp.StatusCode
		return &env.APIError
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(env.Result, out)
}

func parseCookieHeader(h string) []*http.Cookie {
	var out []*http.Cookie
	for _, part := range strings.Split(h, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || name == "" {
			continue
		}
		out = append(out, &http.Cookie{Name: name, Value: strings.TrimSpace(value), Path: "/"})
	}
	return out
}
