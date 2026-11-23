package bitrix

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Shapes below follow frames captured on a live cloud portal (2026-11-19),
// with texts and names replaced.
const frameMessage = `#!NGINXNMS!#{"id":42870518257,"mid":"a1b2","channel":"x","tag":"t1","time":"1794400600","text":{"module_id":"im","command":"message","params":{"chatId":11136,"dialogId":88,"message":{"id":338282,"chatId":11136,"senderId":88,"recipientId":"54","date":"2026-11-19T13:52:41+03:00","text":"hello","params":[]},"users":{"54":{"id":54,"name":"Me"},"88":{"id":88,"name":"Colleague"}},"files":[],"counter":1},"extra":{"server_time_unix":1794400600}}}#!NGINXNME!#`

const frameTwo = `#!NGINXNMS!#{"id":1,"mid":"m1","text":{"module_id":"im","command":"addReaction","params":{"userId":54,"reaction":"okHand","dialogId":54,"actualReactions":{"reaction":{"messageId":338280}}}}}#!NGINXNME!##!NGINXNMS!#{"id":2,"mid":"m2","text":{"module_id":"im","command":"deleteChatAnchors","params":{}}}#!NGINXNME!#`

func TestParseAndNormalizeIncoming(t *testing.T) {
	msgs := ParseFrame([]byte(frameMessage))
	if len(msgs) != 1 {
		t.Fatalf("got %d messages", len(msgs))
	}
	if msgs[0].Mid != "a1b2" {
		t.Fatalf("mid = %q", msgs[0].Mid)
	}
	ev, ok := Normalize(msgs[0], 54)
	if !ok || ev.Event != EvMessage {
		t.Fatalf("normalize: %v %+v", ok, ev)
	}
	p := ev.Payload
	if p.DialogID != "88" || p.ChatID != 11136 || p.MessageID != 338282 || p.SenderID != 88 || p.Text != "hello" || p.IsFromMe {
		t.Fatalf("payload: %+v", p)
	}
	if p.Sender == nil || p.Sender.Name != "Colleague" {
		t.Fatalf("sender: %+v", p.Sender)
	}
	if p.Files != nil {
		t.Fatalf("empty files should be omitted")
	}
}

func TestNormalizeOwnMessageAndReaction(t *testing.T) {
	own := strings.Replace(frameMessage, `"senderId":88`, `"senderId":54`, 1)
	ev, _ := Normalize(ParseFrame([]byte(own))[0], 54)
	if !ev.Payload.IsFromMe {
		t.Fatal("own message must be is_from_me")
	}

	msgs := ParseFrame([]byte(frameTwo))
	if len(msgs) != 2 {
		t.Fatalf("got %d", len(msgs))
	}
	ev, ok := Normalize(msgs[0], 54)
	if !ok || ev.Event != EvReaction || ev.Payload.MessageID != 338280 || ev.Payload.Reaction != "okHand" || !ev.Payload.IsFromMe {
		t.Fatalf("reaction: %+v", ev)
	}
	if _, ok := Normalize(msgs[1], 54); ok {
		t.Fatal("service command must be skipped")
	}
}

func TestParseFrameGarbage(t *testing.T) {
	if got := ParseFrame([]byte("pong")); len(got) != 0 {
		t.Fatal("expected nothing")
	}
	if got := ParseFrame([]byte(`#!NGINXNMS!#{bad#!NGINXNME!#`)); len(got) != 0 {
		t.Fatal("expected nothing for broken json")
	}
}

func TestExtractSession(t *testing.T) {
	page := []byte(`<script>BX.message({'LANGUAGE_ID':'ru','bitrix_sessid':'0123abcdef','USER_ID':'54','SITE_ID':'s1'});</script>`)
	sid, uid, ok := extractSession(page)
	if !ok || sid != "0123abcdef" || uid != 54 {
		t.Fatalf("got %q %d %v", sid, uid, ok)
	}
	if _, _, ok := extractSession([]byte("<form id=login>")); ok {
		t.Fatal("login page must not yield a session")
	}
}

func TestConnectionURL(t *testing.T) {
	var cfg PullConfig
	_ = json.Unmarshal([]byte(`{"server":{"version":4,"mode":"personal","hostname":"p.bitrix24.ru","websocket_secure":"wss://rtc/subws2/"},"api":{"revision_web":19},"channels":{"shared":{"id":"S"},"private":{"id":"P","end":"2026-11-19T15:35:44+03:00"}}}`), &cfg)
	u, err := cfg.ConnectionURL(session{mid: "m9"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"wss://rtc/subws2/?", "CHANNEL_ID=P%2FS", "revision=19", "hostname=p.bitrix24.ru", "mid=m9"} {
		if !strings.Contains(u, want) {
			t.Errorf("url %s lacks %s", u, want)
		}
	}
	cfg.Server.Version = 5
	if _, err := cfg.ConnectionURL(session{}); err == nil {
		t.Error("v5 must be rejected until JSON-RPC is implemented")
	}
}

// fakePortal emulates the parts of a portal the bridge touches.
type fakePortal struct {
	srv      *httptest.Server
	sessid   atomic.Value
	phpsess  atomic.Value // currently valid PHPSESSID
	uidh     atomic.Value // valid "remember me" hash; "" = revoked
	logins   atomic.Int32 // sessions created from UIDH
	calls    atomic.Int32
	wsFrames []string
	// perConn, when set, gives each websocket connection its own frames; the
	// server drops every connection but the last one after sending them.
	perConn  [][]string
	conns    atomic.Int32
	cfgCalls atomic.Int32
	mids     chan string // "mid" query param of each connection
}

func newFakePortal(t *testing.T) *fakePortal {
	f := &fakePortal{}
	f.sessid.Store("a1")
	f.phpsess.Store("abc")
	f.uidh.Store("x")
	mux := http.NewServeMux()
	mux.HandleFunc("/online/", func(w http.ResponseWriter, r *http.Request) {
		c, _ := r.Cookie("PHPSESSID")
		switch {
		case c != nil && c.Value == f.phpsess.Load():
		case c != nil:
			// Strictest behaviour: a dead PHPSESSID is not revived even with
			// UIDH, so the client must drop it itself.
			http.Redirect(w, r, "/auth/", http.StatusFound)
			return
		default:
			u, _ := r.Cookie("BITRIX_SM_UIDH")
			if u == nil || f.uidh.Load() == "" || u.Value != f.uidh.Load() {
				http.Redirect(w, r, "/auth/", http.StatusFound)
				return
			}
			n := f.logins.Add(1)
			sess := fmt.Sprintf("new%d", n)
			f.phpsess.Store(sess)
			f.sessid.Store(fmt.Sprintf("c%d", n))
			http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: sess, Path: "/"})
		}
		fmt.Fprintf(w, `BX.message({'bitrix_sessid':'%s','USER_ID':'54'})`, f.sessid.Load())
	})
	mux.HandleFunc("/rest/", func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		_ = r.ParseForm()
		if c, _ := r.Cookie("PHPSESSID"); c == nil || c.Value != f.phpsess.Load() {
			w.WriteHeader(401)
			fmt.Fprint(w, `{"error":"NO_AUTH_FOUND","error_description":"session"}`)
			return
		}
		if r.PostForm.Get("sessid") != f.sessid.Load() {
			w.WriteHeader(401)
			fmt.Fprint(w, `{"error":"NO_AUTH_FOUND","error_description":"Wrong authorization data"}`)
			return
		}
		switch strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/rest/"), ".json") {
		case "im.message.add":
			if r.PostForm.Get("DIALOG_ID") == "" {
				w.WriteHeader(400)
				fmt.Fprint(w, `{"error":"DIALOG_ID_EMPTY","error_description":"no dialog"}`)
				return
			}
			fmt.Fprint(w, `{"result":338284}`)
		case "im.dialog.get":
			fmt.Fprint(w, `{"result":{"last_message_id":338282,"readed_list":[{"user_id":88,"user_name":"C","message_id":337392},{"user_id":54,"message_id":338282}]}}`)
		case "pull.config.get":
			f.cfgCalls.Add(1)
			ws := "ws" + strings.TrimPrefix(f.srv.URL, "http") + "/subws2/"
			fmt.Fprintf(w, `{"result":{"server":{"version":4,"mode":"personal","websocket_secure":%q},"api":{"revision_web":19},"channels":{"private":{"id":"P"},"shared":{"id":"S"}}}}`, ws)
		default:
			fmt.Fprint(w, `{"error":"ERROR_METHOD_NOT_FOUND","error_description":"x"}`)
		}
	})
	mux.HandleFunc("/subws2/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("CHANNEL_ID") != "P/S" {
			http.Error(w, "bad channel", 400)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		n := int(f.conns.Add(1)) - 1
		if f.mids != nil {
			f.mids <- r.URL.Query().Get("mid")
		}
		frames := f.wsFrames
		if f.perConn != nil && n < len(f.perConn) {
			frames = f.perConn[n]
		}
		for _, fr := range frames {
			_ = c.Write(r.Context(), websocket.MessageText, []byte(fr))
		}
		if f.perConn != nil && n < len(f.perConn)-1 {
			c.Close(websocket.StatusGoingAway, "server restart")
			return
		}
		<-r.Context().Done()
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func newTestClient(t *testing.T, f *fakePortal) *Client {
	c, err := New(Options{Portal: f.srv.URL, Cookie: "PHPSESSID=abc; BITRIX_SM_UIDH=x", MinInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientSessionAndRetryOnStaleSessid(t *testing.T) {
	f := newFakePortal(t)
	c := newTestClient(t, f)
	ctx := context.Background()
	if err := c.RefreshSession(ctx); err != nil {
		t.Fatal(err)
	}
	if c.UserID() != 54 {
		t.Fatalf("user id %d", c.UserID())
	}
	f.sessid.Store("b2") // server rotated the token
	id, err := c.SendMessage(ctx, "54", "hi")
	if err != nil || id != 338284 {
		t.Fatalf("send: %d %v", id, err)
	}
	if n := f.calls.Load(); n != 2 {
		t.Fatalf("expected one failed call + one retry, got %d", n)
	}
	_, err = c.SendMessage(ctx, "", "hi")
	var ae *APIError
	if err == nil || !strings.Contains(err.Error(), "DIALOG_ID_EMPTY") {
		t.Fatalf("api error expected: %v", err)
	}
	_ = ae
}

func TestClientExpiredCookies(t *testing.T) {
	f := newFakePortal(t)
	c, _ := New(Options{Portal: f.srv.URL, Cookie: "PHPSESSID=old", MinInterval: -1})
	if err := c.RefreshSession(context.Background()); err != ErrUnauthorized {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
}

func TestReadStatus(t *testing.T) {
	f := newFakePortal(t)
	c := newTestClient(t, f)
	ctx := context.Background()
	_ = c.RefreshSession(ctx)
	st, err := c.ReadStatus(ctx, "88", 0)
	if err != nil {
		t.Fatal(err)
	}
	if st.MessageID != 338282 || st.Read || len(st.Pending) != 1 || st.Pending[0].UserID != 88 {
		t.Fatalf("status: %+v", st)
	}
	st, _ = c.ReadStatus(ctx, "88", 337000)
	if !st.Read || len(st.ReadBy) != 1 {
		t.Fatalf("older message should be read: %+v", st)
	}
}

func TestListenerEndToEnd(t *testing.T) {
	f := newFakePortal(t)
	// The same frame twice: the second copy must be deduplicated.
	f.wsFrames = []string{frameMessage, frameMessage, frameTwo}
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.RefreshSession(ctx)

	got := make(chan Event, 10)
	l := &Listener{Client: c, Handle: func(e Event) { got <- e }}
	go l.Run(ctx)

	var names []string
	for len(names) < 2 {
		select {
		case e := <-got:
			names = append(names, e.Event)
			if e.Portal == "" {
				t.Error("portal not set")
			}
		case <-ctx.Done():
			t.Fatalf("timeout, got %v", names)
		}
	}
	select {
	case e := <-got:
		t.Fatalf("unexpected extra event %s (dedup failed?)", e.Event)
	case <-time.After(300 * time.Millisecond):
	}
	if names[0] != EvMessage || names[1] != EvReaction {
		t.Fatalf("events: %v", names)
	}
	if !l.Connected() {
		t.Error("listener should report connected")
	}
}

// Captured live 2026-11-19 (name replaced).
func TestNormalizeReadReceiptAndTyping(t *testing.T) {
	ack := `#!NGINXNMS!#{"id":5,"mid":"r1","text":{"module_id":"im","command":"readMessageOpponent","params":{"dialogId":"88","chatId":11136,"userId":88,"userName":"Colleague","lastId":338338,"date":"2026-11-19T14:15:32+03:00","viewedMessages":[338338],"chatMessageStatus":"delivered"}}}#!NGINXNME!#`
	ev, ok := Normalize(ParseFrame([]byte(ack))[0], 54)
	p := ev.Payload
	if !ok || ev.Event != EvMessageAck || p.UserID != 88 || p.UserName != "Colleague" || p.MessageID != 338338 || p.LastID != 338338 || len(p.Viewed) != 1 || p.Viewed[0] != 338338 || p.IsFromMe || p.ChatID != 11136 || p.DialogID != "88" {
		t.Fatalf("ack: %+v", p)
	}
	typing := `#!NGINXNMS!#{"id":6,"mid":"r2","text":{"module_id":"im","command":"startWriting","params":{"dialogId":"88","userId":88,"userName":"Colleague","userFirstName":"C"}}}#!NGINXNME!#`
	ev, ok = Normalize(ParseFrame([]byte(typing))[0], 54)
	if !ok || ev.Event != EvChatPresence || ev.Payload.UserID != 88 || ev.Payload.UserName != "Colleague" || ev.Payload.IsFromMe {
		t.Fatalf("typing: %+v", ev.Payload)
	}
}

func TestSessionRevivedFromRememberMe(t *testing.T) {
	f := newFakePortal(t)
	c := newTestClient(t, f)
	ctx := context.Background()
	var changes []bool
	c.OnSessionChange(func(st SessionState) { changes = append(changes, st.Authorized) })
	if err := c.RefreshSession(ctx); err != nil {
		t.Fatal(err)
	}

	f.phpsess.Store("dead") // PHP session expired on the server
	id, err := c.SendMessage(ctx, "54", "hi")
	if err != nil || id != 338284 {
		t.Fatalf("send after expiry: %d %v", id, err)
	}
	if f.logins.Load() != 1 {
		t.Fatalf("expected one login via UIDH, got %d", f.logins.Load())
	}
	if h := c.CookieHeader(); !strings.Contains(h, "PHPSESSID=new1") || !strings.Contains(h, "BITRIX_SM_UIDH=x") {
		t.Fatalf("jar not updated: %s", h)
	}
	if len(changes) != 1 || !changes[0] || !c.Session().Authorized {
		t.Fatalf("state changes %v", changes)
	}
}

func TestLogoutAndManualCookies(t *testing.T) {
	f := newFakePortal(t)
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var changes []bool
	c.OnSessionChange(func(st SessionState) { changes = append(changes, st.Authorized) })
	_ = c.RefreshSession(ctx)

	f.phpsess.Store("dead")
	f.uidh.Store("") // "log out on all devices"
	if _, err := c.SendMessage(ctx, "54", "hi"); err != ErrUnauthorized {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
	if c.Session().Authorized {
		t.Fatal("must be logged out")
	}

	waited := make(chan error, 1)
	go func() { waited <- c.WaitAuthorized(ctx) }()

	// The user signs in again in a browser and hands over new cookies.
	f.phpsess.Store("fresh")
	f.uidh.Store("y")
	c.ReplaceCookies("PHPSESSID=fresh; BITRIX_SM_UIDH=y")
	if err := c.RefreshSession(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err != nil {
		t.Fatalf("WaitAuthorized: %v", err)
	}
	if fmt.Sprint(changes) != "[true false true]" {
		t.Fatalf("state changes %v", changes)
	}
}

func TestPersistCookies(t *testing.T) {
	f := newFakePortal(t)
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.RefreshSession(ctx)
	path := t.TempDir() + "/cookies.txt"
	go c.PersistCookies(ctx, path, 20*time.Millisecond, nil)

	f.phpsess.Store("dead")
	if err := c.RefreshSession(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		b, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(b), "PHPSESSID=new1") {
			st, _ := os.Stat(path)
			if st.Mode().Perm() != 0o600 {
				t.Fatalf("mode %v", st.Mode())
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("cookies were not persisted")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestNormalizeBotSender(t *testing.T) {
	fr := strings.Replace(frameMessage, `"88":{"id":88,"name":"Colleague"}`, `"88":{"id":88,"name":"CRM","bot":true}`, 1)
	ev, _ := Normalize(ParseFrame([]byte(fr))[0], 54)
	if ev.Payload.Sender == nil || !ev.Payload.Sender.Bot {
		t.Fatalf("bot flag lost: %+v", ev.Payload.Sender)
	}
}

// The push server drops the connection: the listener must fetch a fresh
// config, reconnect with the last mid, and keep delivering events.
func TestListenerReconnects(t *testing.T) {
	f := newFakePortal(t)
	second := strings.Replace(frameTwo, `"mid":"m1"`, `"mid":"m3"`, 1)
	second = strings.Replace(second, `"mid":"m2"`, `"mid":"m4"`, 1)
	f.perConn = [][]string{{frameMessage}, {second}}
	f.mids = make(chan string, 4)
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = c.RefreshSession(ctx)

	got := make(chan Event, 10)
	l := &Listener{Client: c, Handle: func(e Event) { got <- e }}
	go l.Run(ctx)

	var names []string
	for len(names) < 2 {
		select {
		case e := <-got:
			names = append(names, e.Event)
		case <-ctx.Done():
			t.Fatalf("timeout, got %v", names)
		}
	}
	if names[0] != EvMessage || names[1] != EvReaction {
		t.Fatalf("events %v", names)
	}
	if f.conns.Load() < 2 || f.cfgCalls.Load() < 2 {
		t.Fatalf("conns=%d config calls=%d", f.conns.Load(), f.cfgCalls.Load())
	}
	if m1, m2 := <-f.mids, <-f.mids; m1 != "" || m2 != "a1b2" {
		t.Fatalf("mid on connect: first %q, reconnect %q (want the last seen mid)", m1, m2)
	}
}

// KeepAlive notices a dead PHP session and revives it via remember-me
// cookies without any API traffic from the user.
func TestKeepAliveRevivesSession(t *testing.T) {
	f := newFakePortal(t)
	c := newTestClient(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.RefreshSession(ctx); err != nil {
		t.Fatal(err)
	}
	f.phpsess.Store("dead")
	go c.KeepAlive(ctx, 20*time.Millisecond, nil)
	for f.logins.Load() == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("keepalive did not revive the session")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !c.Session().Authorized || !strings.Contains(c.CookieHeader(), "PHPSESSID=new1") {
		t.Fatalf("state %+v cookies %s", c.Session(), c.CookieHeader())
	}
}

func TestReplyID(t *testing.T) {
	for raw, want := range map[string]int64{
		`{"REPLY_ID":"328408"}`:                 328408, // im.v2.Chat.Message.list, live
		`{"IS_EDITED":"Y","REPLY_ID":"328624"}`: 328624,
		`{"REPLY_ID":5}`:                        5, `{"replyId":6}`: 6, `{"REPLY_ID":["7"]}`: 7,
		`[]`: 0, `{}`: 0, `null`: 0, ``: 0, `{"REPLY_ID":"0"}`: 0,
	} {
		if got := replyID([]byte(raw)); got != want {
			t.Errorf("%s: %d, want %d", raw, got, want)
		}
	}
}

func TestNormalizeReply(t *testing.T) {
	fr := strings.Replace(frameMessage, `"params":[]`, `"params":{"REPLY_ID":"10"},"additionalEntities":{"messages":[],"additionalMessages":[{"id":10,"authorId":54,"text":"q"}]}`, 1)
	ev, _ := Normalize(ParseFrame([]byte(fr))[0], 54)
	if r := ev.Payload.ReplyTo; r == nil || r.ID != 10 || r.AuthorID != 54 || r.Text != "q" {
		t.Fatalf("reply %+v", r)
	}
	// Without the target only the id is known; plain messages have no reply.
	fr = strings.Replace(frameMessage, `"params":[]`, `"params":{"REPLY_ID":"10"},"additionalEntities":[]`, 1)
	ev, _ = Normalize(ParseFrame([]byte(fr))[0], 54)
	if r := ev.Payload.ReplyTo; r == nil || r.ID != 10 || r.AuthorID != 0 {
		t.Fatalf("reply %+v", r)
	}
	if ev, _ = Normalize(ParseFrame([]byte(frameMessage))[0], 54); ev.Payload.ReplyTo != nil {
		t.Fatal("not a reply")
	}
}
