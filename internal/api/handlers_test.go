package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"b24bridge/internal/bitrix"
	"b24bridge/internal/inbox"
)

const token = "0123456789abcdef"

// portal fakes the Bitrix24 endpoints the API touches. Sessions:
// PHPSESSID=good → user 54, PHPSESSID=other → user 99, anything else → login page.
type portal struct {
	srv  *httptest.Server
	mu   sync.Mutex
	sent []string // "dialog|text"
}

func newPortal(t *testing.T) *portal {
	p := &portal{}
	user := func(r *http.Request) string {
		c, _ := r.Cookie("PHPSESSID")
		switch {
		case c != nil && c.Value == "good":
			return "54"
		case c != nil && c.Value == "other":
			return "99"
		}
		return ""
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/online/", func(w http.ResponseWriter, r *http.Request) {
		u := user(r)
		if u == "" {
			http.Redirect(w, r, "/auth/", http.StatusFound)
			return
		}
		fmt.Fprintf(w, `BX.message({'bitrix_sessid':'s%s','USER_ID':'%s'})`, u, u)
	})
	mux.HandleFunc("/rest/", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if user(r) == "" {
			w.WriteHeader(401)
			fmt.Fprint(w, `{"error":"NO_AUTH_FOUND","error_description":"x"}`)
			return
		}
		f := r.PostForm
		switch strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/rest/"), ".json") {
		case "im.message.add":
			p.mu.Lock()
			p.sent = append(p.sent, f.Get("DIALOG_ID")+"|"+f.Get("MESSAGE"))
			p.mu.Unlock()
			fmt.Fprint(w, `{"result":500}`)
		case "im.message.update", "im.message.delete":
			if f.Get("MESSAGE_ID") == "404" {
				w.WriteHeader(400)
				fmt.Fprint(w, `{"error":"MESSAGE_NOT_FOUND","error_description":"no such message"}`)
				return
			}
			fmt.Fprint(w, `{"result":true}`)
		case "im.dialog.read":
			fmt.Fprint(w, `{"result":{"chatId":7,"dialogId":"88","lastId":10,"counter":0}}`)
		case "im.recent.list":
			fmt.Fprint(w, `{"result":{"items":[{"id":"88","type":"user","title":"Коллега","counter":1}]}}`)
		case "im.dialog.messages.get":
			fmt.Fprint(w, `{"result":{"chat_id":7,"messages":[{"id":10,"author_id":88,"text":"hi"}],"users":[]}}`)
		case "im.dialog.get":
			fmt.Fprint(w, `{"result":{"last_message_id":10,"readed_list":[{"user_id":88,"message_id":10}]}}`)
		default:
			fmt.Fprint(w, `{"error":"ERROR_METHOD_NOT_FOUND","error_description":"x"}`)
		}
	})
	p.srv = httptest.NewServer(mux)
	t.Cleanup(p.srv.Close)
	return p
}

type env struct {
	h    http.Handler
	b24  *bitrix.Client
	in   *inbox.Inbox
	port *portal
}

func newEnv(t *testing.T) *env {
	p := newPortal(t)
	c, err := bitrix.New(bitrix.Options{Portal: p.srv.URL, Cookie: "PHPSESSID=good; BITRIX_SM_UIDH=h", MinInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RefreshSession(t.Context()); err != nil {
		t.Fatal(err)
	}
	st, err := inbox.Open(t.TempDir() + "/inbox.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	in := inbox.New(st, c, c.UserID)
	return &env{h: (&Server{B24: c, Token: token, Inbox: in}).Handler(), b24: c, in: in, port: p}
}

func (e *env) do(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(method, "http://127.0.0.1:3077"+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	if method == "POST" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	var out map[string]any
	b, _ := io.ReadAll(w.Body)
	_ = json.Unmarshal(b, &out)
	return w.Code, out
}

func TestSendUpdateDeleteRead(t *testing.T) {
	e := newEnv(t)
	code, out := e.do(t, "POST", "/send/message", `{"dialog_id":"88","message":"привет"}`)
	if code != 200 || out["message_id"].(float64) != 500 {
		t.Fatalf("send: %d %v", code, out)
	}
	if e.port.sent[0] != "88|привет" {
		t.Fatalf("sent %v", e.port.sent)
	}
	for _, bad := range []string{`{}`, `{"dialog_id":"88","message":"   "}`, `not json`} {
		if code, _ := e.do(t, "POST", "/send/message", bad); code != 400 {
			t.Errorf("bad body %q: %d", bad, code)
		}
	}
	if code, _ := e.do(t, "POST", "/message/12/update", `{"message":"новый"}`); code != 200 {
		t.Fatalf("update: %d", code)
	}
	if code, _ := e.do(t, "POST", "/message/abc/delete", `{}`); code != 400 {
		t.Fatalf("bad id: %d", code)
	}
	code, out = e.do(t, "POST", "/message/404/delete", `{}`)
	if code != 502 || out["error"] != "MESSAGE_NOT_FOUND" {
		t.Fatalf("bitrix error passthrough: %d %v", code, out)
	}
	if code, out = e.do(t, "POST", "/chat/88/read", `{}`); code != 200 || out["lastId"].(float64) != 10 {
		t.Fatalf("read: %d %v", code, out)
	}
	code, out = e.do(t, "GET", "/chat/88/read-status", "")
	if code != 200 || out["read"] != true {
		t.Fatalf("read-status: %d %v", code, out)
	}
	if code, out = e.do(t, "GET", "/chats?limit=5", ""); code != 200 || len(out["items"].([]any)) != 1 {
		t.Fatalf("chats: %d %v", code, out)
	}
	if code, out = e.do(t, "GET", "/chat/88/messages", ""); code != 200 || len(out["messages"].([]any)) != 1 {
		t.Fatalf("messages: %d %v", code, out)
	}
}

func TestInboxAndDraftsEndpoints(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	ev := bitrix.Event{Event: bitrix.EvMessage, Payload: bitrix.Payload{MessageID: 1, DialogID: "88", SenderID: 88, Text: "вопрос"}}
	if err := e.in.Handle(ctx, ev); err != nil {
		t.Fatal(err)
	}
	code, out := e.do(t, "GET", "/inbox", "")
	items := out["items"].([]any)
	if code != 200 || len(items) != 1 {
		t.Fatalf("inbox: %d %v", code, out)
	}
	id := items[0].(map[string]any)["id"].(float64)

	code, out = e.do(t, "POST", "/drafts", fmt.Sprintf(`{"dialog_id":"88","text":"ответ","inbox_id":%v}`, id))
	if code != 200 || out["status"] != "pending" {
		t.Fatalf("draft: %d %v", code, out)
	}
	if code, _ := e.do(t, "POST", "/drafts", `{"dialog_id":"54","text":"себе"}`); code != 400 {
		t.Fatalf("draft to self chat: %d", code)
	}
	if code, out = e.do(t, "GET", "/drafts?status=pending", ""); code != 200 || len(out["drafts"].([]any)) != 1 {
		t.Fatalf("drafts: %d %v", code, out)
	}
	if code, _ := e.do(t, "POST", "/inbox/done", `{"ids":[]}`); code != 400 {
		t.Fatal("empty ids must be rejected")
	}
	if code, out = e.do(t, "POST", "/inbox/done", fmt.Sprintf(`{"ids":[%v]}`, id)); code != 200 || out["done"].(float64) != 1 {
		t.Fatalf("done: %d %v", code, out)
	}
	if _, out = e.do(t, "GET", "/inbox", ""); len(out["items"].([]any)) != 0 {
		t.Fatal("done item still listed")
	}
	if _, out = e.do(t, "GET", "/inbox?status=all", ""); len(out["items"].([]any)) != 1 {
		t.Fatal("status=all")
	}
}

func TestSetCookies(t *testing.T) {
	e := newEnv(t)
	var changes []bool
	e.b24.OnSessionChange(func(st bitrix.SessionState) { changes = append(changes, st.Authorized) })

	// Wrong cookies must not knock out a working session.
	code, _ := e.do(t, "POST", "/session/cookies", `{"cookie":"PHPSESSID=bad"}`)
	if code != 503 {
		t.Fatalf("bad cookies: %d", code)
	}
	if !e.b24.Session().Authorized || len(changes) != 0 {
		t.Fatalf("session state changed: %v %v", e.b24.Session(), changes)
	}
	if code, _ := e.do(t, "POST", "/send/message", `{"dialog_id":"88","message":"всё ещё работает"}`); code != 200 {
		t.Fatalf("old session lost after bad cookies: %d", code)
	}

	// Cookies of another account are refused: the inbox belongs to user 54.
	code, out := e.do(t, "POST", "/session/cookies", `{"cookie":"PHPSESSID=other"}`)
	if code != 409 {
		t.Fatalf("other user: %d %v", code, out)
	}
	if e.b24.UserID() != 54 {
		t.Fatalf("user switched to %d", e.b24.UserID())
	}

	// Raw header text (as copied from DevTools) is accepted too.
	if code, out = e.do(t, "POST", "/session/cookies", `Cookie: PHPSESSID=good; BITRIX_SM_UIDH=new`); code != 200 {
		t.Fatalf("raw header: %d %v", code, out)
	}
	if !strings.Contains(e.b24.CookieHeader(), "BITRIX_SM_UIDH=new") {
		t.Fatalf("cookies not replaced: %s", e.b24.CookieHeader())
	}
	if code, _ := e.do(t, "POST", "/session/cookies", `{"cookie":"nothing"}`); code != 400 {
		t.Fatal("garbage must be rejected")
	}
}

func TestHealth(t *testing.T) {
	e := newEnv(t)
	r := httptest.NewRequest("GET", "http://127.0.0.1/healthz", nil) // no token needed
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if w.Code != 200 || out["user_id"].(float64) != 54 || out["inbox_new"].(float64) != 0 {
		t.Fatalf("healthz: %d %v", w.Code, out)
	}
}
