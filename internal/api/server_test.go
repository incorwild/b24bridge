package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGuard(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	s := &Server{Token: "0123456789abcdef", MCP: ok, AllowedHosts: []string{"bridge.example.com"}}
	h := s.Handler()

	do := func(method, host, auth, ctype, body string) int {
		r := httptest.NewRequest(method, "http://"+host+"/mcp", strings.NewReader(body))
		r.Host = host
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		if ctype != "" {
			r.Header.Set("Content-Type", ctype)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	good := "Bearer 0123456789abcdef"
	cases := []struct {
		name                   string
		method, host, auth, ct string
		want                   int
	}{
		{"valid", "POST", "127.0.0.1:3077", good, "application/json; charset=utf-8", 200},
		{"localhost", "POST", "localhost:3077", good, "application/json", 200},
		{"ipv6", "POST", "[::1]:3077", good, "application/json", 200},
		{"tunnel host", "POST", "bridge.example.com", good, "application/json", 200},
		{"no token", "POST", "127.0.0.1:3077", "", "application/json", 401},
		{"wrong token", "POST", "127.0.0.1:3077", "Bearer nope", "application/json", 401},
		{"token without Bearer", "POST", "127.0.0.1:3077", "0123456789abcdef", "application/json", 401},
		// A web page can only send these without a CORS preflight:
		{"csrf text/plain", "POST", "127.0.0.1:3077", good, "text/plain", 415},
		{"csrf form", "POST", "127.0.0.1:3077", good, "application/x-www-form-urlencoded", 415},
		// DNS rebinding: the page's own hostname resolves to 127.0.0.1.
		{"rebinding host", "POST", "evil.example.net:3077", good, "application/json", 403},
	}
	for _, c := range cases {
		if got := do(c.method, c.host, c.auth, c.ct, "{}"); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}

	// Empty token fails closed.
	open := (&Server{MCP: ok}).Handler()
	r := httptest.NewRequest("POST", "http://127.0.0.1/mcp", strings.NewReader("{}"))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	open.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("empty token: got %d", w.Code)
	}
}
