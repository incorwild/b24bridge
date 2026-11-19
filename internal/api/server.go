// Package api exposes the bridge over HTTP, mirroring the route style of
// go-whatsapp-web-multidevice (/send/message, /chats, ...).
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"b24bridge/internal/bitrix"
	"b24bridge/internal/inbox"
)

type Server struct {
	B24      *bitrix.Client
	Listener *bitrix.Listener
	Token    string // optional bearer token
	Inbox    *inbox.Inbox
	MCP      http.Handler // mounted at /mcp when set
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /session/cookies", s.setCookies)
	mux.HandleFunc("POST /send/message", s.sendMessage)
	mux.HandleFunc("POST /message/{id}/update", s.updateMessage)
	mux.HandleFunc("POST /message/{id}/delete", s.deleteMessage)
	mux.HandleFunc("GET /chats", s.chats)
	mux.HandleFunc("GET /chat/{dialog}/messages", s.messages)
	mux.HandleFunc("POST /chat/{dialog}/read", s.read)
	mux.HandleFunc("GET /chat/{dialog}/read-status", s.readStatus)
	if s.Inbox != nil {
		mux.HandleFunc("GET /inbox", s.inboxList)
		mux.HandleFunc("POST /inbox/done", s.inboxDone)
		mux.HandleFunc("GET /drafts", s.draftsList)
		mux.HandleFunc("POST /drafts", s.draftCreate)
	}
	if s.MCP != nil {
		mux.Handle("/mcp", s.MCP)
	}
	return s.auth(mux)
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Token != "" && r.URL.Path != "/healthz" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(s.Token)) != 1 {
				writeErr(w, http.StatusUnauthorized, "unauthorized")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{"portal": s.B24.Host(), "user_id": s.B24.UserID(), "session": s.B24.Session()}
	if s.Inbox != nil {
		if n, err := s.Inbox.Store.CountNew(r.Context()); err == nil {
			resp["inbox_new"] = n
		}
	}
	if s.Listener != nil {
		resp["pull_connected"] = s.Listener.Connected()
		if t := s.Listener.LastEventAt(); !t.IsZero() {
			resp["last_event_at"] = t.UTC().Format(time.RFC3339)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// setCookies takes a fresh Cookie header after a manual sign-in, either as
// {"cookie": "..."} or as the raw header text.
func (s *Server) setCookies(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	cookie := strings.TrimSpace(string(body))
	var js struct {
		Cookie string `json:"cookie"`
	}
	if json.Unmarshal(body, &js) == nil && js.Cookie != "" {
		cookie = strings.TrimSpace(js.Cookie)
	}
	cookie = strings.TrimPrefix(cookie, "Cookie: ")
	if !strings.Contains(cookie, "=") {
		writeErr(w, http.StatusBadRequest, "send the browser Cookie header")
		return
	}
	s.B24.ReplaceCookies(cookie)
	if err := s.B24.RefreshSession(r.Context()); err != nil {
		writeB24Err(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user_id": s.B24.UserID(), "session": s.B24.Session()})
}

func (s *Server) sendMessage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DialogID string `json:"dialog_id"`
		Message  string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DialogID == "" || strings.TrimSpace(req.Message) == "" {
		writeErr(w, http.StatusBadRequest, `body must be {"dialog_id": "54" | "chat123", "message": "..."}`)
		return
	}
	id, err := s.B24.SendMessage(r.Context(), req.DialogID, req.Message)
	if err != nil {
		writeB24Err(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message_id": id, "dialog_id": req.DialogID})
}

func (s *Server) updateMessage(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Message == "" {
		writeErr(w, http.StatusBadRequest, `body must be {"message": "..."}`)
		return
	}
	if err := s.B24.UpdateMessage(r.Context(), id, req.Message); err != nil {
		writeB24Err(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message_id": id})
}

func (s *Server) deleteMessage(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt(w, r, "id")
	if !ok {
		return
	}
	if err := s.B24.DeleteMessage(r.Context(), id); err != nil {
		writeB24Err(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message_id": id})
}

func (s *Server) chats(w http.ResponseWriter, r *http.Request) {
	out, err := s.B24.RecentList(r.Context(), queryInt(r, "limit", 50))
	if err != nil {
		writeB24Err(w, err)
		return
	}
	writeRaw(w, out)
}

func (s *Server) messages(w http.ResponseWriter, r *http.Request) {
	out, err := s.B24.DialogMessages(r.Context(), r.PathValue("dialog"), queryInt(r, "limit", 50), int64(queryInt(r, "last_id", 0)))
	if err != nil {
		writeB24Err(w, err)
		return
	}
	writeRaw(w, out)
}

func (s *Server) read(w http.ResponseWriter, r *http.Request) {
	res, err := s.B24.ReadDialog(r.Context(), r.PathValue("dialog"))
	if err != nil {
		writeB24Err(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) readStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.B24.ReadStatus(r.Context(), r.PathValue("dialog"), int64(queryInt(r, "message_id", 0)))
	if err != nil {
		writeB24Err(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func pathInt(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	n, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil || n <= 0 {
		writeErr(w, http.StatusBadRequest, name+" must be a positive integer")
		return 0, false
	}
	return n, true
}

func queryInt(r *http.Request, name string, def int) int {
	if n, err := strconv.Atoi(r.URL.Query().Get(name)); err == nil && n > 0 {
		return n
	}
	return def
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeRaw(w http.ResponseWriter, raw json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeB24Err(w http.ResponseWriter, err error) {
	var ae *bitrix.APIError
	switch {
	case errors.Is(err, bitrix.ErrUnauthorized):
		writeErr(w, http.StatusServiceUnavailable, err.Error())
	case errors.As(err, &ae):
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": ae.Code, "error_description": ae.Description})
	default:
		writeErr(w, http.StatusBadGateway, err.Error())
	}
}

func (s *Server) inboxList(w http.ResponseWriter, r *http.Request) {
	st := r.URL.Query().Get("status")
	switch st {
	case "":
		st = "new"
	case "all":
		st = ""
	}
	items, err := s.Inbox.Store.List(r.Context(), st, queryInt(r, "limit", 50))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) inboxDone(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []int64 `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.IDs) == 0 {
		writeErr(w, http.StatusBadRequest, `body must be {"ids": [1, 2]}`)
		return
	}
	n, err := s.Inbox.Store.Done(r.Context(), req.IDs)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"done": n})
}

func (s *Server) draftsList(w http.ResponseWriter, r *http.Request) {
	ds, err := s.Inbox.Store.ListDrafts(r.Context(), r.URL.Query().Get("status"), queryInt(r, "limit", 50))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"drafts": ds})
}

func (s *Server) draftCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DialogID string `json:"dialog_id"`
		Text     string `json:"text"`
		InboxID  int64  `json:"inbox_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	d, err := s.Inbox.ProposeReply(r.Context(), req.DialogID, req.Text, req.InboxID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}
