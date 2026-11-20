package mcpserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"b24bridge/internal/bitrix"
)

// fakePortal serves /online/ and the REST methods the read tools use. The
// dialog "88" has messages 1..120; LAST_ID pages backwards like the real API.
func fakePortal(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/online/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `BX.message({'bitrix_sessid':'abc123','USER_ID':'54'})`)
	})
	mux.HandleFunc("/rest/", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/rest/"), ".json") {
		case "im.dialog.messages.get":
			limit, _ := strconv.Atoi(r.PostForm.Get("LIMIT"))
			last, _ := strconv.Atoi(r.PostForm.Get("LAST_ID"))
			top := 120
			if last > 0 {
				top = last - 1
			}
			var msgs []map[string]any
			for id := top; id >= 1 && len(msgs) < limit; id-- {
				author := 88
				if id%3 == 0 {
					author = 54
				}
				msgs = append(msgs, map[string]any{"id": id, "author_id": author, "date": "2026-11-20T10:00:00+03:00",
					"text": fmt.Sprintf("m%d [b]x[/b]", id), "params": map[string]any{}})
			}
			json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"chat_id": 1, "messages": msgs,
				"users": []map[string]any{{"id": 88, "name": "Коллега"}, {"id": 54, "name": "Я"}}}})
		case "im.recent.list":
			fmt.Fprint(w, `{"result":{"items":[
			 {"id":"88","type":"user","title":"Коллега Иванов","counter":2,"message":{"id":120,"text":"[b]привет[/b]","date":"2026-11-20T10:00:00+03:00","author_id":88}},
			 {"id":"chat100","type":"chat","title":"Общий","counter":0,"message":{"id":5,"text":"x","date":"2026-11-19T10:00:00+03:00","author_id":20}}]}}`)
		case "tasks.task.list":
			fmt.Fprint(w, `{"result":{"tasks":[{"id":"5314","title":"Отчёт для Иванова","responsibleId":"54","accomplices":[],"chatId":"5986","activityDate":"2026-11-20T09:00:00+03:00"}]}}`)
		default:
			fmt.Fprint(w, `{"error":"ERROR_METHOD_NOT_FOUND","error_description":"x"}`)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func callTool(t *testing.T, h http.Handler, name string, args string) map[string]any {
	t.Helper()
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":%q,"arguments":%s}}`, name, args)
	r := httptest.NewRequest("POST", "/mcp", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var resp struct {
		Result struct {
			IsError           bool             `json:"isError"`
			Content           []map[string]any `json:"content"`
			StructuredContent map[string]any   `json:"structuredContent"`
		} `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: %v: %s", name, err, w.Body.String())
	}
	if resp.Result.IsError {
		t.Fatalf("%s returned error: %v", name, resp.Result.Content)
	}
	return resp.Result.StructuredContent
}

func newHandler(t *testing.T) http.Handler {
	srv := fakePortal(t)
	c, err := bitrix.New(bitrix.Options{Portal: srv.URL, Cookie: "PHPSESSID=x", MinInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RefreshSession(t.Context()); err != nil {
		t.Fatal(err)
	}
	return Handler(Deps{B24: c})
}

func ids(res map[string]any) []int {
	var out []int
	for _, m := range res["messages"].([]any) {
		out = append(out, int(m.(map[string]any)["id"].(float64)))
	}
	return out
}

func TestChatHistoryPagingAndNumericDialog(t *testing.T) {
	h := newHandler(t)

	// dialog_id as a number, as models often send it.
	res := callTool(t, h, "chat_history", `{"dialog_id": 88, "limit": 5}`)
	if got := fmt.Sprint(ids(res)); got != "[116 117 118 119 120]" {
		t.Fatalf("latest page oldest-first: %s", got)
	}
	if res["has_more"] != true || res["next_before_id"].(float64) != 116 {
		t.Fatalf("paging fields: %v %v", res["has_more"], res["next_before_id"])
	}
	msgs := res["messages"].([]any)
	first := msgs[0].(map[string]any)
	if first["text"] != "m116 x" || first["author"] != "Коллега" {
		t.Fatalf("message rendering: %v", first)
	}
	if msgs[1].(map[string]any)["from_me"] != true { // 117 = 3*39: written by the user
		t.Fatalf("from_me: %v", msgs[1])
	}

	// Next page continues without overlap.
	res = callTool(t, h, "chat_history", `{"dialog_id": "88", "limit": 5, "before_id": 116}`)
	if got := fmt.Sprint(ids(res)); got != "[111 112 113 114 115]" {
		t.Fatalf("second page: %s", got)
	}

	// Large limits span several API pages.
	res = callTool(t, h, "chat_history", `{"dialog_id": "88", "limit": 200}`)
	if n := len(ids(res)); n != 120 || res["has_more"] != false {
		t.Fatalf("whole dialog: %d messages, has_more=%v", n, res["has_more"])
	}
}

func TestChatsList(t *testing.T) {
	h := newHandler(t)
	res := callTool(t, h, "chats_list", `{}`)
	chats := res["chats"].([]any)
	if len(chats) != 3 {
		t.Fatalf("recent + task chats: %v", chats)
	}
	task := chats[2].(map[string]any)
	if task["dialog_id"] != "chat5986" || task["type"] != "task" || task["task_role"] != "responsible" {
		t.Fatalf("task chat: %v", task)
	}
	if chats[0].(map[string]any)["last_message"] != "привет" {
		t.Fatalf("last message: %v", chats[0])
	}

	res = callTool(t, h, "chats_list", `{"query": "иванов"}`)
	if n := len(res["chats"].([]any)); n != 2 {
		t.Fatalf("query matches person and task: %d", n)
	}
	res = callTool(t, h, "chats_list", `{"unread_only": true}`)
	if n := len(res["chats"].([]any)); n != 1 {
		t.Fatalf("unread only: %d", n)
	}
}
