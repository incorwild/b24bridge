package inbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Item struct {
	ID         int64    `json:"id"`
	MessageID  int64    `json:"message_id"`
	DialogID   string   `json:"dialog_id"`
	ChatID     int64    `json:"chat_id,omitempty"`
	SenderID   int64    `json:"sender_id"`
	SenderName string   `json:"sender_name,omitempty"`
	Text       string   `json:"text"`
	Date       string   `json:"date"`
	Labels     []string `json:"labels"`
	ChatTitle  string   `json:"chat_title,omitempty"`
	TaskID     int64    `json:"task_id,omitempty"`
	TaskTitle  string   `json:"task_title,omitempty"`
	Status     string   `json:"status"` // new | done
	CreatedAt  string   `json:"created_at"`
}

type Draft struct {
	ID            int64  `json:"id"`
	DialogID      string `json:"dialog_id"`
	Text          string `json:"text"`
	InboxID       int64  `json:"inbox_id,omitempty"`
	Status        string `json:"status"` // pending | sending | sent | rejected | failed
	SentMessageID int64  `json:"sent_message_id,omitempty"`
	Note          string `json:"note,omitempty"`
	CreatedAt     string `json:"created_at"`
}

type Store struct{ db *sql.DB }

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL UNIQUE,
  dialog_id TEXT NOT NULL, chat_id INTEGER, sender_id INTEGER, sender_name TEXT,
  text TEXT, date TEXT, labels TEXT, chat_title TEXT, task_id INTEGER, task_title TEXT,
  status TEXT NOT NULL DEFAULT 'new', created_at TEXT NOT NULL, done_at TEXT);
CREATE INDEX IF NOT EXISTS items_status ON items(status, id);
CREATE TABLE IF NOT EXISTS drafts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dialog_id TEXT NOT NULL, text TEXT NOT NULL, inbox_id INTEGER,
  status TEXT NOT NULL DEFAULT 'pending', sent_message_id INTEGER, note TEXT,
  created_at TEXT NOT NULL);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func now() string { return time.Now().UTC().Format(time.RFC3339) }

// Add stores it unless the message is already there; ok=false for duplicates.
func (s *Store) Add(ctx context.Context, it *Item) (bool, error) {
	it.Status, it.CreatedAt = "new", now()
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO items
 (message_id, dialog_id, chat_id, sender_id, sender_name, text, date, labels, chat_title, task_id, task_title, status, created_at)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		it.MessageID, it.DialogID, it.ChatID, it.SenderID, it.SenderName, it.Text, it.Date,
		strings.Join(it.Labels, ","), it.ChatTitle, it.TaskID, it.TaskTitle, it.Status, it.CreatedAt)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, nil
	}
	it.ID, _ = res.LastInsertId()
	return true, nil
}

const itemCols = `id, message_id, dialog_id, chat_id, sender_id, sender_name, text, date, labels, chat_title, task_id, task_title, status, created_at`

func scanItem(r interface{ Scan(...any) error }) (Item, error) {
	var it Item
	var labels string
	var chatID, taskID sql.NullInt64
	var name, title, ttitle sql.NullString
	err := r.Scan(&it.ID, &it.MessageID, &it.DialogID, &chatID, &it.SenderID, &name, &it.Text, &it.Date,
		&labels, &title, &taskID, &ttitle, &it.Status, &it.CreatedAt)
	it.ChatID, it.TaskID = chatID.Int64, taskID.Int64
	it.SenderName, it.ChatTitle, it.TaskTitle = name.String, title.String, ttitle.String
	it.Labels = []string{}
	if labels != "" {
		it.Labels = strings.Split(labels, ",")
	}
	return it, err
}

// List returns items oldest first. status: "new", "done" or "" for all.
func (s *Store) List(ctx context.Context, status string, limit int) ([]Item, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	q := `SELECT ` + itemCols + ` FROM items`
	args := []any{}
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY id LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Item{}
	for rows.Next() {
		it, err := scanItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func (s *Store) Get(ctx context.Context, id int64) (Item, error) {
	return scanItem(s.db.QueryRowContext(ctx, `SELECT `+itemCols+` FROM items WHERE id = ?`, id))
}

// Done marks items as handled. It returns how many changed.
func (s *Store) Done(ctx context.Context, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := []any{now()}
	for _, id := range ids {
		args = append(args, id)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE items SET status='done', done_at=? WHERE status='new' AND id IN (`+ph+`)`, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DoneDialog marks every new item of a dialog as handled (after a reply).
func (s *Store) DoneDialog(ctx context.Context, dialogID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE items SET status='done', done_at=? WHERE status='new' AND dialog_id=?`, now(), dialogID)
	return err
}

func (s *Store) CountNew(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM items WHERE status='new'`).Scan(&n)
	return n, err
}

func (s *Store) AddDraft(ctx context.Context, d *Draft) error {
	d.Status, d.CreatedAt = "pending", now()
	res, err := s.db.ExecContext(ctx, `INSERT INTO drafts (dialog_id, text, inbox_id, status, created_at) VALUES (?,?,?,?,?)`,
		d.DialogID, d.Text, d.InboxID, d.Status, d.CreatedAt)
	if err != nil {
		return err
	}
	d.ID, _ = res.LastInsertId()
	return nil
}

const draftCols = `id, dialog_id, text, inbox_id, status, sent_message_id, note, created_at`

func scanDraft(r interface{ Scan(...any) error }) (Draft, error) {
	var d Draft
	var inbox, sent sql.NullInt64
	var note sql.NullString
	err := r.Scan(&d.ID, &d.DialogID, &d.Text, &inbox, &d.Status, &sent, &note, &d.CreatedAt)
	d.InboxID, d.SentMessageID, d.Note = inbox.Int64, sent.Int64, note.String
	return d, err
}

func (s *Store) GetDraft(ctx context.Context, id int64) (Draft, error) {
	d, err := scanDraft(s.db.QueryRowContext(ctx, `SELECT `+draftCols+` FROM drafts WHERE id = ?`, id))
	if err == sql.ErrNoRows {
		return d, fmt.Errorf("draft %d not found", id)
	}
	return d, err
}

func (s *Store) ListDrafts(ctx context.Context, status string, limit int) ([]Draft, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	q := `SELECT ` + draftCols + ` FROM drafts`
	args := []any{}
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Draft{}
	for rows.Next() {
		d, err := scanDraft(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MoveDraft changes a draft's status from → to (optionally replacing text).
// The from-guard makes approval idempotent: a second "ok" finds the draft
// no longer pending and sends nothing. ok=false when the draft was not in from.
func (s *Store) MoveDraft(ctx context.Context, id int64, from, to, text, note string, sentID int64) (bool, error) {
	q := `UPDATE drafts SET status=?, note=?, sent_message_id=?`
	args := []any{to, note, sentID}
	if text != "" {
		q += `, text=?`
		args = append(args, text)
	}
	q += ` WHERE id=? AND status=?`
	res, err := s.db.ExecContext(ctx, q, append(args, id, from)...)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
