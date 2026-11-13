package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const historySchema = `
CREATE TABLE IF NOT EXISTS history_pacing(singleton INTEGER PRIMARY KEY CHECK(singleton=1),next_ns INTEGER NOT NULL DEFAULT 0);
INSERT OR IGNORE INTO history_pacing(singleton) VALUES(1);
CREATE TABLE IF NOT EXISTS history_requests(id INTEGER PRIMARY KEY AUTOINCREMENT,chat_jid TEXT NOT NULL,anchor_id TEXT NOT NULL,anchor_time TIMESTAMP NOT NULL,anchor_from_me INTEGER NOT NULL,requested_ns INTEGER NOT NULL,deadline_ns INTEGER NOT NULL,state TEXT NOT NULL,peer_id TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS history_requests_active ON history_requests(state,deadline_ns);
CREATE TABLE IF NOT EXISTS history_observations(id INTEGER PRIMARY KEY AUTOINCREMENT,chat_jid TEXT NOT NULL,sync_type TEXT NOT NULL,digest TEXT NOT NULL,end_transfer INTEGER,terminal_type INTEGER,chunk_order INTEGER,progress INTEGER,observed_ns INTEGER NOT NULL,state TEXT NOT NULL,stored INTEGER NOT NULL DEFAULT 0,failed INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS history_observations_chat ON history_observations(chat_jid,id);
CREATE TABLE IF NOT EXISTS history_crawls(chat_jid TEXT PRIMARY KEY,enabled INTEGER NOT NULL,next_ns INTEGER NOT NULL,state TEXT NOT NULL,cursor_id TEXT NOT NULL DEFAULT '',cursor_time TIMESTAMP,updated_ns INTEGER NOT NULL);
`

// GetHistoryAnchor selects an existing non-synthetic message key. Timestamp
// bounds restrict eligible rows; they never become fabricated protocol keys.
// Existing cache provenance is limited, so reaction mirrors and old SYNC IDs
// are excluded explicitly; empty/no-real-message chats need a bootstrap sync.
func (s *Store) GetHistoryAnchor(chat string, before time.Time) (Message, error) {
	var m Message
	q := `SELECT id,chat_jid,timestamp,is_from_me FROM messages WHERE chat_jid=? AND id<>'' AND id NOT LIKE 'SYNC_%' AND COALESCE(media_type,'')<>'reaction'`
	args := []any{chat}
	// With a bound, the page starts at the newest key at or before it, so the
	// sync covers the messages right before that moment; without one, the
	// oldest key extends the cache backwards.
	order := ` ORDER BY timestamp ASC,id ASC LIMIT 1`
	if !before.IsZero() {
		q += ` AND timestamp<=?`
		args = append(args, before)
		order = ` ORDER BY timestamp DESC,id DESC LIMIT 1`
	}
	q += order
	err := s.db.QueryRow(q, args...).Scan(&m.ID, &m.ChatJID, &m.Timestamp, &m.IsFromMe)
	if err != nil {
		return m, fmt.Errorf("no eligible real history anchor: %w", err)
	}
	return m, nil
}

// ReserveHistoryRequest claims the single global slot and persistent 60s pace
// before network I/O. A crashed reservation remains outstanding for 10min.
func (s *Store) ReserveHistoryRequest(ctx context.Context, a Message, now time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Writing the singleton first serializes competing reserve transactions.
	r, err := tx.ExecContext(ctx, `UPDATE history_pacing SET next_ns=? WHERE singleton=1 AND next_ns<=?`, now.Add(time.Minute).UnixNano(), now.UnixNano())
	if err != nil {
		return 0, err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return 0, fmt.Errorf("history requests require at least 60 seconds between attempts")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE history_requests SET state='waiting_phone' WHERE state IN ('reserved','sent','ambiguous') AND deadline_ns<=?`, now.UnixNano()); err != nil {
		return 0, err
	}
	var active int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM history_requests WHERE state IN ('reserved','sent','ambiguous')`).Scan(&active); err != nil {
		return 0, err
	}
	if active != 0 {
		return 0, fmt.Errorf("another history request is still outstanding")
	}
	if a.ID == "" || a.Timestamp.IsZero() {
		return 0, fmt.Errorf("real history anchor required")
	}
	r, err = tx.ExecContext(ctx, `INSERT INTO history_requests(chat_jid,anchor_id,anchor_time,anchor_from_me,requested_ns,deadline_ns,state) VALUES(?,?,?,?,?,?,'reserved')`, a.ChatJID, a.ID, a.Timestamp, a.IsFromMe, now.UnixNano(), now.Add(10*time.Minute).UnixNano())
	if err != nil {
		return 0, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, tx.Commit()
}
func (s *Store) FinishHistorySend(id int64, peer string, success bool, now time.Time) error {
	state := "ambiguous"
	if success {
		state = "sent"
	}
	_, err := s.db.Exec(`UPDATE history_requests SET state=?,peer_id=? WHERE id=? AND state='reserved'`, state, peer, id)
	return err
}

type HistoryObservation struct {
	ChatJID      string
	SyncType     string
	Digest       string
	End          *bool
	TerminalType *int
	ChunkOrder   *uint32
	Progress     *uint32
}

func (s *Store) BeginHistoryObservation(o HistoryObservation, now time.Time) (int64, error) {
	r, err := s.db.Exec(`INSERT INTO history_observations(chat_jid,sync_type,digest,end_transfer,terminal_type,chunk_order,progress,observed_ns,state) VALUES(?,?,?,?,?,?,?,?,'ingesting')`, o.ChatJID, o.SyncType, o.Digest, o.End, o.TerminalType, o.ChunkOrder, o.Progress, now.UnixNano())
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

// FinishHistoryObservation deliberately never declares upstream exhaustion:
// history events lack request correlation and this implementation has no raw
// payload replay. Terminal flags are evidence, not a completeness assertion.
func (s *Store) FinishHistoryObservation(id int64, stored, failed int, now time.Time) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var chat, kind string
	var terminal sql.NullInt64
	if err = tx.QueryRow(`SELECT chat_jid,sync_type,terminal_type FROM history_observations WHERE id=?`, id).Scan(&chat, &kind, &terminal); err != nil {
		return err
	}
	state := "observed"
	if failed > 0 {
		state = "incomplete"
	}
	if _, err = tx.Exec(`UPDATE history_observations SET state=?,stored=?,failed=? WHERE id=?`, state, stored, failed, id); err != nil {
		return err
	}
	requestState := "observed_incomplete"
	if failed > 0 {
		requestState = "incomplete"
	}
	if kind == "ON_DEMAND" {
		if _, err = tx.Exec(`UPDATE history_requests SET state=? WHERE chat_jid=? AND state IN ('reserved','sent','ambiguous')`, requestState, chat); err != nil {
			return err
		}
		// A committed ON_DEMAND page can advance an opted-in crawl only when a
		// strictly earlier timestamp exists. Ties/missing flags/failures stop.
		var previous time.Time
		var reqID int64
		err = tx.QueryRow(`SELECT id,anchor_time FROM history_requests WHERE chat_jid=? AND state=? ORDER BY id DESC LIMIT 1`, chat, requestState).Scan(&reqID, &previous)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil {
			var oldestID string
			var oldest time.Time
			anchorErr := tx.QueryRow(`SELECT id,timestamp FROM messages WHERE chat_jid=? AND id<>'' AND id NOT LIKE 'SYNC_%' AND COALESCE(media_type,'')<>'reaction' ORDER BY timestamp,id LIMIT 1`, chat).Scan(&oldestID, &oldest)
			crawlState := "incomplete_no_progress"
			enabled := false
			if failed == 0 && anchorErr == nil && oldest.Before(previous) && terminal.Valid && (terminal.Int64 == 0 || terminal.Int64 == 2) {
				crawlState = "queued"
				enabled = true
			}
			if terminal.Valid && terminal.Int64 == 1 {
				crawlState = "incomplete_terminal_observed"
			}
			if terminal.Valid && terminal.Int64 == 3 {
				crawlState = "unavailable_history"
			}
			if failed > 0 {
				crawlState = "incomplete_ingestion"
			}
			if _, err = tx.Exec(`UPDATE history_crawls SET enabled=?,state=?,next_ns=?,cursor_id=?,cursor_time=?,updated_ns=? WHERE chat_jid=? AND enabled=1`, enabled, crawlState, now.Add(time.Minute).UnixNano(), oldestID, oldest, now.UnixNano(), chat); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *Store) SetHistoryCrawl(chat string, enabled bool, now time.Time) error {
	state := "paused"
	if enabled {
		state = "queued"
	}
	_, err := s.db.Exec(`INSERT INTO history_crawls(chat_jid,enabled,next_ns,state,updated_ns) VALUES(?,?,?,?,?) ON CONFLICT(chat_jid) DO UPDATE SET enabled=excluded.enabled,next_ns=excluded.next_ns,state=excluded.state,updated_ns=excluded.updated_ns`, chat, enabled, now.UnixNano(), state, now.UnixNano())
	return err
}
func (s *Store) NextHistoryCrawl(now time.Time) (string, error) {
	// Timeout does not automatically retry: primary availability is unknown and
	// delayed replies cannot be confidently correlated with a replacement job.
	if _, err := s.db.Exec(`UPDATE history_crawls SET enabled=0,state='waiting_phone',updated_ns=? WHERE chat_jid IN (SELECT chat_jid FROM history_requests WHERE state IN ('reserved','sent','ambiguous') AND deadline_ns<=?)`, now.UnixNano(), now.UnixNano()); err != nil {
		return "", err
	}
	if _, err := s.db.Exec(`UPDATE history_requests SET state='waiting_phone' WHERE state IN ('reserved','sent','ambiguous') AND deadline_ns<=?`, now.UnixNano()); err != nil {
		return "", err
	}
	var chat string
	err := s.db.QueryRow(`SELECT chat_jid FROM history_crawls WHERE enabled=1 AND next_ns<=? AND NOT EXISTS(SELECT 1 FROM history_requests WHERE state IN ('reserved','sent','ambiguous')) AND NOT EXISTS(SELECT 1 FROM history_pacing WHERE next_ns>?) ORDER BY updated_ns,chat_jid LIMIT 1`, now.UnixNano(), now.UnixNano()).Scan(&chat)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return chat, err
}
func (s *Store) HoldHistoryCrawl(chat, state string, now time.Time) error {
	_, err := s.db.Exec(`UPDATE history_crawls SET enabled=0,state=?,updated_ns=? WHERE chat_jid=?`, state, now.UnixNano(), chat)
	return err
}
func (s *Store) HistoryStatus(chat string) (map[string]any, error) {
	out := map[string]any{"upstream_complete": false, "inventory_complete": false, "coverage": "cached chats only; terminal evidence is uncorrelated and ingestion has no payload replay"}
	for _, spec := range []struct{ name, q string }{
		{"requests", `SELECT id,chat_jid,anchor_id,anchor_time,anchor_from_me,requested_ns,deadline_ns,state FROM history_requests`},
		{"observations", `SELECT id,chat_jid,sync_type,end_transfer,terminal_type,chunk_order,progress,observed_ns,state,stored,failed FROM history_observations`},
		{"crawls", `SELECT chat_jid,enabled,next_ns,state,cursor_id,cursor_time,updated_ns FROM history_crawls`},
	} {
		q := spec.q
		var args []any
		if chat != "" {
			q += ` WHERE chat_jid=?`
			args = append(args, chat)
		}
		q += ` ORDER BY rowid DESC LIMIT 100`
		rows, err := s.db.Query(q, args...)
		if err != nil {
			return nil, err
		}
		cols, _ := rows.Columns()
		items := []map[string]any{}
		for rows.Next() {
			v := make([]any, len(cols))
			dest := make([]any, len(cols))
			for i := range v {
				dest[i] = &v[i]
			}
			if err = rows.Scan(dest...); err != nil {
				rows.Close()
				return nil, err
			}
			item := map[string]any{}
			for i, c := range cols {
				if b, ok := v[i].([]byte); ok {
					v[i] = string(b)
				}
				item[c] = v[i]
			}
			items = append(items, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		out[spec.name] = items
	}
	return out, nil
}

func (s *Store) CancelHistoryRequest(id int64) error {
	_, err := s.db.Exec(`UPDATE history_requests SET state='cancelled' WHERE id=? AND state='reserved'`, id)
	return err
}
