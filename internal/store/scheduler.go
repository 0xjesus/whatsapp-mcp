package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const schedulerSchema = `
CREATE TABLE IF NOT EXISTS scheduled_messages(
 id INTEGER PRIMARY KEY AUTOINCREMENT, chat_jid TEXT NOT NULL,text TEXT NOT NULL,
 send_at INTEGER NOT NULL,expires_at INTEGER NOT NULL,next_attempt INTEGER NOT NULL,
 idempotency_key TEXT UNIQUE,status TEXT NOT NULL DEFAULT 'pending',
 attempts INTEGER NOT NULL DEFAULT 0,message_id TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT '',
 created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,sender TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS scheduled_due ON scheduled_messages(status,next_attempt,id);
CREATE TABLE IF NOT EXISTS send_budget(at INTEGER NOT NULL,known INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS send_budget_time ON send_budget(at);`

type ScheduledMessage struct {
	Sender         string    `json:"sender,omitempty"`
	ID             string    `json:"job_id"`
	ChatJID        string    `json:"chat_jid"`
	Text           string    `json:"text"`
	SendAt         time.Time `json:"send_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	NextAttempt    time.Time `json:"next_attempt"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	MessageID      string    `json:"message_id,omitempty"`
	Error          string    `json:"error,omitempty"`
}

const scheduledColumns = `id,chat_jid,text,send_at,expires_at,next_attempt,COALESCE(idempotency_key,''),status,attempts,message_id,error,sender`

type scheduledScanner interface{ Scan(...interface{}) error }

func scanScheduled(row scheduledScanner) (*ScheduledMessage, error) {
	var j ScheduledMessage
	var id, send, expiry, next int64
	err := row.Scan(&id, &j.ChatJID, &j.Text, &send, &expiry, &next, &j.IdempotencyKey, &j.Status, &j.Attempts, &j.MessageID, &j.Error, &j.Sender)
	if err != nil {
		return nil, err
	}
	j.ID = strconv.FormatInt(id, 10)
	j.SendAt = time.Unix(0, send).UTC()
	j.ExpiresAt = time.Unix(0, expiry).UTC()
	j.NextAttempt = time.Unix(0, next).UTC()
	return &j, nil
}
func validateSchedule(text string, send, expiry time.Time) error {
	if text == "" || len(text) > 16384 {
		return errors.New("text must contain 1..16384 UTF-8 bytes")
	}
	if send.After(time.Now().Add(366 * 24 * time.Hour)) {
		return errors.New("send_at exceeds 366 days")
	}
	if !expiry.After(send) || expiry.After(send.Add(24*time.Hour)) {
		return errors.New("expires_at must be after send_at and within 24 hours")
	}
	return nil
}
func (s *Store) Schedule(chat, text string, send, expiry time.Time, key string, sender ...string) (*ScheduledMessage, error) {
	if err := validateSchedule(text, send, expiry); err != nil {
		return nil, err
	}
	if len(key) > 256 {
		return nil, errors.New("idempotency_key exceeds 256 bytes")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if key != "" {
		j, e := scanScheduled(tx.QueryRow(`SELECT `+scheduledColumns+` FROM scheduled_messages WHERE idempotency_key=?`, key))
		if e == nil {
			if j.ChatJID != chat || j.Text != text || j.SendAt.UnixNano() != send.UnixNano() || j.ExpiresAt.UnixNano() != expiry.UnixNano() || (len(sender) > 0 && j.Sender != sender[0]) {
				return nil, errors.New("idempotency_key already used with different parameters")
			}
			return j, nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
	}
	if !send.After(time.Now()) {
		return nil, errors.New("send_at must be in the future")
	}
	var count int
	if err = tx.QueryRow(`SELECT count(*) FROM scheduled_messages WHERE status IN ('pending','claimed','dispatching','uncertain')`).Scan(&count); err != nil {
		return nil, err
	}
	if count >= 1000 {
		return nil, errors.New("active queue limit reached (1000)")
	}
	var nullableKey interface{}
	if key != "" {
		nullableKey = key
	}
	own := ""
	if len(sender) > 0 {
		own = sender[0]
	}
	now := time.Now().UnixNano()
	r, err := tx.Exec(`INSERT INTO scheduled_messages(chat_jid,text,send_at,expires_at,next_attempt,idempotency_key,created_at,updated_at,sender) VALUES(?,?,?,?,?,?,?,?,?)`, chat, text, send.UnixNano(), expiry.UnixNano(), send.UnixNano(), nullableKey, now, now, own)
	if err != nil {
		return nil, err
	}
	id, _ := r.LastInsertId()
	j, err := scanScheduled(tx.QueryRow(`SELECT `+scheduledColumns+` FROM scheduled_messages WHERE id=?`, id))
	if err != nil {
		return nil, err
	}
	return j, tx.Commit()
}
func (s *Store) ListScheduled(limit int, cursor, status string) ([]ScheduledMessage, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("limit must be 1..100")
	}
	after := int64(0)
	var err error
	if cursor != "" {
		after, err = strconv.ParseInt(cursor, 10, 64)
		if err != nil || after < 0 {
			return nil, errors.New("invalid cursor")
		}
	}
	switch status {
	case "", "pending", "claimed", "dispatching", "sent", "cancelled", "expired", "failed", "uncertain":
	default:
		return nil, errors.New("invalid status")
	}
	query := `SELECT ` + scheduledColumns + ` FROM scheduled_messages WHERE id>?`
	args := []interface{}{after}
	if status != "" {
		query += ` AND status=?`
		args = append(args, status)
	}
	query += ` ORDER BY id LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []ScheduledMessage{}
	for rows.Next() {
		j, e := scanScheduled(rows)
		if e != nil {
			return nil, e
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}
func requireScheduledChange(r sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("job is missing or is no longer pending")
	}
	return nil
}
func (s *Store) CancelScheduled(id string) error {
	return requireScheduledChange(s.db.Exec(`UPDATE scheduled_messages SET status='cancelled',updated_at=? WHERE id=? AND status='pending'`, time.Now().UnixNano(), id))
}
func (s *Store) Reschedule(id string, send, expiry time.Time) error {
	if err := validateSchedule("valid", send, expiry); err != nil {
		return err
	}
	return requireScheduledChange(s.db.Exec(`UPDATE scheduled_messages SET send_at=?,expires_at=?,next_attempt=?,updated_at=? WHERE id=? AND status='pending'`, send.UnixNano(), expiry.UnixNano(), send.UnixNano(), time.Now().UnixNano(), id))
}
func (s *Store) RecoverScheduled() error {
	_, err := s.db.Exec(`UPDATE scheduled_messages SET status=CASE WHEN status='claimed' THEN 'pending' WHEN message_id<>'' AND EXISTS(SELECT 1 FROM messages WHERE id=scheduled_messages.message_id AND chat_jid=scheduled_messages.chat_jid AND is_from_me=1) THEN 'sent' ELSE 'uncertain' END,error=CASE WHEN status='dispatching' THEN 'restart during network dispatch; check stable ID before resend' ELSE error END WHERE status IN ('claimed','dispatching')`)
	return err
}

// PrepareScheduledID persists the network ID while the scheduler still owns the job.
func (s *Store) PrepareScheduledID(id, messageID string) error {
	return requireScheduledChange(s.db.Exec(`UPDATE scheduled_messages SET message_id=CASE WHEN message_id='' THEN ? ELSE message_id END WHERE id=? AND status='claimed'`, messageID, id))
}
func (s *Store) ClaimScheduled(now time.Time) (*ScheduledMessage, error) {
	if _, err := s.db.Exec(`DELETE FROM scheduled_messages WHERE id IN (SELECT id FROM scheduled_messages WHERE status IN ('sent','cancelled','expired','failed') AND updated_at<? ORDER BY updated_at LIMIT 100)`, now.Add(-7*24*time.Hour).UnixNano()); err != nil {
		return nil, err
	}
	if _, err := s.db.Exec(`UPDATE scheduled_messages SET status='expired',error='delivery deadline passed',updated_at=? WHERE status='pending' AND expires_at<=?`, now.UnixNano(), now.UnixNano()); err != nil {
		return nil, err
	}
	if _, err := s.db.Exec(`UPDATE scheduled_messages SET status='failed',error='delivery attempt limit reached; manual review required',updated_at=? WHERE status='pending' AND attempts>=5`, now.UnixNano()); err != nil {
		return nil, err
	}
	j, err := scanScheduled(s.db.QueryRow(`UPDATE scheduled_messages SET status='claimed',updated_at=? WHERE id=(SELECT id FROM scheduled_messages WHERE status='pending' AND next_attempt<=? AND send_at<=? AND expires_at>? AND attempts<5 ORDER BY next_attempt,id LIMIT 1) AND status='pending' RETURNING `+scheduledColumns, now.UnixNano(), now.UnixNano(), now.UnixNano(), now.UnixNano()))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return j, err
}
func (s *Store) MarkScheduledDispatch(id string) error {
	return requireScheduledChange(s.db.Exec(`UPDATE scheduled_messages SET status='dispatching',attempts=attempts+1,updated_at=? WHERE id=? AND status='claimed' AND expires_at>? AND attempts<5`, time.Now().UnixNano(), id, time.Now().UnixNano()))
}
func (s *Store) FinishScheduled(id, status, messageID, reason string, next time.Time) error {
	switch status {
	case "pending", "sent", "expired", "failed", "uncertain":
	default:
		return fmt.Errorf("invalid finish status %q", status)
	}
	return requireScheduledChange(s.db.Exec(`UPDATE scheduled_messages SET status=?,message_id=CASE WHEN ?='' THEN message_id ELSE ? END,error=?,next_attempt=?,updated_at=? WHERE id=? AND status IN ('claimed','dispatching')`, status, messageID, messageID, reason, next.UnixNano(), time.Now().UnixNano(), id))
}

// SendBudget loads the bounded trailing-hour ledger for restoring the rate guard.
func (s *Store) SendBudget() (all, unknown []time.Time, err error) {
	rows, err := s.db.Query(`SELECT at,known FROM send_budget WHERE at>? ORDER BY at`, time.Now().Add(-time.Hour).UnixNano())
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var at int64
		var known int
		if err = rows.Scan(&at, &known); err != nil {
			return nil, nil, err
		}
		t := time.Unix(0, at)
		all = append(all, t)
		if known == 0 {
			unknown = append(unknown, t)
		}
	}
	return all, unknown, rows.Err()
}
func (s *Store) RecordSendBudget(known bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now()
	if _, err = tx.Exec(`DELETE FROM send_budget WHERE at<=?`, now.Add(-time.Hour).UnixNano()); err != nil {
		return err
	}
	k := 0
	if known {
		k = 1
	}
	if _, err = tx.Exec(`INSERT INTO send_budget VALUES(?,?)`, now.UnixNano(), k); err != nil {
		return err
	}
	return tx.Commit()
}

// BindLegacyScheduledSender upgrades pre-outbox authorizations once at startup.
func (s *Store) BindLegacyScheduledSender(sender string) error {
	if sender == "" {
		return nil
	}
	_, err := s.db.Exec(`UPDATE scheduled_messages SET sender=? WHERE sender='' AND status IN ('pending','claimed')`, sender)
	return err
}
