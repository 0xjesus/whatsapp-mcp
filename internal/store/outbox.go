package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/ratelimit"
)

// OutboxJob is an authorized outbound message. Payload and media bytes are
// private immutable snapshots, deliberately excluded from status responses.
type OutboxJob struct {
	JobID          string    `json:"job_id"`
	MessageID      string    `json:"message_id"`
	Recipient      string    `json:"recipient"`
	State          string    `json:"state"`
	EnqueuedAt     time.Time `json:"enqueued_at"`
	NextAttempt    time.Time `json:"next_attempt"`
	Attempts       int       `json:"attempts"`
	Reason         string    `json:"reason,omitempty"`
	SentID         string    `json:"sent_id,omitempty"`
	Sender         string    `json:"sender_jid"`
	IdempotencyKey string    `json:"-"`
	RequestHash    string    `json:"-"`
	Payload        string    `json:"-"`
	Media          []byte    `json:"-"`
}

const outboxColumns = `job_id,message_id,recipient,state,enqueued_ns,next_ns,attempts,reason,sent_id,sender,idempotency_key,request_hash`

func scanOutbox(row interface{ Scan(...any) error }) (j OutboxJob, err error) {
	var enqueued, next int64
	err = row.Scan(&j.JobID, &j.MessageID, &j.Recipient, &j.State, &enqueued, &next, &j.Attempts, &j.Reason, &j.SentID, &j.Sender, &j.IdempotencyKey, &j.RequestHash)
	j.EnqueuedAt, j.NextAttempt = time.Unix(0, enqueued), time.Unix(0, next)
	return
}

func (s *Store) EnqueueOutbox(ctx context.Context, j OutboxJob) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO outbound_jobs (`+outboxColumns+`,payload,media) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, j.JobID, j.MessageID, j.Recipient, "queued", j.EnqueuedAt.UnixNano(), j.NextAttempt.UnixNano(), 0, "", "", j.Sender, j.IdempotencyKey, j.RequestHash, j.Payload, j.Media)
	return err
}

// EnqueueOutboxOnce atomically deduplicates identical authorized submissions.
func (s *Store) EnqueueOutboxOnce(ctx context.Context, j OutboxJob) (OutboxJob, error) {
	err := s.EnqueueOutbox(ctx, j)
	if err == nil {
		if s.afterOutboxInsert != nil {
			s.afterOutboxInsert()
		}
		// INSERT is committed. Optional reads must not turn acceptance into rejection.
		j.State, j.Attempts, j.Reason, j.SentID = "queued", 0, "", ""
		j.Payload, j.Media = "", nil
		j.EnqueuedAt = time.Unix(0, j.EnqueuedAt.UnixNano())
		j.NextAttempt = time.Unix(0, j.NextAttempt.UnixNano())
		return j, nil
	}
	if j.IdempotencyKey == "" {
		return OutboxJob{}, err
	}
	old, e := scanOutbox(s.db.QueryRowContext(ctx, `SELECT `+outboxColumns+` FROM outbound_jobs WHERE idempotency_key=?`, j.IdempotencyKey))
	if e != nil {
		return OutboxJob{}, err
	}
	if old.RequestHash != j.RequestHash || old.Sender != j.Sender {
		return OutboxJob{}, errors.New("idempotency_key already belongs to a different request or sender")
	}
	return old, nil
}
func (s *Store) LoadOutboxPayload(ctx context.Context, id string) (string, []byte, error) {
	var payload string
	var media []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload,media FROM outbound_jobs WHERE job_id=?`, id).Scan(&payload, &media)
	return payload, media, err
}
func (s *Store) OutboxJob(ctx context.Context, id string) (OutboxJob, error) {
	return scanOutbox(s.db.QueryRowContext(ctx, `SELECT `+outboxColumns+` FROM outbound_jobs WHERE job_id=?`, id))
}
func (s *Store) NextOutbox(ctx context.Context) (OutboxJob, error) {
	// FIFO: an older cooldown job owns its place; a later job cannot overtake it.
	return scanOutbox(s.db.QueryRowContext(ctx, `SELECT `+outboxColumns+` FROM outbound_jobs WHERE state='queued' ORDER BY rowid LIMIT 1`))
}
func (s *Store) ListOutbox(ctx context.Context, id string, limit int) ([]OutboxJob, error) {
	if id != "" {
		j, err := s.OutboxJob(ctx, id)
		if err != nil {
			return nil, err
		}
		return []OutboxJob{j}, nil
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+outboxColumns+` FROM outbound_jobs ORDER BY rowid DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []OutboxJob{}
	for rows.Next() {
		j, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
func (s *Store) OutboxCounts(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT state,COUNT(*) FROM outbound_jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		counts[state] = n
	}
	return counts, rows.Err()
}
func (s *Store) UpdateOutbox(ctx context.Context, id, state, reason, sentID string, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbound_jobs SET state=?,reason=?,sent_id=?,next_ns=? WHERE job_id=? AND state IN ('queued','sending')`, state, reason, sentID, next.UnixNano(), id)
	return err
}
func (s *Store) ClaimOutbox(ctx context.Context, id string) (bool, error) {
	r, err := s.db.ExecContext(ctx, `UPDATE outbound_jobs SET state='sending',reason='' WHERE job_id=? AND state='queued'`, id)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}
func (s *Store) RecordOutboxAttempt(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbound_jobs SET attempts=attempts+1 WHERE job_id=? AND state='sending'`, id)
	return err
}
func (s *Store) CancelOutbox(ctx context.Context, id string) error {
	r, err := s.db.ExecContext(ctx, `UPDATE outbound_jobs SET state='cancelled',reason='cancelled by caller',media=NULL WHERE job_id=? AND state IN ('queued','blocked')`, id)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("job is absent, uncertain, already terminal, or sending; cancellation refused")
	}
	return nil
}
func (s *Store) RecoverOutbox(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT `+outboxColumns+` FROM outbound_jobs WHERE state='sending'`)
	if err != nil {
		return err
	}
	var jobs []OutboxJob
	for rows.Next() {
		j, e := scanOutbox(rows)
		if e != nil {
			rows.Close()
			return e
		}
		jobs = append(jobs, j)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, j := range jobs {
		confirmed, e := s.confirmedOutgoing(ctx, j.MessageID, j.Recipient)
		if e != nil {
			return e
		}
		state, sentID, reason := "needs_review", "", "daemon interrupted delivery; check stable message ID before any new send"
		if confirmed {
			state, sentID, reason = "sent", j.MessageID, "confirmed by outgoing cache after restart"
		}
		if _, err = s.db.ExecContext(ctx, `UPDATE outbound_jobs SET state=?,sent_id=?,reason=? WHERE job_id=? AND state='sending'`, state, sentID, reason, j.JobID); err != nil {
			return err
		}
	}
	return nil
}

// confirmedOutgoing matches only this stable ID, an outgoing message, and the
// submitted recipient or its recorded LID→phone mapping. Never ID alone.
func (s *Store) confirmedOutgoing(ctx context.Context, id, recipient string) (bool, error) {
	if id == "" {
		return false, nil
	}
	canonical := s.ResolveLIDToJID(recipient)
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE id=? AND is_from_me=1 AND chat_jid IN (?,?)`, id, recipient, canonical).Scan(&n)
	return n > 0, err
}

// EnsureSendProtection migrates the legacy ledger once, preserving hourly caps.
// Its old reservation precedes typing, so allow a one-time 90-second cushion.
// Only a missing legacy ledger requires the full-hour fallback.
func (s *Store) EnsureSendProtection(ctx context.Context, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT value FROM outbound_protection WHERE key='initial_guard'`).Scan(&existing)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var available int
	if err = tx.QueryRowContext(ctx, `SELECT value FROM outbound_protection WHERE key='legacy_ledger_available'`).Scan(&available); err != nil {
		return err
	}
	until := now.Add(time.Hour)
	if available == 1 {
		if _, err = tx.ExecContext(ctx, `INSERT INTO outbound_attempts(at_ns,known) SELECT at,known FROM send_budget WHERE at>?`, now.Add(-time.Hour).UnixNano()); err != nil {
			return err
		}
		until = now.Add(90 * time.Second)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbound_protection(key,value) VALUES('initial_guard',?)`, until.UnixNano()); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) ProtectionValue(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM outbound_protection WHERE key=?`, key).Scan(&value)
	return value, err
}
func (s *Store) SetProtectionValue(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO outbound_protection (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

// SendBudget checks or reserves the persistent shared budget. The process holds
// its send mutex around reservations; the daemon's store lock excludes another
// process. An attempted network send consumes budget even on uncertain failure.
func (s *Store) PersistentSendBudget(ctx context.Context, now time.Time, known, reserve bool) (ratelimit.Decision, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ratelimit.Decision{}, err
	}
	defer tx.Rollback()
	var guard int64
	err = tx.QueryRowContext(ctx, `SELECT value FROM outbound_protection WHERE key='initial_guard'`).Scan(&guard)
	if errors.Is(err, sql.ErrNoRows) {
		return ratelimit.Decision{}, errors.New("persistent send protection not initialized")
	}
	if err != nil {
		return ratelimit.Decision{}, err
	}
	if until := time.Unix(0, guard); now.Before(until) {
		return ratelimit.Decision{RetryAfter: until.Sub(now), Reason: "initial persistent-protection migration cooldown"}, nil
	}
	var last sql.NullInt64
	if err = tx.QueryRowContext(ctx, `SELECT MAX(at_ns) FROM outbound_attempts`).Scan(&last); err != nil {
		return ratelimit.Decision{}, err
	}
	interval := 45 * time.Second
	if !known {
		interval = 90 * time.Second
	}
	if last.Valid && now.Before(time.Unix(0, last.Int64).Add(interval)) {
		return ratelimit.Decision{RetryAfter: time.Unix(0, last.Int64).Add(interval).Sub(now), Reason: "minimum send interval not elapsed"}, nil
	}
	cutoff := now.Add(-time.Hour).UnixNano()
	for _, cold := range []bool{false, true} {
		if cold && known {
			continue
		}
		query := `SELECT COUNT(*),MIN(at_ns) FROM outbound_attempts WHERE at_ns>?`
		args := []any{cutoff}
		cap := 30
		if cold {
			query += ` AND known=0`
			cap = 15
		}
		var n int
		var oldest sql.NullInt64
		if err = tx.QueryRowContext(ctx, query, args...).Scan(&n, &oldest); err != nil {
			return ratelimit.Decision{}, err
		}
		if n >= cap {
			reason := "hourly send cap reached"
			if cold {
				reason = "hourly non-contact send cap reached"
			}
			return ratelimit.Decision{RetryAfter: time.Unix(0, oldest.Int64).Add(time.Hour).Sub(now), Reason: reason}, nil
		}
	}
	if reserve {
		if _, err = tx.ExecContext(ctx, `INSERT INTO outbound_attempts (at_ns,known) VALUES (?,?)`, now.UnixNano(), known); err != nil {
			return ratelimit.Decision{}, err
		}
		// Keep legacy restoration/rollback history current in this same transaction.
		// Both ledgers describe the same reservation; caps never sum the ledgers.
		if _, err = tx.ExecContext(ctx, `INSERT INTO send_budget(at,known) VALUES(?,?)`, now.UnixNano(), known); err != nil {
			return ratelimit.Decision{}, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM send_budget WHERE at<=?`, cutoff); err != nil {
			return ratelimit.Decision{}, err
		}
		// Retain the most recent attempt for interval protection, even when old.
		if _, err = tx.ExecContext(ctx, `DELETE FROM outbound_attempts WHERE at_ns<? AND rowid<>(SELECT MAX(rowid) FROM outbound_attempts)`, cutoff); err != nil {
			return ratelimit.Decision{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return ratelimit.Decision{}, err
	}
	return ratelimit.Decision{Allowed: true}, nil
}
