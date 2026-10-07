package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/ratelimit"
)

// Daily quotas are local operator policy, not WhatsApp server limits.
const (
	dailyAttemptLimit          = 120
	dailyUnknownRecipientLimit = 20
	dailyRecipientAttemptLimit = 20
)

// PersistentRecipientSendBudget checks or reserves the shared send budget. The
// caller supplies a canonical non-device JID, resolving known LID mappings.
// Reserve before transport: refused and uncertain sends also consume attempts.
// Empty recipient denotes legacy identity-less activity, never a made-up JID.
func (s *Store) PersistentRecipientSendBudget(ctx context.Context, now time.Time, recipient string, known, reserve bool) (ratelimit.Decision, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ratelimit.Decision{}, err
	}
	defer tx.Rollback()
	// Acquire the SQLite write lock before reading counters. Concurrent callers
	// cannot both read an available slot and then reserve it. This no-op persists
	// no reservation and all failure/denial paths roll the transaction back.
	if _, err = tx.ExecContext(ctx, `UPDATE outbound_protection SET value=value WHERE key='initial_guard'`); err != nil {
		return ratelimit.Decision{}, err
	}
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
	hourCutoff := now.Add(-time.Hour).UnixNano()
	for _, cold := range []bool{false, true} {
		if cold && known {
			continue
		}
		query := `SELECT at_ns FROM outbound_attempts WHERE at_ns>?`
		limit, reason := 30, "hourly send cap reached"
		if cold {
			query += ` AND known=0`
			limit, reason = 15, "hourly non-contact send cap reached"
		}
		d, err := attemptWindowDecision(ctx, tx, now, time.Hour, query, []any{hourCutoff}, limit, reason)
		if err != nil || !d.Allowed {
			return d, err
		}
	}
	dayCutoff := now.Add(-24 * time.Hour).UnixNano()
	d, err := attemptWindowDecision(ctx, tx, now, 24*time.Hour, `SELECT at_ns FROM outbound_attempts WHERE at_ns>?`, []any{dayCutoff}, dailyAttemptLimit, "daily send cap reached")
	if err != nil || !d.Allowed {
		return d, err
	}
	if !known {
		// A known identity already in the unknown ledger needs no new distinct slot.
		// Each legacy unknown row consumes its own unidentified slot. A recipient's
		// slot expires only after its last unknown attempt leaves the daily window.
		var existing int
		if recipient != "" {
			if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbound_attempts WHERE at_ns>? AND known=0 AND recipient=?`, dayCutoff, recipient).Scan(&existing); err != nil {
				return ratelimit.Decision{}, err
			}
		}
		if existing == 0 {
			query := `SELECT MAX(at_ns) AS at_ns FROM outbound_attempts WHERE at_ns>? AND known=0 AND recipient<>'' GROUP BY recipient UNION ALL SELECT at_ns FROM outbound_attempts WHERE at_ns>? AND known=0 AND recipient=''`
			d, err = attemptWindowDecision(ctx, tx, now, 24*time.Hour, query, []any{dayCutoff, dayCutoff}, dailyUnknownRecipientLimit, "daily distinct non-contact cap reached")
			if err != nil || !d.Allowed {
				return d, err
			}
		}
	}
	if recipient != "" {
		d, err = attemptWindowDecision(ctx, tx, now, 24*time.Hour, `SELECT at_ns FROM outbound_attempts WHERE at_ns>? AND (recipient=? OR recipient='')`, []any{dayCutoff, recipient}, dailyRecipientAttemptLimit, "daily recipient send cap reached")
		if err != nil || !d.Allowed {
			return d, err
		}
	}
	if reserve {
		if _, err = tx.ExecContext(ctx, `INSERT INTO outbound_attempts(at_ns,known,recipient) VALUES(?,?,?)`, now.UnixNano(), known, recipient); err != nil {
			return ratelimit.Decision{}, err
		}
		// Compatibility mirror, never summed with the canonical ledger.
		if _, err = tx.ExecContext(ctx, `INSERT INTO send_budget(at,known) VALUES(?,?)`, now.UnixNano(), known); err != nil {
			return ratelimit.Decision{}, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM send_budget WHERE at<=?`, hourCutoff); err != nil {
			return ratelimit.Decision{}, err
		}
		// Keep 24 hours and the chronologically latest attempt for spacing even if
		// it falls outside that window. Row insertion order is not time order.
		if _, err = tx.ExecContext(ctx, `DELETE FROM outbound_attempts WHERE at_ns<=? AND rowid<>(SELECT rowid FROM outbound_attempts ORDER BY at_ns DESC,rowid DESC LIMIT 1)`, dayCutoff); err != nil {
			return ratelimit.Decision{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return ratelimit.Decision{}, err
	}
	return ratelimit.Decision{Allowed: true}, nil
}

// attemptWindowDecision finds when enough retained attempts expire to restore
// one slot, including historical ledgers already above the configured limit.
func attemptWindowDecision(ctx context.Context, tx *sql.Tx, now time.Time, window time.Duration, query string, args []any, limit int, reason string) (ratelimit.Decision, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM (`+query+`)`, args...).Scan(&count); err != nil {
		return ratelimit.Decision{}, err
	}
	if count < limit {
		return ratelimit.Decision{Allowed: true}, nil
	}
	var expires int64
	args = append(args, count-limit)
	if err := tx.QueryRowContext(ctx, `SELECT at_ns FROM (`+query+`) ORDER BY at_ns LIMIT 1 OFFSET ?`, args...).Scan(&expires); err != nil {
		return ratelimit.Decision{}, err
	}
	return ratelimit.Decision{RetryAfter: time.Unix(0, expires).Add(window).Sub(now), Reason: reason}, nil
}
