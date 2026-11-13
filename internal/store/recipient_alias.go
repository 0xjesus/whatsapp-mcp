package store

import (
	"context"
	"database/sql"
	"errors"
)

// RecipientAlias contains only mappings verified by a live WhatsApp phone query.
func (s *Store) RecipientAlias(ctx context.Context, recipient string) (string, error) {
	var canonical string
	err := s.db.QueryRowContext(ctx, `SELECT canonical FROM recipient_aliases WHERE recipient=?`, recipient).Scan(&canonical)
	if errors.Is(err, sql.ErrNoRows) {
		return recipient, nil
	}
	return canonical, err
}

// RecordRecipientAlias preserves all prior attempts when WhatsApp returns a
// canonical phone different from the submitted public phone number.
func (s *Store) RecordRecipientAlias(ctx context.Context, recipient, canonical string) error {
	if recipient == canonical {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO recipient_aliases(recipient,canonical) VALUES(?,?) ON CONFLICT(recipient) DO UPDATE SET canonical=excluded.canonical`, recipient, canonical); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE outbound_attempts SET recipient=? WHERE recipient=?`, canonical, recipient); err != nil {
		return err
	}
	return tx.Commit()
}
