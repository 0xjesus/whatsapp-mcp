package store

import "context"

// ApplyMessageEdit records the latest explicit network edit, including edits
// received before the original message. The original timestamp and media stay
// unchanged. A revoke always wins over an edit or a later history delivery.
func (s *Store) ApplyMessageEdit(ctx context.Context, id, chatJID, content string, timestampMS int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO message_mutations(id,chat_jid,content,revoked,mutation_time_ms)
        VALUES(?,?,?,0,?) ON CONFLICT(id,chat_jid) DO UPDATE SET content=excluded.content,mutation_time_ms=excluded.mutation_time_ms
        WHERE message_mutations.revoked=0 AND excluded.mutation_time_ms>=message_mutations.mutation_time_ms`, id, chatJID, content, timestampMS); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE messages SET content=(SELECT content FROM message_mutations WHERE id=? AND chat_jid=?)
        WHERE id=? AND chat_jid=? AND EXISTS(SELECT 1 FROM message_mutations WHERE id=? AND chat_jid=? AND revoked=0)`,
		id, chatJID, id, chatJID, id, chatJID); err != nil {
		return err
	}
	return tx.Commit()
}

// ApplyMessageRevoke persists an explicit revoke before removing the cached
// message. Source-index DELETE triggers run inside the same transaction.
func (s *Store) ApplyMessageRevoke(ctx context.Context, id, chatJID string, timestampMS int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO message_mutations(id,chat_jid,content,revoked,mutation_time_ms)
        VALUES(?,?,NULL,1,?) ON CONFLICT(id,chat_jid) DO UPDATE SET content=NULL,revoked=1,
        mutation_time_ms=MAX(message_mutations.mutation_time_ms,excluded.mutation_time_ms)`, id, chatJID, timestampMS); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM messages WHERE id=? AND chat_jid=?", id, chatJID); err != nil {
		return err
	}
	return tx.Commit()
}
