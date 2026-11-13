package store

import (
	"context"
	"database/sql"
)

// message_originals keeps the protobuf each stored message was parsed from, so
// a parser fix can re-read rows that were stored with lost text or media
// instead of depending on the phone to send them again.
const originalsSchema = `
CREATE TABLE IF NOT EXISTS message_originals (
 id TEXT NOT NULL, chat_jid TEXT NOT NULL, proto BLOB NOT NULL, parser_version INTEGER NOT NULL,
 PRIMARY KEY(id, chat_jid)
);
`

// KeepOriginal records the original only when its message row exists, so the
// group-monitoring guard on messages also governs what is kept here.
func (s *Store) KeepOriginal(ctx context.Context, id, chatJID string, wire []byte, parserVersion int) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO message_originals(id,chat_jid,proto,parser_version)
 SELECT ?,?,?,? WHERE EXISTS(SELECT 1 FROM messages WHERE id=? AND chat_jid=?)
 ON CONFLICT(id,chat_jid) DO UPDATE SET proto=excluded.proto, parser_version=excluded.parser_version`,
		id, chatJID, wire, parserVersion, id, chatJID)
	return err
}

// StaleOriginal is a kept original parsed by an older parser.
type StaleOriginal struct {
	ID, ChatJID, Content, MediaType string
	Proto                           []byte
}

// StaleOriginals lists up to limit originals parsed by a parser older than version.
func (s *Store) StaleOriginals(ctx context.Context, version, limit int) ([]StaleOriginal, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT o.id,o.chat_jid,o.proto,COALESCE(m.content,''),COALESCE(m.media_type,'')
 FROM message_originals o JOIN messages m ON m.id=o.id AND m.chat_jid=o.chat_jid
 WHERE o.parser_version<? LIMIT ?`, version, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StaleOriginal
	for rows.Next() {
		var r StaleOriginal
		if err := rows.Scan(&r.ID, &r.ChatJID, &r.Proto, &r.Content, &r.MediaType); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RepairFromOriginal fills what an older parser lost: content only when the row
// had none (or a placeholder), media fields only when the row had no media.
// A transcript or edit already on the row is never replaced.
func (s *Store) RepairFromOriginal(ctx context.Context, r StaleOriginal, content string, media Message, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64, version int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if content != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE messages SET content=? WHERE id=? AND chat_jid=?
 AND (COALESCE(content,'')='' OR content LIKE '[sin soporte: %')
 AND NOT EXISTS(SELECT 1 FROM message_mutations WHERE id=? AND chat_jid=?)`,
			content, r.ID, r.ChatJID, r.ID, r.ChatJID); err != nil {
			return err
		}
	}
	if media.MediaType != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE messages SET media_type=?, filename=?, url=?, direct_path=?, media_key=?, file_sha256=?, file_enc_sha256=?, file_length=?
 WHERE id=? AND chat_jid=? AND COALESCE(media_type,'') IN ('','unsupported')`,
			media.MediaType, media.Filename, media.URL, media.DirectPath, mediaKey, fileSHA256, fileEncSHA256, fileLength, r.ID, r.ChatJID); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE message_originals SET parser_version=? WHERE id=? AND chat_jid=?`, version, r.ID, r.ChatJID); err != nil {
		return err
	}
	return tx.Commit()
}

// MessageSender returns the stored sender user of a message, or sql.ErrNoRows.
func (s *Store) MessageSender(chatJID, id string) (string, error) {
	var sender sql.NullString
	err := s.db.QueryRow(`SELECT sender FROM messages WHERE id=? AND chat_jid=?`, id, chatJID).Scan(&sender)
	return sender.String, err
}
