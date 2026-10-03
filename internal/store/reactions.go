package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const emojiMaxBytes = 16
const excerptRunes = 60

// Reaction is one emoji from one reactor on one message. The latest wins per reactor.
type Reaction struct {
	MessageID  string
	ChatJID    string
	Reactor    string // normalized bare phone/user, like Message.Sender
	Emoji      string
	Timestamp  time.Time
	ReactionID string // ID of the live ReactionMessage; "r:<msg>:<reactor>" when it came from history sync
}

// NormalizeEmoji validates UTF-8 and caps the reaction text at emojiMaxBytes without splitting a rune.
func NormalizeEmoji(e string) (string, error) {
	if !utf8.ValidString(e) {
		return "", errors.New("reaction emoji is not valid UTF-8")
	}
	// Reaction text is untrusted: control characters and whitespace would break the one-line
	// mirror row and the ↳ line (spec §9), so they never survive normalization.
	e = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return -1
		}
		return r
	}, e)
	for len(e) > emojiMaxBytes {
		_, size := utf8.DecodeLastRuneInString(e)
		e = e[:len(e)-size]
	}
	if e == "" {
		return "", errors.New("empty reaction emoji")
	}
	return e, nil
}

// UpsertReaction stores a reaction without a mirror. Stale events are ignored.
// The returned ID names a superseded mirror, if any.
func (s *Store) UpsertReaction(ctx context.Context, r Reaction) (string, error) {
	if r.Emoji == "" {
		return "", errors.New("empty reaction emoji")
	}
	return s.applyReaction(ctx, r, nil)
}

// ApplyReaction atomically applies an ordered reaction event and its mirror.
// Empty emoji is a timestamped tombstone, hidden from ReactionsFor but retained
// so delayed history cannot resurrect a removed reaction.
func (s *Store) ApplyReaction(ctx context.Context, r Reaction, mirror *Message) error {
	_, err := s.applyReaction(ctx, r, mirror)
	return err
}

func (s *Store) applyReaction(ctx context.Context, r Reaction, mirror *Message) (string, error) {
	var err error
	if r.Emoji != "" {
		r.Emoji, err = NormalizeEmoji(r.Emoji)
		if err != nil {
			return "", err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	// Acquire the SQLite write lock before reading the previous version. A
	// deferred read-then-write transaction can fail with SQLITE_BUSY_SNAPSHOT
	// when history and live events arrive concurrently, losing the newer event.
	if _, err = tx.ExecContext(ctx, `UPDATE reactions SET timestamp=timestamp WHERE message_id=? AND chat_jid=? AND reactor=?`, r.MessageID, r.ChatJID, r.Reactor); err != nil {
		return "", err
	}
	var previous Reaction
	err = tx.QueryRowContext(ctx, `SELECT emoji,timestamp,COALESCE(reaction_id,'') FROM reactions WHERE message_id=? AND chat_jid=? AND reactor=?`,
		r.MessageID, r.ChatJID, r.Reactor).Scan(&previous.Emoji, &previous.Timestamp, &previous.ReactionID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if err == nil && !reactionSupersedes(r, previous) {
		return "", nil
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO reactions(message_id,chat_jid,reactor,emoji,timestamp,reaction_id)
		VALUES(?,?,?,?,?,?) ON CONFLICT(message_id,chat_jid,reactor) DO UPDATE SET
		emoji=excluded.emoji, timestamp=excluded.timestamp, reaction_id=excluded.reaction_id`,
		r.MessageID, r.ChatJID, r.Reactor, r.Emoji, r.Timestamp, nullIfEmpty(r.ReactionID)); err != nil {
		return "", err
	}
	if previous.ReactionID != "" && (previous.ReactionID != r.ReactionID || r.Emoji == "") {
		if _, err = tx.ExecContext(ctx, `DELETE FROM messages WHERE id=? AND chat_jid=? AND media_type='reaction'`, previous.ReactionID, r.ChatJID); err != nil {
			return "", err
		}
	}
	if r.Emoji != "" && mirror != nil {
		if _, err = tx.ExecContext(ctx, `INSERT INTO messages(id,chat_jid,sender,content,timestamp,is_from_me,media_type)
			VALUES(?,?,?,?,?,?,'reaction') ON CONFLICT(id,chat_jid) DO UPDATE SET
			sender=excluded.sender,content=excluded.content,timestamp=excluded.timestamp,is_from_me=excluded.is_from_me
			WHERE messages.media_type='reaction'`, r.ReactionID, r.ChatJID, r.Reactor, mirror.Content, r.Timestamp, mirror.IsFromMe); err != nil {
			return "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	if previous.Emoji == "" || previous.ReactionID == r.ReactionID {
		return "", nil
	}
	return previous.ReactionID, nil
}

// Equal timestamps have no causal ordering. Resolve ties deterministically:
// removal wins, live IDs outrank synthesized history IDs, then ID and emoji
// provide a stable order independent of delivery order. Exact replays are no-ops.
func reactionSupersedes(next, previous Reaction) bool {
	if !next.Timestamp.Equal(previous.Timestamp) {
		return next.Timestamp.After(previous.Timestamp)
	}
	if (next.Emoji == "") != (previous.Emoji == "") {
		return next.Emoji == ""
	}
	nextLive := next.ReactionID != "" && !strings.HasPrefix(next.ReactionID, "r:")
	previousLive := previous.ReactionID != "" && !strings.HasPrefix(previous.ReactionID, "r:")
	if nextLive != previousLive {
		return nextLive
	}
	if next.ReactionID != previous.ReactionID {
		return next.ReactionID > previous.ReactionID
	}
	return next.Emoji > previous.Emoji
}

// DeleteReaction removes a reaction at the current time, retaining a tombstone.
// Ingested events must use ApplyReaction with the original event timestamp.
func (s *Store) DeleteReaction(ctx context.Context, messageID, chatJID, reactor string) (string, error) {
	return s.applyReaction(ctx, Reaction{MessageID: messageID, ChatJID: chatJID, Reactor: reactor, Timestamp: time.Now()}, nil)
}

// DeleteReactionMirror removes a superseded mirror row (an older emoji by the same reactor).
func (s *Store) DeleteReactionMirror(ctx context.Context, reactionID, chatJID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM messages WHERE id=? AND chat_jid=? AND media_type='reaction'`, reactionID, chatJID)
	return err
}

// ReactionsFor loads the reactions of several messages of one chat in a single query, oldest first.
func (s *Store) ReactionsFor(ctx context.Context, chatJID string, messageIDs []string) (map[string][]Reaction, error) {
	out := map[string][]Reaction{}
	if len(messageIDs) == 0 {
		return out, nil
	}
	args := []any{chatJID}
	marks := make([]string, len(messageIDs))
	for i, id := range messageIDs {
		marks[i] = "?"
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT message_id,reactor,emoji,timestamp,COALESCE(reaction_id,'')
		FROM reactions WHERE chat_jid=? AND emoji<>'' AND message_id IN (%s) ORDER BY timestamp`, strings.Join(marks, ",")), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var r Reaction
		if err := rows.Scan(&r.MessageID, &r.Reactor, &r.Emoji, &r.Timestamp, &r.ReactionID); err != nil {
			return nil, err
		}
		r.ChatJID = chatJID
		out[r.MessageID] = append(out[r.MessageID], r)
	}
	return out, rows.Err()
}

// MessageExcerpt is the short quote a reaction row carries: one line, at most excerptRunes runes.
func (s *Store) MessageExcerpt(ctx context.Context, messageID, chatJID string) (string, bool) {
	var content, mediaType sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT content, media_type FROM messages WHERE id=? AND chat_jid=?`, messageID, chatJID).Scan(&content, &mediaType)
	if err != nil {
		return "", false
	}
	text := strings.Join(strings.Fields(content.String), " ")
	if text == "" && mediaType.String != "" {
		text = "[" + mediaType.String + "]"
	}
	if r := []rune(text); len(r) > excerptRunes {
		text = string(r[:excerptRunes-1]) + "…"
	}
	return text, true
}

func nullIfEmpty(v string) any {
	if v == "" {
		return nil
	}
	return v
}
