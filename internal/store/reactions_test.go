package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func seedReactionChat(t *testing.T) *Store {
	t.Helper()
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.StoreChat("120363@g.us", "Test Group", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(ctx, Message{ID: "m1", ChatJID: "120363@g.us", Sender: "447700000101",
		Content: "Primera línea del mensaje original\nsegunda línea", Timestamp: time.Date(2026, 9, 30, 2, 31, 11, 0, time.UTC)}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(ctx, Message{ID: "v1", ChatJID: "120363@g.us", Sender: "447700000101",
		MediaType: "video", Filename: "video.mp4", Timestamp: time.Date(2026, 9, 30, 2, 24, 7, 0, time.UTC)}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestUpsertReaction_ReplacesSameReactor(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	r := Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "447700000102", Emoji: "👍", Timestamp: time.Unix(1790880000, 0), ReactionID: "AC1"}
	if _, err := s.UpsertReaction(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Emoji, r.ReactionID = "❤️", "AC2"
	if _, err := s.UpsertReaction(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReactionsFor(ctx, "120363@g.us", []string{"m1", "v1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got["m1"]) != 1 || got["m1"][0].Emoji != "❤️" || got["m1"][0].ReactionID != "AC2" {
		t.Fatalf("reactions for m1 = %+v", got["m1"])
	}
	if len(got["v1"]) != 0 {
		t.Fatalf("v1 should have no reactions: %+v", got["v1"])
	}
}

func TestDeleteReaction_ReturnsMirrorID(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	_, _ = s.UpsertReaction(ctx, Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "447700000102", Emoji: "👍", Timestamp: time.Now(), ReactionID: "AC1"})
	id, err := s.DeleteReaction(ctx, "m1", "120363@g.us", "447700000102")
	if err != nil || id != "AC1" {
		t.Fatalf("delete = %q, %v", id, err)
	}
	id, err = s.DeleteReaction(ctx, "m1", "120363@g.us", "447700000102")
	if err != nil || id != "" {
		t.Fatalf("second delete = %q, %v (want empty, nil)", id, err)
	}
	got, _ := s.ReactionsFor(ctx, "120363@g.us", []string{"m1"})
	if len(got["m1"]) != 0 {
		t.Fatalf("still present: %+v", got["m1"])
	}
}

func TestUpsertReaction_RejectsBadEmoji(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	bad := Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "x", Emoji: string([]byte{0xff, 0xfe}), Timestamp: time.Now()}
	if _, err := s.UpsertReaction(ctx, bad); err == nil {
		t.Fatal("invalid UTF-8 emoji accepted")
	}
	long := Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "x", Emoji: "👍👍👍👍👍👍", Timestamp: time.Now()}
	if _, err := s.UpsertReaction(ctx, long); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ReactionsFor(ctx, "120363@g.us", []string{"m1"})
	if len(got["m1"][0].Emoji) > 16 {
		t.Fatalf("emoji not truncated to 16 bytes: %q", got["m1"][0].Emoji)
	}
}

func TestNormalizeEmoji(t *testing.T) {
	if got, err := NormalizeEmoji("👍x"); err != nil || got != "👍x" {
		t.Fatalf("NormalizeEmoji = %q %v", got, err)
	}
	if _, err := NormalizeEmoji(""); err == nil {
		t.Fatal("empty emoji accepted")
	}
}

func TestMessageExcerpt(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	text, found := s.MessageExcerpt(ctx, "m1", "120363@g.us")
	if !found || text != "Primera línea del mensaje original segunda línea" {
		t.Fatalf("excerpt = %q, %v", text, found)
	}
	text, found = s.MessageExcerpt(ctx, "v1", "120363@g.us")
	if !found || text != "[video]" {
		t.Fatalf("video excerpt = %q", text)
	}
	if _, found = s.MessageExcerpt(ctx, "nope", "120363@g.us"); found {
		t.Fatal("missing message reported as found")
	}
	long := Message{ID: "l1", ChatJID: "120363@g.us", Sender: "x", Content: "ñ" + strings.Repeat("0123456789", 7), Timestamp: time.Now()}
	_ = s.StoreMessage(ctx, long, nil, nil, nil, 0)
	text, _ = s.MessageExcerpt(ctx, "l1", "120363@g.us")
	if r := []rune(text); len(r) != 60 || string(r[len(r)-1:]) != "…" {
		t.Fatalf("excerpt not cut to 60 runes with ellipsis: %q (%d)", text, len(r))
	}
}

// --- review fix pass ---

func TestNormalizeEmoji_StripsControlCharacters(t *testing.T) {
	if got, err := NormalizeEmoji("\n👍\r\n"); err != nil || got != "👍" {
		t.Fatalf("NormalizeEmoji(control) = %q %v", got, err)
	}
	if _, err := NormalizeEmoji("\n\t"); err == nil {
		t.Fatal("control-only emoji accepted")
	}
}

func TestUpsertReaction_ReturnsPreviousMirrorID(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	prev, err := s.UpsertReaction(ctx, Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "x", Emoji: "👍", Timestamp: time.Now(), ReactionID: "AC1"})
	if err != nil || prev != "" {
		t.Fatalf("first upsert prev = %q %v", prev, err)
	}
	prev, err = s.UpsertReaction(ctx, Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "x", Emoji: "❤️", Timestamp: time.Now(), ReactionID: "AC2"})
	if err != nil || prev != "AC1" {
		t.Fatalf("second upsert prev = %q %v", prev, err)
	}
}

func TestDeleteReaction_RemovesMirrorRowAtomically(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	_, _ = s.UpsertReaction(ctx, Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "x", Emoji: "👍", Timestamp: time.Now(), ReactionID: "AC1"})
	_ = s.StoreMessage(ctx, Message{ID: "AC1", ChatJID: "120363@g.us", Sender: "x", MediaType: "reaction", Content: "👍 → «…»", Timestamp: time.Now()}, nil, nil, nil, 0)
	id, err := s.DeleteReaction(ctx, "m1", "120363@g.us", "x")
	if err != nil || id != "AC1" {
		t.Fatalf("delete = %q %v", id, err)
	}
	var n int
	_ = s.db.QueryRow("SELECT count(*) FROM messages WHERE id='AC1'").Scan(&n)
	if n != 0 {
		t.Fatal("mirror row survived DeleteReaction")
	}
}

func TestApplyReactionConcurrentOrdering(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.StoreChat("120363@g.us", "Synthetic", time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const events = 20
	errs := make(chan error, events)
	start := make(chan struct{})
	for i := 0; i < events; i++ {
		go func(i int) {
			<-start
			id := fmt.Sprintf("LIVE%d", i)
			errs <- s.ApplyReaction(ctx, Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "synthetic", Emoji: "👍", Timestamp: time.Unix(int64(i+1), 0), ReactionID: id}, &Message{Content: id})
		}(i)
	}
	close(start)
	for i := 0; i < events; i++ {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
	got, err := s.ReactionsFor(ctx, "120363@g.us", []string{"m1"})
	if err != nil || len(got["m1"]) != 1 || got["m1"][0].ReactionID != "LIVE19" {
		t.Fatalf("latest reaction = %+v, %v", got, err)
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE media_type='reaction'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("mirrors = %d, want 1", count)
	}
}

func TestApplyReactionMirrorFailureRollsBack(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	r := Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "synthetic", Emoji: "👍", Timestamp: time.Unix(1, 0), ReactionID: "LIVE1"}
	if err := s.ApplyReaction(ctx, r, &Message{Content: "first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("CREATE TRIGGER fail_reaction_mirror BEFORE INSERT ON messages WHEN NEW.id='LIVE2' BEGIN SELECT RAISE(ABORT,'synthetic mirror failure'); END"); err != nil {
		t.Fatal(err)
	}
	r.Timestamp, r.ReactionID = time.Unix(2, 0), "LIVE2"
	if err := s.ApplyReaction(ctx, r, &Message{Content: "second"}); err == nil {
		t.Fatal("mirror failure was ignored")
	}
	got, err := s.ReactionsFor(ctx, r.ChatJID, []string{"m1"})
	if err != nil || len(got["m1"]) != 1 || got["m1"][0].ReactionID != "LIVE1" {
		t.Fatalf("reaction not rolled back: %+v, %v", got, err)
	}
	var content string
	if err := s.db.QueryRow("SELECT content FROM messages WHERE id='LIVE1'").Scan(&content); err != nil || content != "first" {
		t.Fatalf("old mirror lost: %s, %v", content, err)
	}
}
