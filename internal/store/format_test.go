package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestFormatMessage_FromMe(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	m := Message{
		ID:        "m1",
		ChatJID:   "447700000001@s.whatsapp.net",
		Sender:    "me",
		Content:   "hello",
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		IsFromMe:  true,
		ChatName:  "Alice",
	}
	got := s.FormatMessage(ctx, m, false)
	if !strings.Contains(got, "From: Me:") {
		t.Fatalf("expected 'From: Me:' in output, got %q", got)
	}
	if !strings.HasPrefix(got, "[2026-01-02 03:04:05]") {
		t.Fatalf("expected timestamp prefix, got %q", got)
	}
	if !strings.HasSuffix(got, "\n") {
		t.Fatalf("expected trailing newline, got %q", got)
	}
}

func TestFormatMessage_KnownSender(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	m := Message{
		ID:        "m2",
		ChatJID:   "447700000001@s.whatsapp.net",
		Sender:    "447700000001@s.whatsapp.net", // Alice, per seed data
		Content:   "hi",
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		IsFromMe:  false,
	}
	got := s.FormatMessage(ctx, m, false)
	if !strings.Contains(got, "From: Alice:") {
		t.Fatalf("expected 'From: Alice:' in output, got %q", got)
	}
}

func TestFormatMessage_MediaPrefix(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	m := Message{
		ID:        "m3",
		ChatJID:   "447700000001@s.whatsapp.net",
		Sender:    "me",
		Content:   "caption",
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		IsFromMe:  true,
		MediaType: "image",
	}
	got := s.FormatMessage(ctx, m, false)
	wantPrefix := "[image - Message ID: m3 - Chat JID: 447700000001@s.whatsapp.net]"
	if !strings.Contains(got, wantPrefix) {
		t.Fatalf("expected media prefix %q in output, got %q", wantPrefix, got)
	}
}

func TestFormatMessage_NoChatPrefixWhenNotShown(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	m := Message{
		ID:        "m4",
		ChatJID:   "447700000001@s.whatsapp.net",
		Sender:    "me",
		Content:   "hi",
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		IsFromMe:  true,
		ChatName:  "Alice",
	}
	got := s.FormatMessage(ctx, m, false)
	if strings.Contains(got, "Chat: ") {
		t.Fatalf("did not expect 'Chat:' prefix when showChatInfo=false, got %q", got)
	}

	withChat := s.FormatMessage(ctx, m, true)
	if !strings.Contains(withChat, "Chat: Alice") {
		t.Fatalf("expected 'Chat: Alice' when showChatInfo=true, got %q", withChat)
	}
}

func TestFormatMessagesList_Empty(t *testing.T) {
	s := openTestStore(t)
	got := s.FormatMessagesList(context.Background(), nil, false)
	if got != "No messages to display." {
		t.Fatalf("expected empty-message placeholder, got %q", got)
	}
}

func TestFormatMessagesList_NonEmpty(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	msgs := []Message{
		{
			ID:        "x1",
			ChatJID:   "447700000001@s.whatsapp.net",
			Sender:    "me",
			Content:   "one",
			Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			IsFromMe:  true,
		},
		{
			ID:        "x2",
			ChatJID:   "447700000001@s.whatsapp.net",
			Sender:    "me",
			Content:   "two",
			Timestamp: time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC),
			IsFromMe:  true,
		},
	}
	got := s.FormatMessagesList(ctx, msgs, false)
	// Two messages => two newlines.
	if n := strings.Count(got, "\n"); n != 2 {
		t.Fatalf("expected 2 newlines, got %d in %q", n, got)
	}
	if !strings.Contains(got, "one") || !strings.Contains(got, "two") {
		t.Fatalf("expected both message contents, got %q", got)
	}
}
func TestFormatMessagesList_ShowsReactionsUnderMessage(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	_, _ = s.UpsertReaction(ctx, Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "447700000102", Emoji: "👍", Timestamp: time.Unix(1790880000, 0), ReactionID: "AC1"})
	_, _ = s.UpsertReaction(ctx, Reaction{MessageID: "m1", ChatJID: "120363@g.us", Reactor: "447700000101", Emoji: "❤️", Timestamp: time.Unix(1790880060, 0), ReactionID: "AC2"})
	msgs, err := s.ListMessages(ctx, ListMessagesParams{ChatJID: "120363@g.us", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	out := s.FormatMessagesList(ctx, msgs, false)
	if !strings.Contains(out, "   ↳ 👍 447700000102 · ❤️ 447700000101\n") {
		t.Fatalf("reactions line missing:\n%s", out)
	}
	if strings.Count(out, "↳") != 1 {
		t.Fatalf("reaction line should appear once:\n%s", out)
	}
}

func TestFormatMessagesList_ReactionRowKeepsItsPlace(t *testing.T) {
	s := seedReactionChat(t)
	ctx := context.Background()
	_ = s.StoreMessage(ctx, Message{ID: "AC1", ChatJID: "120363@g.us", Sender: "447700000102", MediaType: "reaction",
		Content: "👍 → «Primera línea del mensaje original»", Timestamp: time.Date(2026, 9, 30, 2, 37, 6, 0, time.UTC)}, nil, nil, nil, 0)
	msgs, _ := s.ListMessages(ctx, ListMessagesParams{ChatJID: "120363@g.us", Limit: 10})
	out := s.FormatMessagesList(ctx, msgs, false)
	if !strings.Contains(out, "[reaction - Message ID: AC1 - Chat JID: 120363@g.us] 👍 → «Primera línea") {
		t.Fatalf("reaction row not rendered:\n%s", out)
	}
}
