package store

import (
	"context"
	"testing"
	"time"
)

func TestStoreMessageRedeliveryPreservesTranscriptAndRowIdentity(t *testing.T) {
	s := openTestStore(t)
	m := Message{ID: "voice-redelivery", ChatJID: "447700000001@s.whatsapp.net", Sender: "447700000001", Content: "[Nota de voz] synthetic transcript", MediaType: "audio", URL: "old", Timestamp: time.Now()}
	if err := s.StoreMessage(context.Background(), m, nil, nil, nil, 10); err != nil {
		t.Fatal(err)
	}
	var before int64
	if err := s.db.QueryRow("SELECT rowid FROM messages WHERE id=? AND chat_jid=?", m.ID, m.ChatJID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	m.Content = ""
	m.URL = "refreshed"
	if err := s.StoreMessage(context.Background(), m, []byte("new-media-key"), nil, nil, 20); err != nil {
		t.Fatal(err)
	}
	var after int64
	var content, url string
	if err := s.db.QueryRow("SELECT rowid,content,url FROM messages WHERE id=? AND chat_jid=?", m.ID, m.ChatJID).Scan(&after, &content, &url); err != nil {
		t.Fatal(err)
	}
	if content != "[Nota de voz] synthetic transcript" {
		t.Errorf("redelivery erased transcript: %q", content)
	}
	if before != after {
		t.Errorf("row identity changed from %d to %d", before, after)
	}
	if url != "refreshed" {
		t.Errorf("media metadata was not refreshed: %q", url)
	}
}

func TestStoreMessageRedeliveryPreservesManualAudioText(t *testing.T) {
	s := openTestStore(t)
	m := Message{ID: "manual-audio-text", ChatJID: "447700000001@s.whatsapp.net", Content: "manually corrected synthetic transcript", MediaType: "audio", Timestamp: time.Now()}
	if err := s.StoreMessage(context.Background(), m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	m.Content = ""
	if err := s.StoreMessage(context.Background(), m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	var content string
	if err := s.db.QueryRow("SELECT content FROM messages WHERE id=? AND chat_jid=?", m.ID, m.ChatJID).Scan(&content); err != nil {
		t.Fatal(err)
	}
	if content != "manually corrected synthetic transcript" {
		t.Fatalf("empty audio redelivery erased manual text: %q", content)
	}
}
