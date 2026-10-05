package client

import (
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"testing"
)

func approveTestGroup(t *testing.T, s *store.Store, jid string) {
	t.Helper()
	_, err := s.DB().Exec(`CREATE TABLE IF NOT EXISTS group_monitoring_consent(chat_jid TEXT PRIMARY KEY,allowed INTEGER,evidence TEXT,updated_at TEXT)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DB().Exec(`INSERT OR REPLACE INTO group_monitoring_consent VALUES(?,1,'explicit synthetic approval','now')`, jid)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnapprovedGroupDoesNotCaptureReaction(t *testing.T) {
	c, s := reactionClient(t)
	if _, err := s.DB().Exec(`DELETE FROM group_monitoring_consent`); err != nil {
		t.Fatal(err)
	}
	c.handleMessage(reactionEvent("denied", "target", "X", "99887766001", false))
	var count int
	if err := s.DB().QueryRow(`SELECT count(*) FROM reactions`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("unapproved group event was captured")
	}
}
