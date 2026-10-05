package store

import "testing"

func TestMonitoringConsentDefaultsAndRevocation(t *testing.T) {
	s := openTestStore(t)
	if !s.MonitoringAllowed("person@s.whatsapp.net") {
		t.Fatal("direct chat should remain enabled")
	}
	if s.MonitoringAllowed("123@g.us") {
		t.Fatal("group must default to denied")
	}
	if _, err := s.db.Exec(`CREATE TABLE group_monitoring_consent(chat_jid TEXT PRIMARY KEY,allowed INTEGER,evidence TEXT,updated_at TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO group_monitoring_consent VALUES('123@g.us',1,'explicit synthetic approval','now')`); err != nil {
		t.Fatal(err)
	}
	if !s.MonitoringAllowed("123@g.us") {
		t.Fatal("explicitly approved group denied")
	}
	if s.MonitoringAllowed("456@g.us") {
		t.Fatal("consent leaked to another group")
	}
	if _, err := s.db.Exec(`UPDATE group_monitoring_consent SET allowed=0`); err != nil {
		t.Fatal(err)
	}
	if s.MonitoringAllowed("123@g.us") {
		t.Fatal("revocation must take effect immediately")
	}
}
