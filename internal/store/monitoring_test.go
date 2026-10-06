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

func TestSmallGroupPolicy(t *testing.T) {
	s := openTestStore(t)
	groups := []MonitoringGroup{{JID: "10@g.us", Name: "ten", Members: 10}, {JID: "11@g.us", Members: 11}, {JID: "0@g.us", Members: 0}}
	if err := s.ReconcileMonitoring(groups); err != nil {
		t.Fatal(err)
	}
	if s.MonitoringAllowed("10@g.us") {
		t.Fatal("policy must be opt in")
	}
	if _, err := s.db.Exec(`UPDATE group_monitoring_policy SET enabled=1`); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileMonitoring(groups); err != nil {
		t.Fatal(err)
	}
	if !s.MonitoringAllowed("10@g.us") || s.MonitoringAllowed("11@g.us") || s.MonitoringAllowed("0@g.us") {
		t.Fatal("threshold or unknown count")
	}
	s.db.Exec(`UPDATE group_monitoring_consent SET evidence='manual off',allowed=0 WHERE chat_jid='10@g.us'`)
	if err := s.ReconcileMonitoring(groups); err != nil {
		t.Fatal(err)
	}
	if s.MonitoringAllowed("10@g.us") {
		t.Fatal("manual off overwritten")
	}
	s.db.Exec(`UPDATE group_monitoring_consent SET allowed=1,evidence='auto:max-members:10',updated_at='2000-01-01T00:00:00Z' WHERE chat_jid='11@g.us'`)
	if s.MonitoringAllowed("11@g.us") {
		t.Fatal("expired automatic grant")
	}
	if err := s.ReconcileMonitoring(nil); err != nil {
		t.Fatal(err)
	}
	if s.MonitoringAllowed("11@g.us") {
		t.Fatal("missing group retained")
	}
}
