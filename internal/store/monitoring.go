package store

import (
	"fmt"
	"strings"
	"time"
)

// MonitoringAllowed fails closed for groups and expires automatic grants.
func (s *Store) MonitoringAllowed(chatJID string) bool {
	if !strings.HasSuffix(chatJID, "@g.us") {
		return chatJID != "status@broadcast"
	}
	var allowed int
	err := s.db.QueryRow(`SELECT allowed FROM group_monitoring_consent WHERE chat_jid=? AND (evidence NOT LIKE 'auto:max-members:%' OR unixepoch(updated_at)>unixepoch('now')-900)`, chatJID).Scan(&allowed)
	return err == nil && allowed == 1
}

type MonitoringGroup struct {
	JID, Name string
	Members   int
}

// ReconcileMonitoring publishes current metadata without overwriting manual decisions.
func (s *Store) ReconcileMonitoring(groups []MonitoringGroup) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`CREATE TABLE IF NOT EXISTS group_monitoring_consent(chat_jid TEXT PRIMARY KEY,allowed INTEGER NOT NULL DEFAULT 0,evidence TEXT NOT NULL,updated_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS group_monitoring_policy(singleton INTEGER PRIMARY KEY CHECK(singleton=1),enabled INTEGER NOT NULL DEFAULT 0,max_members INTEGER NOT NULL DEFAULT 10 CHECK(max_members>0));
 INSERT OR IGNORE INTO group_monitoring_policy VALUES(1,0,10);
 CREATE TABLE IF NOT EXISTS group_monitoring_inventory(chat_jid TEXT PRIMARY KEY,name TEXT NOT NULL,member_count INTEGER,updated_at TEXT NOT NULL);`)
	if err != nil {
		return err
	}
	var enabled, max int
	if err = tx.QueryRow(`SELECT enabled,max_members FROM group_monitoring_policy WHERE singleton=1`).Scan(&enabled, &max); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err = tx.Exec(`DELETE FROM group_monitoring_inventory`); err != nil {
		return err
	}
	for _, g := range groups {
		if !strings.HasSuffix(g.JID, "@g.us") {
			continue
		}
		var count interface{}
		if g.Members > 0 {
			count = g.Members
		}
		if _, err = tx.Exec(`INSERT INTO group_monitoring_inventory VALUES(?,?,?,?)`, g.JID, g.Name, count, now); err != nil {
			return err
		}
		allowed := 0
		if enabled == 1 && g.Members > 0 && g.Members <= max {
			allowed = 1
		}
		_, err = tx.Exec(`INSERT INTO group_monitoring_consent VALUES(?,?,?,?) ON CONFLICT(chat_jid) DO UPDATE SET allowed=excluded.allowed,evidence=excluded.evidence,updated_at=excluded.updated_at WHERE group_monitoring_consent.evidence LIKE 'auto:max-members:%'`, g.JID, allowed, fmt.Sprintf("auto:max-members:%d", max), now)
		if err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`UPDATE group_monitoring_consent SET allowed=0,updated_at=? WHERE evidence LIKE 'auto:max-members:%' AND NOT EXISTS(SELECT 1 FROM group_monitoring_inventory i WHERE i.chat_jid=group_monitoring_consent.chat_jid)`, now); err != nil {
		return err
	}
	return tx.Commit()
}

// InvalidateAutomaticMonitoring denies cached automatic decisions before a refresh.
func (s *Store) InvalidateAutomaticMonitoring() error {
	_, err := s.db.Exec(`UPDATE group_monitoring_consent SET allowed=0 WHERE evidence LIKE 'auto:max-members:%'`)
	return err
}

// InvalidateAutomaticMonitoringGroup affects only the peer whose membership changed.
func (s *Store) InvalidateAutomaticMonitoringGroup(chatJID string) error {
	_, err := s.db.Exec(`UPDATE group_monitoring_consent SET allowed=0 WHERE chat_jid=? AND evidence LIKE 'auto:max-members:%'`, chatJID)
	return err
}
