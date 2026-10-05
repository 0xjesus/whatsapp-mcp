package store

import "strings"

// MonitoringAllowed is fail-closed for groups. Membership is never consent.
// The local consent CLI manages the shared table; no incoming message can grant it.
func (s *Store) MonitoringAllowed(chatJID string) bool {
	if !strings.HasSuffix(chatJID, "@g.us") {
		return chatJID != "status@broadcast"
	}
	var allowed int
	err := s.db.QueryRow("SELECT allowed FROM group_monitoring_consent WHERE chat_jid=?", chatJID).Scan(&allowed)
	return err == nil && allowed == 1
}
