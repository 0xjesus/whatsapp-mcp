package store

import (
	"context"
	"testing"
	"time"
)

func TestHistoryAnchorUsesOldestRealKeyAndTimestampBound(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	chat := "1@s.whatsapp.net"
	now := time.Now().Truncate(time.Second)
	if err = s.StoreChat(chat, "", now); err != nil {
		t.Fatal(err)
	}
	for _, m := range []Message{
		{ID: "reaction", ChatJID: chat, Content: "x", MediaType: "reaction", Timestamp: now.Add(-3 * time.Hour)},
		{ID: "SYNC_123", ChatJID: chat, Content: "x", Timestamp: now.Add(-2 * time.Hour)},
		{ID: "older", ChatJID: chat, Content: "x", Timestamp: now.Add(-time.Hour), IsFromMe: false},
		{ID: "newer", ChatJID: chat, Content: "x", Timestamp: now, IsFromMe: true},
	} {
		if err = s.StoreMessage(context.Background(), m, nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	a, err := s.GetHistoryAnchor(chat, time.Time{})
	if err != nil || a.ID != "older" || a.IsFromMe {
		t.Fatalf("anchor %+v %v", a, err)
	}
	a, err = s.GetHistoryAnchor(chat, now.Add(-30*time.Minute))
	if err != nil || a.ID != "older" {
		t.Fatalf("bounded %+v %v", a, err)
	}
	if _, err = s.GetHistoryAnchor(chat, now.Add(-4*time.Hour)); err == nil {
		t.Fatal("missing real anchor silently accepted")
	}
}

func TestHistoryGlobalSlotAndPacingSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	a := Message{ID: "real", ChatJID: "1@s.whatsapp.net", Timestamp: now.Add(-time.Hour)}
	id, err := s.ReserveHistoryRequest(context.Background(), a, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveHistoryRequest(context.Background(), a, now.Add(time.Minute)); err == nil {
		t.Fatal("allowed simultaneous request")
	}
	if err = s.CancelHistoryRequest(id); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.ReserveHistoryRequest(context.Background(), a, now.Add(59*time.Second)); err == nil {
		t.Fatal("restart bypassed pacing")
	}
	if _, err = s.ReserveHistoryRequest(context.Background(), a, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryObservationKeepsEmptyTerminalAndFailureIncomplete(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now()
	a := Message{ID: "real", ChatJID: "1@s.whatsapp.net", Timestamp: now.Add(-time.Hour)}
	id, err := s.ReserveHistoryRequest(context.Background(), a, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FinishHistorySend(id, "peer", true, now); err != nil {
		t.Fatal(err)
	}
	terminal := 1
	ended := true
	o := HistoryObservation{ChatJID: a.ChatJID, SyncType: "ON_DEMAND", Digest: "empty-terminal", TerminalType: &terminal, End: &ended}
	oid, err := s.BeginHistoryObservation(o, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FinishHistoryObservation(oid, 0, 1, now); err != nil {
		t.Fatal(err)
	}
	var state string
	var typ int
	var failed int
	if err = s.DB().QueryRow(`SELECT state,terminal_type,failed FROM history_observations WHERE id=?`, oid).Scan(&state, &typ, &failed); err != nil {
		t.Fatal(err)
	}
	if state != "incomplete" || typ != 1 || failed != 1 {
		t.Fatalf("lost observation: %s %d %d", state, typ, failed)
	}
	if err = s.DB().QueryRow(`SELECT state FROM history_requests WHERE id=?`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "incomplete" {
		t.Fatalf("false completion %s", state)
	}
}

func TestHistoryInterruptedObservationIsNeverCommitted(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.BeginHistoryObservation(HistoryObservation{ChatJID: "1@s.whatsapp.net", Digest: "interrupt"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var state string
	if err = s.DB().QueryRow(`SELECT state FROM history_observations WHERE id=?`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "ingesting" {
		t.Fatalf("interruption hidden: %s", state)
	}
}

func TestHistoryCrawlAdvancesOnlyStrictOlderCommittedMoreRemains(t *testing.T) {
	for _, tc := range []struct {
		name     string
		older    bool
		terminal *int
		failed   int
		want     string
		enabled  int
	}{
		{"older-more", true, intPtr(2), 0, "queued", 1},
		{"no-progress", false, intPtr(2), 0, "incomplete_no_progress", 0},
		{"no-flags", true, nil, 0, "incomplete_no_progress", 0},
		{"terminal", true, intPtr(1), 0, "incomplete_terminal_observed", 0},
		{"no-access", true, intPtr(3), 0, "unavailable_history", 0},
		{"write-failed", true, intPtr(2), 1, "incomplete_ingestion", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			now := time.Now().Truncate(time.Second)
			chat := "1@s.whatsapp.net"
			if err = s.StoreChat(chat, "", now); err != nil {
				t.Fatal(err)
			}
			a := Message{ID: "anchor", ChatJID: chat, Content: "x", Timestamp: now.Add(-time.Hour)}
			if err = s.StoreMessage(context.Background(), a, nil, nil, nil, 0); err != nil {
				t.Fatal(err)
			}
			if err = s.SetHistoryCrawl(chat, true, now); err != nil {
				t.Fatal(err)
			}
			id, err := s.ReserveHistoryRequest(context.Background(), a, now)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.FinishHistorySend(id, "peer", true, now); err != nil {
				t.Fatal(err)
			}
			if tc.older {
				a.ID = "older"
				a.Timestamp = a.Timestamp.Add(-time.Hour)
				if err = s.StoreMessage(context.Background(), a, nil, nil, nil, 0); err != nil {
					t.Fatal(err)
				}
			}
			oid, err := s.BeginHistoryObservation(HistoryObservation{ChatJID: chat, SyncType: "ON_DEMAND", TerminalType: tc.terminal}, now)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.FinishHistoryObservation(oid, 1, tc.failed, now); err != nil {
				t.Fatal(err)
			}
			var state string
			var enabled int
			if err = s.DB().QueryRow(`SELECT state,enabled FROM history_crawls WHERE chat_jid=?`, chat).Scan(&state, &enabled); err != nil {
				t.Fatal(err)
			}
			if state != tc.want || enabled != tc.enabled {
				t.Fatalf("state %s/%d want %s/%d", state, enabled, tc.want, tc.enabled)
			}
		})
	}
}
func intPtr(v int) *int { return &v }

func TestHistoryTimeoutHoldsUntilExplicitResume(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	chat := "1@s.whatsapp.net"
	a := Message{ID: "real", ChatJID: chat, Timestamp: now.Add(-time.Hour)}
	if err = s.SetHistoryCrawl(chat, true, now); err != nil {
		t.Fatal(err)
	}
	id, err := s.ReserveHistoryRequest(context.Background(), a, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FinishHistorySend(id, "peer", true, now); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.NextHistoryCrawl(now.Add(11 * time.Minute))
	if err != nil || got != "" {
		t.Fatalf("timeout auto retried %q %v", got, err)
	}
	var state string
	if err = s.DB().QueryRow(`SELECT state FROM history_crawls`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "waiting_phone" {
		t.Fatal(state)
	}
	if err = s.SetHistoryCrawl(chat, true, now.Add(12*time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err = s.NextHistoryCrawl(now.Add(12 * time.Minute))
	if err != nil || got != chat {
		t.Fatalf("resume %q %v", got, err)
	}
}

func TestHistoryLateResponseCannotReenablePausedCrawl(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().Truncate(time.Second)
	chat := "1@s.whatsapp.net"
	if err = s.StoreChat(chat, "", now); err != nil {
		t.Fatal(err)
	}
	a := Message{ID: "anchor", ChatJID: chat, Content: "x", Timestamp: now}
	if err = s.StoreMessage(context.Background(), a, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err = s.SetHistoryCrawl(chat, true, now); err != nil {
		t.Fatal(err)
	}
	id, err := s.ReserveHistoryRequest(context.Background(), a, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FinishHistorySend(id, "peer", true, now); err != nil {
		t.Fatal(err)
	}
	if err = s.SetHistoryCrawl(chat, false, now); err != nil {
		t.Fatal(err)
	}
	a.ID = "older"
	a.Timestamp = now.Add(-time.Hour)
	if err = s.StoreMessage(context.Background(), a, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	oid, err := s.BeginHistoryObservation(HistoryObservation{ChatJID: chat, SyncType: "ON_DEMAND", TerminalType: intPtr(2)}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FinishHistoryObservation(oid, 1, 0, now); err != nil {
		t.Fatal(err)
	}
	var state string
	var enabled int
	if err = s.DB().QueryRow(`SELECT state,enabled FROM history_crawls`).Scan(&state, &enabled); err != nil {
		t.Fatal(err)
	}
	if state != "paused" || enabled != 0 {
		t.Fatalf("late response undid pause %s/%d", state, enabled)
	}
}
