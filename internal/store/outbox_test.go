package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestOutboxStatusExcludesPayload(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	j := OutboxJob{JobID: "j", MessageID: "m", Recipient: "1@s.whatsapp.net", EnqueuedAt: time.Now(), NextAttempt: time.Now(), Payload: "private", Media: []byte("secret")}
	if e = s.EnqueueOutbox(ctx, j); e != nil {
		t.Fatal(e)
	}
	got, e := s.OutboxJob(ctx, "j")
	if e != nil {
		t.Fatal(e)
	}
	if got.Payload != "" || len(got.Media) != 0 {
		t.Fatal("status loaded private payload/media")
	}
}
func TestIdempotentConcurrentEnqueue(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	j := OutboxJob{JobID: "one", MessageID: "one", Recipient: "1@s.whatsapp.net", Sender: "2@s.whatsapp.net", IdempotencyKey: "key", RequestHash: "hash", EnqueuedAt: time.Now(), NextAttempt: time.Now(), Payload: "private"}
	got, e := s.EnqueueOutboxOnce(ctx, j)
	if e != nil {
		t.Fatal(e)
	}
	j.JobID = "two"
	j.MessageID = "two"
	again, e := s.EnqueueOutboxOnce(ctx, j)
	if e != nil || again.JobID != got.JobID {
		t.Fatalf("duplicate: %+v %v", again, e)
	}
	j.RequestHash = "changed"
	if _, e = s.EnqueueOutboxOnce(ctx, j); e == nil {
		t.Fatal("changed request accepted")
	}
}
func TestPersistentBudgetFloorsAndRollingCaps(t *testing.T) {
	for _, known := range []bool{true, false} {
		t.Run(fmt.Sprint(known), func(t *testing.T) {
			s, e := Open(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			ctx := context.Background()
			now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
			s.EnsureSendProtection(ctx, now)
			d, e := s.PersistentSendBudget(ctx, now, known, true)
			if e != nil || d.Allowed || d.RetryAfter != time.Hour {
				t.Fatalf("guard=%+v %v", d, e)
			}
			now = now.Add(time.Hour)
			start := now
			cap := 30
			interval := 45 * time.Second
			if !known {
				cap = 15
				interval = 90 * time.Second
			}
			for i := 0; i < cap; i++ {
				d, e = s.PersistentSendBudget(ctx, now, known, true)
				if e != nil || !d.Allowed {
					t.Fatalf("attempt %d denied: %+v %v", i, d, e)
				}
				if i == 0 {
					early, _ := s.PersistentSendBudget(ctx, now.Add(interval-time.Nanosecond), known, false)
					if early.Allowed {
						t.Fatal("interval floor missing")
					}
				}
				now = now.Add(interval)
			}
			d, e = s.PersistentSendBudget(ctx, now, known, true)
			if e != nil || d.Allowed || now.Add(d.RetryAfter) != start.Add(time.Hour) {
				t.Fatalf("cap=%+v %v", d, e)
			}
			now = start.Add(time.Hour)
			d, e = s.PersistentSendBudget(ctx, now, known, true)
			if e != nil || !d.Allowed {
				t.Fatalf("window failed to resume: %+v %v", d, e)
			}
		})
	}
}
func TestRecoverOutboxStableCacheAndUnknownDelivery(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now()
	for _, id := range []string{"cached", "uncertain"} {
		if e = s.EnqueueOutbox(ctx, OutboxJob{JobID: id, MessageID: id, Recipient: "1@s.whatsapp.net", Sender: "2@s.whatsapp.net", EnqueuedAt: now, NextAttempt: now, Payload: "body"}); e != nil {
			t.Fatal(e)
		}
		s.ClaimOutbox(ctx, id)
		s.RecordOutboxAttempt(ctx, id)
	}
	s.StoreChat("1@s.whatsapp.net", "test", now)
	if e = s.StoreMessage(ctx, Message{ID: "cached", ChatJID: "1@s.whatsapp.net", Sender: "2", Content: "body", Timestamp: now, IsFromMe: true}, nil, nil, nil, 0); e != nil {
		t.Fatal(e)
	}
	if e = s.RecoverOutbox(ctx); e != nil {
		t.Fatal(e)
	}
	cached, _ := s.OutboxJob(ctx, "cached")
	uncertain, _ := s.OutboxJob(ctx, "uncertain")
	if cached.State != "sent" || cached.SentID != "cached" || uncertain.State != "needs_review" {
		t.Fatalf("recovery cached=%+v uncertain=%+v", cached, uncertain)
	}
}
func TestPersistentBudgetSurvivesStoreReopen(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	now := time.Now()
	s.EnsureSendProtection(ctx, now.Add(-time.Hour))
	d, e := s.PersistentSendBudget(ctx, now, false, true)
	if e != nil || !d.Allowed {
		t.Fatalf("reserve: %+v %v", d, e)
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	d, e = s.PersistentSendBudget(ctx, now.Add(45*time.Second), false, true)
	if e != nil || d.Allowed || d.RetryAfter != 45*time.Second {
		t.Fatalf("restart lost90s floor: %+v %v", d, e)
	}
}
func TestUpgradePartialOutboxSchema(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.DB().Exec(`DROP TABLE outbound_jobs; CREATE TABLE outbound_jobs(job_id TEXT PRIMARY KEY,message_id TEXT NOT NULL UNIQUE,recipient TEXT NOT NULL,state TEXT NOT NULL,enqueued_ns INTEGER NOT NULL,next_ns INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,reason TEXT NOT NULL DEFAULT '',sent_id TEXT NOT NULL DEFAULT '',payload TEXT NOT NULL,media BLOB);INSERT INTO outbound_jobs VALUES('old','old','1@s.whatsapp.net','queued',1,1,0,'','','private',NULL)`)
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	j, e := s.OutboxJob(context.Background(), "old")
	if e != nil || j.Sender != "" || j.JobID != "old" {
		t.Fatalf("legacy migration: %+v %v", j, e)
	}
}

func TestLegacyLedgerMigrationPreservesCapsOnce(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < 30; i++ {
		if _, e = s.DB().Exec(`INSERT INTO send_budget VALUES(?,1)`, now.Add(-time.Duration(i+1)*time.Minute).UnixNano()); e != nil {
			t.Fatal(e)
		}
	}
	s.SetProtectionValue(ctx, "legacy_ledger_available", "1")
	if e = s.EnsureSendProtection(ctx, now); e != nil {
		t.Fatal(e)
	}
	d, e := s.PersistentSendBudget(ctx, now.Add(90*time.Second), true, false)
	if e != nil || d.Allowed || d.Reason != "hourly send cap reached" {
		t.Fatalf("legacy cap lost: %+v %v", d, e)
	}
	if e = s.EnsureSendProtection(ctx, now.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	var n int
	s.DB().QueryRow(`SELECT COUNT(*) FROM outbound_attempts`).Scan(&n)
	if n != 30 {
		t.Fatalf("migration repeated: %d", n)
	}
}
func TestScheduledExhaustedPendingIsVisibleTerminal(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	now := time.Now()
	j, e := s.Schedule("1@s.whatsapp.net", "text", now.Add(time.Second), now.Add(time.Hour), "")
	if e != nil {
		t.Fatal(e)
	}
	s.DB().Exec(`UPDATE scheduled_messages SET attempts=5 WHERE id=?`, j.ID)
	s.ClaimScheduled(now.Add(2 * time.Second))
	jobs, e := s.ListScheduled(100, "", "failed")
	if e != nil || len(jobs) != 1 {
		t.Fatalf("unclaimable pending job: %v %+v", e, jobs)
	}
}

func TestScheduledStableIDRecoveryAndLegacyBinding(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	now := time.Now()
	j, e := s.Schedule("1@s.whatsapp.net", "text", now.Add(time.Second), now.Add(time.Hour), "")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.BindLegacyScheduledSender("own@s.whatsapp.net"); e != nil {
		t.Fatal(e)
	}
	s.BindLegacyScheduledSender("changed@s.whatsapp.net")
	jobs, _ := s.ListScheduled(100, "", "")
	if jobs[0].Sender != "own@s.whatsapp.net" {
		t.Fatal(jobs)
	}
	s.ClaimScheduled(now.Add(time.Second))
	s.PrepareScheduledID(j.ID, "stable")
	s.MarkScheduledDispatch(j.ID)
	s.StoreChat(j.ChatJID, "test", now)
	if e = s.StoreMessage(context.Background(), Message{ID: "stable", ChatJID: j.ChatJID, Content: "text", IsFromMe: true, Timestamp: now}, nil, nil, nil, 0); e != nil {
		t.Fatal(e)
	}
	if e = s.RecoverScheduled(); e != nil {
		t.Fatal(e)
	}
	jobs, _ = s.ListScheduled(100, "", "sent")
	if len(jobs) != 1 || jobs[0].MessageID != "stable" {
		t.Fatal(jobs)
	}
}

func TestPreSchemaLegacyAvailabilityControlsMigrationGuard(t *testing.T) {
	for _, available := range []bool{false, true} {
		t.Run(fmt.Sprint(available), func(t *testing.T) {
			dir := t.TempDir()
			raw, e := sql.Open("sqlite3", filepath.Join(dir, "messages.db"))
			if e != nil {
				t.Fatal(e)
			}
			if available {
				_, e = raw.Exec(`CREATE TABLE send_budget(at INTEGER NOT NULL,known INTEGER NOT NULL)`)
				if e != nil {
					t.Fatal(e)
				}
			}
			raw.Close()
			s, e := Open(dir)
			if e != nil {
				t.Fatal(e)
			}
			now := time.Now()
			ctx := context.Background()
			if e = s.EnsureSendProtection(ctx, now); e != nil {
				t.Fatal(e)
			}
			d, e := s.PersistentSendBudget(ctx, now, true, false)
			want := time.Hour
			if available {
				want = 90 * time.Second
			}
			if e != nil || d.RetryAfter != want {
				t.Fatalf("guard: %+v %v", d, e)
			}
			s.Close()
			s, e = Open(dir)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			s.EnsureSendProtection(ctx, now.Add(time.Second))
			d, e = s.PersistentSendBudget(ctx, now.Add(time.Second), true, false)
			if e != nil || d.RetryAfter != want-time.Second {
				t.Fatalf("restart renewed guard: %+v %v", d, e)
			}
		})
	}
}

func TestCancelUncertainOutboxRetainsDiagnostics(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now()
	j := OutboxJob{JobID: "uncertain", MessageID: "stable", Recipient: "1@s.whatsapp.net", EnqueuedAt: now, NextAttempt: now, Payload: "private", Media: []byte("snapshot")}
	if e = s.EnqueueOutbox(ctx, j); e != nil {
		t.Fatal(e)
	}
	s.ClaimOutbox(ctx, j.JobID)
	s.UpdateOutbox(ctx, j.JobID, "needs_review", "timeout after write; check stable ID", "", now)
	if e = s.CancelOutbox(ctx, j.JobID); e == nil {
		t.Fatal("uncertain job became ordinary cancelled")
	}
	got, _ := s.OutboxJob(ctx, j.JobID)
	_, data, _ := s.LoadOutboxPayload(ctx, j.JobID)
	if got.State != "needs_review" || got.Reason != "timeout after write; check stable ID" || string(data) != "snapshot" {
		t.Fatalf("diagnostics lost: %+v %q", got, data)
	}
}

func TestMappedLIDCacheRecoveryConstrainedToRecipientAndFromMe(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now()
	// The synthetic mapping models whatsmeow.db; no dependency network calls.
	if s.whatsmeowDB != nil {
		s.whatsmeowDB.Close()
	}
	s.whatsmeowDB = s.DB()
	s.DB().Exec(`CREATE TABLE whatsmeow_lid_map(lid TEXT PRIMARY KEY,pn TEXT);INSERT INTO whatsmeow_lid_map VALUES('99887766','447700000002')`)
	for _, tc := range []struct {
		id, chat string
		fromMe   bool
		sent     bool
	}{{"mapped", "447700000002@s.whatsapp.net", true, true}, {"wrongchat", "999@s.whatsapp.net", true, false}, {"incoming", "447700000002@s.whatsapp.net", false, false}} {
		j := OutboxJob{JobID: tc.id, MessageID: tc.id, Recipient: "99887766@lid", EnqueuedAt: now, NextAttempt: now, Payload: "private"}
		if e = s.EnqueueOutbox(ctx, j); e != nil {
			t.Fatal(e)
		}
		s.ClaimOutbox(ctx, j.JobID)
		sj, e := s.Schedule("99887766@lid", "text", now.Add(time.Second), now.Add(time.Hour), tc.id)
		if e != nil {
			t.Fatal(e)
		}
		s.ClaimScheduled(now.Add(time.Second))
		s.PrepareScheduledID(sj.ID, tc.id)
		s.MarkScheduledDispatch(sj.ID)
		s.StoreChat(tc.chat, "test", now)
		s.StoreMessage(ctx, Message{ID: tc.id, ChatJID: tc.chat, Content: "text", IsFromMe: tc.fromMe, Timestamp: now}, nil, nil, nil, 0)
	}
	if e = s.RecoverOutbox(ctx); e != nil {
		t.Fatal(e)
	}
	if e = s.RecoverScheduled(); e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct{ id, state, status string }{{"mapped", "sent", "sent"}, {"wrongchat", "needs_review", "uncertain"}, {"incoming", "needs_review", "uncertain"}} {
		j, _ := s.OutboxJob(ctx, tc.id)
		if j.State != tc.state {
			t.Fatalf("outbox %s: %+v", tc.id, j)
		}
		var status string
		s.DB().QueryRow(`SELECT status FROM scheduled_messages WHERE idempotency_key=?`, tc.id).Scan(&status)
		if status != tc.status {
			t.Fatalf("scheduled %s: %s", tc.id, status)
		}
	}
	// Avoid double Close on aliased test handles.
	s.whatsmeowDB = nil
}

func TestCommittedOutboxAcceptanceSurvivesCancelledRefresh(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.afterOutboxInsert = cancel
	now := time.Now()
	j := OutboxJob{JobID: "committed", MessageID: "stable", Recipient: "1@s.whatsapp.net", State: "invented", EnqueuedAt: now, NextAttempt: now, Attempts: 7, Reason: "invented", SentID: "invented", Payload: "private", Media: []byte("private")}
	got, e := s.EnqueueOutboxOnce(ctx, j)
	if e != nil || got.JobID != j.JobID || got.State != "queued" || got.Attempts != 0 || got.Reason != "" || got.SentID != "" || got.Payload != "" || len(got.Media) != 0 {
		t.Fatalf("committed insert falsely rejected or incorrect metadata: %+v %v", got, e)
	}
	persisted, e := s.OutboxJob(context.Background(), j.JobID)
	if e != nil || persisted.JobID != got.JobID {
		t.Fatalf("missing committed job: %+v %v", persisted, e)
	}
}

func TestPersistentReservationKeepsLegacyRestoreLedgerCurrent(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now()
	s.EnsureSendProtection(ctx, now.Add(-2*time.Hour))
	d, e := s.PersistentSendBudget(ctx, now, false, true)
	if e != nil || !d.Allowed {
		t.Fatal(d, e)
	}
	all, cold, e := s.SendBudget()
	if e != nil || len(all) != 1 || len(cold) != 1 || all[0].UnixNano() != now.UnixNano() {
		t.Fatalf("legacy restoration misses new shared reservation: %v %v %v", all, cold, e)
	}
}
