package store

import (
	"testing"
	"time"
)

func TestScheduleLifecycle(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.db.Exec(schedulerSchema); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	a, err := s.Schedule("123@s.whatsapp.net", "hello", now.Add(time.Minute), now.Add(time.Hour), "same")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Schedule("123@s.whatsapp.net", "hello", now.Add(time.Minute), now.Add(time.Hour), "same")
	if err != nil || a.ID != b.ID {
		t.Fatal("dedupe", err)
	}
	if _, err = s.ClaimScheduled(now); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.ListScheduled(100, "", "pending")
	if err != nil || len(jobs) != 1 {
		t.Fatal(jobs, err)
	}
	if err = s.Reschedule(a.ID, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	job, err := s.ClaimScheduled(now)
	if err != nil || job == nil {
		t.Fatal("due", err)
	}
	if err = s.CancelScheduled(a.ID); err == nil {
		t.Fatal("claimed cancellation")
	}
	if err = s.RecoverScheduled(); err != nil {
		t.Fatal(err)
	}
	job, err = s.ClaimScheduled(now)
	if err != nil || job == nil {
		t.Fatal("claimed recovery", err)
	}
	if err = s.MarkScheduledDispatch(job.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.RecoverScheduled(); err != nil {
		t.Fatal(err)
	}
	jobs, err = s.ListScheduled(100, "", "uncertain")
	if err != nil || len(jobs) != 1 {
		t.Fatal("dispatch recovery", err)
	}
	if err = s.Reschedule(job.ID, now.Add(time.Minute), now.Add(time.Hour)); err == nil {
		t.Fatal("uncertain must require manual review")
	}
}

func TestScheduleLimitsAndExpiry(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.db.Exec(schedulerSchema); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	send := now.Add(time.Hour)
	expiry := send.Add(time.Hour)
	for i := 0; i < 1000; i++ {
		if _, err := s.Schedule("123@s.whatsapp.net", "hi", send, expiry, ""); err != nil {
			t.Fatal(i, err)
		}
	}
	if _, err := s.Schedule("123@s.whatsapp.net", "hi", send, expiry, ""); err == nil {
		t.Fatal("queue cap")
	}
	jobs, err := s.ListScheduled(100, "", "pending")
	if err != nil || len(jobs) != 100 {
		t.Fatal(err, len(jobs))
	}
	next, err := s.ListScheduled(100, jobs[99].ID, "pending")
	if err != nil || next[0].ID == jobs[0].ID {
		t.Fatal("cursor", err)
	}
	if err := s.CancelScheduled(jobs[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Schedule("123@s.whatsapp.net", "new", send, expiry, ""); err != nil {
		t.Fatal("cancel frees active slot", err)
	}
	if _, err := s.ListScheduled(101, "", ""); err == nil {
		t.Fatal("list cap")
	}
	if _, err := s.ClaimScheduled(expiry.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	jobs, err = s.ListScheduled(100, "", "expired")
	if err != nil || len(jobs) != 100 {
		t.Fatal("expiry", err, len(jobs))
	}
	if _, err := s.Schedule("123@s.whatsapp.net", string(make([]byte, 16385)), send, expiry, ""); err == nil {
		t.Fatal("text cap")
	}
	if _, err := s.Schedule("123@s.whatsapp.net", "hi", now.Add(367*24*time.Hour), now.Add(367*24*time.Hour+time.Hour), ""); err == nil {
		t.Fatal("horizon")
	}
	if _, err := s.Schedule("123@s.whatsapp.net", "hi", send, send.Add(25*time.Hour), ""); err == nil {
		t.Fatal("deadline cap")
	}
}

func TestSchedulePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	due := time.Now().Add(time.Hour)
	j, err := s.Schedule("123@s.whatsapp.net", "persistent", due, due.Add(time.Hour), "persist")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordSendBudget(false); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	jobs, err := s.ListScheduled(100, "", "")
	if err != nil || len(jobs) != 1 || jobs[0].ID != j.ID {
		t.Fatal("persistence", err, jobs)
	}
	all, unknown, err := s.SendBudget()
	if err != nil || len(all) != 1 || len(unknown) != 1 {
		t.Fatal("budget persistence", err)
	}
}

func TestScheduleIdempotencyAfterDueAndBoundedRetention(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.db.Exec(schedulerSchema); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	due := now.Add(-time.Minute)
	expiry := now.Add(time.Hour)
	_, err := s.db.Exec(`INSERT INTO scheduled_messages(chat_jid,text,send_at,expires_at,next_attempt,idempotency_key,status,created_at,updated_at) VALUES('123@s.whatsapp.net','same',?,?,?,'replay','sent',?,?)`, due.UnixNano(), expiry.UnixNano(), due.UnixNano(), now.UnixNano(), now.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.Schedule("123@s.whatsapp.net", "same", due, expiry, "replay")
	if err != nil || j.Status != "sent" {
		t.Fatal("idempotent replay after due", err, j)
	}
	if _, err := s.Schedule("123@s.whatsapp.net", "new", due, expiry, ""); err == nil {
		t.Fatal("new past job accepted")
	}
	old := now.Add(-8 * 24 * time.Hour).UnixNano()
	for i := 0; i < 104; i++ {
		_, err = s.db.Exec(`INSERT INTO scheduled_messages(chat_jid,text,send_at,expires_at,next_attempt,status,created_at,updated_at) VALUES('123@s.whatsapp.net','old',?,?,?,'sent',?,?)`, due.UnixNano(), expiry.UnixNano(), due.UnixNano(), old, old)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = s.db.Exec(`INSERT INTO scheduled_messages(chat_jid,text,send_at,expires_at,next_attempt,status,created_at,updated_at) VALUES('123@s.whatsapp.net','review',?,?,?,'uncertain',?,?)`, due.UnixNano(), expiry.UnixNano(), due.UnixNano(), old, old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ClaimScheduled(now); err != nil {
		t.Fatal(err)
	}
	var n int
	s.db.QueryRow(`SELECT count(*) FROM scheduled_messages`).Scan(&n)
	if n != 6 {
		t.Fatal("retention not bounded100", n)
	}
	jobs, err := s.ListScheduled(100, "", "uncertain")
	if err != nil || len(jobs) != 1 {
		t.Fatal("uncertain removed", err, jobs)
	}
}
