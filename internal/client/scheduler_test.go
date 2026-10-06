package client

import (
	"context"
	"testing"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
)

func scheduledTestClient(t *testing.T) (*Client, *store.Store) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return &Client{store: s}, s
}
func TestScheduledWorkerNeverEarlyAndMarksNetworkBoundary(t *testing.T) {
	c, s := scheduledTestClient(t)
	due := time.Now().Add(time.Minute)
	j, err := s.Schedule("123@s.whatsapp.net", "test", due, due.Add(time.Hour), "")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.scheduledSend = func(ctx context.Context, chat, text string) SendResult {
		calls++
		hook := ctx.Value(scheduledDispatchKey{}).(func() error)
		if err := hook(); err != nil {
			t.Fatal(err)
		}
		return SendResult{Success: true, ID: "fake-message"}
	}
	c.runScheduled(context.Background(), due.Add(-time.Nanosecond))
	if calls != 0 {
		t.Fatal("early delivery")
	}
	c.runScheduled(context.Background(), due)
	if calls != 1 {
		t.Fatal("due job not dispatched")
	}
	jobs, err := s.ListScheduled(100, "", "sent")
	if err != nil || len(jobs) != 1 || jobs[0].ID != j.ID || jobs[0].MessageID != "fake-message" || jobs[0].Attempts != 1 {
		t.Fatal("sent result", err, jobs)
	}
	c.runScheduled(context.Background(), due.Add(time.Second))
	if calls != 1 {
		t.Fatal("duplicate send")
	}
}
func TestScheduledCooldownAndAmbiguousFailure(t *testing.T) {
	c, s := scheduledTestClient(t)
	now := time.Now()
	_, err := s.Schedule("123@s.whatsapp.net", "test", now.Add(time.Second), now.Add(time.Hour), "")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.scheduledSend = func(ctx context.Context, chat, text string) SendResult {
		calls++
		return SendResult{BeforeNetwork: true, FailureKind: "rate_limit", RetryAfter: 5 * time.Minute}
	}
	c.runScheduled(context.Background(), now.Add(2*time.Second))
	jobs, err := s.ListScheduled(100, "", "pending")
	if err != nil || len(jobs) != 1 || jobs[0].Attempts != 0 || jobs[0].NextAttempt.Before(now.Add(5*time.Minute)) {
		t.Fatal("cooldown", err, jobs)
	}
	c.runScheduled(context.Background(), now.Add(time.Minute))
	if calls != 1 {
		t.Fatal("cooldown ignored")
	}
	c.scheduledSend = func(ctx context.Context, chat, text string) SendResult {
		calls++
		if err := ctx.Value(scheduledDispatchKey{}).(func() error)(); err != nil {
			t.Fatal(err)
		}
		return SendResult{Message: "ambiguous error with private body"}
	}
	c.runScheduled(context.Background(), now.Add(6*time.Minute))
	jobs, err = s.ListScheduled(100, "", "uncertain")
	if err != nil || len(jobs) != 1 || jobs[0].Attempts != 1 {
		t.Fatal("uncertain", err, jobs)
	}
	if jobs[0].Error == "ambiguous error with private body" {
		t.Fatal("error leaked message body")
	}
	c.runScheduled(context.Background(), now.Add(7*time.Minute))
	if calls != 2 {
		t.Fatal("uncertain retried")
	}
}
func TestScheduledTimesRequireTimezoneAndFuture(t *testing.T) {
	for _, input := range []string{"2026-10-06T12:00:00", "invalid"} {
		if _, _, err := scheduledTimes(input, ""); err == nil {
			t.Fatal(input)
		}
	}
}

func TestMonitoringGenerationRejectsStaleSnapshot(t *testing.T) {
	c, s := scheduledTestClient(t)
	groups := []store.MonitoringGroup{{JID: "123@g.us", Members: 10}}
	if err := s.ReconcileMonitoring(groups); err != nil {
		t.Fatal(err)
	}
	s.DB().Exec(`UPDATE group_monitoring_policy SET enabled=1`)
	if err := c.publishMonitoringPolicy(0, groups); err != nil {
		t.Fatal(err)
	}
	if !s.MonitoringAllowed("123@g.us") {
		t.Fatal("initial grant")
	}
	c.invalidateMonitoringPolicy()
	if err := c.publishMonitoringPolicy(0, groups); err != nil {
		t.Fatal(err)
	}
	if s.MonitoringAllowed("123@g.us") {
		t.Fatal("stale metadata reinstated automatic permission")
	}
	if err := c.publishMonitoringPolicy(1, []store.MonitoringGroup{{JID: "123@g.us", Members: 11}}); err != nil {
		t.Fatal(err)
	}
	if s.MonitoringAllowed("123@g.us") {
		t.Fatal("over threshold")
	}
}
