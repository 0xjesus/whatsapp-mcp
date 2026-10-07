package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
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
	c.invalidateMonitoringPolicy("123@g.us")
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

func TestScheduledUsesSynchronousDeliveryAndSharedBudget(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	j, e := c.store.Schedule("123@s.whatsapp.net", "scheduled", time.Now().Add(time.Second), time.Now().Add(time.Hour), "", c.pairedSender())
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
		calls++
		return whatsmeow.SendResponse{ID: id}, nil
	}
	c.store.PersistentSendBudget(ctx, *now, false, true)
	c.runScheduled(ctx, time.Now().Add(2*time.Second))
	jobs, _ := c.store.ListScheduled(100, "", "pending")
	out, _ := c.GetOutbox(ctx, "", 100)
	if len(jobs) != 1 || calls != 0 || len(out) != 0 {
		t.Fatalf("throttle orphan: jobs=%+v out=%+v calls=%d", jobs, out, calls)
	}
	*now = now.Add(90 * time.Second)
	c.runScheduled(ctx, time.Now().Add(2*time.Minute))
	jobs, _ = c.store.ListScheduled(100, "", "sent")
	if len(jobs) != 1 || jobs[0].ID != j.ID || jobs[0].MessageID == "" || calls != 1 {
		t.Fatalf("scheduled resume: %+v calls=%d", jobs, calls)
	}
	r := c.Send(ctx, "123@s.whatsapp.net", "immediate")
	c.processOutbox(ctx)
	o, _ := c.store.OutboxJob(ctx, r.JobID)
	if calls != 1 || o.State != "queued" {
		t.Fatalf("shared limit: %+v calls=%d", o, calls)
	}
}

func TestScheduledSenderDeadlineAndFiniteRefusals(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	real := time.Now()
	j, e := c.store.Schedule("123@s.whatsapp.net", "scheduled", real.Add(time.Second), real.Add(time.Hour*3), "", c.pairedSender())
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
		calls++
		return whatsmeow.SendResponse{}, fmt.Errorf("outer: %w", fmt.Errorf("%w %d", whatsmeow.ErrServerReturnedError, 429))
	}
	*now = real.Add(2 * time.Second)
	for attempt := 1; attempt <= 3; attempt++ {
		c.runScheduled(ctx, *now)
		jobs, _ := c.store.ListScheduled(100, "", "")
		if len(jobs) != 1 {
			t.Fatal(jobs)
		}
		got := jobs[0]
		if attempt < 3 {
			if got.Status != "pending" || got.NextAttempt.Sub(*now) != time.Duration(attempt)*30*time.Minute {
				t.Fatalf("attempt%d: %+v", attempt, got)
			}
			*now = got.NextAttempt
		} else if got.Status != "failed" || got.Attempts != 3 {
			t.Fatalf("exhausted: %+v", got)
		}
	}
	if calls != 3 {
		t.Fatal(calls)
	}
	// A changed sender cannot inherit an old authorization.
	j, e = c.store.Schedule("123@s.whatsapp.net", "sender test", real.Add(time.Second), real.Add(time.Hour), "", c.pairedSender())
	if e != nil {
		t.Fatal(e)
	}
	c.senderIdentity = func() string { return "different@s.whatsapp.net" }
	c.runScheduled(ctx, real.Add(time.Second*2))
	jobs, _ := c.store.ListScheduled(100, "", "failed")
	if len(jobs) != 2 || calls != 3 {
		t.Fatalf("sender %+v calls%d", jobs, calls)
	}
	// Cancelled caller context returns before the network boundary.
	c.senderIdentity = func() string { return j.Sender }
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	r := c.sendScheduledNow(cancelled, *j)
	if !r.BeforeNetwork || calls != 3 {
		t.Fatal(r, calls)
	}
}
