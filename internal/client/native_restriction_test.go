package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func nativeEvent(at time.Time, active bool, until time.Time) *events.NotifyAccountReachoutTimelock {
	return &events.NotifyAccountReachoutTimelock{Mex: events.MexNotificationData{Timestamp: at, OpName: "xwa2_notify_account_reachout_timelock"}, IsActive: active, EnforcementType: "reachout", TimeEnforcementEnds: jsontime.UnixString{Time: until}}
}

func TestNativeEventPausesOutboxAndResumesFIFO(t *testing.T) {
	for _, finite := range []bool{false, true} {
		t.Run(map[bool]string{false: "indefinite", true: "finite"}[finite], func(t *testing.T) {
			c, now := outboxTestClient(t)
			ctx := context.Background()
			var calls []string
			c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
				calls = append(calls, id)
				return whatsmeow.SendResponse{ID: id}, nil
			}
			until := time.Time{}
			if finite {
				until = now.Add(time.Hour)
			}
			c.handleEvent(nativeEvent(*now, true, until))
			a := c.Send(ctx, "447700000001", "first")
			b := c.Send(ctx, "447700000002", "second")
			c.processOutbox(ctx)
			j, _ := c.store.OutboxJob(ctx, a.JobID)
			if len(calls) != 0 || j.State != "queued" || j.Attempts != 0 || !j.NextAttempt.After(*now) {
				t.Fatalf("active native restriction dispatched/lost authorization: calls=%v job=%+v", calls, j)
			}
			if got := c.HealthSnapshot()["state"]; got != HealthRestricted {
				t.Fatalf("native signal hidden in health: %v", got)
			}
			*now = now.Add(2 * time.Second)
			c.handleEvent(&events.Connected{})
			c.noteSendOK()
			if c.sendGate(true) == nil {
				t.Fatal("reconnect or success erased native restriction")
			}
			c.handleEvent(nativeEvent(*now, true, until))
			c.handleEvent(nativeEvent(now.Add(-time.Second), false, time.Time{}))
			if c.sendGate(true) == nil {
				t.Fatal("stale inactive notification cleared newer restriction")
			}
			*now = now.Add(time.Second)
			c.handleEvent(nativeEvent(*now, false, time.Time{}))
			c.processOutbox(ctx)
			j, _ = c.store.OutboxJob(ctx, a.JobID)
			if j.State != "sent" || len(calls) != 1 {
				t.Fatalf("early explicit release did not resume first: %+v calls=%v", j, calls)
			}
			c.processOutbox(ctx)
			j, _ = c.store.OutboxJob(ctx, b.JobID)
			if j.State != "queued" || len(calls) != 1 {
				t.Fatalf("release bypassed shared spacing: %+v calls=%v", j, calls)
			}
			*now = j.NextAttempt
			c.processOutbox(ctx)
			j, _ = c.store.OutboxJob(ctx, b.JobID)
			if j.State != "sent" || len(calls) != 2 {
				t.Fatalf("second job lost: %+v calls=%v", j, calls)
			}
		})
	}
}

func TestNativeFiniteExpiryAndPersistence(t *testing.T) {
	c, now := outboxTestClient(t)
	c.handleEvent(nativeEvent(*now, true, now.Add(time.Minute)))
	restored := newDisconnectedClient()
	restored.store = c.store
	restored.clock = c.clock
	if err := restored.initSendProtection(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restored.sendGate(true) == nil {
		t.Fatal("restart erased native restriction")
	}
	*now = now.Add(time.Minute)
	if err := restored.sendGate(false); err != nil {
		t.Fatalf("finite expiry did not release: %v", err)
	}
}

func TestNativeReleaseDoesNotClearBaseSafety(t *testing.T) {
	for _, state := range []HealthState{HealthRestricted, HealthTempBanned, HealthLoggedOut, HealthOutdated} {
		t.Run(string(state), func(t *testing.T) {
			c, now := outboxTestClient(t)
			until := now.Add(time.Hour)
			c.setHealth(state, until, "base restriction", true)
			c.handleEvent(nativeEvent(*now, true, time.Time{}))
			*now = now.Add(time.Second)
			c.handleEvent(nativeEvent(*now, false, time.Time{}))
			if c.sendGate(true) == nil {
				t.Fatal("native release erased base safety")
			}
			if got := c.HealthSnapshot()["state"]; got != state {
				t.Fatalf("stronger state hidden: %v", got)
			}
		})
	}
}

func TestLateWeakerRefusalCannotEraseStrongBaseState(t *testing.T) {
	for _, state := range []HealthState{HealthTempBanned, HealthLoggedOut, HealthOutdated} {
		t.Run(string(state), func(t *testing.T) {
			c, now := outboxTestClient(t)
			c.setHealth(state, now.Add(time.Hour), "strong block", true)
			c.noteSendError(fmt.Errorf("%w %d", whatsmeow.ErrServerReturnedError, 463))
			if got := c.HealthSnapshot()["state"]; got != state {
				t.Fatalf("late refusal erased strong block: %v", got)
			}
		})
	}
	c, now := outboxTestClient(t)
	c.setHealth(HealthRestricted, now.Add(time.Hour), "all recipient cooldown", true)
	c.noteSendError(fmt.Errorf("%w %d", whatsmeow.ErrServerReturnedError, 463))
	if c.sendGate(true) == nil {
		t.Fatal("new-contact refusal weakened all-recipient cooldown")
	}
}

func TestRegisteredNativeEventHandler(t *testing.T) {
	c, now := outboxTestClient(t)
	c.wa = &whatsmeow.Client{}
	c.StartEventHandler()
	id := c.handlerID
	c.StartEventHandler()
	if id != c.handlerID {
		t.Fatal("registered duplicate handler")
	}
	if c.wa.DangerousInternals().DispatchEvent(nativeEvent(*now, true, time.Time{})) {
		t.Fatal("registered handler panicked")
	}
	if c.sendGate(true) == nil {
		t.Fatal("registered event switch ignored native restriction")
	}
}

func TestRecipientBudgetProductionDispatch(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	r := c.Send(ctx, "447700000001:4@s.whatsapp.net", "authorized")
	c.processOutbox(ctx)
	j, _ := c.store.OutboxJob(ctx, r.JobID)
	if j.State != "sent" {
		t.Fatalf("setup send: %+v", j)
	}
	var recipient string
	if err := c.store.DB().QueryRow(`SELECT recipient FROM outbound_attempts`).Scan(&recipient); err != nil {
		t.Fatal(err)
	}
	if recipient != "447700000001@s.whatsapp.net" {
		t.Fatalf("production outbox skipped canonical recipient budget: %q", recipient)
	}
	*now = now.Add(2 * time.Minute)
	if _, err := c.sendFeatureMessage(ctx, types.JID{User: "447700000002", Device: 7, Server: types.DefaultUserServer}, &waProto.Message{}); err != nil {
		t.Fatal(err)
	}
	if err := c.store.DB().QueryRow(`SELECT recipient FROM outbound_attempts ORDER BY at_ns DESC LIMIT 1`).Scan(&recipient); err != nil {
		t.Fatal(err)
	}
	if recipient != "447700000002@s.whatsapp.net" {
		t.Fatalf("feature skipped canonical recipient budget: %q", recipient)
	}
}

func TestNativeRestrictionDuringMediaPreparation(t *testing.T) {
	for _, uploadFails := range []bool{false, true} {
		t.Run(fmt.Sprint(uploadFails), func(t *testing.T) {
			c, now := outboxTestClient(t)
			ctx := context.Background()
			calls := 0
			c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
				calls++
				return whatsmeow.SendResponse{ID: id}, nil
			}
			uploads := 0
			c.uploadMedia = func(_ context.Context, b []byte, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
				uploads++
				if uploads == 1 {
					c.handleEvent(nativeEvent(*now, true, time.Time{}))
					if uploadFails {
						return whatsmeow.UploadResponse{}, errors.New("upload interrupted")
					}
				}
				return whatsmeow.UploadResponse{URL: "https://example.invalid/media", FileLength: uint64(len(b))}, nil
			}
			r := c.enqueueSnapshot(ctx, "447700000001", outboundPayload{MediaPath: "image.png"}, []byte("image"), nil)
			c.processOutbox(ctx)
			j, _ := c.store.OutboxJob(ctx, r.JobID)
			if j.State != "queued" || calls != 0 || j.Attempts != 0 {
				t.Fatalf("restriction at media boundary lost job or dispatched: %+v calls=%d", j, calls)
			}
			*now = now.Add(2 * time.Second)
			c.handleEvent(nativeEvent(*now, false, time.Time{}))
			c.processOutbox(ctx)
			j, _ = c.store.OutboxJob(ctx, r.JobID)
			if j.State != "sent" || calls != 1 {
				t.Fatalf("media did not resume: %+v calls=%d", j, calls)
			}
		})
	}
}

func TestNativeRestrictionScheduledResume(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	*now = time.Now().Add(time.Minute)
	sendAt := *now
	j, err := c.store.Schedule("447700000001@s.whatsapp.net", "scheduled", sendAt, sendAt.Add(time.Hour), "", c.pairedSender())
	if err != nil {
		t.Fatal(err)
	}
	c.handleEvent(nativeEvent(*now, true, time.Time{}))
	c.runScheduled(ctx, *now)
	jobs, err := c.store.ListScheduled(100, "", "pending")
	if err != nil || len(jobs) != 1 || jobs[0].Attempts != 0 {
		t.Fatalf("scheduled restriction lost authorization: %+v %v", jobs, err)
	}
	if !jobs[0].SendAt.Equal(sendAt) || !jobs[0].ExpiresAt.Equal(j.ExpiresAt) {
		t.Fatal("native pause changed authorization window")
	}
	*now = now.Add(2 * time.Second)
	c.handleEvent(nativeEvent(*now, false, time.Time{}))
	c.runScheduled(ctx, *now)
	jobs, err = c.store.ListScheduled(100, "", "sent")
	if err != nil || len(jobs) != 1 || jobs[0].Attempts != 1 {
		t.Fatalf("scheduled job did not resume: %+v %v", jobs, err)
	}
	var recipient string
	if err = c.store.DB().QueryRow(`SELECT recipient FROM outbound_attempts`).Scan(&recipient); err != nil {
		t.Fatal(err)
	}
	if recipient != j.ChatJID {
		t.Fatalf("scheduler skipped recipient budget: %q", recipient)
	}
}

func TestNativePersistenceErrorRemainsFailClosedAfterBaseSave(t *testing.T) {
	c, now := outboxTestClient(t)
	_, err := c.store.DB().Exec(`CREATE TRIGGER reject_native BEFORE INSERT ON outbound_protection WHEN NEW.key='native_restriction' BEGIN SELECT RAISE(ABORT,'injected persistence failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	c.handleEvent(nativeEvent(*now, true, now.Add(time.Second)))
	c.noteSendOK()
	*now = now.Add(2 * time.Second)
	if c.sendGate(true) == nil {
		t.Fatal("base successful persistence erased native persistence error")
	}
}

func TestRecipientDailyQuotaRetainsOutboxUntilExactExpiry(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	recipient := "447700000001@s.whatsapp.net"
	oldest := now.Add(-23 * time.Hour)
	for i := 0; i < 20; i++ {
		if _, err := c.store.DB().Exec(`INSERT INTO outbound_attempts(at_ns,known,recipient) VALUES(?,0,?)`, oldest.Add(time.Duration(i)*time.Minute).UnixNano(), recipient); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
		calls++
		return whatsmeow.SendResponse{ID: id}, nil
	}
	a := c.Send(ctx, recipient, "first")
	b := c.Send(ctx, "447700000002", "second")
	c.processOutbox(ctx)
	j, _ := c.store.OutboxJob(ctx, a.JobID)
	if j.State != "queued" || j.Attempts != 0 || calls != 0 || !j.NextAttempt.Equal(oldest.Add(24*time.Hour)) {
		t.Fatalf("quota not retained to exact expiry: %+v calls=%d", j, calls)
	}
	*now = j.NextAttempt.Add(-time.Nanosecond)
	c.processOutbox(ctx)
	if calls != 0 {
		t.Fatal("daily quota expired early")
	}
	*now = now.Add(time.Nanosecond)
	c.processOutbox(ctx)
	j, _ = c.store.OutboxJob(ctx, a.JobID)
	if j.State != "sent" || calls != 1 {
		t.Fatalf("capacity expiry did not resume: %+v", j)
	}
	c.processOutbox(ctx)
	j, _ = c.store.OutboxJob(ctx, b.JobID)
	if j.State != "queued" || calls != 1 || j.NextAttempt.Sub(*now) != 90*time.Second {
		t.Fatalf("daily release bypassed shared spacing: %+v", j)
	}
}

func TestRecipientBudgetLIDAliasesShareFeatureQuota(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	waDB, err := sql.Open("sqlite3", filepath.Join(c.store.Dir(), "whatsapp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer waDB.Close()
	if _, err = waDB.Exec(`CREATE TABLE whatsmeow_lid_map(lid TEXT PRIMARY KEY,pn TEXT);INSERT INTO whatsmeow_lid_map VALUES('99887766','447700000001')`); err != nil {
		t.Fatal(err)
	}
	jid := types.JID{User: "99887766", Device: 4, Server: types.HiddenUserServer}
	if _, err = c.sendFeatureMessage(ctx, jid, &waProto.Message{}); err != nil {
		t.Fatal(err)
	}
	var recipient string
	if err = c.store.DB().QueryRow(`SELECT recipient FROM outbound_attempts`).Scan(&recipient); err != nil {
		t.Fatal(err)
	}
	if recipient != "447700000001@s.whatsapp.net" {
		t.Fatalf("LID alias escaped phone quota: %q", recipient)
	}
	for i := 0; i < 19; i++ {
		if _, err = c.store.DB().Exec(`INSERT INTO outbound_attempts(at_ns,known,recipient) VALUES(?,0,?)`, now.Add(-2*time.Hour-time.Duration(i)*time.Minute).UnixNano(), recipient); err != nil {
			t.Fatal(err)
		}
	}
	*now = now.Add(90 * time.Second)
	if _, err = c.sendFeatureMessage(ctx, types.JID{User: "447700000001", Server: types.DefaultUserServer}, &waProto.Message{}); err == nil {
		t.Fatal("phone alias bypassed LID's daily recipient quota")
	}
}

func TestNativeRestartKeepsOrderingAndQueuedAuthorization(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	c.handleEvent(nativeEvent(*now, true, time.Time{}))
	r := c.Send(ctx, "447700000001", "authorized")
	c.processOutbox(ctx)
	dir := c.store.Dir()
	c.store.Close()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	restored := newDisconnectedClient()
	restored.store, restored.clock, restored.online = s, c.clock, c.online
	restored.senderIdentity, restored.networkSend, restored.redactor = c.senderIdentity, c.networkSend, c.redactor
	c = restored
	if err = c.initSendProtection(ctx); err != nil {
		t.Fatal(err)
	}
	c.handleEvent(nativeEvent(now.Add(-time.Second), false, time.Time{}))
	*now = now.Add(2 * time.Second)
	c.processOutbox(ctx)
	j, _ := s.OutboxJob(ctx, r.JobID)
	if j.State != "queued" || j.Attempts != 0 {
		t.Fatalf("restart lost restriction or authorization: %+v", j)
	}
	c.handleEvent(nativeEvent(*now, false, time.Time{}))
	*now = now.Add(time.Second)
	c.processOutbox(ctx)
	j, _ = s.OutboxJob(ctx, r.JobID)
	if j.State != "sent" || j.Attempts != 1 {
		t.Fatalf("restarted queue failed to resume: %+v", j)
	}
}

func TestNativeReleaseLeavesTerminalAndBaseCooldownJobsAlone(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	for _, state := range []string{"blocked", "needs_review", "cancelled", "failed"} {
		r := c.Send(ctx, "447700000001", state)
		if err := c.store.UpdateOutbox(ctx, r.JobID, state, "manual review or terminal", "", *now); err != nil {
			t.Fatal(err)
		}
	}
	c.setHealth(HealthRestricted, now.Add(30*time.Minute), "server rate-overlimit (429)", true)
	r := c.Send(ctx, "447700000002", "waiting")
	c.handleEvent(nativeEvent(*now, true, time.Time{}))
	c.processOutbox(ctx)
	j, _ := c.store.OutboxJob(ctx, r.JobID)
	fullCooldown := j.NextAttempt
	*now = now.Add(time.Second)
	c.handleEvent(nativeEvent(*now, false, time.Time{}))
	c.processOutbox(ctx)
	j, _ = c.store.OutboxJob(ctx, r.JobID)
	if j.State != "queued" || j.Attempts != 0 || !j.NextAttempt.Equal(fullCooldown) {
		t.Fatalf("native release bypassed server cooldown: %+v", j)
	}
	jobs, err := c.store.ListOutbox(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		if job.JobID != r.JobID && job.State == "queued" {
			t.Fatalf("terminal job resurrected: %+v", job)
		}
	}
}

func TestNativeScheduledRestrictionAtFinalGateRetainsJob(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	*now = time.Now().Add(time.Minute)
	if _, err := c.store.Schedule("447700000001@s.whatsapp.net", "scheduled", *now, now.Add(time.Hour), "", c.pairedSender()); err != nil {
		t.Fatal(err)
	}
	checks := 0
	c.online = func() bool {
		checks++
		if checks == 2 {
			c.handleEvent(nativeEvent(*now, true, time.Time{}))
		}
		return true
	}
	c.runScheduled(ctx, *now)
	jobs, err := c.store.ListScheduled(100, "", "pending")
	if err != nil || len(jobs) != 1 || jobs[0].Attempts != 0 {
		t.Fatalf("late scheduled restriction lost job: %+v %v", jobs, err)
	}
	*now = now.Add(2 * time.Second)
	c.handleEvent(nativeEvent(*now, false, time.Time{}))
	c.runScheduled(ctx, *now)
	jobs, err = c.store.ListScheduled(100, "", "sent")
	if err != nil || len(jobs) != 1 || jobs[0].Attempts != 1 {
		t.Fatalf("late-gated schedule did not resume: %+v %v", jobs, err)
	}
}

func TestNativeFiniteExpiryAutomaticallyResumesOutbox(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	until := now.Add(time.Minute)
	c.handleEvent(nativeEvent(*now, true, until))
	r := c.Send(ctx, "447700000001", "authorized")
	c.processOutbox(ctx)
	*now = until.Add(-time.Nanosecond)
	c.processOutbox(ctx)
	j, _ := c.store.OutboxJob(ctx, r.JobID)
	if j.State != "queued" || j.Attempts != 0 || !j.NextAttempt.Equal(until) {
		t.Fatalf("expiry sent early: %+v", j)
	}
	*now = until
	c.processOutbox(ctx)
	j, _ = c.store.OutboxJob(ctx, r.JobID)
	if j.State != "sent" || j.Attempts != 1 {
		t.Fatalf("finite expiry did not automatically resume: %+v", j)
	}
}

func TestInFlightBanThenTemporaryRefusalRequiresReview(t *testing.T) {
	for _, scheduled := range []bool{false, true} {
		t.Run(fmt.Sprint(scheduled), func(t *testing.T) {
			c, now := outboxTestClient(t)
			ctx := context.Background()
			*now = time.Now().Add(time.Minute)
			c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, _ string) (whatsmeow.SendResponse, error) {
				c.setHealth(HealthTempBanned, now.Add(time.Hour), "ban during network request", true)
				return whatsmeow.SendResponse{}, fmt.Errorf("%w 429", whatsmeow.ErrServerReturnedError)
			}
			if scheduled {
				if _, err := c.store.Schedule("447700000001@s.whatsapp.net", "scheduled", *now, now.Add(2*time.Hour), "", c.pairedSender()); err != nil {
					t.Fatal(err)
				}
				c.runScheduled(ctx, *now)
				jobs, err := c.store.ListScheduled(100, "", "failed")
				if err != nil || len(jobs) != 1 {
					t.Fatalf("in-flight ban must leave scheduled job for review: %+v %v", jobs, err)
				}
			} else {
				r := c.Send(ctx, "447700000001", "authorized")
				c.processOutbox(ctx)
				j, _ := c.store.OutboxJob(ctx, r.JobID)
				if j.State != "blocked" {
					t.Fatalf("in-flight ban must block review job, not retry after ban expiry: %+v", j)
				}
			}
		})
	}
}
