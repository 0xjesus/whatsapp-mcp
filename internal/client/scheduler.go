package client

import (
	"context"
	"errors"
	"fmt"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
	"strings"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
)

type scheduledDispatchKey struct{}

// ScheduleMessage persists a bounded text job after checking current send safety.
func (c *Client) ScheduleMessage(chat, text, sendAt, expiresAt, key string) (*store.ScheduledMessage, error) {
	jid, err := parseRecipient(chat)
	if err != nil {
		return nil, err
	}
	if err = c.sendGate(c.IsKnownContact(jid)); err != nil {
		return nil, err
	}
	send, expiry, err := scheduledTimes(sendAt, expiresAt)
	if err != nil {
		return nil, err
	}
	sender := c.pairedSender()
	if sender == "" {
		return nil, errors.New("paired sender unavailable")
	}
	return c.store.Schedule(jid.String(), text, send, expiry, key, sender)
}
func scheduledTimes(sendAt, expiresAt string) (time.Time, time.Time, error) {
	send, err := time.Parse(time.RFC3339, sendAt)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("send_at requires RFC3339 with timezone")
	}
	expiry := send.Add(24 * time.Hour)
	if expiresAt != "" {
		expiry, err = time.Parse(time.RFC3339, expiresAt)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("expires_at requires RFC3339 with timezone")
		}
	}
	return send, expiry, nil
}
func (c *Client) RescheduleMessage(id, sendAt, expiresAt string) error {
	send, expiry, err := scheduledTimes(sendAt, expiresAt)
	if err != nil {
		return err
	}
	if !send.After(time.Now()) {
		return errors.New("send_at must be in the future")
	}
	return c.store.Reschedule(id, send, expiry)
}
func (c *Client) ListScheduledMessages(limit int, cursor, status string) ([]store.ScheduledMessage, error) {
	return c.store.ListScheduled(limit, cursor, status)
}
func (c *Client) CancelScheduledMessage(id string) error { return c.store.CancelScheduled(id) }

// StartScheduler uses one polling worker; there are no goroutines or timers per job.
func (c *Client) StartScheduler(ctx context.Context) error {
	var startErr error
	c.schedulerOnce.Do(func() {
		if startErr = c.store.BindLegacyScheduledSender(c.pairedSender()); startErr != nil {
			return
		}
		if startErr = c.store.RecoverScheduled(); startErr != nil {
			return
		}
		workerCtx, cancel := context.WithCancel(ctx)
		c.schedulerCancel = cancel
		c.schedulerDone = make(chan struct{})
		go func() {
			defer close(c.schedulerDone)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-ticker.C:
					c.runScheduled(workerCtx, time.Now())
				}
			}
		}()
	})
	return startErr
}
func (c *Client) runScheduled(ctx context.Context, now time.Time) {
	job, err := c.store.ClaimScheduled(now)
	if err != nil || job == nil {
		return
	}
	deadline := time.Now().Add(time.Minute)
	if job.ExpiresAt.Before(deadline) {
		deadline = job.ExpiresAt
	}
	sendCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	sendCtx = context.WithValue(sendCtx, scheduledDispatchKey{}, func() error { return c.store.MarkScheduledDispatch(job.ID) })
	sender := func(ctx context.Context, chat, text string) SendResult { return c.sendScheduledNow(ctx, *job) }
	if c.scheduledSend != nil {
		sender = c.scheduledSend
	}
	result := sender(sendCtx, job.ChatJID, job.Text)
	status, reason, messageID := "uncertain", "network dispatch result ambiguous; manual review required", ""
	next := now
	if result.Success {
		status, reason, messageID = "sent", "", result.ID
	} else if result.FailureKind == "temporary_refusal" {
		status, reason = "failed", "definite server refusal; attempt limit reached"
		if job.Attempts+1 < 3 {
			next = c.currentTime().Add(result.RetryAfter)
			status, reason = "pending", "waiting full account cooldown after definite refusal"
			if !next.Before(job.ExpiresAt) {
				status, reason = "expired", "cooldown exceeds delivery deadline"
			}
		}
	} else if result.BeforeNetwork {
		status, reason = "failed", "send rejected by safety gate before network dispatch"
		if result.FailureKind == "rate_limit" || result.FailureKind == "offline" {
			delay := result.RetryAfter
			if delay < time.Second {
				delay = time.Second
			}
			// Never shorten RetryAfter. Jobs expire rather than bypassing a long cooldown.
			next = c.currentTime().Add(delay)
			status, reason = "pending", "waiting for send safety cooldown"
			if !next.Before(job.ExpiresAt) {
				status, reason = "expired", "cooldown exceeds delivery deadline"
			}
		}
	}
	if !time.Now().Before(job.ExpiresAt) && status == "pending" {
		status, reason = "expired", "delivery deadline passed"
	}
	// Failure here leaves claimed/dispatching for conservative restart recovery.
	_ = c.store.FinishScheduled(job.ID, status, messageID, reason, next)
}

// StopScheduler waits for the single worker to stop before closing its store.
func (c *Client) StopScheduler() {
	if c.schedulerCancel != nil {
		c.schedulerCancel()
		<-c.schedulerDone
	}
}

// sendScheduledNow keeps the scheduler as sole durable owner and shares the
// outbox dispatch mutex/budget. Accepted asynchronous sends must never reach here.
func (c *Client) sendScheduledNow(ctx context.Context, job store.ScheduledMessage) SendResult {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	reject := func(kind string, delay time.Duration, reason string) SendResult {
		return SendResult{BeforeNetwork: true, FailureKind: kind, RetryAfter: delay, Message: reason}
	}
	if ctx.Err() != nil {
		return reject("offline", time.Minute, "send context cancelled")
	}
	jid, err := parseRecipient(job.ChatJID)
	if err != nil {
		return reject("", 0, err.Error())
	}
	sender := c.pairedSender()
	if sender == "" || job.Sender != sender {
		return reject("", 0, "scheduled authorization belongs to a different or unavailable sender")
	}
	known := c.IsKnownContact(jid)
	if err = c.sendGate(known); err != nil {
		c.health.mu.Lock()
		until, state, reason := c.health.until, c.health.state, c.health.reason
		c.health.mu.Unlock()
		if state == HealthRestricted && !until.IsZero() && !strings.HasPrefix(reason, "circuit breaker:") {
			return reject("rate_limit", until.Sub(c.currentTime()), err.Error())
		}
		return reject("", 0, err.Error())
	}
	if !c.outboxConnected() {
		return reject("offline", time.Minute, "transport disconnected")
	}
	if d, e := c.store.PersistentSendBudget(ctx, c.currentTime(), known, false); e != nil {
		return reject("", 0, e.Error())
	} else if !d.Allowed {
		return reject("rate_limit", d.RetryAfter, d.Reason)
	}
	msg := &waProto.Message{Conversation: proto.String(job.Text)}
	if c.networkSend == nil {
		msg.MessageContextInfo = c.ephemeralContextInfo(ctx, jid)
		c.humanizeBeforeSend(ctx, jid, len(job.Text), false)
	}
	if c.pairedSender() != sender {
		return reject("", 0, "paired sender changed before dispatch")
	}
	if ctx.Err() != nil || !c.outboxConnected() {
		return reject("offline", time.Minute, "send context cancelled or disconnected")
	}
	if err = c.sendGate(known); err != nil {
		return reject("", 0, "health changed before dispatch")
	}
	id := job.MessageID
	if id == "" {
		id = whatsmeow.GenerateMessageID()
	}
	if err = c.store.PrepareScheduledID(job.ID, id); err != nil {
		return reject("", 0, err.Error())
	}
	d, e := c.reserveSend(ctx, known)
	if e != nil {
		return reject("", 0, e.Error())
	}
	if !d.Allowed {
		return reject("rate_limit", d.RetryAfter, d.Reason)
	}
	if ctx.Err() != nil {
		return reject("offline", time.Minute, "send context cancelled before network boundary")
	}
	if hook, ok := ctx.Value(scheduledDispatchKey{}).(func() error); !ok {
		return reject("", 0, "scheduled boundary hook absent")
	} else if err = hook(); err != nil {
		return reject("", 0, "scheduled dispatch state changed")
	}
	var resp whatsmeow.SendResponse
	if c.networkSend != nil {
		resp, err = c.networkSend(ctx, jid, msg, id)
	} else {
		resp, err = c.wa.SendMessage(ctx, jid, msg, whatsmeow.SendRequestExtra{ID: id})
	}
	if err != nil {
		c.noteSendError(err)
		if code, definite := definiteRefusalCode(err); definite {
			if code == 429 || code == 463 || code == 475 {
				c.health.mu.Lock()
				until := c.health.until
				c.health.mu.Unlock()
				if job.Attempts > 0 {
					until = c.currentTime().Add(until.Sub(c.currentTime()) * time.Duration(job.Attempts+1))
					c.setHealth(HealthRestricted, until, "repeated definite temporary server refusal", true)
				}
				return SendResult{FailureKind: "temporary_refusal", RetryAfter: until.Sub(c.currentTime())}
			}
			return SendResult{BeforeNetwork: true, Message: "server refused delivery"}
		}
		return SendResult{Message: "network outcome uncertain"}
	}
	c.noteSendOK()
	if resp.ID != id {
		return SendResult{Message: "unexpected network message ID"}
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err = c.persistSent(saveCtx, jid, id, job.Text, "", msg); err != nil {
		return SendResult{Success: true, ID: id, Message: fmt.Sprintf("delivered; outgoing cache failed: %v", err)}
	}
	return SendResult{Success: true, ID: id}
}
