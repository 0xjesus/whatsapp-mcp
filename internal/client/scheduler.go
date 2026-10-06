package client

import (
	"context"
	"errors"
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
	return c.store.Schedule(jid.String(), text, send, expiry, key)
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
	sender := c.Send
	if c.scheduledSend != nil {
		sender = c.scheduledSend
	}
	result := sender(sendCtx, job.ChatJID, job.Text)
	status, reason, messageID := "uncertain", "network dispatch result ambiguous; manual review required", ""
	next := now
	if result.Success {
		status, reason, messageID = "sent", "", result.ID
	} else if result.BeforeNetwork {
		status, reason = "failed", "send rejected by safety gate before network dispatch"
		if result.FailureKind == "rate_limit" || result.FailureKind == "offline" {
			delay := result.RetryAfter
			if delay < time.Second {
				delay = time.Second
			}
			// Never shorten RetryAfter. Jobs expire rather than bypassing a long cooldown.
			next = time.Now().Add(delay)
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
