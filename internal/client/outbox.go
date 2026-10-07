package client

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/ratelimit"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

type outboundPayload struct {
	Body        string
	MediaPath   string // basename only after snapshot; never re-read at delivery
	ViewOnce    bool
	ReplyID     string
	ReplySender string
	MarkRead    bool
}

func (c *Client) currentTime() time.Time {
	if c.clock != nil {
		return c.clock()
	}
	return time.Now()
}
func (c *Client) outboxConnected() bool {
	if c.online != nil {
		return c.online()
	}
	return c.IsConnected()
}

type sendOptionsKey struct{}
type SendOptions struct {
	IdempotencyKey string
	MediaName      string // stable name for transcoded temporary audio
	MarkRead       bool
}

// WithSendOptions carries durable submission options through legacy send APIs.
func WithSendOptions(ctx context.Context, opts SendOptions) context.Context {
	return context.WithValue(ctx, sendOptionsKey{}, opts)
}
func (c *Client) pairedSender() string {
	if c.senderIdentity != nil {
		return c.senderIdentity()
	}
	if c.wa != nil && c.wa.Store != nil && c.wa.Store.ID != nil {
		return c.wa.Store.ID.ToNonAD().String()
	}
	return ""
}

// StartOutbox is called only by serve, with the daemon lifetime context. Login
// and smoke must not dispatch old jobs. Exactly one worker owns delivery.
func (c *Client) StartOutbox(ctx context.Context) error {
	if err := c.store.RecoverOutbox(ctx); err != nil {
		return err
	}
	c.outboxOnce.Do(func() {
		c.outboxWake = make(chan struct{}, 1)
		c.outboxDone = make(chan struct{})
		go func() {
			defer close(c.outboxDone)
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				if ctx.Err() != nil {
					return
				}
				c.processOutbox(ctx)
				select {
				case <-ctx.Done():
					return
				case <-c.outboxWake:
				case <-tick.C:
				}
			}
		}()
	})
	return nil
}

// WaitOutbox ensures store teardown cannot race a worker's final bookkeeping.
func (c *Client) WaitOutbox() {
	if c.outboxDone != nil {
		<-c.outboxDone
	}
}

func (c *Client) enqueueSend(ctx context.Context, recipient string, p outboundPayload) SendResult {
	jid, err := parseRecipient(recipient)
	if err != nil {
		return SendResult{Message: err.Error(), Status: "rejected"}
	}
	if c.store == nil {
		return SendResult{Message: "Not connected to WhatsApp; durable store unavailable", Status: "rejected"}
	}
	sender := c.pairedSender()
	if sender == "" {
		return SendResult{Message: "not paired; authorized outbox requires a paired sender", Status: "rejected"}
	}
	opts, _ := ctx.Value(sendOptionsKey{}).(SendOptions)
	p.MarkRead = opts.MarkRead
	var data []byte
	if p.MediaPath != "" {
		path, err := c.ValidateMediaPath(p.MediaPath)
		if err != nil {
			return SendResult{Message: err.Error(), Status: "rejected"}
		}
		f, err := os.Open(path)
		if err != nil {
			return SendResult{Message: err.Error(), Status: "rejected"}
		}
		data, err = io.ReadAll(io.LimitReader(f, 64*1024*1024+1))
		_ = f.Close()
		if err != nil {
			return SendResult{Message: err.Error(), Status: "rejected"}
		}
		if len(data) > 64*1024*1024 {
			return SendResult{Message: "media exceeds durable queue limit of 64 MiB", Status: "rejected"}
		}
		if len(data) == 0 {
			return SendResult{Message: "media file is empty", Status: "rejected"}
		}
		p.MediaPath = filepath.Base(path)
		if opts.MediaName != "" {
			p.MediaPath = filepath.Base(opts.MediaName)
		}
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return SendResult{Message: err.Error(), Status: "rejected"}
	}
	id := whatsmeow.GenerateMessageID()
	if c.wa != nil && c.wa.Store != nil {
		id = c.wa.GenerateMessageID()
	}
	now := c.currentTime()
	j := store.OutboxJob{JobID: "outbox-" + id, MessageID: id, Recipient: jid.ToNonAD().String(), State: "queued", EnqueuedAt: now, NextAttempt: now, Payload: string(payload), Media: data}
	j.Sender = sender
	j.IdempotencyKey = opts.IdempotencyKey
	hash := sha256.New()
	hash.Write([]byte(j.Recipient))
	hash.Write([]byte{0})
	hash.Write(payload)
	hash.Write([]byte{0})
	hash.Write(data)
	j.RequestHash = fmt.Sprintf("%x", hash.Sum(nil))
	if j, err = c.store.EnqueueOutboxOnce(ctx, j); err != nil {
		return SendResult{Message: "outbox persistence failed: " + err.Error(), Status: "rejected"}
	}
	if c.outboxWake != nil {
		select {
		case c.outboxWake <- struct{}{}:
		default:
		}
		// Preserve confirmation for an immediately deliverable first send. Durable
		// ownership survives caller cancellation and this bounded synchronous wait.
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		poll := time.NewTicker(25 * time.Millisecond)
		defer poll.Stop()
		for {
			saved, e := c.store.OutboxJob(ctx, j.JobID)
			if e == nil {
				j = saved
				if j.State != "sending" && (j.State != "queued" || j.Reason != "") {
					break
				}
			}
			select {
			case <-ctx.Done():
				return outboxResult(j)
			case <-deadline.C:
				return outboxResult(j)
			case <-poll.C:
			}
		}
	}
	return outboxResult(j)
}

func outboxResult(j store.OutboxJob) SendResult {
	r := SendResult{Accepted: true, JobID: j.JobID, Status: j.State, Message: "Authorized message retained in durable outbox; inspect get_outbox using JobID. Do not submit it again."}
	if j.Reason != "" {
		r.Message += " " + j.Reason
	}
	if j.State == "sent" {
		r.Success = true
		r.ID = j.SentID
		r.Message = "Message sent to " + j.Recipient
	}
	return r
}

func (c *Client) reserveSend(ctx context.Context, known bool) (ratelimit.Decision, error) {
	if c.store != nil {
		d, e := c.store.PersistentSendBudget(ctx, c.currentTime(), known, false)
		if e != nil || !d.Allowed {
			return d, e
		}
	}
	// The send mutex serializes both checks. A stricter memory denial must not
	// consume durable budget because no network call will follow.
	if c.limiter != nil {
		d := c.limiter.AllowSend(known)
		if !d.Allowed {
			return d, nil
		}
	}
	if c.store != nil {
		return c.store.PersistentSendBudget(ctx, c.currentTime(), known, true)
	}
	return ratelimit.Decision{Allowed: true}, nil
}

func (c *Client) processOutbox(ctx context.Context) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	j, err := c.store.NextOutbox(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return
	}
	if err != nil {
		c.log.Errorf("outbox read failed: %v", err)
		return
	}
	now := c.currentTime()
	if j.Sender == "" || j.Sender != c.pairedSender() {
		_ = c.store.UpdateOutbox(ctx, j.JobID, "blocked", "paired sender changed or unavailable; authorization belongs to original account", "", now)
		return
	}
	if now.Before(j.NextAttempt) {
		return
	}
	defer func() {
		if err != nil {
			c.log.Errorf("outbox state persistence failed for %s: %v", j.JobID, err)
		}
	}()
	jid, err := parseRecipient(j.Recipient)
	if err != nil {
		err = c.store.UpdateOutbox(ctx, j.JobID, "failed", err.Error(), "", now)
		return
	}
	known := c.IsKnownContact(jid)
	if gate := c.sendGate(known); gate != nil {
		c.health.mu.Lock()
		state, until, healthReason := c.health.state, c.health.until, c.health.reason
		c.health.mu.Unlock()
		jobState := "blocked"
		if state == HealthRestricted && !until.IsZero() && !strings.HasPrefix(healthReason, "circuit breaker:") {
			jobState = "queued"
		}
		err = c.store.UpdateOutbox(ctx, j.JobID, jobState, gate.Error(), "", until)
		return
	}
	if !c.outboxConnected() {
		err = c.store.UpdateOutbox(ctx, j.JobID, "queued", "transport disconnected before delivery; retry scheduled", "", now.Add(30*time.Second))
		return
	}
	d, e := c.store.PersistentSendBudget(ctx, now, known, false)
	if e != nil {
		err = c.store.UpdateOutbox(ctx, j.JobID, "blocked", "persistent send protection failed: "+e.Error(), "", now)
		return
	}
	if !d.Allowed {
		err = c.store.UpdateOutbox(ctx, j.JobID, "queued", "local pacing: "+d.Reason, "", now.Add(d.RetryAfter))
		return
	}
	j.Payload, j.Media, err = c.store.LoadOutboxPayload(ctx, j.JobID)
	if err != nil {
		return
	}
	var p outboundPayload
	if err = json.Unmarshal([]byte(j.Payload), &p); err != nil {
		err = c.store.UpdateOutbox(ctx, j.JobID, "failed", "invalid persisted payload", "", now)
		return
	}
	msg := &waProto.Message{}
	if c.networkSend == nil {
		msg.MessageContextInfo = c.ephemeralContextInfo(ctx, jid)
	}
	if p.MediaPath != "" {
		if len(j.Media) == 0 {
			err = c.store.UpdateOutbox(ctx, j.JobID, "failed", "immutable media snapshot missing", "", now)
			return
		}
		if c.networkSend == nil {
			if e = c.attachMediaData(ctx, msg, p.MediaPath, j.Media, p.Body, p.ViewOnce); e != nil {
				err = c.store.UpdateOutbox(ctx, j.JobID, "failed", e.Error(), "", now)
				return
			}
		}
	} else if p.ReplyID != "" {
		ci := &waProto.ContextInfo{StanzaID: proto.String(p.ReplyID)}
		if p.ReplySender != "" {
			ci.Participant = proto.String(p.ReplySender)
		}
		msg.ExtendedTextMessage = &waProto.ExtendedTextMessage{Text: proto.String(p.Body), ContextInfo: ci}
	} else {
		msg.Conversation = proto.String(p.Body)
	}
	if c.networkSend == nil {
		c.humanizeBeforeSend(ctx, jid, len(p.Body), msg.AudioMessage != nil)
	}
	// Recheck health/transport after slow upload/presence; take budget at the
	// actual network boundary, preserving minimum spacing for feature sends.
	if e = c.sendGate(known); e != nil {
		err = c.store.UpdateOutbox(ctx, j.JobID, "queued", e.Error(), "", c.currentTime().Add(time.Second))
		return
	}
	if c.pairedSender() != j.Sender {
		err = c.store.UpdateOutbox(ctx, j.JobID, "blocked", "paired sender changed before delivery", "", c.currentTime())
		return
	}
	if !c.outboxConnected() || ctx.Err() != nil {
		return
	}
	claimed, e := c.store.ClaimOutbox(ctx, j.JobID)
	if e != nil {
		err = e
		return
	}
	if !claimed {
		return
	}
	d, e = c.reserveSend(ctx, known)
	if e != nil {
		err = c.store.UpdateOutbox(context.WithoutCancel(ctx), j.JobID, "blocked", e.Error(), "", now)
		return
	}
	if !d.Allowed {
		err = c.store.UpdateOutbox(context.WithoutCancel(ctx), j.JobID, "queued", "local pacing: "+d.Reason, "", c.currentTime().Add(d.RetryAfter))
		return
	}
	if err = c.store.RecordOutboxAttempt(context.WithoutCancel(ctx), j.JobID); err != nil {
		return
	}
	j.Attempts++
	sendCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var resp whatsmeow.SendResponse
	if c.networkSend != nil {
		resp, e = c.networkSend(sendCtx, jid, msg, j.MessageID)
	} else {
		resp, e = c.wa.SendMessage(sendCtx, jid, msg, whatsmeow.SendRequestExtra{ID: j.MessageID})
	}
	// Bookkeeping survives shutdown cancellation; if the process dies before
	// this write the persisted 'sending' state requires review on restart.
	saveCtx, saveCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer saveCancel()
	if e != nil {
		c.noteSendError(e)
		jobState, reason, next := "needs_review", "delivery uncertain: "+e.Error(), c.currentTime()
		var iq *whatsmeow.IQError
		if errors.As(e, &iq) {
			jobState = "failed"
			reason = "server refused delivery: " + e.Error()
			if iq.Code == 429 || iq.Code == 463 || iq.Code == 475 {
				c.health.mu.Lock()
				next = c.health.until
				c.health.mu.Unlock()
				if j.Attempts > 1 {
					cooldown := next.Sub(c.currentTime())
					next = c.currentTime().Add(cooldown * time.Duration(j.Attempts))
					c.setHealth(HealthRestricted, next, "repeated definite temporary server refusal", true)
				}
				jobState = "blocked"
				if j.Attempts < 3 && !next.IsZero() {
					jobState = "queued"
					reason = "definite temporary server refusal; waiting full account cooldown: " + e.Error()
				}
			} else if iq.Code == 401 || iq.Code == 402 || iq.Code == 403 {
				jobState = "blocked"
			}
		}
		err = c.store.UpdateOutbox(saveCtx, j.JobID, jobState, reason, "", next)
		return
	}
	c.noteSendOK()
	if resp.ID != j.MessageID {
		err = c.store.UpdateOutbox(saveCtx, j.JobID, "needs_review", "transport returned unexpected message ID; do not resend", resp.ID, c.currentTime())
		return
	}
	// Cache with stable ID before terminal update, enabling safe restart recovery.
	if e = c.persistSent(saveCtx, jid, resp.ID, p.Body, p.MediaPath, msg); e != nil {
		err = c.store.UpdateOutbox(saveCtx, j.JobID, "sent", "delivered but outgoing cache failed: "+e.Error(), resp.ID, c.currentTime())
		return
	}
	err = c.store.UpdateOutbox(saveCtx, j.JobID, "sent", "", resp.ID, c.currentTime())
	if err == nil && p.MarkRead {
		_, _ = c.MarkChatRead(saveCtx, j.Recipient, 50)
	}
}

func (c *Client) GetOutbox(ctx context.Context, id string, limit int) ([]store.OutboxJob, error) {
	return c.store.ListOutbox(ctx, id, limit)
}
func (c *Client) CancelOutbox(ctx context.Context, id string) error {
	return c.store.CancelOutbox(ctx, id)
}
func (c *Client) OutboxSnapshot(ctx context.Context) map[string]any {
	if c.store == nil {
		return map[string]any{"error": "store unavailable"}
	}
	counts, err := c.store.OutboxCounts(ctx)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	out := map[string]any{"counts": counts}
	if j, e := c.store.NextOutbox(ctx); e == nil {
		out["next_job_id"] = j.JobID
		out["next_attempt"] = j.NextAttempt
		out["pause_reason"] = j.Reason
	}
	return out
}

// Health persistence is synchronous and fails closed. A disk error must never
// turn an observed ban into a healthy state on the next send.
type persistedHealth struct {
	State           HealthState
	Until           time.Time
	Reason          string
	AllSends        bool
	SetAt           time.Time
	ConsecutiveErrs int
	LastErrAt       time.Time
	LastErr         string
	LastOKAt        time.Time
}

func (c *Client) initSendProtection(ctx context.Context) error {
	if err := c.store.EnsureSendProtection(ctx, c.currentTime()); err != nil {
		return err
	}
	raw, err := c.store.ProtectionValue(ctx, "health")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if raw != "" {
		var h persistedHealth
		if err = json.Unmarshal([]byte(raw), &h); err != nil {
			return fmt.Errorf("load persisted health: %w", err)
		}
		c.health.state = h.State
		c.health.until = h.Until
		c.health.reason = h.Reason
		c.health.allSends = h.AllSends
		c.health.setAt = h.SetAt
		c.health.consecutiveErrs = h.ConsecutiveErrs
		c.health.lastErrAt = h.LastErrAt
		c.health.lastErr = h.LastErr
		c.health.lastOKAt = h.LastOKAt
	}
	c.healthPersistent = true
	return nil
}

// Caller holds health.mu.
func (c *Client) persistHealthLocked() {
	if !c.healthPersistent {
		return
	}
	h := &c.health
	p := persistedHealth{h.state, h.until, h.reason, h.allSends, h.setAt, h.consecutiveErrs, h.lastErrAt, h.lastErr, h.lastOKAt}
	raw, err := json.Marshal(p)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = c.store.SetProtectionValue(ctx, "health", string(raw))
	}
	c.healthPersistErr = err
	if err != nil {
		c.log.Errorf("health persistence failed: %v", err)
	}
}
