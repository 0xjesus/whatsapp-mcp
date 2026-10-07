package client

// Local patch: account-health state machine + human-like send pacing.
//
// WhatsApp restricts or bans accounts that look automated. The daemon already
// rate-limits outbound sends (see internal/ratelimit). This file adds the
// missing pieces:
//
//   - a health state that remembers server-side restrictions (463 reachout
//     timelock, 475 message capping, 429 rate-overlimit, 402 temporary ban,
//     401 logged-out, client-outdated) and refuses sends while they last,
//     instead of retrying and digging the hole deeper;
//   - a consecutive-failure circuit breaker;
//   - "composing" presence + a length-proportional pause before every text
//     send, so outgoing traffic resembles a person typing.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
)

// HealthState is the daemon's view of how WhatsApp currently treats us.
type HealthState string

const (
	HealthOK         HealthState = "ok"
	HealthRestricted HealthState = "restricted"      // server refused sends; cooling down
	HealthTempBanned HealthState = "temp_banned"     // events.TemporaryBan / 402
	HealthLoggedOut  HealthState = "logged_out"      // session revoked; needs re-pair
	HealthOutdated   HealthState = "client_outdated" // whatsmeow too old; rebuild needed
)

type health struct {
	mu       sync.Mutex
	state    HealthState
	allSends bool // restriction applies to every send, not only new contacts
	until    time.Time
	reason   string
	setAt    time.Time

	consecutiveErrs int
	lastErrAt       time.Time
	lastErr         string
	lastOKAt        time.Time
}

// setHealth records a non-OK state. until may be zero for open-ended states.
func (c *Client) setHealth(state HealthState, until time.Time, reason string, allSends bool) {
	h := &c.health
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != "" && h.state != HealthOK && (h.until.IsZero() || c.currentTime().Before(h.until)) {
		if healthPriority(h.state) > healthPriority(state) {
			return
		}
		if healthPriority(h.state) == healthPriority(state) {
			until = longerRestriction(h.until, until)
			allSends = allSends || h.allSends
			if strings.HasPrefix(h.reason, "circuit breaker:") {
				reason = h.reason
			}
		}
	}
	h.state, h.until, h.reason, h.allSends, h.setAt = state, until, reason, allSends, c.currentTime()
	c.persistHealthLocked()
	c.log.Errorf("ACCOUNT HEALTH → %s (%s) until=%s allSends=%v", state, reason, fmtUntil(until), allSends)
}

// clearHealth clears connection failures on reconnect, but a working transport
// does not lift an account restriction or temporary ban.
func (c *Client) clearHealth(why string) {
	h := &c.health
	h.mu.Lock()
	defer h.mu.Unlock()
	if (h.state == HealthRestricted || h.state == HealthTempBanned) && (h.until.IsZero() || c.currentTime().Before(h.until)) {
		return
	}
	if h.state != HealthOK && h.state != "" {
		c.log.Infof("ACCOUNT HEALTH → ok (%s)", why)
	}
	h.state, h.until, h.reason, h.allSends = HealthOK, time.Time{}, "", false
	c.persistHealthLocked()
}

func fmtUntil(t time.Time) string {
	if t.IsZero() {
		return "indefinite"
	}
	return t.Format(time.RFC3339)
}

// sendGate reports whether a send may proceed given the current health.
// Expired restrictions clear themselves here.
func (c *Client) sendGate(isContact bool) error {
	err, _, _ := c.sendSafety(isContact)
	return err
}

// noteSendError classifies a failed send. Server-side abuse signals put the
// account into a cooling-off state; repeated unexplained failures trip a
// circuit breaker so an agent loop cannot hammer the server.
func (c *Client) noteSendError(err error) {
	if err == nil {
		return
	}
	now := c.currentTime()
	h := &c.health
	h.mu.Lock()
	if now.Sub(h.lastErrAt) > 10*time.Minute {
		h.consecutiveErrs = 0
	}
	h.consecutiveErrs++
	h.lastErrAt, h.lastErr = now, err.Error()
	consecutive := h.consecutiveErrs
	c.persistHealthLocked()
	h.mu.Unlock()

	code, _ := definiteRefusalCode(err)
	switch code {
	case 463:
		c.setHealth(HealthRestricted, now.Add(6*time.Hour), "reachout timelock (463): too many messages to new contacts", false)
	case 475:
		c.setHealth(HealthRestricted, now.Add(24*time.Hour), "message capping (475): per-cycle quota for new contacts exhausted", false)
	case 429:
		c.setHealth(HealthRestricted, now.Add(30*time.Minute), "server rate-overlimit (429)", true)
	case 403:
		c.setHealth(HealthRestricted, now.Add(12*time.Hour), "forbidden (403): recipient or account restricted", false)
	case 402:
		c.setHealth(HealthTempBanned, now.Add(24*time.Hour), "temporarily banned (402)", true)
	case 401:
		c.setHealth(HealthLoggedOut, time.Time{}, "not-authorized (401): session revoked", true)
	default:
		if consecutive >= 3 {
			c.setHealth(HealthRestricted, now.Add(15*time.Minute), fmt.Sprintf("circuit breaker: %d consecutive send failures (last: %s)", consecutive, truncate(err.Error(), 120)), true)
		}
	}
}

func (c *Client) noteSendOK() {
	h := &c.health
	h.mu.Lock()
	h.consecutiveErrs = 0
	h.lastOKAt = c.currentTime()
	c.persistHealthLocked()
	h.mu.Unlock()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// HealthSnapshot is exposed through get_status so agents can explain outages
// instead of retrying blindly.
func (c *Client) HealthSnapshot() map[string]any {
	h := &c.health
	h.mu.Lock()
	defer h.mu.Unlock()
	state := h.state
	if state == "" {
		state = HealthOK
	}
	if state != HealthOK && !h.until.IsZero() && !c.currentTime().Before(h.until) {
		state = HealthOK
	}
	reason, until, allSends := h.reason, h.until, h.allSends
	nativeActive := c.nativeRestriction.active(c.currentTime())
	if nativeActive {
		if state == HealthOK {
			state, reason, until = HealthRestricted, "native account restriction: "+c.nativeRestriction.EnforcementType, c.nativeRestriction.Until
		} else if state == HealthRestricted {
			until = longerRestriction(until, c.nativeRestriction.Until)
		}
		allSends = true
	}
	out := map[string]any{"state": state, "humanize": os.Getenv("WHATSAPP_MCP_HUMANIZE") != "0"}
	if !c.nativeRestriction.EventAt.IsZero() {
		out["native_restriction"] = c.nativeRestriction
		out["native_restriction_active"] = nativeActive
	}
	if c.nativePersistErr != nil || c.healthPersistErr != nil {
		out["send_protection_error"] = "persistence failed; sends blocked"
	}
	if state != HealthOK {
		out["reason"] = reason
		out["until"] = fmtUntil(until)
		out["blocks_all_sends"] = allSends
		out["advice"] = "Do not retry, restart or re-pair. Wait for `until`, and tell the user."
	}
	if h.lastErr != "" {
		out["last_send_error"] = truncate(h.lastErr, 200)
		out["last_send_error_at"] = h.lastErrAt.Format(time.RFC3339)
	}
	if !h.lastOKAt.IsZero() {
		out["last_send_ok_at"] = h.lastOKAt.Format(time.RFC3339)
	}
	return out
}

// humanizeBeforeSend shows "typing…" (or "recording…") to the recipient for a
// duration proportional to the message length, then pauses briefly, so the
// send does not arrive as an instantaneous burst. Disable with
// WHATSAPP_MCP_HUMANIZE=0.
func (c *Client) humanizeBeforeSend(ctx context.Context, chat types.JID, textLen int, audio bool) {
	if os.Getenv("WHATSAPP_MCP_HUMANIZE") == "0" {
		return
	}
	if chat.Server == types.BroadcastServer || chat.Server == types.NewsletterServer {
		return
	}
	media := types.ChatPresenceMediaText
	if audio {
		media = types.ChatPresenceMediaAudio
	}
	d := 1200*time.Millisecond + time.Duration(textLen)*45*time.Millisecond + time.Duration(rand.Intn(1500))*time.Millisecond
	if d > 7*time.Second {
		d = 7 * time.Second
	}
	if err := c.wa.SendChatPresence(ctx, chat, types.ChatPresenceComposing, media); err != nil {
		c.log.Debugf("humanize: composing presence failed: %v", err)
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(d):
	}
	_ = c.wa.SendChatPresence(ctx, chat, types.ChatPresencePaused, media)
	select {
	case <-ctx.Done():
	case <-time.After(time.Duration(200+rand.Intn(500)) * time.Millisecond):
	}
}

// mutationGate covers outbound feature actions, including actions that do not
// send a message. An empty recipient is an account-level action, which remains
// allowed under a restriction applying only to new contacts.
func (c *Client) mutationGate(recipient string) error {
	// Reject broad restrictions before any potentially slow cache lookup, then
	// check health again after classification: a ban can arrive during the lookup.
	if err := c.sendGate(true); err != nil {
		return err
	}
	known := recipient == ""
	if !known {
		jid, err := parseRecipient(recipient)
		known = err == nil && (jid.Server == types.GroupServer || (c.store != nil && c.IsKnownContact(jid)))
	}
	return c.sendGate(known)
}

// sendFeatureMessage applies the same account protection and send budget to
// polls, votes, cards, replies, reactions, edits and revokes as ordinary messages.
func (c *Client) sendFeatureMessage(ctx context.Context, recipient types.JID, msg *waProto.Message) (whatsmeow.SendResponse, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if err := c.mutationGate(recipient.String()); err != nil {
		return whatsmeow.SendResponse{}, err
	}
	known := recipient.Server == types.GroupServer || (c.store != nil && c.IsKnownContact(recipient))
	d, err := c.reserveRecipientSend(ctx, c.recipientBudgetKey(recipient), known)
	if err != nil {
		return whatsmeow.SendResponse{}, err
	}
	if !d.Allowed {
		return whatsmeow.SendResponse{}, fmt.Errorf("rate limited: %s — retry in %s", d.Reason, d.RetryAfter.Round(time.Second))
	}
	var resp whatsmeow.SendResponse
	if c.networkSend != nil {
		resp, err = c.networkSend(ctx, recipient, msg, whatsmeow.GenerateMessageID())
	} else {
		resp, err = c.wa.SendMessage(ctx, recipient, msg)
	}
	c.noteMutationResult(err)
	if err == nil {
		c.noteSendOK()
	}
	return resp, err
}

// Successful metadata actions do not prove message sends work, so they must
// not reset the send circuit breaker or claim a last successful send.
func (c *Client) noteMutationResult(err error) error {
	if err != nil {
		c.noteSendError(err)
	}
	return err
}
