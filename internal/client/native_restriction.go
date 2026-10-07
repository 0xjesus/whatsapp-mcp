package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types/events"
)

// Native notifications are independent of legacy refusal/connection health.
// Server ordering survives restart; equal timestamps favor an active signal.
type nativeRestriction struct {
	IsActive        bool      `json:"is_active"`
	EnforcementType string    `json:"enforcement_type"`
	Until           time.Time `json:"until"`
	EventAt         time.Time `json:"event_at"`
	ObservedAt      time.Time `json:"observed_at"`
	OpName          string    `json:"op_name"`
}

func (n nativeRestriction) active(now time.Time) bool {
	return n.IsActive && (n.Until.IsZero() || now.Before(n.Until))
}

func (c *Client) handleNativeRestriction(v *events.NotifyAccountReachoutTimelock) {
	c.health.mu.Lock()
	defer c.health.mu.Unlock()
	at := v.Mex.Timestamp
	// A release without ordering evidence cannot erase an observed active signal.
	if at.IsZero() {
		if !v.IsActive && c.nativeRestriction.IsActive {
			return
		}
		at = c.currentTime()
	}
	old := c.nativeRestriction
	if at.Before(old.EventAt) || (at.Equal(old.EventAt) && !v.IsActive) {
		return
	}
	until := v.TimeEnforcementEnds.Time
	if at.Equal(old.EventAt) && old.IsActive && v.IsActive {
		until = longerRestriction(old.Until, until)
	}
	c.nativeRestriction = nativeRestriction{IsActive: v.IsActive, EnforcementType: v.EnforcementType, Until: until, EventAt: at, ObservedAt: c.currentTime(), OpName: v.Mex.OpName}
	if !c.healthPersistent {
		return
	}
	raw, err := json.Marshal(c.nativeRestriction)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = c.store.SetProtectionValue(ctx, "native_restriction", string(raw))
	}
	c.nativePersistErr = err
	if err != nil {
		c.log.Errorf("native account restriction persistence failed: %v", err)
	}
}

func (c *Client) loadNativeRestriction(ctx context.Context) error {
	raw, err := c.store.ProtectionValue(ctx, "native_restriction")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if raw != "" {
		if err = json.Unmarshal([]byte(raw), &c.nativeRestriction); err != nil {
			return fmt.Errorf("load native restriction: %w", err)
		}
	}
	return nil
}

func longerRestriction(a, b time.Time) time.Time {
	if a.IsZero() || b.IsZero() {
		return time.Time{}
	}
	if a.After(b) {
		return a
	}
	return b
}

func healthPriority(state HealthState) int {
	switch state {
	case HealthLoggedOut, HealthOutdated:
		return 3
	case HealthTempBanned:
		return 2
	case HealthRestricted:
		return 1
	default:
		return 0
	}
}

// sendSafety is the single source of temporary pause classification for every
// worker gate. The retry time is a gate recheck, never permission to dispatch.
func (c *Client) sendSafety(isContact bool) (err error, temporary bool, next time.Time) {
	h := &c.health
	h.mu.Lock()
	defer h.mu.Unlock()
	now := c.currentTime()
	if c.healthPersistErr != nil {
		return fmt.Errorf("SEND BLOCKED: health persistence failed: %w", c.healthPersistErr), false, now
	}
	if c.nativePersistErr != nil {
		return fmt.Errorf("SEND BLOCKED: native restriction persistence failed: %w", c.nativePersistErr), false, now
	}
	if h.state != "" && h.state != HealthOK && !h.until.IsZero() && !now.Before(h.until) {
		h.state, h.until, h.reason, h.allSends = HealthOK, time.Time{}, "", false
		c.persistHealthLocked()
		if c.healthPersistErr != nil {
			return fmt.Errorf("SEND BLOCKED: health persistence failed: %w", c.healthPersistErr), false, now
		}
	}
	if h.state != "" && h.state != HealthOK && (h.state != HealthRestricted || h.allSends || !isContact) {
		err = fmt.Errorf("SEND BLOCKED: account state is %s (%s), until %s", h.state, h.reason, fmtUntil(h.until))
		temporary = h.state == HealthRestricted && !h.until.IsZero() && !strings.HasPrefix(h.reason, "circuit breaker:")
		return err, temporary, h.until
	}
	if c.nativeRestriction.active(now) {
		n := c.nativeRestriction
		// Poll finite signals too, so an explicit early release resumes promptly.
		next = now.Add(time.Second)
		if !n.Until.IsZero() && n.Until.Before(next) {
			next = n.Until
		}
		return fmt.Errorf("SEND BLOCKED: native account restriction (%s), until %s", n.EnforcementType, fmtUntil(n.Until)), true, next
	}
	return nil, false, now
}

func (c *Client) outboxSafety(known bool) (err error, state string, next time.Time) {
	err, temporary, next := c.sendSafety(known)
	state = "blocked"
	if temporary {
		state = "queued"
	}
	return err, state, next
}
