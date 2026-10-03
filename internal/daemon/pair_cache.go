// Package daemon runs the long-lived whatsapp-mcp process: it owns the
// HTTP listener, mounts the MCP endpoint, serves the /pair UI, and drives
// the pairing state machine against a whatsmeow client.
package daemon

import (
	"sync"
	"time"
)

// PairCache holds the latest QR code bytes emitted by whatsmeow's pairing
// channel and a flag indicating whether the device is currently paired.
// Safe for concurrent readers and a single writer (the pairing goroutine).
type PairCache struct {
	mu     sync.RWMutex
	qr     string
	paired bool
	// lastView is the last time a human loaded /pair (or its QR image).
	// Local patch: drives on-demand pairing in cmd/whatsapp-mcp.
	lastView time.Time
}

// Touch records that /pair was just viewed.
func (c *PairCache) Touch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastView = time.Now()
}

// RecentlyViewed reports whether /pair was loaded within d.
func (c *PairCache) RecentlyViewed(d time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.lastView.IsZero() && time.Since(c.lastView) < d
}

// NewPairCache returns an empty cache in the "unpaired, no QR yet" state.
func NewPairCache() *PairCache {
	return &PairCache{}
}

// SetQR stores the latest pairing code. Clears the paired flag.
func (c *PairCache) SetQR(code string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.qr = code
	c.paired = false
}

// SetPaired flips the paired flag on and clears any pending QR.
func (c *PairCache) SetPaired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.qr = ""
	c.paired = true
}

// Reset drops the paired flag and clears any pending QR. Used when a
// user-driven re-pair starts.
func (c *PairCache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.qr = ""
	c.paired = false
}

// Paired reports whether the device is currently paired.
func (c *PairCache) Paired() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.paired
}

// QR returns the latest cached pairing code, or "" if none has been emitted
// since the last Reset or SetPaired.
func (c *PairCache) QR() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.qr
}
