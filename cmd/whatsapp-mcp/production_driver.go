package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types/events"

	"github.com/sealjay/mcp-whatsapp/internal/client"
)

// productionDriver adapts *client.Client to the daemon.pairDriver interface.
// It does not install any internal state — the Client itself owns all
// whatsmeow-facing state.
type productionDriver struct {
	c *client.Client

	// viewerActive reports whether a human is currently looking at /pair.
	// Local patch: pairing sockets are only opened on demand, so an
	// unattended daemon does not hammer WhatsApp's pairing endpoint in a
	// loop (which is exactly what an automation fingerprint looks like).
	viewerActive func() bool

	mu      sync.Mutex
	pairing bool
}

func newProductionDriver(c *client.Client) *productionDriver {
	return &productionDriver{c: c}
}

// SetViewerProbe installs the /pair demand signal. Optional; without it the
// loop behaves as if someone is always watching.
func (p *productionDriver) SetViewerProbe(f func() bool) { p.viewerActive = f }

func (p *productionDriver) IsLoggedIn() bool { return p.c.IsLoggedIn() }

// StartPairing launches the pairing loop in the background and returns
// immediately. Only one loop runs at a time, however many times the daemon
// asks (e.g. repeated LoggedOut events).
func (p *productionDriver) StartPairing(ctx context.Context, onQR func(string), onSuccess func()) error {
	p.mu.Lock()
	if p.pairing {
		p.mu.Unlock()
		return nil
	}
	p.pairing = true
	p.mu.Unlock()
	go func() {
		defer func() {
			p.mu.Lock()
			p.pairing = false
			p.mu.Unlock()
		}()
		p.pairLoop(ctx, onQR, onSuccess)
	}()
	return nil
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, time.Now().Format("15:04:05.000")+" [Pairing] "+format+"\n", args...)
}

// pairLoop: wait for a viewer → open QR channel → connect → forward codes →
// on timeout close the socket, back off, repeat. whatsmeow closes the QR
// channel after ~2-3 minutes without a scan; upstream simply gave up there.
func (p *productionDriver) pairLoop(ctx context.Context, onQR func(string), onSuccess func()) {
	var backoff time.Duration
	attempt := 0
	for {
		// 1. Demand gate.
		waited := false
		for p.viewerActive != nil && !p.viewerActive() {
			if !waited {
				logf("idle: no one is viewing /pair; not opening a pairing socket until someone does")
				waited = true
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			if p.c.IsLoggedIn() {
				onSuccess()
				return
			}
		}
		if waited {
			backoff = 0 // a fresh viewer gets an immediate code
		}
		// 2. Backoff between consecutive unattended cycles.
		if backoff > 0 {
			logf("waiting %s before issuing new QR codes", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		if p.c.IsLoggedIn() {
			onSuccess()
			return
		}
		// 3. Open channel + connect (order mandated by whatsmeow).
		qrCh, err := p.c.QRChannel(ctx)
		if err != nil {
			logf("QR channel: %v", err)
			backoff = nextBackoff(backoff)
			continue
		}
		if err := p.c.Connect(ctx, client.ConnectOpts{AllowUnpaired: true}); err != nil {
			logf("connect for pairing: %v", err)
			p.c.Disconnect()
			backoff = nextBackoff(backoff)
			continue
		}
		attempt++
		logf("attempt %d: pairing socket open, QR codes live", attempt)
		// 4. Pump codes until the channel closes.
		success := false
		for item := range qrCh {
			switch item.Event {
			case "code":
				onQR(item.Code)
			case "success":
				success = true
			default:
				logf("QR channel event %q", item.Event)
			}
		}
		if success || p.c.IsLoggedIn() {
			logf("paired successfully")
			onSuccess()
			return
		}
		onQR("") // clear the stale code so /pair shows "generating"
		p.c.Disconnect()
		backoff = nextBackoff(backoff)
	}
}

// nextBackoff: 10s → 20s → 40s → 60s (cap). A viewer arriving resets it.
func nextBackoff(cur time.Duration) time.Duration {
	if cur == 0 {
		return 10 * time.Second
	}
	cur *= 2
	if cur > time.Minute {
		cur = time.Minute
	}
	return cur
}

func (p *productionDriver) Connect(ctx context.Context, onLoggedOut func()) error {
	p.c.AddLoggedOutHandler(func(_ *events.LoggedOut) { onLoggedOut() })
	p.c.StartEventHandler()
	if p.c.IsConnected() {
		// The pairing flow already connected this client; just install
		// the post-pair handlers and return.
		return nil
	}
	return p.c.Connect(ctx, client.ConnectOpts{})
}

func (p *productionDriver) Logout(ctx context.Context) error { return p.c.Logout(ctx) }

func (p *productionDriver) Disconnect() { p.c.Disconnect() }
