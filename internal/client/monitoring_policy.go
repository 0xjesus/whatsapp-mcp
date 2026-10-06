package client

import (
	"context"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"time"
)

func (c *Client) refreshMonitoringPolicy(parent context.Context) {
	c.monitoringStateMu.Lock()
	generation := c.monitoringGeneration
	c.monitoringStateMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	groups, err := c.wa.GetJoinedGroups(ctx)
	if err != nil {
		c.store.InvalidateAutomaticMonitoring()
		c.log.Warnf("Group monitoring metadata refresh failed: %v", err)
		return
	}
	inventory := make([]store.MonitoringGroup, 0, len(groups))
	for _, g := range groups {
		count := g.ParticipantCount
		if len(g.Participants) > count {
			count = len(g.Participants)
		}
		inventory = append(inventory, store.MonitoringGroup{JID: g.JID.String(), Name: g.Name, Members: count})
	}
	if err := c.publishMonitoringPolicy(generation, inventory); err != nil {
		c.log.Warnf("Group monitoring reconciliation failed: %v", err)
	}
}

func (c *Client) invalidateMonitoringPolicy() {
	c.monitoringStateMu.Lock()
	defer c.monitoringStateMu.Unlock()
	c.monitoringGeneration++
	c.store.InvalidateAutomaticMonitoring()
}
func (c *Client) publishMonitoringPolicy(generation uint64, inventory []store.MonitoringGroup) error {
	c.monitoringStateMu.Lock()
	defer c.monitoringStateMu.Unlock()
	if generation != c.monitoringGeneration {
		return nil
	}
	return c.store.ReconcileMonitoring(inventory)
}

// requestMonitoringRefresh coalesces event bursts into a single buffered wake.
// Membership invalidation happens synchronously before this is called.
func (c *Client) requestMonitoringRefresh() {
	c.monitoringOnce.Do(func() {
		c.monitoringWake = make(chan struct{}, 1)
		ctx := c.monitoringContext
		if ctx == nil {
			ctx = context.Background()
		}
		go runMonitoringWorker(ctx, c.monitoringWake, c.refreshMonitoringPolicy, c.wa.IsConnected, time.Second, 5*time.Minute)
	})
	select {
	case c.monitoringWake <- struct{}{}:
	default:
	}
}

// runMonitoringWorker owns the only metadata request loop and debounce timer.
// One wake received during an in-flight request schedules one follow-up snapshot.
func runMonitoringWorker(ctx context.Context, wake <-chan struct{}, refresh func(context.Context), connected func() bool, debounce, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(debounce)
	timer.Stop()
	defer timer.Stop()
	var pending <-chan time.Time
	schedule := func() {
		if pending == nil {
			timer.Reset(debounce)
			pending = timer.C
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			schedule()
		case <-ticker.C:
			schedule()
		case <-pending:
			pending = nil
			// Signals already received belong to this snapshot, not another request.
			select {
			case <-wake:
			default:
			}
			if ctx.Err() == nil && connected() {
				refresh(ctx)
			}
		}
	}
}
