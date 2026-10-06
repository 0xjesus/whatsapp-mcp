package client

import (
	"context"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"time"
)

func (c *Client) refreshMonitoringPolicy() {
	c.monitoringMu.Lock()
	defer c.monitoringMu.Unlock()
	c.monitoringStateMu.Lock()
	generation := c.monitoringGeneration
	c.monitoringStateMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
