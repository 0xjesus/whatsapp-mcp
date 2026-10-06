package client

import (
	"context"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"sync/atomic"
	"testing"
	"time"
)

func TestMonitoringWorkerCoalescesBurstAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	done := make(chan struct{})
	var calls atomic.Int32
	go func() {
		defer close(done)
		runMonitoringWorker(ctx, wake, func(ctx context.Context) {
			calls.Add(1)
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
		}, func() bool { return true }, 20*time.Millisecond, time.Hour)
	}()
	trigger := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	for i := 0; i < 1000; i++ {
		trigger()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("no refresh")
	}
	for i := 0; i < 1000; i++ {
		trigger()
	}
	release <- struct{}{}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("in-flight burst not refreshed")
	}
	release <- struct{}{}
	time.Sleep(70 * time.Millisecond)
	if calls.Load() != 2 {
		t.Fatal("redundant metadata calls", calls.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}
func TestMonitoringWorkerSkipsDisconnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	done := make(chan struct{})
	var calls atomic.Int32
	go func() {
		defer close(done)
		runMonitoringWorker(ctx, wake, func(context.Context) { calls.Add(1) }, func() bool { return false }, time.Millisecond, 5*time.Millisecond)
	}()
	wake <- struct{}{}
	time.Sleep(25 * time.Millisecond)
	cancel()
	<-done
	if calls.Load() != 0 {
		t.Fatal("disconnected metadata call")
	}
}

func TestMonitoringInvalidationTargetsChangedGroup(t *testing.T) {
	c, s := scheduledTestClient(t)
	groups := make([]store.MonitoringGroup, 105)
	for i := range groups {
		groups[i] = store.MonitoringGroup{JID: fmt.Sprintf("%d@g.us", 1000+i), Members: 10}
	}
	if err := s.ReconcileMonitoring(groups); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE group_monitoring_policy SET enabled=1`); err != nil {
		t.Fatal(err)
	}
	if err := c.publishMonitoringPolicy(0, groups); err != nil {
		t.Fatal(err)
	}
	c.invalidateMonitoringPolicy(groups[0].JID)
	for _, group := range groups[1:] {
		if !s.MonitoringAllowed(group.JID) {
			t.Fatal("unrelated permit invalidated", group.JID)
		}
	}
	if err := c.publishMonitoringPolicy(0, groups); err != nil {
		t.Fatal(err)
	}
	if s.MonitoringAllowed(groups[0].JID) {
		t.Fatal("stale snapshot regranted changedgroup")
	}
}
