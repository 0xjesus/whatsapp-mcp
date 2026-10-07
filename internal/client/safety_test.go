package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/ratelimit"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestReconnectPreservesActiveRestrictions(t *testing.T) {
	for _, state := range []HealthState{HealthRestricted, HealthTempBanned} {
		for _, until := range []time.Time{time.Now().Add(time.Hour), {}} {
			c := newDisconnectedClient()
			c.setHealth(state, until, "test restriction", true)
			c.clearHealth("connected")
			if err := c.sendGate(true); err == nil {
				t.Errorf("reconnect cleared %s until %v", state, until)
			}
		}
	}
}

func TestReconnectClearsExpiredRestrictionsAndConnectionFailures(t *testing.T) {
	for _, state := range []HealthState{HealthRestricted, HealthTempBanned, HealthLoggedOut, HealthOutdated} {
		c := newDisconnectedClient()
		until := time.Time{}
		if state == HealthRestricted || state == HealthTempBanned {
			until = time.Now().Add(-time.Second)
		}
		c.setHealth(state, until, "resolved", true)
		c.clearHealth("connected")
		if err := c.sendGate(true); err != nil {
			t.Errorf("reconnect retained %s: %v", state, err)
		}
	}
}

func TestFeatureMutationsRespectAccountRestriction(t *testing.T) {
	ctx := context.Background()
	chat := "447700000001@s.whatsapp.net"
	group := "120363000000000001@g.us"
	tests := map[string]func(*Client) string{
		"poll":         func(c *Client) string { return c.SendPoll(ctx, chat, "Question?", []string{"a", "b"}, 1).Message },
		"vote":         func(c *Client) string { return c.SendPollVote(ctx, chat, "poll", []string{"a"}).Message },
		"contact":      func(c *Client) string { return c.SendContactCard(ctx, chat, "Test", "447700000002", "").Message },
		"reaction":     func(c *Client) string { return errorText(c.SendReaction(ctx, chat, "target", "", "👍")) },
		"edit":         func(c *Client) string { return errorText(c.EditMessage(ctx, chat, "target", "new")) },
		"revoke":       func(c *Client) string { return errorText(c.DeleteMessage(ctx, chat, "target", "")) },
		"read":         func(c *Client) string { return errorText(c.MarkRead(ctx, chat, []string{"target"}, "")) },
		"chat read":    func(c *Client) string { _, err := c.MarkChatRead(ctx, chat, 10); return errorText(err) },
		"typing":       func(c *Client) string { return errorText(c.SendTyping(ctx, chat, true, "text")) },
		"create group": func(c *Client) string { _, _, err := c.CreateGroup(ctx, "Test", []string{chat}); return errorText(err) },
		"leave group":  func(c *Client) string { return errorText(c.LeaveGroup(ctx, group)) },
		"participants": func(c *Client) string {
			_, err := c.UpdateGroupParticipants(ctx, group, []string{chat}, "add")
			return errorText(err)
		},
		"group name":     func(c *Client) string { return errorText(c.SetGroupName(ctx, group, "Test")) },
		"group topic":    func(c *Client) string { return errorText(c.SetGroupTopic(ctx, group, "Test")) },
		"group announce": func(c *Client) string { return errorText(c.SetGroupAnnounce(ctx, group, true)) },
		"group locked":   func(c *Client) string { return errorText(c.SetGroupLocked(ctx, group, true)) },
		"reset invite":   func(c *Client) string { _, err := c.GetGroupInviteLink(ctx, group, true); return errorText(err) },
		"join group":     func(c *Client) string { _, err := c.JoinGroupWithLink(ctx, "synthetic-code"); return errorText(err) },
		"block":          func(c *Client) string { return errorText(c.BlockContact(ctx, chat)) },
		"unblock":        func(c *Client) string { return errorText(c.UnblockContact(ctx, chat)) },
		"presence":       func(c *Client) string { return errorText(c.SendPresence(ctx, "available")) },
		"privacy":        func(c *Client) string { _, err := c.SetPrivacySetting(ctx, "last", "contacts"); return errorText(err) },
		"status":         func(c *Client) string { return errorText(c.SetStatusMessage(ctx, "Test")) },
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			c := newDisconnectedClient() // no connection or network capability: the health gate must reject first
			c.setHealth(HealthRestricted, time.Now().Add(time.Hour), "server rate limit", true)
			if got := call(c); !strings.Contains(got, "SEND BLOCKED") {
				t.Fatalf("mutation bypassed health gate: %s", got)
			}
		})
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestMutationResultRecognizesTemporaryBan(t *testing.T) {
	c := newDisconnectedClient()
	err := &whatsmeow.IQError{Code: 402}
	if got := c.noteMutationResult(err); got != err {
		t.Fatalf("lost original error: %v", got)
	}
	if got := c.HealthSnapshot()["state"]; got != HealthTempBanned {
		t.Fatalf("health = %v, want temp_banned", got)
	}
}

func TestFeatureSendSharesRateLimitAndCannotOverrideHealth(t *testing.T) {
	c := newDisconnectedClient()
	c.limiter = ratelimit.New(ratelimit.DefaultConfig())
	if d := c.limiter.AllowSend(true); !d.Allowed {
		t.Fatal("initial budget unexpectedly unavailable")
	}
	chat := types.JID{User: "120363000000000001", Server: types.GroupServer}
	if _, err := c.sendFeatureMessage(context.Background(), chat, nil); err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("feature send bypassed send budget: %v", err)
	}
	c.setHealth(HealthRestricted, time.Now().Add(time.Hour), "server restriction", true)
	if _, err := c.sendFeatureMessage(ratelimit.WithBypass(context.Background()), chat, nil); err == nil || !strings.Contains(err.Error(), "SEND BLOCKED") {
		t.Fatalf("operator limiter override bypassed account health: %v", err)
	}
}

func TestMutationGateRetainsKnownContactException(t *testing.T) {
	c, _ := reactionClient(t)
	c.setHealth(HealthRestricted, time.Now().Add(time.Hour), "new contacts only", false)
	for _, target := range []string{"", reactionGroup} {
		if err := c.mutationGate(target); err != nil {
			t.Errorf("known/account context blocked: %v", err)
		}
	}
	if err := c.mutationGate("447700009999@s.whatsapp.net"); err == nil {
		t.Fatal("cold contact bypassed restriction")
	}
}

func TestSuccessfulMetadataMutationDoesNotResetSendFailures(t *testing.T) {
	c := newDisconnectedClient()
	c.noteSendError(errors.New("synthetic send failure"))
	c.noteMutationResult(nil)
	if c.health.consecutiveErrs != 1 || !c.health.lastOKAt.IsZero() {
		t.Fatal("successful metadata action erased send failure accounting")
	}
}

func TestDiscoveryRespectsGlobalAccountRestriction(t *testing.T) {
	c := newDisconnectedClient()
	c.setHealth(HealthRestricted, time.Now().Add(time.Hour), "server rate limit", true)
	_, err := c.IsOnWhatsApp(context.Background(), []string{"447700000001"})
	if err == nil || !strings.Contains(err.Error(), "SEND BLOCKED") {
		t.Fatalf("discovery bypassed account restriction: %v", err)
	}
}

func TestAddingColdGroupParticipantsRespectsReachoutRestriction(t *testing.T) {
	for _, action := range []string{"add", " ADD "} {
		c := newDisconnectedClient()
		c.setHealth(HealthRestricted, time.Now().Add(time.Hour), "new contacts only", false)
		_, err := c.UpdateGroupParticipants(context.Background(), reactionGroup, []string{"447700009999"}, action)
		if err == nil || !strings.Contains(err.Error(), "SEND BLOCKED") {
			t.Fatalf("action %q bypassed reachout restriction: %v", action, err)
		}
	}
}

func TestMutationGateRechecksHealthAfterContactLookup(t *testing.T) {
	c, s := reactionClient(t)
	ctx := context.Background()
	chat := "447700000001@s.whatsapp.net"
	if err := s.StoreChat(chat, "Synthetic", originalTime); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(ctx, store.Message{ID: "synthetic-inbound", ChatJID: chat, Sender: "447700000001", Content: "synthetic", Timestamp: originalTime}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	c.setHealth(HealthRestricted, time.Now().Add(time.Hour), "reachout only", false)
	s.DB().SetMaxOpenConns(1)
	conn, err := s.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitCount := s.DB().Stats().WaitCount
	result := make(chan error, 1)
	go func() { result <- c.mutationGate(chat) }()
	deadline := time.Now().Add(time.Second)
	for s.DB().Stats().WaitCount == waitCount {
		if time.Now().After(deadline) {
			t.Fatal("contact lookup did not start")
		}
		runtime.Gosched()
	}
	c.setHealth(HealthTempBanned, time.Now().Add(time.Hour), "ban arrived during lookup", true)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("contact lookup allowed a send after ban arrived")
	}
}

func TestHealthRefusalClassifierRequiresLibraryContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		blocked bool
	}{
		{"ack", fmt.Errorf("%w %d", whatsmeow.ErrServerReturnedError, 402), true},
		{"wrapped ack", fmt.Errorf("outer: %w", fmt.Errorf("%w %d", whatsmeow.ErrServerReturnedError, 402)), true},
		{"typed IQ", fmt.Errorf("outer: %w", &whatsmeow.IQError{Code: 402}), true},
		{"untrusted text", errors.New("server returned error 402"), false},
		{"extra digits", fmt.Errorf("%w 1402", whatsmeow.ErrServerReturnedError), false},
		{"extra suffix", fmt.Errorf("%w 402 timeout", whatsmeow.ErrServerReturnedError), false},
		{"bare sentinel", whatsmeow.ErrServerReturnedError, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDisconnectedClient()
			c.noteSendError(tc.err)
			if got := c.sendGate(true) != nil; got != tc.blocked {
				t.Fatalf("blocked=%v want%v", got, tc.blocked)
			}
		})
	}
}
