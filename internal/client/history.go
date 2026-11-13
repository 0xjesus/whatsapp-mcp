package client

import (
	"context"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// RequestHistorySync requests one backward page using a REAL oldest cached
// key, optionally bounded by timestamp. Submission does not prove delivery or
// exhaustion. Independent durable pacing applies even across daemon restarts.
func (c *Client) RequestHistorySync(ctx context.Context, chatJID string, before time.Time) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if !c.store.MonitoringAllowed(chatJID) {
		return "", fmt.Errorf("group monitoring requires explicit consent")
	}
	if !c.outboxConnected() {
		return "", fmt.Errorf("client not connected")
	}
	own, err := types.ParseJID(c.pairedSender())
	if err != nil || own.IsEmpty() {
		return "", fmt.Errorf("device not paired")
	}
	// Fail closed on native restriction and persistent safety errors; treating
	// this as non-contact for gating is deliberately conservative.
	if err = c.sendGate(false); err != nil {
		return "", err
	}
	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return "", fmt.Errorf("invalid JID: %w", err)
	}
	if jid.Server != types.DefaultUserServer && jid.Server != "lid" && jid.Server != types.GroupServer {
		return "", fmt.Errorf("only own cached direct/group chats support history crawl")
	}
	canonical := c.store.ResolveLIDToJID(chatJID)
	if !c.store.MonitoringAllowed(canonical) {
		return "", fmt.Errorf("monitoring consent revoked")
	}
	anchor, err := c.store.GetHistoryAnchor(canonical, before)
	if err != nil {
		return "", err
	}
	requestJID := jid
	if jid.Server == types.DefaultUserServer && c.wa != nil && c.wa.Store != nil && c.wa.Store.LIDs != nil {
		lid, e := c.wa.Store.LIDs.GetLIDForPN(ctx, jid)
		if e == nil && !lid.IsEmpty() {
			requestJID = lid
		}
	}
	info := &types.MessageInfo{MessageSource: types.MessageSource{Chat: requestJID, IsFromMe: anchor.IsFromMe}, ID: anchor.ID, Timestamp: anchor.Timestamp}
	id, err := c.store.ReserveHistoryRequest(ctx, anchor, c.currentTime())
	if err != nil {
		return "", err
	}
	// Recheck immediately before network I/O. Revocations never cause a send.
	if !c.store.MonitoringAllowed(chatJID) || !c.store.MonitoringAllowed(canonical) {
		_ = c.store.CancelHistoryRequest(id)
		return "", fmt.Errorf("monitoring consent revoked")
	}
	var resp whatsmeow.SendResponse
	if c.historyPeerSend != nil {
		resp, err = c.historyPeerSend(ctx, own.ToNonAD(), info)
	} else {
		msg := c.wa.BuildHistorySyncRequest(info, 50)
		if msg == nil {
			err = fmt.Errorf("cannot build history request")
		} else {
			resp, err = c.wa.SendMessage(ctx, own.ToNonAD(), msg, whatsmeow.SendRequestExtra{Peer: true})
		}
	}
	persistErr := c.store.FinishHistorySend(id, resp.ID, err == nil, c.currentTime())
	if err != nil {
		c.noteSendError(err)
		return "", fmt.Errorf("history request failed; delivery may be ambiguous: %w", err)
	}
	if persistErr != nil {
		return "", fmt.Errorf("history submitted but ledger update failed: %w", persistErr)
	}
	return fmt.Sprintf("History page job %d submitted: 50 messages before a real cached key at %s. Asynchronous delivery; upstream completeness remains unproven.", id, anchor.Timestamp.Format(time.RFC3339)), nil
}

// ConfigureHistoryCrawl opts a cached chat into bounded progression. Timeout,
// ambiguity, no progress or ingestion failure requires explicit review/resume.
func (c *Client) ConfigureHistoryCrawl(chat string, enabled bool) error {
	if enabled {
		if !c.store.MonitoringAllowed(chat) {
			return fmt.Errorf("monitoring consent required")
		}
		if _, err := c.store.GetHistoryAnchor(chat, time.Time{}); err != nil {
			return err
		}
	}
	return c.store.SetHistoryCrawl(chat, enabled, c.currentTime())
}
func (c *Client) HistoryCrawlStatus(chat string) (map[string]any, error) {
	return c.store.HistoryStatus(chat)
}

func (c *Client) processHistoryCrawl(ctx context.Context) {
	if !c.outboxConnected() {
		return
	}
	chat, err := c.store.NextHistoryCrawl(c.currentTime())
	if err != nil || chat == "" {
		return
	}
	if !c.store.MonitoringAllowed(chat) {
		_ = c.store.HoldHistoryCrawl(chat, "consent_revoked", c.currentTime())
		return
	}
	if _, err = c.RequestHistorySync(ctx, chat, time.Time{}); err != nil {
		_ = c.store.HoldHistoryCrawl(chat, "incomplete_request_failed", c.currentTime())
	}
}
func (c *Client) StartHistoryCrawl(ctx context.Context) {
	c.historyOnce.Do(func() {
		c.historyDone = make(chan struct{})
		go func() {
			defer close(c.historyDone)
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					c.processHistoryCrawl(ctx)
				}
			}
		}()
	})
}
func (c *Client) WaitHistoryCrawl() {
	if c.historyDone != nil {
		<-c.historyDone
	}
}
