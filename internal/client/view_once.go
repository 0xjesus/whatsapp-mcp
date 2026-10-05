package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func (c *Client) ViewOnceStatus(chat, id string) (store.ViewOnce, error) {
	return c.store.GetViewOnce(c.store.ResolveLIDToJID(chat), id)
}

func (c *Client) handleUnavailable(evt *events.UndecryptableMessage) {
	if evt == nil || !c.store.MonitoringAllowed(evt.Info.Chat.String()) || !evt.IsUnavailable || evt.UnavailableType != events.UnavailableTypeViewOnce {
		return
	}
	r := store.ViewOnce{MessageID: evt.Info.ID, ChatJID: c.store.ResolveLIDToJID(evt.Info.Chat.String()), SenderJID: evt.Info.Sender.String(), State: "unavailable"}
	if err := c.store.PutViewOnce(context.Background(), r); err != nil {
		c.log.Warnf("Record unavailable view-once: %v", err)
	}
}

func isViewOnceMessage(evt *events.Message) bool {
	body := evt.Message
	return evt.IsViewOnce || body.GetImageMessage().GetViewOnce() || body.GetVideoMessage().GetViewOnce() || body.GetAudioMessage().GetViewOnce()
}

func (c *Client) captureViewOnce(evt *events.Message, chat string) {
	body := evt.Message
	if body == nil {
		return
	}
	isViewOnce := isViewOnceMessage(evt)
	previous, err := c.store.GetViewOnce(chat, evt.Info.ID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		c.log.Warnf("Read view-once status: %v", err)
		return
	}
	if !isViewOnce && errors.Is(err, sql.ErrNoRows) {
		return
	}
	if previous.State == "saved" {
		return
	}
	mediaType, _, _, _, _, _, _, _ := extractMediaInfo(body)
	if mediaType == "" {
		return
	}
	r := store.ViewOnce{MessageID: evt.Info.ID, ChatJID: chat, SenderJID: evt.Info.Sender.String(), State: "received"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.store.PutViewOnce(ctx, r); err != nil {
		c.log.Warnf("Record view-once receipt: %v", err)
		return
	}
	download := c.captureDownload
	if download == nil {
		download = c.Download
	}
	result := download(ctx, evt.Info.ID, chat, "")
	if result.Success {
		r.State = "saved"
		r.Path = result.Path
	} else {
		r.State = "download_failed"
		r.Error = result.Message
	}
	if err := c.store.PutViewOnce(context.Background(), r); err != nil {
		c.log.Warnf("Record view-once download: %v", err)
	}
	c.log.Infof("View-once %s: %s", c.redactor.MsgID(evt.Info.ID), r.State)
}

// RequestViewOnceRecovery asks only our own primary phone. It sends no chat
// message and no read/played receipt. Payload delivery is asynchronous.
// Direct chats resolve the other party's current LID; groups use the sender
// recorded with the unavailable notice as the participant to ask about.
func (c *Client) RequestViewOnceRecovery(ctx context.Context, chat, id string) (store.ViewOnce, error) {
	var zero store.ViewOnce
	jid, err := types.ParseJID(chat)
	isGroup := err == nil && jid.Server == types.GroupServer
	if err != nil || jid.User == "" || jid.Device != 0 || (!isGroup && jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer) {
		return zero, fmt.Errorf("chat_jid must be a direct-chat phone/LID JID or a group JID")
	}
	if len(id) == 0 || len(id) > 128 || strings.ContainsAny(id, " \t\r\n/") {
		return zero, fmt.Errorf("invalid message_id")
	}
	if err := c.sendGate(false); err != nil {
		return zero, err
	}
	if !c.isOnline() {
		return zero, fmt.Errorf("client not connected and paired")
	}
	normalized := chat
	if !isGroup {
		normalized = c.store.ResolveLIDToJID(chat)
	}
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	if time.Since(c.lastRecovery) < 45*time.Second {
		return zero, fmt.Errorf("recovery cooldown: wait 45 seconds between requests")
	}
	r, err := c.store.GetViewOnce(normalized, id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return zero, err
	}
	if r.State == "saved" {
		return r, nil
	}
	requestJID, participant := jid, jid
	if isGroup {
		if errors.Is(err, sql.ErrNoRows) || r.SenderJID == "" {
			return zero, fmt.Errorf("group recovery needs the original sender; nothing recorded for this message")
		}
		if participant, err = types.ParseJID(r.SenderJID); err != nil {
			return zero, fmt.Errorf("stored sender_jid is invalid: %w", err)
		}
		participant = participant.ToNonAD()
		if participant.Server == types.DefaultUserServer && c.wa != nil && c.wa.Store != nil && c.wa.Store.LIDs != nil {
			if lid, e := c.wa.Store.LIDs.GetLIDForPN(ctx, participant); e == nil && !lid.IsEmpty() {
				participant = lid
			}
		}
	} else {
		if jid.Server == types.DefaultUserServer {
			lid, e := c.wa.Store.LIDs.GetLIDForPN(ctx, jid)
			if e != nil {
				return zero, fmt.Errorf("resolve phone LID: %w", e)
			}
			if !lid.IsEmpty() {
				requestJID = lid
			}
		}
		participant = requestJID
		if errors.Is(err, sql.ErrNoRows) {
			r = store.ViewOnce{MessageID: id, ChatJID: normalized, SenderJID: requestJID.String(), State: "unavailable"}
			if err = c.store.PutViewOnce(ctx, r); err != nil {
				return zero, err
			}
		}
	}
	reserved, err := c.store.ReserveViewOnceRequest(ctx, normalized, id, time.Now().Unix())
	if err != nil {
		return zero, err
	}
	if !reserved {
		return zero, fmt.Errorf("message already saved or recovery requested within 5 minutes; inspect view_once_status")
	}
	c.lastRecovery = time.Now()
	sendCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	resp, sendErr := c.sendPeerRequest(sendCtx, requestJID, participant, id)
	reason := ""
	if sendErr != nil {
		reason = sendErr.Error()
		c.noteSendError(sendErr)
	}
	if err := c.store.FinishViewOnceRequest(context.Background(), normalized, id, resp.ID, reason); err != nil {
		return zero, err
	}
	if sendErr != nil {
		return zero, fmt.Errorf("phone request failed: %w", sendErr)
	}
	return c.store.GetViewOnce(normalized, id)
}

func (c *Client) isOnline() bool {
	if c.online != nil {
		return c.online()
	}
	return c.wa != nil && c.wa.IsConnected() && c.wa.Store.ID != nil
}

func (c *Client) sendPeerRequest(ctx context.Context, chat, participant types.JID, id string) (whatsmeow.SendResponse, error) {
	if c.peerRequest != nil {
		return c.peerRequest(ctx, chat, participant, id)
	}
	return c.wa.SendPeerMessage(ctx, c.wa.BuildUnavailableMessageRequest(chat, participant, id))
}
