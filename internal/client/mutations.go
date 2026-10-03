package client

import (
	"context"
	"time"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types/events"
)

// applyIncomingMutation accepts only explicit, supported protocol actions for
// this chat. A missing Type must never become REVOKE via protobuf's zero value.
func (c *Client) applyIncomingMutation(chatJID string, raw *waProto.Message, received time.Time) bool {
	unwrapped := (&events.Message{RawMessage: raw}).UnwrapRaw().Message
	protocol := unwrapped.GetProtocolMessage()
	if protocol == nil {
		return false
	}
	if protocol.Type == nil || protocol.Key == nil || protocol.Key.GetID() == "" {
		return true
	}
	if remote := protocol.Key.GetRemoteJID(); remote != "" && c.store.ResolveLIDToJID(remote) != chatJID {
		return true
	}
	timestampMS := protocol.GetTimestampMS()
	if timestampMS <= 0 {
		timestampMS = received.UnixMilli()
	}
	var err error
	switch protocol.GetType() {
	case waProto.ProtocolMessage_REVOKE:
		err = c.store.ApplyMessageRevoke(context.Background(), protocol.Key.GetID(), chatJID, timestampMS)
	case waProto.ProtocolMessage_MESSAGE_EDIT:
		content, ok := editedTextContent(protocol.GetEditedMessage())
		if !ok {
			return true
		}
		err = c.store.ApplyMessageEdit(context.Background(), protocol.Key.GetID(), chatJID, content, timestampMS)
	}
	if err != nil {
		c.log.Warnf("Failed to persist incoming message mutation: %v", err)
	}
	return true
}

// Only fields that explicitly contain text count as edits. An unsupported or
// incomplete payload cannot erase cached content. A present empty caption can.
func editedTextContent(msg *waProto.Message) (string, bool) {
	if msg == nil {
		return "", false
	}
	switch {
	case msg.Conversation != nil:
		return msg.GetConversation(), true
	case msg.GetExtendedTextMessage() != nil && msg.GetExtendedTextMessage().Text != nil:
		return msg.GetExtendedTextMessage().GetText(), true
	case msg.GetImageMessage() != nil && msg.GetImageMessage().Caption != nil:
		return msg.GetImageMessage().GetCaption(), true
	case msg.GetVideoMessage() != nil && msg.GetVideoMessage().Caption != nil:
		return msg.GetVideoMessage().GetCaption(), true
	case msg.GetDocumentMessage() != nil && msg.GetDocumentMessage().Caption != nil:
		return msg.GetDocumentMessage().GetCaption(), true
	default:
		return "", false
	}
}
