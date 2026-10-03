package client

import (
	"context"
	"strings"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// handleReaction persists a live ReactionMessage. Returns true when the event was a reaction
// (stored or discarded) so handleMessage stops there: a reaction is never a message row of its own
// beyond the mirror row applyReaction writes.
func (c *Client) handleReaction(msg *events.Message, chatJID string) bool {
	rm := msg.Message.GetReactionMessage()
	if rm == nil {
		return false
	}
	key := rm.GetKey()
	if key == nil || key.GetID() == "" {
		c.log.Debugf("Reaction without target key from %s; dropped", c.redactor.JID(msg.Info.Sender.String()))
		return true
	}
	// The key is built from the reactor's point of view: in a 1:1 chat its RemoteJID is OUR jid,
	// so only group reactions can be scoped by it (same reasoning as handlePollVote).
	if strings.HasSuffix(chatJID, "@g.us") {
		if remote := key.GetRemoteJID(); remote != "" && c.store.ResolveLIDToJID(remote) != chatJID {
			c.log.Debugf("Reaction for another group (%s) delivered in %s; dropped", c.redactor.JID(remote), c.redactor.JID(chatJID))
			return true
		}
	}
	reactor := strings.Split(c.store.ResolveLIDToJID(msg.Info.Sender.ToNonAD().String()), "@")[0] // device-agnostic identity
	at := msg.Info.Timestamp
	if ms := rm.GetSenderTimestampMS(); ms > 0 {
		at = time.UnixMilli(ms)
	}
	// The mirror row references chats(jid): make sure the chat exists even when a reaction is the
	// first thing we ever see from it. The cached name is kept (or left empty for GetChatName to
	// resolve on the next real message); a reaction is not worth a network round-trip for a name.
	if err := c.store.StoreChat(chatJID, c.store.FindChatName(chatJID), msg.Info.Timestamp); err != nil {
		c.log.Warnf("Failed to store chat for reaction: %v", err)
	}
	c.applyReaction(context.Background(), chatJID, key.GetID(), reactor, rm.GetText(), msg.Info.ID, at, msg.Info.IsFromMe)
	return true
}

// applyReaction is shared by live events and history sync. Empty emoji removes the reaction.
func (c *Client) applyReaction(ctx context.Context, chatJID, targetID, reactor, emoji, reactionID string, at time.Time, isFromMe bool) {
	if emoji != "" {
		var err error
		emoji, err = store.NormalizeEmoji(emoji)
		if err != nil {
			c.log.Warnf("Reaction from %s dropped: %v", c.redactor.JID(reactor), err)
			return
		}
	}
	if reactionID == "" {
		reactionID = "r:" + targetID + ":" + reactor
	}
	r := store.Reaction{MessageID: targetID, ChatJID: chatJID, Reactor: reactor, Emoji: emoji, Timestamp: at, ReactionID: reactionID}
	var mirror *store.Message
	if emoji != "" {
		excerpt, found := c.store.MessageExcerpt(ctx, targetID, chatJID)
		if !found {
			excerpt = "mensaje no disponible"
		}
		mirror = &store.Message{Content: reactionMirrorContent(emoji, excerpt), IsFromMe: isFromMe}
	}
	if err := c.store.ApplyReaction(ctx, r, mirror); err != nil {
		c.log.Warnf("Failed to apply reaction: %v", err)
	}
}

func reactionMirrorContent(emoji, excerpt string) string {
	return emoji + " → «" + excerpt + "»"
}

// applyHistoryReactions mirrors the aggregated reactions WhatsApp attaches to each history message.
func (c *Client) applyHistoryReactions(ctx context.Context, chatJID, targetID, ownID string, reactions []*waWeb.Reaction) {
	for _, r := range reactions {
		if r == nil || r.GetText() == "" {
			continue // history never carries removals; an empty text is noise
		}
		key := r.GetKey()
		reactor, fromMe := "", false
		switch {
		case key != nil && key.GetFromMe():
			reactor, fromMe = ownID, true
			if reactor == "" {
				reactor = "me"
			}
		case key != nil && key.GetParticipant() != "":
			participant := key.GetParticipant()
			if jid, err := types.ParseJID(participant); err == nil {
				participant = jid.ToNonAD().String()
			}
			reactor = strings.Split(c.store.ResolveLIDToJID(participant), "@")[0]
		default:
			reactor = strings.Split(chatJID, "@")[0] // direct chat: the other party
		}
		at := time.UnixMilli(r.GetSenderTimestampMS())
		if r.GetSenderTimestampMS() <= 0 {
			// Undated history has unknown age; never treat a replay as a new event.
			at = time.Time{}
		}
		c.applyReaction(ctx, chatJID, targetID, reactor, r.GetText(), "", at, fromMe)
	}
}
