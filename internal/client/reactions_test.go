package client

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	wmstore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const reactionGroup = "120363000000000001@g.us"

func reactionClient(t *testing.T) (*Client, *store.Store) {
	t.Helper()
	s := newTestStoreWithLIDMap(t, map[string]string{"99887766001": "447700000102"})
	approveTestGroup(t, s, reactionGroup)
	c := newClientWithStore(t, s)
	c.wa = &whatsmeow.Client{Store: &wmstore.Device{}} // history sync reads own ID from here
	if err := s.StoreChat(reactionGroup, "Test Group", originalTime); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(context.Background(), store.Message{ID: "target", ChatJID: reactionGroup, Sender: "447700000101",
		Content: "Primera línea del mensaje original", Timestamp: originalTime}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	return c, s
}

func reactionEvent(id, targetID, emoji, senderLID string, fromMe bool) *events.Message {
	chat, _ := types.ParseJID(reactionGroup)
	sender, _ := types.ParseJID(senderLID + "@lid")
	raw := &waProto.Message{ReactionMessage: &waProto.ReactionMessage{
		Key:               &waProto.MessageKey{RemoteJID: proto.String(reactionGroup), ID: proto.String(targetID), FromMe: proto.Bool(false)},
		Text:              proto.String(emoji),
		SenderTimestampMS: proto.Int64(originalTime.Add(time.Hour).UnixMilli()),
	}}
	return (&events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsFromMe: fromMe, IsGroup: true},
		ID: id, Timestamp: originalTime.Add(time.Hour)}, RawMessage: raw}).UnwrapRaw()
}

func mirrorRow(t *testing.T, s *store.Store, id string) (content, mediaType, sender string, fromMe bool, err error) {
	t.Helper()
	err = s.DB().QueryRow("SELECT content, media_type, sender, is_from_me FROM messages WHERE id=? AND chat_jid=?", id, reactionGroup).Scan(&content, &mediaType, &sender, &fromMe)
	return
}

func TestReaction_LiveAddStoresReactionAndMirror(t *testing.T) {
	c, s := reactionClient(t)
	c.handleMessage(reactionEvent("AC1", "target", "👍", "99887766001", false))
	got, _ := s.ReactionsFor(context.Background(), reactionGroup, []string{"target"})
	if len(got["target"]) != 1 || got["target"][0].Reactor != "447700000102" || got["target"][0].Emoji != "👍" {
		t.Fatalf("reactions = %+v", got["target"])
	}
	content, mediaType, sender, fromMe, err := mirrorRow(t, s, "AC1")
	if err != nil || mediaType != "reaction" || sender != "447700000102" || fromMe {
		t.Fatalf("mirror = %q %q %q %v %v", content, mediaType, sender, fromMe, err)
	}
	if content != "👍 → «Primera línea del mensaje original»" {
		t.Fatalf("mirror content = %q", content)
	}
}

func TestReaction_LiveRemoveDeletesBoth(t *testing.T) {
	c, s := reactionClient(t)
	c.handleMessage(reactionEvent("AC1", "target", "👍", "99887766001", false))
	c.handleMessage(reactionEvent("AC2", "target", "", "99887766001", false))
	got, _ := s.ReactionsFor(context.Background(), reactionGroup, []string{"target"})
	if len(got["target"]) != 0 {
		t.Fatalf("reaction still present: %+v", got["target"])
	}
	if _, _, _, _, err := mirrorRow(t, s, "AC1"); err != sql.ErrNoRows {
		t.Fatalf("mirror row still present: %v", err)
	}
	if _, _, _, _, err := mirrorRow(t, s, "AC2"); err != sql.ErrNoRows {
		t.Fatalf("removal envelope must not be stored: %v", err)
	}
}

func TestReaction_TargetMissing(t *testing.T) {
	c, s := reactionClient(t)
	c.handleMessage(reactionEvent("AC9", "ghost", "🔥", "99887766001", false))
	content, _, _, _, err := mirrorRow(t, s, "AC9")
	if err != nil || content != "🔥 → «mensaje no disponible»" {
		t.Fatalf("mirror = %q %v", content, err)
	}
}

func TestReaction_FromMe(t *testing.T) {
	c, s := reactionClient(t)
	c.handleMessage(reactionEvent("AC3", "target", "❤️", "99887766001", true))
	_, _, _, fromMe, err := mirrorRow(t, s, "AC3")
	if err != nil || !fromMe {
		t.Fatalf("own reaction mirror is_from_me = %v %v", fromMe, err)
	}
}

func TestReaction_InvalidEmojiIsDropped(t *testing.T) {
	c, s := reactionClient(t)
	c.handleMessage(reactionEvent("AC4", "target", string([]byte{0xff}), "99887766001", false))
	if _, _, _, _, err := mirrorRow(t, s, "AC4"); err != sql.ErrNoRows {
		t.Fatalf("invalid emoji produced a row: %v", err)
	}
}

func historyWithReactions(t *testing.T, reactions []*waWeb.Reaction) *events.HistorySync {
	t.Helper()
	return &events.HistorySync{Data: &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{{
		ID: proto.String(reactionGroup),
		Messages: []*waHistorySync.HistorySyncMsg{{Message: &waWeb.WebMessageInfo{
			Key:              &waProto.MessageKey{RemoteJID: proto.String(reactionGroup), ID: proto.String("hist1"), Participant: proto.String("447700000101@s.whatsapp.net")},
			MessageTimestamp: proto.Uint64(uint64(originalTime.Unix())),
			Message:          &waProto.Message{Conversation: proto.String("los assets ya están listos")},
			Reactions:        reactions,
		}}},
	}}}}
}

func TestHistoryReactions_StoredWithSyntheticMirrorID(t *testing.T) {
	c, s := reactionClient(t)
	c.handleHistorySync(historyWithReactions(t, []*waWeb.Reaction{{
		Key:               &waProto.MessageKey{RemoteJID: proto.String(reactionGroup), ID: proto.String("hist1"), Participant: proto.String("99887766001@lid")},
		Text:              proto.String("🧙"),
		SenderTimestampMS: proto.Int64(originalTime.Add(time.Minute).UnixMilli()),
	}}))
	got, _ := s.ReactionsFor(context.Background(), reactionGroup, []string{"hist1"})
	if len(got["hist1"]) != 1 || got["hist1"][0].Reactor != "447700000102" || got["hist1"][0].ReactionID != "r:hist1:447700000102" {
		t.Fatalf("history reaction = %+v", got["hist1"])
	}
	content, mediaType, _, _, err := mirrorRow(t, s, "r:hist1:447700000102")
	if err != nil || mediaType != "reaction" || content != "🧙 → «los assets ya están listos»" {
		t.Fatalf("mirror = %q %q %v", content, mediaType, err)
	}
}

func TestHistoryReactions_Idempotent(t *testing.T) {
	c, s := reactionClient(t)
	h := historyWithReactions(t, []*waWeb.Reaction{{
		Key:  &waProto.MessageKey{RemoteJID: proto.String(reactionGroup), ID: proto.String("hist1"), Participant: proto.String("99887766001@lid")},
		Text: proto.String("🧙"), SenderTimestampMS: proto.Int64(originalTime.Add(time.Minute).UnixMilli()),
	}})
	c.handleHistorySync(h)
	c.handleHistorySync(h)
	var n, m int
	_ = s.DB().QueryRow("SELECT count(*) FROM reactions WHERE message_id='hist1'").Scan(&n)
	_ = s.DB().QueryRow("SELECT count(*) FROM messages WHERE media_type='reaction' AND chat_jid=?", reactionGroup).Scan(&m)
	if n != 1 || m != 1 {
		t.Fatalf("not idempotent: reactions=%d mirrors=%d", n, m)
	}
}

func TestHistoryReactions_OwnReactionUsesOwnID(t *testing.T) {
	c, s := reactionClient(t)
	c.handleHistorySync(historyWithReactions(t, []*waWeb.Reaction{{
		Key:  &waProto.MessageKey{RemoteJID: proto.String(reactionGroup), ID: proto.String("hist1"), FromMe: proto.Bool(true)},
		Text: proto.String("👍"), SenderTimestampMS: proto.Int64(originalTime.Add(time.Minute).UnixMilli()),
	}}))
	got, _ := s.ReactionsFor(context.Background(), reactionGroup, []string{"hist1"})
	if len(got["hist1"]) != 1 || got["hist1"][0].Reactor == "" {
		t.Fatalf("own history reaction = %+v", got["hist1"])
	}
	_, _, _, fromMe, _ := mirrorRow(t, s, got["hist1"][0].ReactionID)
	if !fromMe {
		t.Fatal("own history reaction mirror should be is_from_me")
	}
}

// --- review fix pass ---

func TestReaction_DirectChatRemoteJIDIsOwnJID(t *testing.T) {
	// In a 1:1 chat the reactor's key names OUR jid (their chat), never the chat jid we observe.
	c, s := reactionClient(t)
	dm := "447700000001@s.whatsapp.net"
	if err := s.StoreChat(dm, "Ana", originalTime); err != nil {
		t.Fatal(err)
	}
	_ = s.StoreMessage(context.Background(), store.Message{ID: "dm-target", ChatJID: dm, Sender: "447700000100", IsFromMe: true, Content: "hola", Timestamp: originalTime}, nil, nil, nil, 0)
	chat, _ := types.ParseJID(dm)
	raw := &waProto.Message{ReactionMessage: &waProto.ReactionMessage{
		Key:  &waProto.MessageKey{RemoteJID: proto.String("447700000100@s.whatsapp.net"), ID: proto.String("dm-target"), FromMe: proto.Bool(false)},
		Text: proto.String("🔥"), SenderTimestampMS: proto.Int64(originalTime.Add(time.Hour).UnixMilli()),
	}}
	ev := (&events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: chat}, ID: "DM1", Timestamp: originalTime.Add(time.Hour)}, RawMessage: raw}).UnwrapRaw()
	c.handleMessage(ev)
	got, _ := s.ReactionsFor(context.Background(), dm, []string{"dm-target"})
	if len(got["dm-target"]) != 1 || got["dm-target"][0].Emoji != "🔥" {
		t.Fatalf("direct-chat reaction dropped: %+v", got["dm-target"])
	}
}

func TestReaction_EmojiChangeReplacesMirror(t *testing.T) {
	c, s := reactionClient(t)
	c.handleMessage(reactionEvent("AC1", "target", "👍", "99887766001", false))
	c.handleMessage(reactionEvent("AC2", "target", "❤️", "99887766001", false))
	got, _ := s.ReactionsFor(context.Background(), reactionGroup, []string{"target"})
	if len(got["target"]) != 1 || got["target"][0].Emoji != "❤️" {
		t.Fatalf("reactions = %+v", got["target"])
	}
	if _, _, _, _, err := mirrorRow(t, s, "AC1"); err != sql.ErrNoRows {
		t.Fatalf("old mirror row AC1 still present: %v", err)
	}
	if content, _, _, _, err := mirrorRow(t, s, "AC2"); err != nil || content != "❤️ → «Primera línea del mensaje original»" {
		t.Fatalf("new mirror = %q %v", content, err)
	}
}

func TestReaction_UnseededChatIsCreated(t *testing.T) {
	c, s := reactionClient(t)
	fresh := "120363999999999999@g.us"
	approveTestGroup(t, s, fresh)
	chat, _ := types.ParseJID(fresh)
	sender, _ := types.ParseJID("99887766001@lid")
	raw := &waProto.Message{ReactionMessage: &waProto.ReactionMessage{
		Key:  &waProto.MessageKey{RemoteJID: proto.String(fresh), ID: proto.String("unknown"), FromMe: proto.Bool(false)},
		Text: proto.String("👀"), SenderTimestampMS: proto.Int64(originalTime.Add(time.Hour).UnixMilli()),
	}}
	ev := (&events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsGroup: true}, ID: "NEW1", Timestamp: originalTime.Add(time.Hour)}, RawMessage: raw}).UnwrapRaw()
	c.handleMessage(ev)
	var content string
	if err := s.DB().QueryRow("SELECT content FROM messages WHERE id='NEW1' AND chat_jid=?", fresh).Scan(&content); err != nil || content != "👀 → «mensaje no disponible»" {
		t.Fatalf("mirror in unseeded chat = %q %v", content, err)
	}
	var n int
	_ = s.DB().QueryRow("SELECT count(*) FROM chats WHERE jid=?", fresh).Scan(&n)
	if n != 1 {
		t.Fatalf("chat row not created: %d", n)
	}
}

func TestReaction_ReactorIgnoresDeviceSuffix(t *testing.T) {
	// A reaction from a linked device arrives as <lid>:<device>@lid; the reactor identity must not
	// depend on the device, or the same person gets one row per device.
	c, s := reactionClient(t)
	chat, _ := types.ParseJID(reactionGroup)
	sender, _ := types.ParseJID("99887766001:20@lid")
	raw := &waProto.Message{ReactionMessage: &waProto.ReactionMessage{
		Key:  &waProto.MessageKey{RemoteJID: proto.String(reactionGroup), ID: proto.String("target"), FromMe: proto.Bool(false)},
		Text: proto.String("👍"), SenderTimestampMS: proto.Int64(originalTime.Add(time.Hour).UnixMilli()),
	}}
	ev := (&events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsGroup: true}, ID: "DEV1", Timestamp: originalTime.Add(time.Hour)}, RawMessage: raw}).UnwrapRaw()
	c.handleMessage(ev)
	got, _ := s.ReactionsFor(context.Background(), reactionGroup, []string{"target"})
	if len(got["target"]) != 1 || got["target"][0].Reactor != "447700000102" {
		t.Fatalf("reactor with device suffix = %+v (want 447700000102)", got["target"])
	}
	unknown, _ := types.ParseJID("99999999999999:7@lid")
	ev.Info.Sender = unknown
	ev.Info.ID = "DEV2"
	c.handleMessage(ev)
	got, _ = s.ReactionsFor(context.Background(), reactionGroup, []string{"target"})
	for _, r := range got["target"] {
		if strings.Contains(r.Reactor, ":") {
			t.Fatalf("unmapped reactor keeps device suffix: %q", r.Reactor)
		}
	}
}

func TestReaction_EventOrdering(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		firstEmoji, firstID   string
		secondEmoji, secondID string
		secondOffset          time.Duration
		wantEmoji, wantID     string
	}{
		{"stale replacement", "❤️", "LIVE2", "👍", "LIVE1", -time.Second, "❤️", "LIVE2"},
		{"stale removal", "❤️", "LIVE2", "", "REMOVE1", -time.Second, "❤️", "LIVE2"},
		{"history after removal", "", "REMOVE2", "👍", "", -time.Second, "", ""},
		{"equal time history after live", "❤️", "LIVE2", "👍", "", 0, "❤️", "LIVE2"},
		{"equal time history after removal", "", "REMOVE2", "👍", "", 0, "", ""},
		{"equal time live after removal", "", "REMOVE2", "👍", "LIVE3", 0, "", ""},
		{"equal time reversed live delivery", "❤️", "LIVE2", "👍", "LIVE1", 0, "❤️", "LIVE2"},
		{"new add after removal", "", "REMOVE2", "👍", "LIVE3", time.Second, "👍", "LIVE3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, s := reactionClient(t)
			ctx := context.Background()
			at := originalTime.Add(time.Hour)
			c.applyReaction(ctx, reactionGroup, "target", "reactor", tc.firstEmoji, tc.firstID, at, false)
			c.applyReaction(ctx, reactionGroup, "target", "reactor", tc.secondEmoji, tc.secondID, at.Add(tc.secondOffset), false)
			got, err := s.ReactionsFor(ctx, reactionGroup, []string{"target"})
			if err != nil {
				t.Fatal(err)
			}
			wantCount := 0
			if tc.wantEmoji != "" {
				wantCount = 1
				if len(got["target"]) != 1 || got["target"][0].Emoji != tc.wantEmoji || got["target"][0].ReactionID != tc.wantID {
					t.Fatalf("reactions = %+v, want %s %s", got["target"], tc.wantEmoji, tc.wantID)
				}
			} else if len(got["target"]) != 0 {
				t.Fatalf("reaction resurrected: %+v", got["target"])
			}
			var count int
			if err := s.DB().QueryRow("SELECT count(*) FROM messages WHERE chat_jid=? AND media_type='reaction'", reactionGroup).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != wantCount {
				t.Fatalf("mirror count = %d, want %d", count, wantCount)
			}
			if tc.wantID != "" {
				content, _, _, _, err := mirrorRow(t, s, tc.wantID)
				if err != nil || !strings.HasPrefix(content, tc.wantEmoji) {
					t.Fatalf("mirror = %q, %v", content, err)
				}
			}
		})
	}
}

func TestReaction_UndatedHistoryCannotOverrideLive(t *testing.T) {
	c, s := reactionClient(t)
	ctx := context.Background()
	c.applyReaction(ctx, reactionGroup, "target", "447700000102", "", "REMOVED", originalTime, false)
	c.applyHistoryReactions(ctx, reactionGroup, "target", "", []*waWeb.Reaction{{
		Key: &waProto.MessageKey{Participant: proto.String("99887766001@lid")}, Text: proto.String("👍"),
	}})
	got, err := s.ReactionsFor(ctx, reactionGroup, []string{"target"})
	if err != nil || len(got["target"]) != 0 {
		t.Fatalf("undated history resurrected removal: %+v, %v", got, err)
	}
}

func TestHistoryReactionDeviceSuffixCannotBypassRemoval(t *testing.T) {
	c, s := reactionClient(t)
	ctx := context.Background()
	c.applyReaction(ctx, reactionGroup, "target", "447700000102", "", "REMOVED", originalTime.Add(time.Hour), false)
	c.applyHistoryReactions(ctx, reactionGroup, "target", "", []*waWeb.Reaction{{
		Key:  &waProto.MessageKey{Participant: proto.String("99887766001:20@lid")},
		Text: proto.String("👍"), SenderTimestampMS: proto.Int64(originalTime.UnixMilli()),
	}})
	got, err := s.ReactionsFor(ctx, reactionGroup, []string{"target"})
	if err != nil || len(got["target"]) != 0 {
		t.Fatalf("history device suffix bypassed tombstone: %+v, %v", got, err)
	}
}
