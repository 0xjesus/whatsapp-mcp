package client

import (
	"context"
	"database/sql"
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

const mutationChat = "447700000001@s.whatsapp.net"

var originalTime = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func mutationClient(t *testing.T) (*Client, *store.Store) {
	t.Helper()
	s := newTestStoreWithLIDMap(t, nil)
	c := newClientWithStore(t, s)
	if err := s.StoreChat(mutationChat, "Synthetic", originalTime); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(context.Background(), store.Message{ID: "target", ChatJID: mutationChat, Sender: "447700000001", Content: "original", Timestamp: originalTime}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`CREATE TABLE observed_changes(seq INTEGER PRIMARY KEY,op TEXT,id TEXT,chat_jid TEXT);
        CREATE TRIGGER observe_update AFTER UPDATE OF content ON messages WHEN old.content IS NOT new.content BEGIN
            INSERT INTO observed_changes(op,id,chat_jid) VALUES('upsert',new.id,new.chat_jid);END;
        CREATE TRIGGER observe_delete AFTER DELETE ON messages BEGIN
            INSERT INTO observed_changes(op,id,chat_jid) VALUES('delete',old.id,old.chat_jid);END;`); err != nil {
		t.Fatal(err)
	}
	return c, s
}

func mutationEvent(raw *waProto.Message) *events.Message {
	jid, _ := types.ParseJID(mutationChat)
	return (&events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, Sender: jid}, ID: "protocol-envelope", Timestamp: originalTime.Add(time.Hour)}, RawMessage: raw}).UnwrapRaw()
}

func protocolMutation(kind waProto.ProtocolMessage_Type, body string, at int64) *waProto.Message {
	p := &waProto.ProtocolMessage{Type: kind.Enum(), Key: &waProto.MessageKey{RemoteJID: proto.String(mutationChat), ID: proto.String("target")}, TimestampMS: proto.Int64(at)}
	if kind == waProto.ProtocolMessage_MESSAGE_EDIT {
		p.EditedMessage = &waProto.Message{Conversation: proto.String(body)}
	}
	return &waProto.Message{ProtocolMessage: p}
}

func cachedBody(t *testing.T, s *store.Store) (string, time.Time, error) {
	t.Helper()
	var body string
	var timestamp time.Time
	err := s.DB().QueryRow("SELECT content,timestamp FROM messages WHERE id='target' AND chat_jid=?", mutationChat).Scan(&body, &timestamp)
	return body, timestamp, err
}

func TestIncomingEditUpdatesOriginalAndEmitsSourceChange(t *testing.T) {
	c, s := mutationClient(t)
	raw := protocolMutation(waProto.ProtocolMessage_MESSAGE_EDIT, "edited", originalTime.Add(time.Hour).UnixMilli())
	c.handleMessage(mutationEvent(&waProto.Message{EditedMessage: &waProto.FutureProofMessage{Message: raw}}))
	body, ts, err := cachedBody(t, s)
	if err != nil || body != "edited" {
		t.Fatalf("edit not applied: %q %v", body, err)
	}
	if !ts.Equal(originalTime) {
		t.Errorf("edit moved original timestamp: %v", ts)
	}
	var op string
	if err := s.DB().QueryRow("SELECT op FROM observed_changes ORDER BY seq DESC LIMIT 1").Scan(&op); err != nil || op != "upsert" {
		t.Fatalf("edit not captured: %q %v", op, err)
	}
	c.handleMessage(mutationEvent(protocolMutation(waProto.ProtocolMessage_MESSAGE_EDIT, "older edit", originalTime.Add(30*time.Minute).UnixMilli())))
	if err := s.StoreMessage(context.Background(), store.Message{ID: "target", ChatJID: mutationChat, Content: "stale history", Timestamp: originalTime}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	body, _, _ = cachedBody(t, s)
	if body != "edited" {
		t.Fatalf("stale replay overwrote latest edit: %q", body)
	}
}

func TestIncomingRevokeDeletesCompositeKeyAndBlocksRedelivery(t *testing.T) {
	c, s := mutationClient(t)
	if err := s.StoreChat("other@s.whatsapp.net", "Other", originalTime); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(context.Background(), store.Message{ID: "target", ChatJID: "other@s.whatsapp.net", Content: "other chat", Timestamp: originalTime}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	c.handleMessage(mutationEvent(protocolMutation(waProto.ProtocolMessage_REVOKE, "", 0)))
	if _, _, err := cachedBody(t, s); err != sql.ErrNoRows {
		t.Fatalf("revoked message remains: %v", err)
	}
	var op string
	if err := s.DB().QueryRow("SELECT op FROM observed_changes ORDER BY seq DESC LIMIT 1").Scan(&op); err != nil || op != "delete" {
		t.Fatalf("revoke not captured: %q %v", op, err)
	}
	if err := s.StoreMessage(context.Background(), store.Message{ID: "target", ChatJID: mutationChat, Content: "stale history", Timestamp: originalTime}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cachedBody(t, s); err != sql.ErrNoRows {
		t.Fatalf("stale delivery resurrected revoke: %v", err)
	}
	var count int
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM messages WHERE id='target' AND chat_jid='other@s.whatsapp.net'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("other chat affected: %d %v", count, err)
	}
}

func TestIncomingMutationBeforeOriginalIsAppliedAfterHistoryArrives(t *testing.T) {
	for _, kind := range []waProto.ProtocolMessage_Type{waProto.ProtocolMessage_MESSAGE_EDIT, waProto.ProtocolMessage_REVOKE} {
		t.Run(kind.String(), func(t *testing.T) {
			c, s := mutationClient(t)
			if _, err := s.DB().Exec("DELETE FROM messages WHERE id='target' AND chat_jid=?", mutationChat); err != nil {
				t.Fatal(err)
			}
			c.handleMessage(mutationEvent(protocolMutation(kind, "edited before backfill", originalTime.Add(time.Hour).UnixMilli())))
			if err := s.StoreMessage(context.Background(), store.Message{ID: "target", ChatJID: mutationChat, Content: "original", Timestamp: originalTime}, nil, nil, nil, 0); err != nil {
				t.Fatal(err)
			}
			body, _, err := cachedBody(t, s)
			if kind == waProto.ProtocolMessage_REVOKE {
				if err != sql.ErrNoRows {
					t.Fatal("revocation lost before backfill")
				}
			} else if err != nil || body != "edited before backfill" {
				t.Fatalf("pending edit lost: %q %v", body, err)
			}
		})
	}
}

func TestUnknownOrMismatchedProtocolNeverDeletes(t *testing.T) {
	for _, name := range []string{"missing_type", "unknown_type", "different_chat", "missing_key"} {
		t.Run(name, func(t *testing.T) {
			c, s := mutationClient(t)
			raw := protocolMutation(waProto.ProtocolMessage_REVOKE, "", 0)
			switch name {
			case "missing_type":
				raw.ProtocolMessage.Type = nil
			case "unknown_type":
				raw.ProtocolMessage.Type = waProto.ProtocolMessage_Type(999).Enum()
			case "different_chat":
				raw.ProtocolMessage.Key.RemoteJID = proto.String("other@s.whatsapp.net")
			case "missing_key":
				raw.ProtocolMessage.Key = nil
			}
			c.handleMessage(mutationEvent(raw))
			body, _, err := cachedBody(t, s)
			if err != nil || body != "original" {
				t.Fatalf("unknown protocol changed original: %q %v", body, err)
			}
		})
	}
}

func TestPassiveHistoryMutationUsesSameRules(t *testing.T) {
	c, s := mutationClient(t)
	// Offline SDK struct only; no account client is connected or authenticated.
	c.wa = &whatsmeow.Client{Store: &wmstore.Device{}}
	raw := protocolMutation(waProto.ProtocolMessage_MESSAGE_EDIT, "history edit", originalTime.Add(time.Hour).UnixMilli())
	c.handleHistorySync(&events.HistorySync{Data: &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{{ID: proto.String(mutationChat), Messages: []*waHistorySync.HistorySyncMsg{{Message: &waWeb.WebMessageInfo{Key: &waProto.MessageKey{ID: proto.String("edit-envelope")}, MessageTimestamp: proto.Uint64(uint64(originalTime.Add(time.Hour).Unix())), Message: &waProto.Message{EditedMessage: &waProto.FutureProofMessage{Message: raw}}}}}}}}})
	body, _, err := cachedBody(t, s)
	if err != nil || body != "history edit" {
		t.Fatalf("history edit not applied: %q %v", body, err)
	}
}
