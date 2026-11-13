package client

import (
	"context"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestHistoryUsesRealOldestAnchorAndSelfPeerOnly(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().Truncate(time.Second)
	chat := "123@s.whatsapp.net"
	if err = s.StoreChat(chat, "", now); err != nil {
		t.Fatal(err)
	}
	for _, m := range []store.Message{{ID: "old-real", ChatJID: chat, Content: "x", Timestamp: now.Add(-time.Hour)}, {ID: "new-real", ChatJID: chat, Content: "x", Timestamp: now, IsFromMe: true}} {
		if err = s.StoreMessage(context.Background(), m, nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	c := &Client{store: s, online: func() bool { return true }, senderIdentity: func() string { return "999@s.whatsapp.net" }, clock: func() time.Time { return now }, historyPeerSend: func(ctx context.Context, own types.JID, info *types.MessageInfo) (whatsmeow.SendResponse, error) {
		calls++
		if own.String() != "999@s.whatsapp.net" || info.ID != "old-real" || info.IsFromMe || !info.Timestamp.Equal(now.Add(-time.Hour)) {
			t.Fatalf("wrong peer anchor %+v own %s", info, own)
		}
		return whatsmeow.SendResponse{ID: "peer"}, nil
	}}
	if _, err = c.RequestHistorySync(context.Background(), chat, now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls %d", calls)
	}
	if _, err = c.RequestHistorySync(context.Background(), chat, time.Time{}); err == nil {
		t.Fatal("inflight bypass")
	}
}

func TestHistoryMissingAnchorNeverSendsAndRevocationRespected(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := &Client{store: s, online: func() bool { return true }, senderIdentity: func() string { return "999@s.whatsapp.net" }, historyPeerSend: func(context.Context, types.JID, *types.MessageInfo) (whatsmeow.SendResponse, error) {
		t.Fatal("unexpected peer send")
		return whatsmeow.SendResponse{}, nil
	}}
	if _, err = c.RequestHistorySync(context.Background(), "123@s.whatsapp.net", time.Time{}); err == nil {
		t.Fatal("missing anchor accepted")
	}
	if _, err = c.RequestHistorySync(context.Background(), "123@g.us", time.Time{}); err == nil {
		t.Fatal("unconsented group accepted")
	}
}

func TestHistoryEmptyTerminalObservationSurvivesWithoutPhoneClient(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := &Client{store: s, log: NewStderrLogger("test", "ERROR", false)}
	c.handleHistorySync(&events.HistorySync{Data: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String("123@s.whatsapp.net"), EndOfHistoryTransfer: proto.Bool(true), EndOfHistoryTransferType: waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum()}}}})
	var ended, terminal int
	var state string
	if err = s.DB().QueryRow(`SELECT end_transfer,terminal_type,state FROM history_observations`).Scan(&ended, &terminal, &state); err != nil {
		t.Fatal(err)
	}
	if ended != 1 || terminal != 1 || state != "observed" {
		t.Fatalf("terminal lost %d %d %s", ended, terminal, state)
	}
}

func TestHistoryIngestionFailureIsRecordedAndStopsCrawl(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().Truncate(time.Second)
	chat := "123@s.whatsapp.net"
	if err = s.StoreChat(chat, "Synthetic", now); err != nil {
		t.Fatal(err)
	}
	if err = s.SetHistoryCrawl(chat, true, now); err != nil {
		t.Fatal(err)
	}
	id, err := s.ReserveHistoryRequest(context.Background(), store.Message{ID: "anchor", ChatJID: chat, Timestamp: now}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FinishHistorySend(id, "peer", true, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec(`CREATE TRIGGER fail_history BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'synthetic failure');END`); err != nil {
		t.Fatal(err)
	}
	c := newClientWithStore(t, s)
	c.handleHistorySync(&events.HistorySync{Data: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String(chat), EndOfHistoryTransferType: waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_BUT_MORE_MSG_REMAIN_ON_PRIMARY.Enum(), Messages: []*waHistorySync.HistorySyncMsg{{Message: &waWeb.WebMessageInfo{Key: &waProto.MessageKey{ID: proto.String("old-real"), FromMe: proto.Bool(false)}, MessageTimestamp: proto.Uint64(uint64(now.Add(-time.Hour).Unix())), Message: &waProto.Message{Conversation: proto.String("synthetic fixture")}}}}}}}})
	var state string
	var failed int
	if err = s.DB().QueryRow(`SELECT state,failed FROM history_observations`).Scan(&state, &failed); err != nil {
		t.Fatal(err)
	}
	if state != "incomplete" || failed != 1 {
		t.Fatalf("failure hidden %s %d", state, failed)
	}
	if err = s.DB().QueryRow(`SELECT state FROM history_crawls`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "incomplete_ingestion" {
		t.Fatal(state)
	}
}

func TestHistoryWorkerDoesNotSendAfterManualGroupRevocation(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	chat := "123@g.us"
	now := time.Now()
	if err = s.ReconcileMonitoring(nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec(`INSERT INTO group_monitoring_consent VALUES(?,1,'manual:test',?)`, chat, now); err != nil {
		t.Fatal(err)
	}
	if err = s.StoreChat(chat, "Synthetic", now); err != nil {
		t.Fatal(err)
	}
	if err = s.StoreMessage(context.Background(), store.Message{ID: "real", ChatJID: chat, Content: "fixture", Timestamp: now}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	c := newClientWithStore(t, s)
	c.online = func() bool { return true }
	c.senderIdentity = func() string { return "999@s.whatsapp.net" }
	c.historyPeerSend = func(context.Context, types.JID, *types.MessageInfo) (whatsmeow.SendResponse, error) {
		t.Fatal("sent after revocation")
		return whatsmeow.SendResponse{}, nil
	}
	if err = c.ConfigureHistoryCrawl(chat, true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec(`UPDATE group_monitoring_consent SET allowed=0 WHERE chat_jid=?`, chat); err != nil {
		t.Fatal(err)
	}
	c.processHistoryCrawl(context.Background())
	var state string
	if err = s.DB().QueryRow(`SELECT state FROM history_crawls`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "consent_revoked" {
		t.Fatal(state)
	}
}

func TestHistoryMetadataFailureStillCapturesOriginalAndHoldsCrawl(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().Truncate(time.Second)
	chat := "123@s.whatsapp.net"
	if err = s.StoreChat(chat, "Synthetic", now); err != nil {
		t.Fatal(err)
	}
	if err = s.SetHistoryCrawl(chat, true, now); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB().Exec(`CREATE TRIGGER fail_observation BEFORE INSERT ON history_observations BEGIN SELECT RAISE(ABORT,'synthetic ledger failure');END`); err != nil {
		t.Fatal(err)
	}
	c := newClientWithStore(t, s)
	c.handleHistorySync(&events.HistorySync{Data: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{{Message: &waWeb.WebMessageInfo{Key: &waProto.MessageKey{ID: proto.String("original"), FromMe: proto.Bool(false)}, MessageTimestamp: proto.Uint64(uint64(now.Unix())), Message: &waProto.Message{Conversation: proto.String("synthetic original")}}}}}}}})
	var count int
	if err = s.DB().QueryRow(`SELECT count(*) FROM messages WHERE id='original'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("metadata failure discarded original")
	}
	var state string
	var enabled int
	if err = s.DB().QueryRow(`SELECT state,enabled FROM history_crawls`).Scan(&state, &enabled); err != nil {
		t.Fatal(err)
	}
	if state != "incomplete_ledger" || enabled != 0 {
		t.Fatalf("metadata failure did not hold crawl %s/%d", state, enabled)
	}
}
