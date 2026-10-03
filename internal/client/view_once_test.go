package client

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	wmstore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestViewOnceUnavailableRecordedWithoutFakeMessage(t *testing.T) {
	s := newTestStoreWithLIDMap(t, map[string]string{"99887766": "447700000002"})
	c := newClientWithStore(t, s)
	jid, _ := types.ParseJID("99887766@lid")
	c.handleUnavailable(&events.UndecryptableMessage{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, Sender: jid}, ID: "vo-missing", Timestamp: time.Now()}, IsUnavailable: true, UnavailableType: events.UnavailableTypeViewOnce})
	r, err := c.ViewOnceStatus("447700000002@s.whatsapp.net", "vo-missing")
	if err != nil || r.State != "unavailable" || r.SenderJID != jid.String() {
		t.Fatalf("status: %+v %v", r, err)
	}
	var count int
	if err := s.DB().QueryRow("SELECT count(*) FROM messages WHERE id='vo-missing'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("fake message stored: %d %v", count, err)
	}
}

func TestViewOnceReceivedMediaSaved(t *testing.T) {
	for _, kind := range []string{"v1", "v2", "extension", "direct_image", "direct_video", "direct_audio"} {
		t.Run(kind, func(t *testing.T) {
			s := newTestStoreWithLIDMap(t, nil)
			c := newClientWithStore(t, s)
			jid, _ := types.ParseJID("447700000001@s.whatsapp.net")
			if err := s.StoreChat(jid.String(), "Synthetic", time.Now()); err != nil {
				t.Fatal(err)
			}
			body := &waProto.Message{ImageMessage: &waProto.ImageMessage{ViewOnce: proto.Bool(true)}}
			if kind == "direct_video" {
				body = &waProto.Message{VideoMessage: &waProto.VideoMessage{ViewOnce: proto.Bool(true)}}
			}
			if kind == "direct_audio" {
				body = &waProto.Message{AudioMessage: &waProto.AudioMessage{ViewOnce: proto.Bool(true)}}
			}
			raw := body
			switch kind {
			case "v1":
				raw = &waProto.Message{ViewOnceMessage: &waProto.FutureProofMessage{Message: body}}
			case "v2":
				raw = &waProto.Message{ViewOnceMessageV2: &waProto.FutureProofMessage{Message: body}}
			case "extension":
				raw = &waProto.Message{ViewOnceMessageV2Extension: &waProto.FutureProofMessage{Message: body}}
			}
			calls := 0
			c.captureDownload = func(ctx context.Context, id, chat, out string) DownloadResult {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Error("download has no deadline")
				}
				mt, _, _, _, _, _, _, _, err := s.GetMediaInfo(id, chat)
				if err != nil || mt == "" {
					t.Fatalf("download ran before persistence: %s %v", mt, err)
				}
				p := filepath.Join(s.Dir(), "fixture.bin")
				if err := os.WriteFile(p, []byte("synthetic fixture"), 0600); err != nil {
					t.Fatal(err)
				}
				return DownloadResult{Success: true, Path: p}
			}
			evt := (&events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, Sender: jid}, ID: "vo-full", Timestamp: time.Now()}, RawMessage: raw}).UnwrapRaw()
			c.handleMessage(evt)
			r, err := c.ViewOnceStatus(jid.String(), "vo-full")
			if err != nil || r.State != "saved" || r.Path == "" || calls != 1 {
				t.Fatalf("capture: %+v calls=%d err=%v", r, calls, err)
			}
			c.handleUnavailable(&events.UndecryptableMessage{Info: evt.Info, IsUnavailable: true, UnavailableType: events.UnavailableTypeViewOnce})
			r, err = c.ViewOnceStatus(jid.String(), "vo-full")
			if err != nil || r.State != "saved" {
				t.Fatalf("late placeholder downgraded saved file: %+v %v", r, err)
			}
		})
	}
}

func TestViewOnceDownloadFailureIsNotSaved(t *testing.T) {
	s := newTestStoreWithLIDMap(t, nil)
	c := newClientWithStore(t, s)
	jid, _ := types.ParseJID("447700000001@s.whatsapp.net")
	_ = s.StoreChat(jid.String(), "Synthetic", time.Now())
	c.captureDownload = func(context.Context, string, string, string) DownloadResult {
		return DownloadResult{Message: "media unavailable"}
	}
	c.handleMessage((&events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: jid, Sender: jid}, ID: "vo-fail", Timestamp: time.Now()}, RawMessage: &waProto.Message{ImageMessage: &waProto.ImageMessage{ViewOnce: proto.Bool(true)}}}).UnwrapRaw())
	r, err := c.ViewOnceStatus(jid.String(), "vo-fail")
	if err != nil || r.State != "download_failed" || r.Path != "" || r.Error == "" {
		t.Fatalf("false success: %+v %v", r, err)
	}
}

func TestViewOnceRecoveryValidationOffline(t *testing.T) {
	c := newClientWithStore(t, newTestStoreWithLIDMap(t, nil))
	for _, chat := range []string{"", "bad", "1@g.us", "1@broadcast", "1@s.whatsapp.net"} {
		if _, err := c.RequestViewOnceRecovery(context.Background(), chat, "target"); err == nil {
			t.Fatalf("accepted invalid or offline request %q", chat)
		}
	}
}

func TestViewOnceRecoveryGroupNeedsSender(t *testing.T) {
	s := newTestStoreWithLIDMap(t, nil)
	c := newClientWithStore(t, s)
	c.wa = &whatsmeow.Client{Store: &wmstore.Device{}}
	c.online = func() bool { return true }
	_, err := c.RequestViewOnceRecovery(context.Background(), "120363000000000001@g.us", "vo-1")
	if err == nil || !strings.Contains(err.Error(), "original sender") {
		t.Fatalf("group without recorded sender should fail clearly, got %v", err)
	}
}

func TestViewOnceRecoveryGroupBuildsParticipantRequest(t *testing.T) {
	s := newTestStoreWithLIDMap(t, nil)
	c := newClientWithStore(t, s)
	c.wa = &whatsmeow.Client{Store: &wmstore.Device{}}
	c.online = func() bool { return true }
	group, sender := "120363000000000001@g.us", "99887766001@lid"
	if err := s.PutViewOnce(context.Background(), store.ViewOnce{MessageID: "vo-1", ChatJID: group, SenderJID: sender, State: "unavailable"}); err != nil {
		t.Fatal(err)
	}
	var gotChat, gotSender types.JID
	c.peerRequest = func(ctx context.Context, chat, participant types.JID, id string) (whatsmeow.SendResponse, error) {
		gotChat, gotSender = chat, participant
		return whatsmeow.SendResponse{ID: "REQ1"}, nil
	}
	r, err := c.RequestViewOnceRecovery(context.Background(), group, "vo-1")
	if err != nil || r.RequestID != "REQ1" || r.State != "requested" {
		t.Fatalf("recovery = %+v %v", r, err)
	}
	if gotChat.String() != group || gotSender.String() != sender {
		t.Fatalf("request built for %s / %s", gotChat, gotSender)
	}
}

func TestViewOnceRecoveryDirectChatStillValidated(t *testing.T) {
	c := newClientWithStore(t, newTestStoreWithLIDMap(t, nil))
	c.online = func() bool { return true }
	for _, chat := range []string{"1@broadcast", "bad", "", "1:5@s.whatsapp.net"} {
		if _, err := c.RequestViewOnceRecovery(context.Background(), chat, "x"); err == nil {
			t.Fatalf("accepted %q", chat)
		}
	}
}

func TestViewOnceRecoveryGroupStripsDeviceFromSender(t *testing.T) {
	s := newTestStoreWithLIDMap(t, nil)
	c := newClientWithStore(t, s)
	c.wa = &whatsmeow.Client{Store: &wmstore.Device{}}
	c.online = func() bool { return true }
	group := "120363000000000001@g.us"
	_ = s.PutViewOnce(context.Background(), store.ViewOnce{MessageID: "vo-2", ChatJID: group, SenderJID: "99887766001:7@lid", State: "unavailable"})
	var got types.JID
	c.peerRequest = func(ctx context.Context, chat, participant types.JID, id string) (whatsmeow.SendResponse, error) {
		got = participant
		return whatsmeow.SendResponse{ID: "REQ2"}, nil
	}
	if _, err := c.RequestViewOnceRecovery(context.Background(), group, "vo-2"); err != nil {
		t.Fatal(err)
	}
	if got.String() != "99887766001@lid" {
		t.Fatalf("participant = %s, want device-agnostic LID", got)
	}
}
