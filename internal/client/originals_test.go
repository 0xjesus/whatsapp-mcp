package client

import (
	"context"
	"testing"
	"time"

	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"

	"github.com/sealjay/mcp-whatsapp/internal/store"
)

// An album photo arrives wrapped in associatedChildMessage. Its caption and its
// media reference must both be stored, not a "[sin soporte: ...]" placeholder.
func TestAlbumItemKeepsCaptionAndMedia(t *testing.T) {
	c := newClientWithStore(t, newTestStoreWithLIDMap(t, nil))
	img := &waProto.ImageMessage{Caption: proto.String("we do not want the customer to enter the box size"), DirectPath: proto.String("/v/t62/x")}
	raw := &waProto.Message{AssociatedChildMessage: &waProto.FutureProofMessage{Message: &waProto.Message{ImageMessage: img}}}
	nm := c.normalizeIncomingMessage("15550000001@s.whatsapp.net", "15550000001@s.whatsapp.net", "15550000001", false, false, "", "ID1", time.Now(), raw)
	if nm.msg.Content != img.GetCaption() || nm.msg.MediaType != "image" || nm.msg.DirectPath != "/v/t62/x" {
		t.Fatalf("got content=%q media=%q path=%q", nm.msg.Content, nm.msg.MediaType, nm.msg.DirectPath)
	}
}

// A row stored by an older parser is filled from its kept original on start,
// without the phone: this is what stops a parser gap from losing text for good.
func TestRepairFromOriginalsFillsLostCaption(t *testing.T) {
	s := newTestStoreWithLIDMap(t, nil)
	c := newClientWithStore(t, s)
	ctx := context.Background()
	const chat, id = "15550000001@s.whatsapp.net", "ID2"
	if err := s.StoreChat(chat, "x", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(ctx, store.Message{ID: id, ChatJID: chat, Sender: "15550000001", Content: "[sin soporte: associatedChildMessage]", MediaType: "unsupported", Timestamp: time.Now()}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	raw := &waProto.Message{AssociatedChildMessage: &waProto.FutureProofMessage{Message: &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("full text"), DirectPath: proto.String("/v/p")}}}}
	wire, _ := proto.Marshal(raw)
	if err := s.KeepOriginal(ctx, id, chat, wire, 1); err != nil {
		t.Fatal(err)
	}
	n, err := c.RepairFromOriginals(ctx)
	if err != nil || n != 1 {
		t.Fatalf("repair n=%d err=%v", n, err)
	}
	msgs, err := s.ListMessages(ctx, store.ListMessagesParams{ChatJID: chat, Limit: 5})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("list: %v %d", err, len(msgs))
	}
	if msgs[0].Content != "full text" || msgs[0].MediaType != "image" {
		t.Fatalf("after repair content=%q media=%q", msgs[0].Content, msgs[0].MediaType)
	}
	if n, _ := c.RepairFromOriginals(ctx); n != 0 {
		t.Fatalf("second pass re-read %d originals", n)
	}
}
