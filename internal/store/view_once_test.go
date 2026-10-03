package store

import (
	"context"
	"testing"
)

func TestViewOnceRecoveryCooldownAndLateAck(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	r := ViewOnce{MessageID: "fixture", ChatJID: "1@lid", SenderJID: "1@lid", State: "unavailable"}
	if err := s.PutViewOnce(ctx, r); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.ReserveViewOnceRequest(ctx, r.ChatJID, r.MessageID, 1000); err != nil || !ok {
		t.Fatalf("initial reservation: %v %v", ok, err)
	}
	if ok, err := s.ReserveViewOnceRequest(ctx, r.ChatJID, r.MessageID, 1001); err != nil || ok {
		t.Fatalf("duplicate reservation: %v %v", ok, err)
	}
	r.State = "saved"
	r.Path = "/synthetic/file"
	if err := s.PutViewOnce(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishViewOnceRequest(ctx, r.ChatJID, r.MessageID, "request-1", ""); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetViewOnce(r.ChatJID, r.MessageID)
	if err != nil || got.State != "saved" || got.Path != r.Path || got.RequestID != "request-1" {
		t.Fatalf("late ack lost capture: %+v %v", got, err)
	}
	if ok, err := s.ReserveViewOnceRequest(ctx, r.ChatJID, r.MessageID, 1400); err != nil || ok {
		t.Fatalf("requested saved media: %v %v", ok, err)
	}
}
