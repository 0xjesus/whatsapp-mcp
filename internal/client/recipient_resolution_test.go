package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
)

func TestOutboxUsesServerCanonicalPhone(t *testing.T) {
	c, now := outboxTestClient(t)
	c.recipientLookup = func(_ context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error) {
		if len(phones) != 1 || phones[0] != "+525550000001" {
			t.Fatalf("unexpected lookup: %v", phones)
		}
		return []types.IsOnWhatsAppResponse{{Query: "+525550000001", IsIn: true, JID: types.NewJID("100000000000001", types.HiddenUserServer), PhoneNumber: types.NewJID("5215550000001", types.DefaultUserServer)}}, nil
	}
	_, err := c.store.DB().Exec(`INSERT INTO outbound_attempts(at_ns,known,recipient) VALUES(?,?,?)`, now.Add(-2*time.Minute).UnixNano(), false, "525550000001@s.whatsapp.net")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.networkSend = func(_ context.Context, jid types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
		calls++
		if jid.String() != "5215550000001@s.whatsapp.net" {
			return whatsmeow.SendResponse{}, fmt.Errorf("no LID found for %s from server", jid)
		}
		return whatsmeow.SendResponse{ID: id}, nil
	}
	ctx := context.Background()
	r := c.Send(ctx, "525550000001", "approved pitch with video")
	c.processOutbox(ctx)
	j, err := c.store.OutboxJob(ctx, r.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != "sent" || j.SentID != j.MessageID || calls != 1 {
		t.Fatalf("canonical recipient not delivered: state=%s reason=%s calls=%d", j.State, j.Reason, calls)
	}
	var canonicalAttempts int
	if err := c.store.DB().QueryRow(`SELECT COUNT(*) FROM outbound_attempts WHERE recipient=?`, "5215550000001@s.whatsapp.net").Scan(&canonicalAttempts); err != nil {
		t.Fatal(err)
	}
	if canonicalAttempts != 2 {
		t.Fatalf("prior attempts lost when canonicalizing: %d", canonicalAttempts)
	}
}

func TestRecipientResolutionFailureNeverDispatches(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rows  []types.IsOnWhatsAppResponse
		err   error
		state string
	}{
		{name: "not registered", rows: []types.IsOnWhatsAppResponse{{Query: "+525550000001", IsIn: false}}, state: "failed"},
		{name: "unrelated response", rows: []types.IsOnWhatsAppResponse{{Query: "+447700000001", IsIn: true, PhoneNumber: types.NewJID("447700000001", types.DefaultUserServer)}}, state: "queued"},
		{name: "query timeout", err: context.DeadlineExceeded, state: "queued"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := outboxTestClient(t)
			c.recipientLookup = func(context.Context, []string) ([]types.IsOnWhatsAppResponse, error) { return tc.rows, tc.err }
			calls := 0
			c.networkSend = func(context.Context, types.JID, *waProto.Message, string) (whatsmeow.SendResponse, error) {
				calls++
				return whatsmeow.SendResponse{}, nil
			}
			ctx := context.Background()
			r := c.Send(ctx, "525550000001", "approved")
			c.processOutbox(ctx)
			j, err := c.store.OutboxJob(ctx, r.JobID)
			if err != nil {
				t.Fatal(err)
			}
			var n int
			if err = c.store.DB().QueryRow(`SELECT COUNT(*) FROM outbound_attempts`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if j.State != tc.state || j.Attempts != 0 || calls != 0 || n != 0 {
				t.Fatalf("failed resolution dispatched or consumed budget: job=%+v calls=%d budget=%d", j, calls, n)
			}
		})
	}
}

func TestCanonicalAliasConfirmsInterruptedSend(t *testing.T) {
	c, _ := outboxTestClient(t)
	ctx := context.Background()
	r := c.Send(ctx, "525550000001", "approved")
	j, _ := c.store.OutboxJob(ctx, r.JobID)
	if err := c.store.RecordRecipientAlias(ctx, j.Recipient, "5215550000001@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.DB().Exec(`UPDATE outbound_jobs SET state='sending' WHERE job_id=?`, j.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.DB().Exec(`INSERT INTO chats(jid,name) VALUES(?,?)`, "5215550000001@s.whatsapp.net", "Test Venue"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.DB().Exec(`INSERT INTO messages(id,chat_jid,sender,is_from_me,timestamp,content) VALUES(?,?,?,1,?,?)`, j.MessageID, "5215550000001@s.whatsapp.net", j.Sender, time.Now().Unix(), "approved"); err != nil {
		t.Fatal(err)
	}
	if err := c.store.RecoverOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	j, err := c.store.OutboxJob(ctx, j.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != "sent" || j.SentID != j.MessageID {
		t.Fatalf("canonical cached delivery not recovered: %+v", j)
	}
}
