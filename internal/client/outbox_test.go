package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/media"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/ratelimit"
	"github.com/sealjay/mcp-whatsapp/internal/security"
	"github.com/sealjay/mcp-whatsapp/internal/store"
)

func TestDisconnectedSendIsDurablyAccepted(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newDisconnectedClient()
	c.redactor = &security.Redactor{}
	c.store = s
	c.senderIdentity = func() string { return "999@s.whatsapp.net" }
	r := c.Send(context.Background(), "+447700000001", "approved message")
	b, _ := json.Marshal(r)
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	if got["Accepted"] != true || got["Status"] != "queued" || got["JobID"] == "" || r.Success || r.ID != "" {
		t.Fatalf("disconnected authorized send disappeared instead of queuing: %s", b)
	}
}

func TestOutbox429ResumesAtFullCooldownAndStopsAfterThree(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	calls := 0
	c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
		calls++
		return whatsmeow.SendResponse{}, fmt.Errorf("%w %d", whatsmeow.ErrServerReturnedError, 429)
	}
	r := c.Send(ctx, "447700000001", "authorized")
	c.processOutbox(ctx)
	j, _ := c.store.OutboxJob(ctx, r.JobID)
	if j.Attempts != 1 || j.State != "queued" || j.NextAttempt.Sub(*now) != 30*time.Minute {
		t.Fatalf("first refusal: %+v", j)
	}
	*now = j.NextAttempt.Add(-time.Nanosecond)
	c.processOutbox(ctx)
	if calls != 1 {
		t.Fatal("retried before full cooldown")
	}
	*now = j.NextAttempt
	c.processOutbox(ctx)
	j, _ = c.store.OutboxJob(ctx, r.JobID)
	if calls != 2 || j.NextAttempt.Sub(*now) != time.Hour {
		t.Fatalf("second refusal should double cooldown: calls=%d job=%+v", calls, j)
	}
	*now = j.NextAttempt
	c.processOutbox(ctx)
	j, _ = c.store.OutboxJob(ctx, r.JobID)
	if calls != 3 || j.State != "blocked" || j.Attempts != 3 {
		t.Fatalf("bounded refusal: calls=%d job=%+v", calls, j)
	}
	*now = now.Add(48 * time.Hour)
	c.processOutbox(ctx)
	if calls != 3 {
		t.Fatal("retried exhausted job")
	}
}
func outboxTestClient(t *testing.T) (*Client, *time.Time) {
	t.Helper()
	s, e := store.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	c := newDisconnectedClient()
	c.redactor = &security.Redactor{}
	c.store = s
	c.clock = func() time.Time { return now }
	c.online = func() bool { return true }
	c.senderIdentity = func() string { return "999@s.whatsapp.net" }
	if e = c.initSendProtection(context.Background()); e != nil {
		t.Fatal(e)
	}
	now = now.Add(time.Hour)
	c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
		return whatsmeow.SendResponse{ID: id}, nil
	}
	return c, &now
}
func TestDeniedMemoryBudgetDoesNotConsumePersistentAttempt(t *testing.T) {
	c, _ := outboxTestClient(t)
	ctx := context.Background()
	c.limiter = ratelimit.New(ratelimit.DefaultConfig())
	c.limiter.AllowSend(false)
	r := c.Send(ctx, "447700000001", "authorized")
	c.processOutbox(ctx)
	j, _ := c.store.OutboxJob(ctx, r.JobID)
	var n int
	c.store.DB().QueryRow(`SELECT COUNT(*) FROM outbound_attempts`).Scan(&n)
	if j.Attempts != 0 || n != 0 {
		t.Fatalf("denied boundary consumed attempts: job=%d budget=%d", j.Attempts, n)
	}
}
func TestOutboxWaitResumeFIFOAndSharedBudget(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	var ids []string
	c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, id string) (whatsmeow.SendResponse, error) {
		ids = append(ids, id)
		return whatsmeow.SendResponse{ID: id}, nil
	}
	a := c.Send(ctx, "447700000001", "first")
	b := c.Send(ctx, "447700000002", "second")
	var workers sync.WaitGroup
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); c.processOutbox(ctx) }()
	}
	workers.Wait()
	j, _ := c.store.OutboxJob(ctx, b.JobID)
	if len(ids) != 1 || j.Attempts != 0 || j.State != "queued" || j.NextAttempt.Sub(*now) != 90*time.Second {
		t.Fatalf("wait state: %+v calls=%d", j, len(ids))
	}
	*now = now.Add(90 * time.Second)
	d, e := c.reserveSend(ctx, true)
	if e != nil || !d.Allowed {
		t.Fatalf("feature reservation: %+v %v", d, e)
	}
	c.processOutbox(ctx)
	if len(ids) != 1 {
		t.Fatal("outbox ignored feature budget")
	}
	*now = now.Add(90 * time.Second)
	c.processOutbox(ctx)
	saved, _ := c.store.OutboxJob(ctx, a.JobID)
	j, _ = c.store.OutboxJob(ctx, b.JobID)
	if len(ids) != 2 || ids[0] != saved.MessageID || ids[1] != j.MessageID || j.State != "sent" {
		t.Fatalf("FIFO drain: ids=%v job=%+v", ids, j)
	}
}
func TestOutboxUnknownDeliveryNeedsReviewWithoutRetry(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	calls := 0
	c.networkSend = func(_ context.Context, _ types.JID, _ *waProto.Message, _ string) (whatsmeow.SendResponse, error) {
		calls++
		return whatsmeow.SendResponse{}, errors.New("timeout after write")
	}
	r := c.Send(ctx, "447700000001", "authorized")
	c.processOutbox(ctx)
	*now = now.Add(48 * time.Hour)
	c.processOutbox(ctx)
	j, _ := c.store.OutboxJob(ctx, r.JobID)
	if j.State != "needs_review" || j.Attempts != 1 || calls != 1 {
		t.Fatalf("uncertain retry: %+v calls=%d", j, calls)
	}
}
func TestOutboxCancellationSenderChangeAndUnpaired(t *testing.T) {
	c, _ := outboxTestClient(t)
	ctx := context.Background()
	r := c.Send(ctx, "447700000001", "cancel")
	if e := c.CancelOutbox(ctx, r.JobID); e != nil {
		t.Fatal(e)
	}
	c.processOutbox(ctx)
	j, _ := c.store.OutboxJob(ctx, r.JobID)
	if j.State != "cancelled" || j.Attempts != 0 {
		t.Fatalf("cancelled sent: %+v", j)
	}
	r = c.Send(ctx, "447700000001", "old account")
	c.senderIdentity = func() string { return "888@s.whatsapp.net" }
	c.processOutbox(ctx)
	j, _ = c.store.OutboxJob(ctx, r.JobID)
	if j.State != "blocked" || j.Attempts != 0 {
		t.Fatalf("wrong sender: %+v", j)
	}
	c.senderIdentity = func() string { return "" }
	if r = c.Send(ctx, "447700000001", "unpaired"); r.Accepted {
		t.Fatal("unpaired accepted")
	}
}
func TestOutboxConcurrentIdempotency(t *testing.T) {
	c, _ := outboxTestClient(t)
	ctx := WithSendOptions(context.Background(), SendOptions{IdempotencyKey: "authorization"})
	var wg sync.WaitGroup
	results := make(chan SendResult, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- c.Send(ctx, "+447700000001", "same") }()
	}
	wg.Wait()
	close(results)
	id := ""
	for r := range results {
		if !r.Accepted {
			t.Fatalf("not accepted: %+v", r)
		}
		if id == "" {
			id = r.JobID
		}
		if r.JobID != id {
			t.Fatal("duplicate job")
		}
	}
	if r := c.Send(ctx, "447700000001@s.whatsapp.net", "same"); r.JobID != id {
		t.Fatalf("normalized retry differs: %+v", r)
	}
	if r := c.Send(ctx, "447700000001", "changed"); r.Accepted {
		t.Fatal("changed request reused key")
	}
	jobs, e := c.GetOutbox(context.Background(), "", 100)
	if e != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%d err=%v", len(jobs), e)
	}
}
func TestOutboxImmutableMediaAndReadOption(t *testing.T) {
	c, _ := outboxTestClient(t)
	ctx := WithSendOptions(context.Background(), SendOptions{MarkRead: true})
	root := t.TempDir()
	c.allowedMediaRoot = root
	path := filepath.Join(root, "image.png")
	if e := os.WriteFile(path, []byte("original"), 0600); e != nil {
		t.Fatal(e)
	}
	r := c.SendMediaWithOptions(ctx, SendMediaOptions{Recipient: "447700000001", MediaPath: path, Caption: "caption"})
	if !r.Accepted {
		t.Fatalf("media rejected: %+v", r)
	}
	os.WriteFile(path, []byte("changed"), 0600)
	payload, data, e := c.store.LoadOutboxPayload(ctx, r.JobID)
	var p outboundPayload
	json.Unmarshal([]byte(payload), &p)
	if e != nil || string(data) != "original" || !p.MarkRead || p.MediaPath != "image.png" {
		t.Fatalf("snapshot=%q opts=%+v err=%v", data, p, e)
	}
}
func TestOutboxPersistedHealthAndMigrationGuard(t *testing.T) {
	c, now := outboxTestClient(t)
	ctx := context.Background()
	raw, _ := c.store.ProtectionValue(ctx, "initial_guard")
	*now = now.Add(time.Hour)
	if e := c.store.EnsureSendProtection(ctx, *now); e != nil {
		t.Fatal(e)
	}
	again, _ := c.store.ProtectionValue(ctx, "initial_guard")
	if raw != again {
		t.Fatal("restart extended migration guard")
	}
	until := now.Add(8 * time.Hour)
	c.setHealth(HealthTempBanned, until, "persisted ban", true)
	restarted := newDisconnectedClient()
	restarted.store = c.store
	restarted.clock = c.clock
	if e := restarted.initSendProtection(ctx); e != nil {
		t.Fatal(e)
	}
	if e := restarted.sendGate(true); e == nil {
		t.Fatal("restart erased ban")
	}
}

func TestConvertedAudioRetryUsesStablePayloadName(t *testing.T) {
	c, _ := outboxTestClient(t)
	root := t.TempDir()
	c.allowedMediaRoot = root
	ctx := WithSendOptions(context.Background(), SendOptions{IdempotencyKey: "audio", MediaName: "voice.ogg"})
	paths := []string{filepath.Join(root, "converted-a.ogg"), filepath.Join(root, "converted-b.ogg")}
	id := ""
	for _, path := range paths {
		os.WriteFile(path, []byte("same conversion"), 0600)
		r := c.SendMediaWithOptions(ctx, SendMediaOptions{Recipient: "447700000001", MediaPath: path})
		if !r.Accepted {
			t.Fatalf("same audio rejected: %+v", r)
		}
		if id == "" {
			id = r.JobID
		}
		if r.JobID != id {
			t.Fatal("conversion temp name produced duplicate")
		}
	}
}

func TestOutboxMediaPreparationRetainsBeforeRecipientDispatch(t *testing.T) {
	for _, mode := range []string{"offline", "temporary", "cancelled", "deadline", "ban", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			c, now := outboxTestClient(t)
			root := t.TempDir()
			c.allowedMediaRoot = root
			path := filepath.Join(root, "image.png")
			data := []byte("image")
			if mode == "malformed" {
				path = filepath.Join(root, "voice.ogg")
				data = []byte("OggS")
			}
			os.WriteFile(path, data, 0600)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			online := true
			c.online = func() bool { return online }
			uploads, sends := 0, 0
			c.preparationTimeout = 10 * time.Millisecond
			c.uploadMedia = func(ctx context.Context, b []byte, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
				uploads++
				if !bytes.Equal(b, data) {
					t.Fatal("snapshot changed")
				}
				if uploads == 1 {
					switch mode {
					case "offline":
						online = false
						return whatsmeow.UploadResponse{}, errors.New("disconnected during upload")
					case "temporary":
						return whatsmeow.UploadResponse{}, errors.New("upload failed with status code 503")
					case "cancelled":
						cancel()
						return whatsmeow.UploadResponse{}, context.Canceled
					case "deadline":
						<-ctx.Done()
						return whatsmeow.UploadResponse{}, ctx.Err()
					case "ban":
						return whatsmeow.UploadResponse{}, fmt.Errorf("%w 402", whatsmeow.ErrServerReturnedError)
					}
				}
				return whatsmeow.UploadResponse{URL: "https://example.invalid/snapshot", FileLength: uint64(len(b))}, nil
			}
			c.networkSend = func(_ context.Context, _ types.JID, m *waProto.Message, id string) (whatsmeow.SendResponse, error) {
				sends++
				if m.ImageMessage == nil {
					t.Fatal("upload assembly bypassed")
				}
				return whatsmeow.SendResponse{ID: id}, nil
			}
			r := c.SendMediaWithOptions(context.Background(), SendMediaOptions{Recipient: "447700000001", MediaPath: path})
			if !r.Accepted {
				t.Fatal(r)
			}
			c.processOutbox(ctx)
			j, _ := c.store.OutboxJob(context.Background(), r.JobID)
			want := "queued"
			if mode == "ban" {
				want = "blocked"
			}
			if mode == "malformed" {
				want = "failed"
			}
			if j.State != want || j.Attempts != 0 || sends != 0 {
				t.Fatalf("pre-dispatch media lost/dispatched: %+v sends%d uploads%d", j, sends, uploads)
			}
			if mode == "malformed" && uploads != 0 {
				t.Fatal("malformed media uploaded")
			}
			if want == "queued" {
				online = true
				c.preparationTimeout = time.Second
				*now = j.NextAttempt
				c.processOutbox(context.Background())
				j, _ = c.store.OutboxJob(context.Background(), r.JobID)
				if j.State != "sent" || j.Attempts != 1 || sends != 1 {
					t.Fatalf("preparation did not resume: %+v sends%d", j, sends)
				}
			}
		})
	}
}

func TestRealConvertedAudioIdentityAndDefaultAllowlist(t *testing.T) {
	if _, e := exec.LookPath("ffmpeg"); e != nil {
		t.Skip("ffmpeg unavailable")
	}
	c, _ := outboxTestClient(t)
	root := t.TempDir()
	c.allowedMediaRoot = root
	source := filepath.Join(root, "voice.wav")
	makeWAV := func(freq string) {
		t.Helper()
		cmd := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi", "-i", "sine=frequency="+freq+":duration=0.1", "-y", source)
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("wav generation: %v %s", e, out)
		}
	}
	makeWAV("440")
	ctx := WithSendOptions(context.Background(), SendOptions{IdempotencyKey: "real-audio", MediaName: "voice.ogg"})
	// Exercise real randomized Ogg muxing, not identical literal bytes.
	a, e := media.ConvertToOpusOgg(context.Background(), source)
	if e != nil {
		t.Fatal(e)
	}
	defer os.Remove(a)
	b, e := media.ConvertToOpusOgg(context.Background(), source)
	if e != nil {
		t.Fatal(e)
	}
	defer os.Remove(b)
	aa, _ := os.ReadFile(a)
	bb, _ := os.ReadFile(b)
	if bytes.Equal(aa, bb) {
		t.Fatal("fixture did not expose randomized conversion bytes")
	}
	// The public source passes allowlisting, but converted system-temp paths must
	// never become an exception to public path validation.
	if _, e = c.ValidateMediaPath(a); e == nil {
		t.Fatal("conversion temp unexpectedly public")
	}
	r := c.SendAudioWithOptions(ctx, SendMediaOptions{Recipient: "447700000001", MediaPath: source, ViewOnce: true})
	if !r.Accepted {
		t.Fatal(r)
	}
	_, first, e := c.store.LoadOutboxPayload(context.Background(), r.JobID)
	if e != nil || !bytes.HasPrefix(first, []byte("OggS")) {
		t.Fatalf("converted snapshot missing: %v", e)
	}
	repeat := c.SendAudioWithOptions(ctx, SendMediaOptions{Recipient: "447700000001@s.whatsapp.net", MediaPath: source, ViewOnce: true})
	if !repeat.Accepted || repeat.JobID != r.JobID {
		t.Fatalf("same source changed authorization: %+v", repeat)
	}
	_, after, _ := c.store.LoadOutboxPayload(context.Background(), r.JobID)
	if !bytes.Equal(first, after) {
		t.Fatal("first immutable conversion replaced")
	}
	makeWAV("880")
	changed := c.SendAudioWithOptions(ctx, SendMediaOptions{Recipient: "447700000001", MediaPath: source, ViewOnce: true})
	if changed.Accepted {
		t.Fatal("changed original audio reused authorization")
	}
	outside := filepath.Join(t.TempDir(), "voice.wav")
	os.WriteFile(outside, []byte("outside"), 0600)
	if got := c.SendAudioWithOptions(ctx, SendMediaOptions{Recipient: "447700000001", MediaPath: outside}); got.Accepted {
		t.Fatal("outside source accepted")
	}
}
