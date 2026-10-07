package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	protocol "github.com/mark3labs/mcp-go/mcp"
	"github.com/sealjay/mcp-whatsapp/internal/client"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

type offlineAudioTransport struct{}

func (offlineAudioTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("synthetic test blocks all HTTP")
}

func TestAudioMCPHandlerRetainsOriginalAuthorizationWithDefaultRoot(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg unavailable")
	}
	// Use a synthetic persisted device solely to test the actual tool handler.
	// No Connect/worker is started, and even version-check HTTP is blocked.
	previous := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: offlineAudioTransport{}}
	defer func() { http.DefaultClient = previous }()
	ctx := context.Background()
	dir := t.TempDir()
	root := filepath.Join(dir, "uploads")
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+filepath.Join(dir, "whatsapp.db")+"?_foreign_keys=on", nil)
	if err != nil {
		t.Fatal(err)
	}
	device := container.NewDevice()
	jid := types.NewJID("447700000099", types.DefaultUserServer)
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte("synthetic"), AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	if err = device.Save(ctx); err != nil {
		container.Close()
		t.Fatal(err)
	}
	container.Close()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c, err := client.New(ctx, client.Config{StoreDir: dir, Store: st, AllowedMediaRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	// The daemon normally creates its documented uploads root.
	if err = os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "voice.wav")
	wav := func(freq string) {
		t.Helper()
		if out, e := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi", "-i", "sine=frequency="+freq+":duration=0.1", "-y", source).CombinedOutput(); e != nil {
			t.Fatalf("ffmpeg: %v %s", e, out)
		}
	}
	wav("440")
	s := NewServer(c, nil)
	tool := s.MCP().ListTools()["send_audio_message"]
	call := func() client.SendResult {
		t.Helper()
		res, e := tool.Handler(ctx, protocol.CallToolRequest{Params: protocol.CallToolParams{Arguments: map[string]any{"recipient": "447700000001", "media_path": source, "idempotency_key": "voice-authorization", "view_once": true, "mark_chat_read": true}}})
		if e != nil || res.IsError {
			t.Fatalf("handler: %+v %v", res, e)
		}
		var r client.SendResult
		if e = json.Unmarshal([]byte(firstText(t, res)), &r); e != nil {
			t.Fatal(e)
		}
		return r
	}
	first := call()
	repeat := call()
	if !first.Accepted || !repeat.Accepted || first.JobID != repeat.JobID || first.Status != "queued" {
		t.Fatalf("handler acceptance: first=%+v repeat=%+v", first, repeat)
	}
	wav("880")
	changed := call()
	if changed.Accepted {
		t.Fatal("handler accepted changed original source with same key")
	}
	jobs, e := c.GetOutbox(ctx, "", 100)
	if e != nil || len(jobs) != 1 {
		t.Fatalf("handler duplicate: %+v %v", jobs, e)
	}
	payload, data, e := st.LoadOutboxPayload(ctx, first.JobID)
	if e != nil || len(data) == 0 {
		t.Fatal(e)
	}
	var options struct{ MarkRead, ViewOnce bool }
	json.Unmarshal([]byte(payload), &options)
	if !options.MarkRead || !options.ViewOnce {
		t.Fatal(payload)
	}
}
