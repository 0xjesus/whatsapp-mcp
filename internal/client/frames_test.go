package client

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestExtractFrames_StaticVideoSamplesWholeDuration(t *testing.T) {
	dir := t.TempDir()
	video := filepath.Join(dir, "v.mp4")
	_ = os.WriteFile(video, []byte("fake"), 0o600)
	bin := filepath.Join(dir, "ffmpeg")
	log := filepath.Join(dir, "calls")
	body := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + strconv.Quote(log) + "\n" +
		"case \"$*\" in *select=*) exit 0;; *fps=*) for a in \"$@\"; do out=\"$a\"; done; printf '\\377\\330\\377\\331' > \"$(dirname \"$out\")/frame-01.jpg\";; *) echo '  Duration: 00:01:00.00, start: 0.000000, bitrate: 64 kb/s' >&2; exit 1;; esac\n"
	if err := os.WriteFile(bin, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	frames, err := extractFrames(context.Background(), video, bin)
	if err != nil || len(frames) != 1 {
		t.Fatalf("frames=%v err=%v", frames, err)
	}
	data, _ := os.ReadFile(log)
	if !strings.Contains(string(data), "fps=0.1:round=up,scale=512:-2") {
		t.Fatalf("expected six frames spread across sixty seconds, commands: %s", data)
	}
}

// fakeFFmpeg writes N jpg files into the output pattern's directory, imitating ffmpeg's -frames:v.
func fakeFFmpeg(t *testing.T, frames int) string {
	t.Helper()
	dir := t.TempDir()
	script := filepath.Join(dir, "ffmpeg")
	body := "#!/bin/sh\nout=\"\"\nfor a in \"$@\"; do out=\"$a\"; done\nd=$(dirname \"$out\")\ni=1\nwhile [ $i -le " + strconv.Itoa(frames) + " ]; do printf '\\377\\330\\377\\331' > \"$d/frame-$(printf %02d $i).jpg\"; i=$((i+1)); done\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return script
}

func TestExtractFrames_UsesSceneDetectionResult(t *testing.T) {
	video := filepath.Join(t.TempDir(), "v.mp4")
	_ = os.WriteFile(video, []byte("fake"), 0o600)
	frames, err := extractFrames(context.Background(), video, fakeFFmpeg(t, 4))
	if err != nil || len(frames) != 4 {
		t.Fatalf("frames = %v, %v", frames, err)
	}
	if filepath.Dir(frames[0]) != video+".frames" {
		t.Fatalf("frames dir = %s", filepath.Dir(frames[0]))
	}
	again, _ := extractFrames(context.Background(), video, "/nonexistent/ffmpeg")
	if len(again) != 4 {
		t.Fatalf("cached frames not reused: %v", again)
	}
}

func TestExtractFrames_FFmpegMissing(t *testing.T) {
	video := filepath.Join(t.TempDir(), "v.mp4")
	_ = os.WriteFile(video, []byte("fake"), 0o600)
	frames, err := extractFrames(context.Background(), video, "/nonexistent/ffmpeg")
	if err == nil || len(frames) != 0 {
		t.Fatalf("expected error without ffmpeg, got %v %v", frames, err)
	}
}

func TestExtractFrames_DoesNotReuseUnfinishedCache(t *testing.T) {
	video := filepath.Join(t.TempDir(), "v.mp4")
	_ = os.WriteFile(video, []byte("fake"), 0o600)
	_ = os.Mkdir(video+".frames", 0o700)
	_ = os.WriteFile(filepath.Join(video+".frames", "frame-01.jpg"), []byte("partial"), 0o600)
	frames, err := extractFrames(context.Background(), video, fakeFFmpeg(t, 4))
	if err != nil || len(frames) != 4 {
		t.Fatalf("incomplete cache was reused: %v %v", frames, err)
	}
}

func TestExtractFrames_RealFFmpegIfAvailable(t *testing.T) {
	bin, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	video := filepath.Join(dir, "v.mp4")
	// 3-second synthetic clip; testsrc changes every frame so scene detection has material.
	gen := exec.Command(bin, "-y", "-f", "lavfi", "-i", "testsrc=size=320x240:rate=10:duration=3", "-pix_fmt", "yuv420p", video)
	if out, err := gen.CombinedOutput(); err != nil {
		t.Skipf("cannot synthesize clip: %v %s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	frames, err := extractFrames(ctx, video, bin)
	if err != nil || len(frames) < 1 || len(frames) > 6 {
		t.Fatalf("frames = %d, %v", len(frames), err)
	}
}

func TestExtractFrames_CompletedFrameCacheDoesNotQueueBehindUnrelatedVideo(t *testing.T) {
	video := filepath.Join(t.TempDir(), "synthetic.mp4")
	if err := os.Mkdir(video+".frames", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(video+".frames", ".complete-v2"), []byte("complete\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(video+".frames", "frame-01.jpg"), []byte("fake"), 0600); err != nil {
		t.Fatal(err)
	}
	frameExtraction <- struct{}{}
	defer func() { <-frameExtraction }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	frames, err := extractFrames(ctx, video, "missing-ffmpeg")
	if err != nil || len(frames) != 1 {
		t.Fatalf("completed cache blocked by unrelated work: frames=%v err=%v", frames, err)
	}
}

func TestExtractFrames_VideoShorterThanContainerDuration(t *testing.T) {
	bin, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg unavailable")
	}
	video := filepath.Join(t.TempDir(), "synthetic.mp4")
	cmd := exec.Command(bin, "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "color=c=red:s=16x16:r=1:d=1", "-f", "lavfi", "-i", "anullsrc=r=8000:cl=mono:d=60", "-c:v", "mpeg4", "-threads", "1", "-c:a", "aac", video)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	frames, err := extractFrames(context.Background(), video, bin)
	if err != nil || len(frames) == 0 {
		t.Fatalf("valid video lost frames when audio extends duration: frames=%v err=%v", frames, err)
	}
}
