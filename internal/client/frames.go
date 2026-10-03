package client

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

const maxFrames = 6
const frameTimeout = 60 * time.Second

var ffmpegBinary = "ffmpeg"

var frameExtraction = make(chan struct{}, 1)
var durationPattern = regexp.MustCompile(`Duration: (\d+):(\d+):(\d+(?:\.\d+)?)`)

// extractFrames writes up to maxFrames JPEG key frames of videoPath into <videoPath>.frames/ and
// returns their paths. Existing frames are reused. Static clips are sampled across their duration.
func extractFrames(ctx context.Context, videoPath, ffmpeg string) ([]string, error) {
	dir := videoPath + ".frames"
	marker := filepath.Join(dir, ".complete-v2")
	cached := func() []string {
		if _, err := os.Stat(marker); err == nil {
			return listFrames(dir)
		}
		return nil
	}
	// Completed caches are immutable and need no slot in the extraction queue.
	if frames := cached(); len(frames) > 0 {
		return frames, nil
	}
	ctx, cancel := context.WithTimeout(ctx, frameTimeout)
	defer cancel()
	// Bound CPU/disk work and keep callers from observing a partially written cache.
	select {
	case frameExtraction <- struct{}{}:
		defer func() { <-frameExtraction }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if frames := cached(); len(frames) > 0 {
		return frames, nil
	}
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	pattern := filepath.Join(dir, "frame-%02d.jpg")
	run := func(vf string) error {
		cmd := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-y", "-i", videoPath,
			"-vf", vf, "-frames:v", fmt.Sprint(maxFrames), "-vsync", "vfr", "-q:v", "4", pattern)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("ffmpeg: %v: %s", err, truncate(string(out), 200))
		}
		return nil
	}
	if err := run("select='gt(scene,0.3)',scale=512:-2"); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	if frames := listFrames(dir); len(frames) >= 2 {
		if err := os.WriteFile(marker, []byte("complete\n"), 0o600); err != nil {
			return nil, err
		}
		return frames, nil
	}
	// ffmpeg prints container duration without decoding the clip. A probe with no
	// output exits nonzero; the parsed metadata, not that exit code, is what matters.
	probe := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-i", videoPath)
	metadata, _ := probe.CombinedOutput()
	parts := durationPattern.FindSubmatch(metadata)
	if len(parts) != 4 {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("ffmpeg did not report a usable video duration")
	}
	hours, _ := strconv.ParseFloat(string(parts[1]), 64)
	minutes, _ := strconv.ParseFloat(string(parts[2]), 64)
	seconds, _ := strconv.ParseFloat(string(parts[3]), 64)
	duration := hours*3600 + minutes*60 + seconds
	if duration <= 0 {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("video duration must be positive")
	}
	// Static clip (or a single scene): six evenly spaced samples over the full clip.
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	// Round up so a video stream shorter than its audio/container still yields a frame.
	if err := run("fps=" + strconv.FormatFloat(float64(maxFrames)/duration, 'g', -1, 64) + ":round=up,scale=512:-2"); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	frames := listFrames(dir)
	if len(frames) == 0 {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("ffmpeg produced no frames")
	}
	if err := os.WriteFile(marker, []byte("complete\n"), 0o600); err != nil {
		return nil, err
	}
	return frames, nil
}

func listFrames(dir string) []string {
	matches, _ := filepath.Glob(filepath.Join(dir, "frame-*.jpg"))
	sort.Strings(matches)
	if len(matches) > maxFrames {
		matches = matches[:maxFrames]
	}
	return matches
}

// withFrames attaches key frames to a successful video download; failures are reported, never fatal.
func withFrames(ctx context.Context, result DownloadResult, absPath string) DownloadResult {
	if !result.Success || result.MediaType != "video" {
		return result
	}
	frames, err := extractFrames(ctx, absPath, ffmpegBinary)
	if err != nil {
		result.FramesError = err.Error()
		return result
	}
	result.Frames = frames
	return result
}
