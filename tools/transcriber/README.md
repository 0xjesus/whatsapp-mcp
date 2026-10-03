# Optional audio and video transcription

This worker transcribes WhatsApp voice notes and the audio track of videos with
local faster-whisper. It writes completed text into the message cache, so normal
message listing and search can read it. Video captions remain alongside the
`[Audio del video]` transcript. Existing manual captions and completed video
transcript corrections are preserved.

Run it on the same machine as the WhatsApp daemon. Media download paths must
refer to the same filesystem for both processes. Use only one transcription
worker for a store.

## Install and run

Use Python 3.10 or newer in an activated virtual environment. Install ffmpeg with
your operating system's package manager, then install the Python dependency:

```sh
python3 -m pip install faster-whisper
ffmpeg -version
```

Pair and start the WhatsApp daemon first. Set `WA_STORE` to the absolute store
directory used by that daemon. Do not point the worker at a different cache.
From the cloned repository's root directory:

```sh
: "${WA_STORE:?Set WA_STORE to the daemon's absolute store directory}"
export WA_STORE
export WA_MCP_URL=http://127.0.0.1:8765/mcp
python3 -m tools.transcriber.transcribe_daemon
```

The first model load may download the selected Whisper model. Subsequent
transcription uses the local model on CPU with int8 computation. The worker
creates its temporary media directory with mode `0700` and applies a `0077`
umask. It does not log transcript text or message identifiers.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `WA_STORE` | User-local WhatsApp store | Absolute store directory. Set explicitly to match the daemon. |
| `WA_MCP_URL` | `http://127.0.0.1:8765/mcp` | Daemon MCP endpoint. |
| `WA_WHISPER_MODEL` | `small` | faster-whisper model name or local model directory. |
| `WA_WHISPER_THREADS` | `2` | CPU inference threads. |
| `WA_WINDOW_DAYS` | `60` | Recent history eligible for transcription and repair. |
| `WA_POLL_SECONDS` | `30` | Delay after each batch. |
| `WA_BATCH` | `4` | Maximum messages selected per batch, newest first. |
| `WA_FFMPEG` | `ffmpeg` | Executable name on `PATH` or absolute executable path. |

## Limits and recovery

- Videos are limited to 600 seconds of extracted audio. Longer audio tracks are
  marked `failed_permanent` with `video_too_long`; a partial transcript is never
  published as complete. Extraction has a 120-second process timeout.
- A video without an audio stream is marked `failed_permanent` with `no_audio`.
  This worker does not describe video frames or images.
- Each message gets at most three transcription attempts. Other failures remain
  eligible until that limit. Failed messages need explicit operator review to
  be retried after reaching `failed_permanent`.
- Completed empty transcripts are terminal. Manual audio text is not repeatedly
  queued. If synchronization removes completed text, a repair uses the saved
  transcript without inference. Video repair also restores a transcript beside
  a surviving caption without duplicating an existing transcript marker.
- Selection is newest first within the configured window. Continuous new media
  can delay older work; increasing the window alone does not guarantee complete
  historical coverage. WhatsApp must still make each media item available.
- Status stories are excluded. Downloaded media is temporary; failed attempts
  may retain audio for a retry. Terminal failures and successes remove that
  cached audio.
- HTTP 400/404 responses trigger at most one MCP session renewal per call. A
  repeated error propagates to the batch's normal failure handling.

## Tests

From the repository root, resolve the test directory to an absolute path and run
the synthetic SQLite tests. They need neither WhatsApp credentials nor a model:

```sh
TRANSCRIBER_DIR="$(python3 -c 'import pathlib; import tools.transcriber.transcribe_daemon as t; print(pathlib.Path(t.__file__).resolve().parent)')"
python3 -m unittest discover -s "$TRANSCRIBER_DIR" -p 'test_*.py'
```

The suite covers completed-message queue repair, manual edits, video captions,
silent and oversized videos, atomic extraction, private temporary-directory
creation, and bounded MCP session renewal. Media extraction and model inference
are simulated in these tests; test actual ffmpeg and model availability on the
deployment machine before enabling the worker.
