# WhatsApp MCP Server (0xjesus fork)

[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Go 1.25+](https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![MCP](https://img.shields.io/badge/MCP-protocol-6366f1)](https://modelcontextprotocol.io/)
[![whatsmeow](https://img.shields.io/badge/whatsmeow-multidevice-25D366?logo=whatsapp&logoColor=white)](https://github.com/tulir/whatsmeow)

A single-binary Go [MCP](https://modelcontextprotocol.io/) server that wraps [whatsmeow](https://github.com/tulir/whatsmeow) to expose a personal WhatsApp account to LLM agents. `whatsapp-mcp serve` runs as a loopback HTTP daemon; MCP clients (Claude Code, Claude Desktop, Cursor, …) connect to it over HTTP. Messages are cached in local SQLite. Tool calls may send selected content to the agent’s model. The optional history worker also sends indexed text to the configured embedding provider in the background; choose a local provider to keep that step on your own machine.

This is a fork of [Sealjay/mcp-whatsapp](https://github.com/Sealjay/mcp-whatsapp) (itself a Go rewrite of [lharries/whatsapp-mcp](https://github.com/lharries/whatsapp-mcp)). All credit for the base server goes to those projects; this fork exists because we run the daemon 24/7 on an always-on Linux box as the WhatsApp memory of a personal assistant, and that use case needed a few things upstream does not have yet (see [What this fork adds](#what-this-fork-adds)).

> **Unaffiliated.** Independent open-source project. Not affiliated with, endorsed by, or associated with Meta Platforms, Inc., WhatsApp, or whatsmeow. "WhatsApp" is a trademark of Meta Platforms, Inc., used nominatively to describe interoperability.

> **Keep runtime data private.** Session state (`store/whatsapp.db`), the message cache (`store/messages.db`), downloaded media and any token are created at runtime under `store/` (git-ignored) or passed through environment variables. Test fixtures use synthetic `4477009…` numbers and invented names. If you deploy this, keep your `store/` directory private: it holds your WhatsApp session keys and your message history.

## What this fork adds

Everything below is additive: an upstream build still opens the same SQLite files (new tables are created with `IF NOT EXISTS`).

| Area | What changed | Where |
|---|---|---|
| **Full history sync** | Pairing requests the full history window (`WHATSAPP_MCP_FULL_SYNC=1`, 5000 days / 10 GB) so a fresh install gets as much past as WhatsApp will serve. | `internal/client/client.go` |
| **Account health and send limits** | A health state machine (`ok` / `restricted` / `temp_banned` / `logged_out` / `client_outdated`), send budgets for message actions, typing delays for text, and on-demand pairing with QR re-issue. Reconnection preserves active restrictions. These controls cannot guarantee account safety. `get_status` reports `health`. | `internal/client/safety.go`, `internal/daemon/` |
| **Edits and revocations** | Incoming edits update the cached message (original timestamp kept); revocations delete it and leave a tombstone so a later history batch cannot resurrect it. | `internal/client/mutations.go`, `docs/memory.md` |
| **Reactions** | Live `ReactionMessage`s and history-sync reactions are stored with timestamp ordering and removal tombstones (latest emoji per reactor per message) and mirrored as `media_type = "reaction"` rows so external indexers see them. `list_messages` prints `↳ 👍 Name` under the reacted message. | `internal/client/reactions.go`, `internal/store/reactions.go`, [`docs/reactions.md`](docs/reactions.md) |
| **Nothing is dropped silently** | Stickers (downloadable as images), locations, live locations and contact cards become rows; any other populated kind becomes a `[sin soporte: <field>]` placeholder with a warning instead of vanishing. | `internal/client/unsupported.go` |
| **Video key frames** | `download_media` of a video also extracts up to six key frames with ffmpeg and returns them inline as images, so an agent can "see" a clip. | `internal/client/frames.go` |
| **View-once capture** | Unavailable view-once notices are recorded; complete payloads are cached when WhatsApp delivers them; `request_view_once_recovery` asks your own primary phone for an exact direct-chat or group message. Group recovery requires a recorded sender. WhatsApp does **not** deliver view-once content to linked devices by design, so this is best-effort. | `internal/client/view_once.go`, [`docs/view-once.md`](docs/view-once.md) |
| **History index tools** | `semantic_search`, `index_status`, `history_analytics` proxy to an optional local history service (Postgres + pgvector) at `127.0.0.1:7256`. They return an availability error without it. The optional service and installer are included. | `internal/mcp/tools_memory.go`, [`docs/memory.md`](docs/memory.md) |
| **Media cache keyed by message ID** | History-synced media used to collide on generated filenames; the cache is now `<message_id>_<filename>`. | `internal/client/download.go` |
| **Document sending fixes** | Proper MIME types and `FileName` for documents so phones open them. | `internal/client/send.go` |
| **Audio and video transcription (optional)** | A Python worker transcribes voice notes and the audio track of videos with local faster-whisper. Captions and transcripts are retained together; unavailable media and videos without audio are reported. | `tools/transcriber/` |

### What is indexed

The optional history service is distributed with this repository. Follow [the installation guide](docs/history-install.md) to provision Postgres + pgvector, the worker and the loopback API.

| Content | Semantic index coverage |
|---|---|
| Message text and media captions | Text is indexed after ingestion and embedding. |
| Reactions, contacts and locations | The textual representation is indexed. |
| Voice notes and video speech | Completed, nonempty transcripts are indexed. Transcription must be running. |
| Images, stickers and video frames | Captions or descriptive placeholders only. There is no automatic visual embedding or OCR. Agents can inspect downloaded images and up to six video frames. |
| Attached documents | Metadata and captions only; document contents are not automatically extracted. |
| View-once or expired media | Only content actually delivered and downloaded is available. |

Check `index_status` for the WhatsApp-specific coverage, shared embedding queue size and snapshot age. Capturing a message, transcribing its media and generating its embedding are separate steps. An operational service does not imply the historical backlog is complete. Cached coverage can be stale, and completion covers only the locally available history.

## Setup

### Prerequisites

- Go 1.25+ (build-time only; the runtime needs just the binary).
- An MCP client that speaks HTTP.
- FFmpeg (optional): needed for `send_audio_message` conversions and for video key frames.
- Linux or macOS. **Windows** needs CGO, see [docs/windows.md](docs/windows.md).

### Build

```bash
git clone https://github.com/0xjesus/whatsapp-mcp.git
cd whatsapp-mcp
make build          # writes ./bin/whatsapp-mcp
make lint test      # go vet + gofmt + unit tests
```

The binary is static enough to copy to another machine of the same OS/arch (we build on a laptop and `scp` it to the server; keep the previous binary next to it as a rollback).

### Pair your phone (first run only)

```bash
./bin/whatsapp-mcp serve            # 127.0.0.1:8765 by default
open http://127.0.0.1:8765/pair     # or visit the URL manually
```

Scan the QR with WhatsApp (*Settings → Linked Devices → Link a Device*). The session persists in `./store/whatsapp.db`. When WhatsApp rotates the linked-device session, `/pair` serves a fresh QR; visit it again. Headless alternative: `./bin/whatsapp-mcp login` renders the QR in the terminal.

### Connect your MCP client

```jsonc
// Claude Code — .claude/mcp.json (project) or ~/.claude/mcp.json (user)
{ "mcpServers": { "whatsapp": { "type": "http", "url": "http://127.0.0.1:8765/mcp" } } }
```

```jsonc
// Claude Desktop — ~/Library/Application Support/Claude/claude_desktop_config.json
{ "mcpServers": { "whatsapp": { "url": "http://127.0.0.1:8765/mcp" } } }
```

If the daemon runs on another machine, do **not** expose it on the network: forward the port over SSH (`ssh -L 8765:127.0.0.1:8765 host`) and keep the loopback bind. `-allow-remote` exists but requires `WHATSAPP_MCP_TOKEN` and is not how we run it.

### Run it as a service

**Linux (systemd user unit)** — template in [`docs/systemd/whatsapp-mcp.service`](docs/systemd/whatsapp-mcp.service). The variables we set:

```ini
Environment=WHATSAPP_MCP_ADDR=127.0.0.1:8765
Environment=WHATSAPP_MCP_MEDIA_ROOT=%h/.local/share/whatsapp-mcp/store/uploads
Environment=WHATSAPP_MCP_FULL_SYNC=1
Environment=WHATSAPP_MCP_HUMANIZE=1
# add the directory that holds ffmpeg to PATH if it is not system-wide
```

`systemctl --user enable --now whatsapp-mcp` and `loginctl enable-linger $USER` so it survives logout.

**macOS (launchd)** — template in `docs/launchd/`. **Claude Code hook** — `docs/hooks/setup.sh` for project-scoped lifetimes.

### Sending and receiving files

`WHATSAPP_MCP_MEDIA_ROOT` bounds both directions. `send_file` / `send_audio_message` read from it; `download_media` writes decrypted media to the daemon cache at `<store>/<chat_jid>/` and, with `output_path`, also under the root. Symlinks are resolved before the check. Do not keep secrets inside the root.

Sandboxed clients cannot read the daemon cache: point `WHATSAPP_MCP_MEDIA_ROOT` at a directory they can read and always pass `output_path`. For images, stickers, audio (≤ 5 MiB) and video key frames the bytes are also embedded in the tool result, so most agents never need the path.

## Flags and environment

| Name | Meaning |
|---|---|
| `-addr host:port` / `WHATSAPP_MCP_ADDR` | Bind address (default `127.0.0.1:8765`). |
| `-store DIR` | Data directory (default `./store`). |
| `WHATSAPP_MCP_MEDIA_ROOT` | Allowed root for sending and for `output_path`. |
| `WHATSAPP_MCP_FULL_SYNC=1` | Request the full history window at pairing. |
| `WHATSAPP_MCP_HUMANIZE=1` | Space sends out, show typing first, cap sends per hour. |
| `WHATSAPP_MCP_DEBUG=1` / `-debug` | Verbose logs with partial phone-number redaction. |
| `-allow-remote` + `WHATSAPP_MCP_TOKEN` | Bind a non-loopback address with a bearer token. Avoid; use SSH forwarding. |

## Tools

50 tools. The ones this fork adds or changes are marked **(fork)**.

### Read / query

| Tool | Purpose |
|---|---|
| `search_contacts` | Substring search across cached contact names and numbers |
| `list_messages` | Query and filter messages; **(fork)** shows `[reaction …]` rows and a `↳` line with reactions under each message |
| `list_chats`, `get_chat`, `get_message_context` | Chat listing, metadata and context windows |
| `download_media` | Download persisted media; inlines images, stickers and audio ≤ 5 MiB; **(fork)** returns `Frames` (≤ 6 key frames) for videos |
| `request_sync` | Ask WhatsApp to backfill history for one chat |
| `semantic_search`, `index_status`, `history_analytics` **(fork)** | Query the optional local history index (see [`docs/memory.md`](docs/memory.md)) |
| `view_once_status`, `request_view_once_recovery` **(fork)** | Inspect and best-effort recover view-once media (see [`docs/view-once.md`](docs/view-once.md)) |

### Send

`send_message`, `send_file` (with `view_once`), `send_audio_message`, `send_poll`, `send_poll_vote`, `get_poll_results`, `send_contact_card`.

### Message actions

`mark_read`, `mark_chat_read`, `send_reaction`, `send_reply`, `edit_message`, `delete_message`, `send_typing`.

### Groups

`create_group`, `leave_group`, `list_groups`, `get_group_info`, `update_group_participants`, `set_group_name`, `set_group_topic`, `set_group_announce`, `set_group_locked`, `get_group_invite_link`, `join_group_with_link`.

### Blocklist, privacy, presence, admin

`get_blocklist`, `block_contact`, `unblock_contact`, `send_presence`, `get_privacy_settings`, `set_privacy_setting`, `set_status_message`, `is_on_whatsapp`, `get_status` (**(fork)** includes `health`), `pairing_status`.

## Architecture

```
cmd/whatsapp-mcp/       login / serve / smoke subcommands
internal/client/        whatsmeow wrapper: send, download, events, history, reactions, frames, view-once
internal/daemon/        HTTP server, pairing state machine, /pair endpoint
internal/mcp/           mark3labs/mcp-go server + tool registrations
internal/media/         ogg parsing, waveform synthesis, ffmpeg shell-out
internal/security/      path allowlisting, filename sanitisation, log redaction
internal/store/         SQLite cache (messages, chats, reactions, mutations, view-once), LID resolution, queries
tools/transcriber/      optional audio/video transcription worker (Python)
tools/history/          optional text embedding worker and search API (Python)
```

`serve` is a long-lived daemon; MCP clients connect and disconnect freely. Events are persisted **only while the daemon runs**; after a gap, whatsmeow's history sync backfills what WhatsApp still retains, and `request_sync` can target one chat. One instance per store (`flock` on `store/.lock`).

Data under `./store/`: `messages.db` (cache, including `reactions`, `message_mutations`, `view_once_media`, `transcripts`), `whatsapp.db` (whatsmeow session), `<chat_jid>/` media cache, `uploads/` media root.

## Limitations

- **Prompt injection.** Incoming messages are untrusted text reaching an LLM; see [the lethal trifecta](https://simonwillison.net/2025/Jun/16/the-lethal-trifecta/). The daemon escapes and truncates what it renders (reaction excerpts, emoji) but the agent still reads raw message bodies.
- **Account risk.** This is an unofficial client. Keep `WHATSAPP_MCP_HUMANIZE=1`, never bulk-send, and stop on `health.state != ok`. Our agent-side rules are in the skill that drives it, not in this repo.
- **View-once** cannot be guaranteed: WhatsApp withholds it from linked devices.
- **Gaps** while the daemon is down follow WhatsApp's retention, not ours.
- **Video frames** are limited to six. When scene detection finds fewer than two frames, sampling spans the clip duration. This is a preview, not an exhaustive visual analysis.
- **Log redaction** is obfuscation, not anonymisation.

## Development

```bash
make test          # unit tests
make test-race     # with -race
make lint          # go vet + gofmt
make e2e           # build + JSON-RPC smoke over HTTP (-tags=e2e)
make upgrade-check # bump whatsmeow@main, tidy, build, test
```

Tests use real SQLite (in-memory, schema from `internal/store/testdata/seed.sql`) and synthetic fixtures; ffmpeg-dependent tests skip when ffmpeg is absent. The Python workers include unit tests and an isolated Postgres integration test; see the history installation guide for commands.

## Troubleshooting

- **`connect failed …`** — not paired; open `/pair`.
- **`another whatsapp-mcp instance is already running`** — one `serve` per store.
- **`health.state` is `restricted` / `temp_banned`** — stop sending, do not restart in a loop, do not re-pair; wait for `health.until`.
- **No reactions / frames appear after upgrading** — restart the daemon; tables are created at start and history is not re-synced on restart (new events are captured from then on).
- **`ffmpeg not found`** — needed for audio conversion and video frames; `download_media` still returns the file with `FramesError` set.

## Licence

MIT, see [LICENSE](LICENSE). Upstream copyright belongs to the respective authors of [Sealjay/mcp-whatsapp](https://github.com/Sealjay/mcp-whatsapp) and [lharries/whatsapp-mcp](https://github.com/lharries/whatsapp-mcp).
