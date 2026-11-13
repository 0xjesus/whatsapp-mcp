# Historical crawl correction: review and deployment handoff

Implementation commit: `f666319`, with review fixes `ce0575c`. Isolation branch: `fix/history-crawl`, at `../whatsapp-mcp-history-crawl`. No production deployment, sync request, message read, contact send/read receipt, consent-table modification, restart or pairing action occurred.

## Baseline and parity

The original checkout `../whatsapp-mcp-current` remains untouched and dirty. Its HEAD is `14fd7365c7b676e75986986d8a6802cc34d72437`, matching the installed executable's embedded VCS revision. The installed binary reports vcs.modified=true, so matching HEAD does **not** prove matching code. The entire existing tracked working diff and three untracked recipient-resolution files were copied into isolation and recorded separately as baseline commit `7c200c1`. Review that preservation commit together with `f666319`; do not overwrite the original checkout or discard its existing changes.

Previously audited installed binary on **macmini**: `~/.local/share/whatsapp-mcp/bin/whatsapp-mcp`, SHA256 `2dae2247d3a2b6f65b3bb331860afe67bb88d67f60b65b97c1a40a8e9672fafb`. Pinned whatsmeow dependency remains `v0.0.0-20260718134955-fac667d55293`. Remote read-only platform verification: x86_64, glibc2.43. Exact dirty-source parity with that installed binary remains unknown; no source snapshot for its dirty build was discovered.

## Behavior

`request_sync` keeps its existing chat_jid/from_timestamp arguments. Missing timestamp selects the oldest eligible cached key. Supplied timestamp restricts real eligible cached keys to timestamp<=bound; no fabricated SYNC ID or assumed from-me flag remains. No eligible key fails visibly. PN-to-LID resolution applies to either path. Reaction mirror rows, empty IDs and legacy SYNC-prefix rows cannot anchor requests. Existing cache provenance is limited; eligible legacy rows are presumed real except those explicit exclusions.

Every request is a self-peer message to the paired account with count50; no contact-send, typing, read-receipt, humanization or re-pair flow is added. Existing native restriction/persistent safety gates and monitoring consent are rechecked. A durable SQLite singleton serializes reservation and enforces at least60seconds between attempts across restarts. One locally outstanding reservation/send/ambiguous send blocks additional requests for up to10minutes. Network errors are durably marked ambiguous; no retry bypass is added. Request I/O has a30second context deadline.

Additive tables: history_pacing, history_requests, history_observations and history_crawls. Observation metadata is attempted before per-conversation ingestion, including empty terminal conversations, nullable terminal/progress/chunk fields and payload digest. Invalid/denied/unsupported or failed records remain incomplete. Successful processing does not imply exhaustion. If observation persistence fails, originals are still ingested, crawl is held incomplete_ledger, and no missing observation is finalized. Late responses update crawl state only while enabled=1, preserving pauses and existing holds. Interrupted observations remain visibly ingesting; there is no destructive schema migration.

The opt-in worker advances only after an ON_DEMAND observation with explicit more-remains type0/2, zero recorded ingestion failures and a strictly older timestamp in the cache. Tied timestamps stop conservatively. Missing flags, no progress, ingestion error, type3/no-access or type1/exhaustion observation stop the crawl. Type1 is recorded as incomplete_terminal_observed, never ALL. Timeout enters waiting_phone and requires explicit resume, with pacing preserved. Manual group revocations stop future sends; this feature does not grant group consent.

## Exact interfaces and activation

Current tunnel remains MCP `POST http://127.0.0.1:7343/mcp` using the existing authenticated/session-initialized transport. No new REST route.

```json
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"request_sync","arguments":{"chat_jid":"<canonical cached JID>","from_timestamp":"<optional ISO8601>"}}}
```

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"configure_history_crawl","arguments":{"chat_jid":"<canonical cached JID>","enabled":true}}}
```

Set enabled=false to pause; true explicitly resumes a stopped crawl. An eligible anchor and monitoring authorization are required. Unknown/no-anchor chats need a separate bootstrap path. This tool does not discover or authorize every account chat.

```json
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"history_crawl_status","arguments":{"chat_jid":"<optional canonical JID>"}}}
```

Status is read-only, returns up to100 newest rows per category, no message bodies, and explicitly includes upstream_complete=false and inventory_complete=false. Request/observation histories are not aggregated into a false completeness count.

No new service environment variable is required. The existing serve subcommand starts a worker with no active chats on first migration. Jobs are activated per chat through configure_history_crawl and persist across daemon restarts. Existing service on **macmini**: `~/.config/systemd/user/whatsapp-mcp.service`. Review and deployment are root-owned; do not start login/smoke/pairing commands to activate the feature. Existing paired data remains in the private directory on **macmini** `~/.local/share/whatsapp-mcp/store/`.

## Verified checks

- Baseline: go test ./internal/store ./internal/client ./internal/mcp passed before implementation.
- Test-first additions failed for missing anchor/reservation/observation APIs and missing new tool registrations before implementation.
- go test ./... passed all8packages.
- go test -race ./internal/store ./internal/client ./internal/mcp passed.
- go vet ./internal/store ./internal/client ./internal/mcp passed.
- git diff --check passed.
- Focused tests cover oldest key and from-me=false, timestamp bounds/no anchor, synthetic exclusion, self-peer destination, globally outstanding request, persistent pace after restart, empty terminal response, null terminal fields, interrupted ingestion, real SQLite write failure, strict older progression, no-access/type1/missing flags, timeout/manual resume and manual group revocation.

## Remaining limits and review plan

This is the explicitly allowed anchor+observation fallback, with a conservative bounded opt-in crawler. Incoming payload rows still use existing individual writes; raw payloads are not durably queued for full replay, and mutation helpers cannot report all persistence results. HistorySync lacks a direct job-correlation ID. Correlation currently uses ON_DEMAND type plus matching canonical chat; late/unsolicited same-chat pages can be ambiguous. The single local request slot reduces ambiguity but cannot establish protocol-level response identity. Therefore every terminal result remains incomplete and no account-wide coverage guarantee is produced.

There is no authenticated manifest of every uncached direct chat, no attachment payload guarantee, no recovery of deleted/disappearing/inaccessible primary history and no automatic grant to large groups. These are visible coverage limitations, not reasons to bypass restrictions or declare the193kcache complete.

Before deployment, root should review both commits and installed dirty-build parity, retain the previous executable for rollback, and back up the two existing SQLite databases consistently without exposing them. Deploying this additive migration cannot make the old executable aware of the new ledger. After deployment, inspect the new catalog/status and conduct one explicitly opted-in chat pilot; verify older-key progress and observation states before activating more chats. Network synchronization and production replacement have not been performed by this agent.

Review regressions: late response after manual pause and failed observation insert were reproduced in failing tests before correction. Both are now covered, including original-message capture despite metadata failure.

## Final reviewed build

Local executable: `/tmp/whatsapp-mcp-history-crawl-linux-amd64`.
SHA256: `29c19d47cdaf501f3ac6925509a10144f5ba431b4b13838209579cda86f7b89a`.
Linux amd64, GOAMD64=v1, CGO_ENABLED=1, Go1.27.1, trimpath enabled. Embedded VCS revision `ce0575c7ed012dab7da15f659eeb84edbebd264c`, vcs.modified=false. Built after the pause/metadata review corrections; all listed checks were repeated successfully after those corrections. No executable was copied to production or run against the paired account.
