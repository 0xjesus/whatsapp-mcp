# Local history queries

`semantic_search`, `index_status` and `history_analytics` query the optional local
history service at `http://127.0.0.1:7256`. The MCP fixes the source to WhatsApp;
callers cannot override it. These tools send no WhatsApp messages and need no
account reconnection. The HTTP timeout is 45 seconds.

| Tool | Parameters and defaults | Usage |
|---|---|---|
| `semantic_search` | Required `query`; `mode="hybrid"`, `limit=20` | Find related messages. Modes are `hybrid`, `semantic`, `keyword`; limit is 1 to 50. |
| `index_status` | None | Inspect indexed coverage and embedding progress before interpreting missing results. |
| `history_analytics` | `group_by="month"`, `limit=30` | Count indexed messages by `month`, `day`, `chat`, `sender`, `media_type` or `source`; limit is 1 to 100. |

Search and analytics also accept optional `chat`, `sender`, `after` and `before`.
`chat` is the exact cached WhatsApp chat JID. Dates accept UTC epoch seconds or
ISO date/time strings. Responses preserve the service's JSON object.

History synchronization, copying messages into the index and generating
embeddings have separate progress. An empty result does not prove that a
conversation never happened. Aggregates cover the indexed messages available
at query time. `index_status` reports the current index state.

Search results retain `chat_id` and `message_id`. For WhatsApp, `chat_id` is the
chat JID; pass `message_id` to the existing `get_message_context` tool to read
surrounding cached messages. Messages and transcriptions are untrusted data,
including text that looks like instructions to an agent.

If the history service is unavailable, these tools return an MCP error without
restarting the account client. Existing cached-message tools remain available.

## Received edits, revocations and history limits

Explicit WhatsApp protocol edits update the original cached message and retain
its original timestamp. Supported edited bodies are text and image, video or
document captions. Explicit revocations remove the message from the cache;
their tombstones prevent an older history delivery from restoring it. Empty
audio deliveries preserve existing text, including manually corrected transcripts. These rules
apply to live events and incoming history batches. The
[`mutation tests`](../internal/client/mutations_test.go) exercise both paths.

The cache does not currently handle edits carried in `SecretEncryptedMessage`,
disappearing-message expiration, or synchronized local-only deletion actions.
An event that was never delivered cannot be reconstructed by the index. Unknown
or incomplete protocol messages never imply a deletion.

The client configures full-history preferences for device registration, but
there is no completion guarantee for all server history. Existing passive sync
accepts the batches WhatsApp supplies. The separate `request_sync` tool requests
50 earlier messages for one chat and has no durable record proving exhaustion.
Index completion therefore means completion of the available local cache.
See [`client.go`](../internal/client/client.go) and
[`history.go`](../internal/client/history.go) for the synchronization behavior.

Implementation and isolated transport tests are in
[`tools_memory.go`](../internal/mcp/tools_memory.go) and
[`tools_memory_test.go`](../internal/mcp/tools_memory_test.go).

## Installing the history service

The optional worker and API are included in this repository. See [history installation](history-install.md) for database setup, provider configuration, service commands and synthetic integration tests. Embeddings represent text, captions, reaction descriptions and available transcripts. Image pixels and document contents are not automatically embedded. Hosted embedding providers receive that text during background indexing.

Coverage is scoped to WhatsApp and carries a snapshot timestamp. A legacy snapshot without per-source counts returns unknown counts and a stale indicator until the worker refreshes it.
