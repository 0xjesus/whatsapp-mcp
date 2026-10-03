# View-once capture

The daemon records unavailable view-once notifications separately from chat messages in `view_once_media`. Complete incoming image, video and audio payloads are cached immediately with a 30-second download timeout. History payloads use the same capture path. No read or played receipt is sent by this feature.

`view_once_status(chat_jid, message_id)` returns availability, local path, error and the last phone request ID. `saved` identifies a completed download. `unavailable`, `requested`, `received`, `download_failed` and `request_failed` do not mean a file exists. Paths are on the backend host.

`request_view_once_recovery(chat_jid, message_id)` asks the paired primary phone for that exact message using whatsmeow's placeholder resend protocol. It resolves phone JIDs to LIDs, checks account health, limits calls to one per 45 seconds and one per message per five minutes, and never sends a chat message to the contact. Poll the status tool separately: successful submission does not establish delivery. WhatsApp may withhold content or the phone may no longer have it. There is no automatic retry loop.

Existing sessions and network settings are unchanged. The new table is additive; an older binary can still use the message database.

## Groups

`request_view_once_recovery` also accepts a group JID. It needs an unavailable notice already
recorded for that message (the notice carries the sender's LID, which becomes the participant of the
placeholder request). Without a recorded sender the call fails with a clear error instead of guessing.

A successful request only confirms submission. The phone may never return the payload; rely on `state: saved` and a verified local file before claiming recovery.
