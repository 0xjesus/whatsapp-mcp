# Reactions, stickers, locations, contacts

Live `ReactionMessage`s and the `reactions` field of history-sync messages are stored in the
`reactions` table (latest emoji per reactor per message). Each stored reaction also has a mirror
row in `messages` with `media_type = "reaction"` and content `👍 → «first 60 characters of the
original»` so the shared history index ingests it and `list_messages` shows it in its own
chronological place. Removing a reaction deletes both. Mirror IDs are the live message ID or
`r:<message_id>:<reactor>` for history.

`list_messages` adds `   ↳ 👍 Name · ❤️ Name` under any message with reactions.

Stickers are `media_type = "sticker"` (downloadable like images; `download_media` inlines them).
Locations, live locations and contacts become text rows (`[ubicación] …`, `[contacto] …`).
Any other populated kind becomes `media_type = "unsupported"` with `[sin soporte: <field>]` and a
warning in the log; signal-only kinds (protocol, sender keys, poll updates, reactions, keep-in-chat)
are logged at debug level and not stored.

`download_media` of a video also returns up to six key frames (ffmpeg, scene detection, 512 px) as
inline images and lists them under `Frames`; `FramesError` explains when extraction failed. Frames
are cached next to the video as `<file>.frames/frame-NN.jpg`.

Implementation: `internal/store/reactions.go`, `internal/client/reactions.go`,
`internal/client/unsupported.go`, `internal/client/frames.go`; tests beside each.
