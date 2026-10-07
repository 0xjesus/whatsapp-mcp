package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"

	"github.com/sealjay/mcp-whatsapp/internal/media"
	"github.com/sealjay/mcp-whatsapp/internal/store"
)

// SendResult is the public return value from Send/SendMediaWithOptions.
type SendResult struct {
	Success       bool
	Message       string
	ID            string
	BeforeNetwork bool          `json:"-"`
	RetryAfter    time.Duration `json:"-"`
	FailureKind   string        `json:"-"`
	Accepted      bool
	Status        string
	JobID         string
}

// SendMediaOptions bundles the inputs to SendMediaWithOptions so callers can
// add per-message flags (view_once, etc.) without growing the argument list.
type SendMediaOptions struct {
	Recipient string
	Caption   string
	MediaPath string
	ViewOnce  bool
}

// Send sends a text message.
func (c *Client) Send(ctx context.Context, recipient, message string) SendResult {
	return c.send(ctx, recipient, message, "", false)
}

// SendMediaWithOptions is the extensible entry point for media sends.
func (c *Client) SendMediaWithOptions(ctx context.Context, opts SendMediaOptions) SendResult {
	return c.send(ctx, opts.Recipient, opts.Caption, opts.MediaPath, opts.ViewOnce)
}

// send is the unified implementation shared by Send and SendMediaWithOptions.
func (c *Client) send(ctx context.Context, recipient, message, mediaPath string, viewOnce bool) SendResult {
	return c.enqueueSend(ctx, recipient, outboundPayload{Body: message, MediaPath: mediaPath, ViewOnce: viewOnce})
}

// ephemeralContextInfo returns the MessageContextInfo carrying the group's
// disappearing-message timer for chat, or nil if chat isn't a group,
// GetGroupInfo fails, or no timer is set. Shared by send and SendReply.
func (c *Client) ephemeralContextInfo(ctx context.Context, chat types.JID) *waProto.MessageContextInfo {
	if chat.Server != "g.us" {
		return nil
	}
	gi, err := c.wa.GetGroupInfo(ctx, chat)
	if err != nil || gi.DisappearingTimer == 0 {
		return nil
	}
	c.log.Infof("Auto-detected group disappearing timer: %d seconds", gi.DisappearingTimer)
	return &waProto.MessageContextInfo{
		MessageAddOnDurationInSecs: proto.Uint32(gi.DisappearingTimer),
	}
}

// IsKnownContact classifies a send target for the rate limiter. Groups we can
// send to are inherently known contexts, so they are never treated as cold.
// For 1:1 recipients, "known" means we have prior inbound history from that
// JID in the cache. On a store error we fail closed — treat the target as a
// non-contact so the stricter limit applies.
func (c *Client) IsKnownContact(jid types.JID) bool {
	if jid.Server == types.GroupServer {
		return true
	}
	known, err := c.store.HasInboundFrom(jid.String())
	if err != nil {
		c.log.Warnf("contact lookup failed for %s, treating as non-contact: %v", c.redactor.JID(jid.String()), err)
		return false
	}
	return known
}

// parseRecipient normalizes a phone number or JID string into types.JID.
func parseRecipient(recipient string) (types.JID, error) {
	canonical, err := NormalizeRecipient(recipient)
	if err != nil {
		return types.JID{}, err
	}
	if strings.Contains(canonical, "@") {
		jid, err := types.ParseJID(canonical)
		if err != nil {
			return types.JID{}, fmt.Errorf("Error parsing JID: %v", err)
		}
		return jid, nil
	}
	return types.JID{User: canonical, Server: types.DefaultUserServer}, nil
}

// attachMedia uploads mediaPath and populates the appropriate message field.
// When viewOnce is true, the resulting Image/Video/Audio submessage is flagged
// as view-once. DocumentMessage has no view-once support in WhatsApp clients,
// so the flag is silently ignored for documents.
func (c *Client) attachMedia(ctx context.Context, msg *waProto.Message, mediaPath, caption string, viewOnce bool) error {
	safePath, err := c.ValidateMediaPath(mediaPath)
	if err != nil {
		return fmt.Errorf("media_path rejected: %w", err)
	}
	mediaData, err := os.ReadFile(safePath)
	if err != nil {
		return fmt.Errorf("read media file: %w", err)
	}

	return c.attachMediaData(ctx, msg, safePath, mediaData, caption, viewOnce)
}

func (c *Client) attachMediaData(ctx context.Context, msg *waProto.Message, mediaPath string, mediaData []byte, caption string, viewOnce bool) error {
	mediaType, mimeType := mediaTypeFromExt(mediaPath)

	resp, err := c.wa.Upload(ctx, mediaData, mediaType)
	if err != nil {
		return fmt.Errorf("Error uploading media: %v", err)
	}
	c.log.Infof("Media uploaded: url=%s bytes=%d", c.redactor.URL(resp.URL), resp.FileLength)

	switch mediaType {
	case whatsmeow.MediaImage:
		msg.ImageMessage = &waProto.ImageMessage{
			Caption:       proto.String(caption),
			Mimetype:      proto.String(mimeType),
			URL:           &resp.URL,
			DirectPath:    &resp.DirectPath,
			MediaKey:      resp.MediaKey,
			FileEncSHA256: resp.FileEncSHA256,
			FileSHA256:    resp.FileSHA256,
			FileLength:    &resp.FileLength,
		}
		if viewOnce {
			msg.ImageMessage.ViewOnce = proto.Bool(true)
		}
	case whatsmeow.MediaAudio:
		var seconds uint32 = 30
		var waveform []byte
		if strings.Contains(mimeType, "ogg") {
			s, w, aerr := media.AnalyzeOggOpus(mediaData)
			if aerr != nil {
				return fmt.Errorf("Failed to analyze Ogg Opus file: %v", aerr)
			}
			seconds = s
			waveform = w
		} else {
			c.log.Warnf("Not an Ogg Opus file: %s", mimeType)
		}
		msg.AudioMessage = &waProto.AudioMessage{
			Mimetype:      proto.String(mimeType),
			URL:           &resp.URL,
			DirectPath:    &resp.DirectPath,
			MediaKey:      resp.MediaKey,
			FileEncSHA256: resp.FileEncSHA256,
			FileSHA256:    resp.FileSHA256,
			FileLength:    &resp.FileLength,
			Seconds:       proto.Uint32(seconds),
			PTT:           proto.Bool(true),
			Waveform:      waveform,
		}
		if viewOnce {
			msg.AudioMessage.ViewOnce = proto.Bool(true)
		}
	case whatsmeow.MediaVideo:
		msg.VideoMessage = &waProto.VideoMessage{
			Caption:       proto.String(caption),
			Mimetype:      proto.String(mimeType),
			URL:           &resp.URL,
			DirectPath:    &resp.DirectPath,
			MediaKey:      resp.MediaKey,
			FileEncSHA256: resp.FileEncSHA256,
			FileSHA256:    resp.FileSHA256,
			FileLength:    &resp.FileLength,
		}
		if viewOnce {
			msg.VideoMessage.ViewOnce = proto.Bool(true)
		}
	case whatsmeow.MediaDocument:
		// View-once on documents is not supported by WhatsApp clients; silently ignored.
		msg.DocumentMessage = &waProto.DocumentMessage{
			Title:         proto.String(filepath.Base(mediaPath)),
			FileName:      proto.String(filepath.Base(mediaPath)), // local patch: iOS shows "datos" and cannot open documents without FileName
			Caption:       proto.String(caption),
			Mimetype:      proto.String(mimeType),
			URL:           &resp.URL,
			DirectPath:    &resp.DirectPath,
			MediaKey:      resp.MediaKey,
			FileEncSHA256: resp.FileEncSHA256,
			FileSHA256:    resp.FileSHA256,
			FileLength:    &resp.FileLength,
		}
	}
	return nil
}

// mediaTypeFromExt maps a file extension to a whatsmeow MediaType + mime type.
func mediaTypeFromExt(path string) (whatsmeow.MediaType, string) {
	ext := ""
	if idx := strings.LastIndex(path, "."); idx >= 0 {
		ext = strings.ToLower(path[idx+1:])
	}
	switch ext {
	case "jpg", "jpeg":
		return whatsmeow.MediaImage, "image/jpeg"
	case "png":
		return whatsmeow.MediaImage, "image/png"
	case "gif":
		return whatsmeow.MediaImage, "image/gif"
	case "webp":
		return whatsmeow.MediaImage, "image/webp"
	case "ogg":
		return whatsmeow.MediaAudio, "audio/ogg; codecs=opus"
	case "mp4":
		return whatsmeow.MediaVideo, "video/mp4"
	case "avi":
		return whatsmeow.MediaVideo, "video/avi"
	case "mov":
		return whatsmeow.MediaVideo, "video/quicktime"
	// Local patch: real MIME types for common documents; iOS refuses to open
	// documents sent as application/octet-stream.
	case "pdf":
		return whatsmeow.MediaDocument, "application/pdf"
	case "docx":
		return whatsmeow.MediaDocument, "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case "xlsx":
		return whatsmeow.MediaDocument, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case "pptx":
		return whatsmeow.MediaDocument, "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case "csv":
		return whatsmeow.MediaDocument, "text/csv"
	case "txt", "md":
		return whatsmeow.MediaDocument, "text/plain"
	case "zip":
		return whatsmeow.MediaDocument, "application/zip"
	default:
		return whatsmeow.MediaDocument, "application/octet-stream"
	}
}

// persistSent writes a cache row for a message we sent so the local history
// stays complete regardless of device multiplexing.
func (c *Client) persistSent(ctx context.Context, recipientJID types.JID, msgID, message, mediaPath string, msg *waProto.Message) error {
	chatJID := recipientJID.String()
	if !strings.HasSuffix(chatJID, "@g.us") {
		chatJID = c.store.ResolveLIDToJID(chatJID)
	}

	chatName := c.store.FindChatName(chatJID)
	if c.networkSend == nil {
		chatName = c.resolveChatNameFallback(ctx, recipientJID, "")
	}

	now := time.Now()
	if err := c.store.StoreChat(chatJID, chatName, now); err != nil {
		c.log.Warnf("Failed to store chat for sent message: %v", err)
	}

	ourJID := ""
	if c.wa != nil && c.wa.Store != nil && c.wa.Store.ID != nil {
		ourJID = c.wa.Store.ID.User
	}

	var (
		storedMediaType, storedFilename, storedURL, storedDirectPath string
		storedMediaKey, storedFileSHA256, storedFileEnc              []byte
		storedFileLength                                             uint64
	)
	if mediaPath != "" {
		storedMediaType, storedFilename, storedURL, storedDirectPath,
			storedMediaKey, storedFileSHA256, storedFileEnc, storedFileLength = extractMediaInfo(msg)
	}

	err := c.store.StoreMessage(ctx, store.Message{
		ID:         msgID,
		ChatJID:    chatJID,
		Sender:     ourJID,
		Content:    message,
		Timestamp:  now,
		IsFromMe:   true,
		MediaType:  storedMediaType,
		Filename:   storedFilename,
		URL:        storedURL,
		DirectPath: storedDirectPath,
	}, storedMediaKey, storedFileSHA256, storedFileEnc, storedFileLength)
	if err != nil {
		c.log.Warnf("Failed to store sent message: %v", err)
		return err
	}
	c.log.Infof("[%s] -> %s [%s]: %s", now.Format("2006-01-02 15:04:05"), c.redactor.JID(chatJID), c.redactor.MsgID(msgID), c.redactor.Body(message))
	return nil
}
