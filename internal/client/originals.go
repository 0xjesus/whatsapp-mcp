package client

import (
	"context"

	"github.com/sealjay/mcp-whatsapp/internal/store"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

// ParserVersion is bumped whenever extractTextContent or extractMediaInfo
// learns to read something new. On start, every kept original parsed by an
// older version is read again and the rows it lost are filled in.
//
//	2: captions, album items (associatedChildMessage), documentWithCaption, albums.
const ParserVersion = 2

// unwrapContent returns the message that carries the content: an album item
// arrives inside associatedChildMessage and a captioned document inside
// documentWithCaptionMessage.
func unwrapContent(msg *waProto.Message) *waProto.Message {
	for i := 0; msg != nil && i < 4; i++ {
		if c := msg.GetAssociatedChildMessage(); c != nil && c.GetMessage() != nil {
			msg = c.GetMessage()
			continue
		}
		if d := msg.GetDocumentWithCaptionMessage(); d != nil && d.GetMessage() != nil {
			msg = d.GetMessage()
			continue
		}
		break
	}
	return msg
}

// keepOriginal stores the protobuf a message row was parsed from.
func (c *Client) keepOriginal(ctx context.Context, id, chatJID string, msg *waProto.Message) {
	if msg == nil || id == "" || c.store == nil {
		return
	}
	wire, err := proto.Marshal(msg)
	if err != nil {
		c.log.Warnf("Keep original %s: %v", c.redactor.MsgID(id), err)
		return
	}
	if err := c.store.KeepOriginal(ctx, id, chatJID, wire, ParserVersion); err != nil {
		c.log.Warnf("Keep original %s: %v", c.redactor.MsgID(id), err)
	}
}

// RepairFromOriginals re-reads originals parsed by an older parser and fills
// text or media their rows are missing. It returns how many rows it re-read.
func (c *Client) RepairFromOriginals(ctx context.Context) (int, error) {
	total := 0
	for {
		batch, err := c.store.StaleOriginals(ctx, ParserVersion, 200)
		if err != nil || len(batch) == 0 {
			return total, err
		}
		for _, r := range batch {
			var msg waProto.Message
			if err := proto.Unmarshal(r.Proto, &msg); err != nil {
				c.log.Warnf("Original %s unreadable: %v", c.redactor.MsgID(r.ID), err)
			}
			body := unwrapContent(&msg)
			content := extractTextContent(body)
			mediaType, filename, url, directPath, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(body)
			m := store.Message{MediaType: mediaType, Filename: filename, URL: url, DirectPath: directPath}
			if err := c.store.RepairFromOriginal(ctx, r, content, m, mediaKey, fileSHA256, fileEncSHA256, fileLength, ParserVersion); err != nil {
				return total, err
			}
			total++
		}
	}
}
