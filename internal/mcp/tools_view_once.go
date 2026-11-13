package mcp

import (
	"context"
	"github.com/mark3labs/mcp-go/mcp"
)

type viewOnceArgs struct {
	ChatJID   string `json:"chat_jid"`
	MessageID string `json:"message_id"`
}

func (s *Server) registerViewOnceTools() {
	for _, name := range []string{"view_once_status", "request_view_once_recovery", "request_message_resend"} {
		recovery := name != "view_once_status"
		desc := "Read cached view-once availability and recovery status. Only state=saved with path means a file was downloaded. Paths belong to the backend host. No network side effects."
		if recovery {
			desc = "Ask our own primary phone once for a specific unavailable message (direct chat, or group with a recorded notice). Sends no visible chat message and no read receipt. A request ID is NOT recovery success; inspect view_once_status afterwards. Cooldown: 45 seconds globally and 5 minutes per message. WhatsApp may not deliver view-once content."
		}
		if name == "request_message_resend" {
			desc = "Ask our own primary phone once to send again one stored message whose text or media was lost (for example a caption an older build did not read). Direct chat or any stored group message. Sends no visible chat message and no read receipt; the phone must be online. A request ID is NOT recovery success: re-read the message afterwards. Cooldown: 45 seconds globally and 5 minutes per message."
		}
		tool := mcp.NewTool(name, mcp.WithDescription(desc), mcp.WithString("chat_jid", mcp.Required(), mcp.Description("Direct chat phone/LID JID, or a group JID (needs a recorded unavailable notice for that message)")), mcp.WithString("message_id", mcp.Required(), mcp.Description("Exact original message ID")), mcp.WithReadOnlyHintAnnotation(!recovery), mcp.WithDestructiveHintAnnotation(false), mcp.WithIdempotentHintAnnotation(!recovery), mcp.WithOpenWorldHintAnnotation(recovery))
		s.mcp.AddTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a viewOnceArgs) (*mcp.CallToolResult, error) {
			if r := requireNonEmpty("chat_jid", a.ChatJID); r != nil {
				return r, nil
			}
			if r := requireNonEmpty("message_id", a.MessageID); r != nil {
				return r, nil
			}
			if recovery {
				r, err := s.client.RequestViewOnceRecovery(ctx, a.ChatJID, a.MessageID)
				if err != nil {
					return mcp.NewToolResultError(err.Error()), nil
				}
				return resultJSON(r)
			}
			r, err := s.client.ViewOnceStatus(a.ChatJID, a.MessageID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return resultJSON(r)
		}))
	}
}
