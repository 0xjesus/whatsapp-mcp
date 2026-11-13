package mcp

import (
	"context"
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
)

type historyStatusArgs struct {
	ChatJID string `json:"chat_jid,omitempty"`
}
type historyCrawlArgs struct {
	ChatJID string `json:"chat_jid"`
	Enabled bool   `json:"enabled"`
}

func (s *Server) registerHistoryCrawl() {
	s.mcp.AddTool(mcp.NewTool("history_crawl_status",
		mcp.WithDescription("Read durable history requests, terminal observations and crawl states. Returns at most 100 newest rows per category. upstream_complete and inventory_complete remain false: cached/index counts and uncorrelated terminal events do not prove ALL history."),
		mcp.WithString("chat_jid", mcp.Description("Optional cached canonical chat JID")),
		mcp.WithReadOnlyHintAnnotation(true), mcp.WithIdempotentHintAnnotation(true), mcp.WithDestructiveHintAnnotation(false),
	), mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a historyStatusArgs) (*mcp.CallToolResult, error) {
		status, err := s.client.HistoryCrawlStatus(a.ChatJID)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		data, err := json.Marshal(status)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(string(data)), nil
	}))
	s.mcp.AddTool(mcp.NewTool("configure_history_crawl",
		mcp.WithDescription("Enable/resume or pause a bounded backward crawl for one cached authorized chat. Uses only self-peer history requests, one globally outstanding page, 50 messages/page, minimum 60 seconds between attempts across restarts. Stops on timeout, missing terminal metadata, no strict older timestamp, ingestion failure or revoked monitoring consent. Does not grant group consent or prove ALL history."),
		mcp.WithString("chat_jid", mcp.Required(), mcp.Description("Cached canonical chat JID")),
		mcp.WithBoolean("enabled", mcp.Required(), mcp.Description("true enables/resumes; false pauses future requests")),
		mcp.WithReadOnlyHintAnnotation(false), mcp.WithIdempotentHintAnnotation(true), mcp.WithDestructiveHintAnnotation(false), mcp.WithOpenWorldHintAnnotation(true),
	), mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a historyCrawlArgs) (*mcp.CallToolResult, error) {
		if a.ChatJID == "" {
			return mcp.NewToolResultError("chat_jid required"), nil
		}
		if err := s.client.ConfigureHistoryCrawl(a.ChatJID, a.Enabled); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText("History crawl configuration persisted. Inspect history_crawl_status; no completeness guarantee."), nil
	}))
}
