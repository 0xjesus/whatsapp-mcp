package mcp

import (
	"context"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerOutboxTools() {
	get := mcp.NewTool("get_outbox",
		mcp.WithDescription(offlineSafePrefix+"Inspect durably accepted sends without resending them. Returns jobs with stable job/message IDs, recipient, state, attempts, next_attempt, reason and sent_id; excludes message content and media. queued/sending means not yet confirmed, sent means confirmed, needs_review means uncertain delivery and must never be blindly resent. blocked/failed require user review. FIFO queue automatically waits local pacing and finite account cooldown. Optional job_id retrieves one job; otherwise latest 100 (limit capped at 100)."),
		mcp.WithString("job_id", mcp.Description("JobID returned by a send tool")), mcp.WithNumber("limit", mcp.DefaultNumber(100)),
		mcp.WithReadOnlyHintAnnotation(true), mcp.WithIdempotentHintAnnotation(true), mcp.WithOpenWorldHintAnnotation(false))
	s.mcp.AddTool(get, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a struct {
		JobID string `json:"job_id"`
		Limit int    `json:"limit"`
	}) (*mcp.CallToolResult, error) {
		jobs, err := s.client.GetOutbox(ctx, a.JobID, a.Limit)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return resultJSON(jobs)
	}))
	cancel := mcp.NewTool("cancel_outbox",
		mcp.WithDescription("Cancel one queued or blocked authorized job so it cannot start delivery. Refuses sending, needs_review or already terminal jobs; uncertain delivery retains its original diagnostics. Does not revoke sent messages. Returns Cancelled and JobID."),
		mcp.WithString("job_id", mcp.Required()), mcp.WithReadOnlyHintAnnotation(false), mcp.WithDestructiveHintAnnotation(false), mcp.WithIdempotentHintAnnotation(false), mcp.WithOpenWorldHintAnnotation(false))
	s.mcp.AddTool(cancel, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a struct {
		JobID string `json:"job_id"`
	}) (*mcp.CallToolResult, error) {
		if err := s.client.CancelOutbox(ctx, a.JobID); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return resultJSON(map[string]any{"Cancelled": true, "JobID": a.JobID})
	}))
}
