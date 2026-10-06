package mcp

import (
	"context"
	"github.com/mark3labs/mcp-go/mcp"
)

type scheduleMessageArgs struct {
	ChatJID        string `json:"chat_jid"`
	Text           string `json:"text"`
	SendAt         string `json:"send_at"`
	ExpiresAt      string `json:"expires_at,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}
type listScheduledArgs struct {
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	Status string `json:"status,omitempty"`
}
type scheduledJobArgs struct {
	JobID     string `json:"job_id"`
	SendAt    string `json:"send_at,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

func (s *Server) registerSchedulerTools() {
	s.mcp.AddTool(mcp.NewTool("schedule_message",
		mcp.WithDescription("Persist a future WhatsApp text delivery. RFC3339 timestamps must include timezone; send_at must be within 366 days. Delivery expires after 24 hours by default. At most 1000 active jobs and 16 KiB of text. Current send safety applies now and at delivery. Ambiguous network outcomes become uncertain for manual review and are never blindly resent. Cancellation is possible only while pending."),
		mcp.WithString("chat_jid", mcp.Required(), mcp.Description(recipientDesc)), mcp.WithString("text", mcp.Required()), mcp.WithString("send_at", mcp.Required()), mcp.WithString("expires_at"), mcp.WithString("idempotency_key"), mcp.WithReadOnlyHintAnnotation(false), mcp.WithOpenWorldHintAnnotation(true)),
		mcp.NewTypedToolHandler(func(ctx context.Context, req mcp.CallToolRequest, a scheduleMessageArgs) (*mcp.CallToolResult, error) {
			j, err := s.client.ScheduleMessage(a.ChatJID, a.Text, a.SendAt, a.ExpiresAt, a.IdempotencyKey)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return resultJSON(j)
		}))
	s.mcp.AddTool(mcp.NewTool("list_scheduled_messages", mcp.WithDescription("List durable scheduled WhatsApp deliveries in job order; limit defaults to 25 and cannot exceed 100. Pass next_cursor to fetch another page. uncertain requires manual delivery review."), mcp.WithNumber("limit", mcp.DefaultNumber(25)), mcp.WithString("cursor"), mcp.WithString("status"), mcp.WithReadOnlyHintAnnotation(true)),
		mcp.NewTypedToolHandler(func(ctx context.Context, req mcp.CallToolRequest, a listScheduledArgs) (*mcp.CallToolResult, error) {
			if a.Limit == 0 {
				a.Limit = 25
			}
			jobs, err := s.client.ListScheduledMessages(a.Limit, a.Cursor, a.Status)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			cursor := ""
			if len(jobs) == a.Limit {
				cursor = jobs[len(jobs)-1].ID
			}
			return resultJSON(map[string]interface{}{"jobs": jobs, "next_cursor": cursor})
		}))
	s.mcp.AddTool(mcp.NewTool("cancel_scheduled_message", mcp.WithDescription("Atomically cancel a pending job. Claimed, dispatching and terminal jobs cannot be cancelled."), mcp.WithString("job_id", mcp.Required()), mcp.WithReadOnlyHintAnnotation(false), mcp.WithOpenWorldHintAnnotation(false)),
		mcp.NewTypedToolHandler(func(ctx context.Context, req mcp.CallToolRequest, a scheduledJobArgs) (*mcp.CallToolResult, error) {
			if err := s.client.CancelScheduledMessage(a.JobID); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return resultJSON(map[string]string{"job_id": a.JobID, "status": "cancelled"})
		}))
	s.mcp.AddTool(mcp.NewTool("reschedule_message", mcp.WithDescription("Atomically change delivery time of a pending job. Timestamps require RFC3339 timezone. Default deadline is new send_at plus 24 hours. Claimed, dispatching and uncertain jobs cannot be rescheduled."), mcp.WithString("job_id", mcp.Required()), mcp.WithString("send_at", mcp.Required()), mcp.WithString("expires_at"), mcp.WithReadOnlyHintAnnotation(false), mcp.WithOpenWorldHintAnnotation(false)),
		mcp.NewTypedToolHandler(func(ctx context.Context, req mcp.CallToolRequest, a scheduledJobArgs) (*mcp.CallToolResult, error) {
			if err := s.client.RescheduleMessage(a.JobID, a.SendAt, a.ExpiresAt); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return resultJSON(map[string]string{"job_id": a.JobID, "status": "pending"})
		}))
}
