package store

import "context"

// ViewOnce records availability separately from message content. A request ID
// only means the phone was asked; only State=saved identifies a downloaded file.
type ViewOnce struct {
	MessageID   string `json:"message_id"`
	ChatJID     string `json:"chat_jid"`
	SenderJID   string `json:"sender_jid"`
	State       string `json:"state"`
	Path        string `json:"path,omitempty"`
	Error       string `json:"error,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	RequestedAt int64  `json:"requested_at,omitempty"`
}

func (s *Store) GetViewOnce(chat, id string) (r ViewOnce, err error) {
	err = s.db.QueryRow(`SELECT id,chat_jid,sender_jid,state,path,error,request_id,requested_at FROM view_once_media WHERE id=? AND chat_jid=?`, id, chat).Scan(&r.MessageID, &r.ChatJID, &r.SenderJID, &r.State, &r.Path, &r.Error, &r.RequestID, &r.RequestedAt)
	return
}

func (s *Store) PutViewOnce(ctx context.Context, r ViewOnce) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO view_once_media(id,chat_jid,sender_jid,state,path,error) VALUES(?,?,?,?,?,?)
 ON CONFLICT(id,chat_jid) DO UPDATE SET sender_jid=excluded.sender_jid,state=excluded.state,path=excluded.path,error=excluded.error
 WHERE view_once_media.state!='saved' AND (excluded.state!='unavailable' OR view_once_media.state='unavailable')`, r.MessageID, r.ChatJID, r.SenderJID, r.State, r.Path, r.Error)
	return err
}

// ReserveViewOnceRequest enforces a persistent per-message cooldown, including
// failed attempts. Concurrent requests cannot both reserve the same message.
func (s *Store) ReserveViewOnceRequest(ctx context.Context, chat, id string, now int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE view_once_media SET requested_at=?,request_id='',error='',state=CASE WHEN state IN ('unavailable','requested','request_failed') THEN 'requested' ELSE state END
 WHERE id=? AND chat_jid=? AND state!='saved' AND requested_at<=?`, now, id, chat, now-300)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Store) FinishViewOnceRequest(ctx context.Context, chat, id, requestID, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE view_once_media SET request_id=?,error=CASE WHEN state='requested' THEN ? ELSE error END,
 state=CASE WHEN state='requested' AND ?!='' THEN 'request_failed' ELSE state END WHERE id=? AND chat_jid=?`, requestID, reason, reason, id, chat)
	return err
}
