package client

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.mau.fi/whatsmeow/types"
)

var errRecipientNotRegistered = errors.New("recipient is not registered on WhatsApp")

// resolveSendRecipient uses server identity, never country-code guesses. The
// persisted authorization keeps its original recipient and idempotency hash.
func (c *Client) resolveSendRecipient(ctx context.Context, jid types.JID) (types.JID, error) {
	if jid.Server != types.DefaultUserServer {
		return jid, nil
	}
	alias, err := c.store.RecipientAlias(ctx, jid.String())
	if err != nil {
		return types.JID{}, err
	}
	if alias != jid.String() {
		return types.ParseJID(alias)
	}
	// Existing synthetic send fixtures do not make live recipient queries.
	if c.recipientLookup == nil && c.networkSend != nil {
		return jid, nil
	}
	if c.recipientLookup == nil {
		cached, err := c.wa.Store.LIDs.GetLIDForPN(ctx, jid)
		if err != nil {
			return types.JID{}, err
		}
		if !cached.IsEmpty() {
			return jid, nil
		}
	}
	if c.limiter != nil {
		d := c.limiter.AllowUsync()
		if !d.Allowed {
			return types.JID{}, fmt.Errorf("recipient lookup pacing: %s; retry in %s", d.Reason, d.RetryAfter)
		}
	}
	lookup := c.recipientLookup
	if lookup == nil {
		lookup = c.wa.IsOnWhatsApp
	}
	results, err := lookup(ctx, []string{"+" + jid.User})
	c.noteMutationResult(err)
	if err != nil {
		return types.JID{}, fmt.Errorf("recipient lookup: %w", err)
	}
	for _, r := range results {
		if strings.TrimPrefix(r.Query, "+") != jid.User {
			continue
		}
		if !r.IsIn {
			return types.JID{}, errRecipientNotRegistered
		}
		canonical := r.PhoneNumber.ToNonAD()
		if canonical.Server != types.DefaultUserServer || canonical.User == "" {
			canonical = r.JID.ToNonAD()
		}
		if canonical.Server != types.DefaultUserServer || !isAllDigits(canonical.User) || canonical.User == "" {
			return types.JID{}, errors.New("registered recipient has no canonical phone identity")
		}
		if err := c.store.RecordRecipientAlias(ctx, jid.String(), canonical.String()); err != nil {
			return types.JID{}, err
		}
		return canonical, nil
	}
	return types.JID{}, errors.New("recipient lookup did not return the requested phone")
}
