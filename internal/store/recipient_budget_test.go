package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/ratelimit"
)

func recipientBudget(t *testing.T, s *Store, now time.Time, recipient string, known, reserve bool) ratelimit.Decision {
	t.Helper()
	d, err := s.PersistentRecipientSendBudget(context.Background(), now, recipient, known, reserve)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func readyRecipientStore(t *testing.T) (*Store, time.Time) {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	if err = s.EnsureSendProtection(context.Background(), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	return s, now
}
func seedRecipientAttempt(t *testing.T, s *Store, at time.Time, recipient string, known bool) {
	t.Helper()
	if _, err := s.DB().Exec(`INSERT INTO outbound_attempts(at_ns,known,recipient) VALUES(?,?,?)`, at.UnixNano(), known, recipient); err != nil {
		t.Fatal(err)
	}
}
func ledgerCount(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestRecipientBudgetDailyLimits(t *testing.T) {
	for _, tc := range []struct {
		name              string
		count             int
		known             bool
		recipient, reason string
	}{
		{"overall", 120, true, "target@s.whatsapp.net", "daily send cap reached"},
		{"per recipient", 20, true, "target@s.whatsapp.net", "daily recipient send cap reached"},
		{"distinct unknown", 20, false, "new@s.whatsapp.net", "daily distinct non-contact cap reached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, now := readyRecipientStore(t)
			for i := 0; i < tc.count; i++ {
				seedRecipientAttempt(t, s, now.Add(-2*time.Hour-time.Duration(i)*time.Minute), "", tc.known)
			}
			before := ledgerCount(t, s, "outbound_attempts")
			d := recipientBudget(t, s, now, tc.recipient, tc.known, true)
			if d.Allowed || d.Reason != tc.reason || d.RetryAfter <= 0 {
				t.Fatalf("missing daily protection: %+v", d)
			}
			if ledgerCount(t, s, "outbound_attempts") != before || ledgerCount(t, s, "send_budget") != 0 {
				t.Fatal("denial mutated reservation ledgers")
			}
		})
	}
}
func TestRecipientBudgetDistinctRepeatsAndKnownRecipient(t *testing.T) {
	s, now := readyRecipientStore(t)
	for i := 0; i < 20; i++ {
		seedRecipientAttempt(t, s, now.Add(-2*time.Hour), fmt.Sprintf("%d@s.whatsapp.net", i), false)
	}
	if d := recipientBudget(t, s, now, "0@s.whatsapp.net", false, false); !d.Allowed {
		t.Fatalf("repeat consumed another distinct slot: %+v", d)
	}
	if d := recipientBudget(t, s, now, "new@s.whatsapp.net", false, false); d.Allowed {
		t.Fatal("21st distinct unknown allowed")
	}
	if d := recipientBudget(t, s, now, "new@s.whatsapp.net", true, true); !d.Allowed {
		t.Fatalf("known recipient incorrectly subject to unknown quota: %+v", d)
	}
	if ledgerCount(t, s, "outbound_attempts") != 21 || ledgerCount(t, s, "send_budget") != 1 {
		t.Fatal("check reserved or dual ledger was not written")
	}
}
func TestRecipientBudgetPerRecipientAndLegacyCounts(t *testing.T) {
	s, now := readyRecipientStore(t)
	for i := 0; i < 19; i++ {
		seedRecipientAttempt(t, s, now.Add(-2*time.Hour), "a@s.whatsapp.net", true)
	}
	seedRecipientAttempt(t, s, now.Add(-3*time.Hour), "", true)
	if d := recipientBudget(t, s, now, "a@s.whatsapp.net", true, true); d.Allowed {
		t.Fatal("identity-less legacy row did not charge recipient cap")
	}
	if d := recipientBudget(t, s, now, "b@s.whatsapp.net", true, true); !d.Allowed {
		t.Fatalf("another known recipient denied: %+v", d)
	}
}
func TestRecipientBudgetExactDailyExpiry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		n         int
		known     bool
		recipient string
	}{
		{"overall", 120, true, "a@s.whatsapp.net"}, {"per recipient", 20, true, "a@s.whatsapp.net"}, {"distinct", 20, false, "new@s.whatsapp.net"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, now := readyRecipientStore(t)
			for i := 0; i < tc.n; i++ {
				seedRecipientAttempt(t, s, now.Add(-24*time.Hour), "", tc.known)
			}
			if d := recipientBudget(t, s, now.Add(-time.Nanosecond), tc.recipient, tc.known, false); d.Allowed || d.RetryAfter != time.Nanosecond {
				t.Fatalf("window before boundary: %+v", d)
			}
			if d := recipientBudget(t, s, now, tc.recipient, tc.known, true); !d.Allowed {
				t.Fatalf("exact boundary did not expire: %+v", d)
			}
			if ledgerCount(t, s, "outbound_attempts") != 1 {
				t.Fatal("expired rows not pruned")
			}
		})
	}
}
func TestRecipientBudgetRetainedAcrossReopen(t *testing.T) {
	s, now := readyRecipientStore(t)
	dir := s.Dir()
	for i := 0; i < 20; i++ {
		if d := recipientBudget(t, s, now.Add(time.Duration(i)*5*time.Minute), "a@s.whatsapp.net", true, true); !d.Allowed {
			t.Fatalf("reserve %d: %+v", i, d)
		}
	}
	if ledgerCount(t, s, "outbound_attempts") != 20 {
		t.Fatal("canonical ledger discarded daily history")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if d := recipientBudget(t, reopened, now.Add(3*time.Hour), "a@s.whatsapp.net", true, true); d.Allowed || d.Reason != "daily recipient send cap reached" {
		t.Fatalf("restart lost daily recipient budget: %+v", d)
	}
	if d := recipientBudget(t, reopened, now.Add(3*time.Hour), "b@s.whatsapp.net", true, true); !d.Allowed {
		t.Fatalf("daily history counted both ledgers: %+v", d)
	}
}
func TestRecipientBudgetLegacySchemaMigration(t *testing.T) {
	s, now := readyRecipientStore(t)
	dir := s.Dir()
	if _, err := s.DB().Exec(`DROP TABLE outbound_attempts; CREATE TABLE outbound_attempts(at_ns INTEGER NOT NULL,known INTEGER NOT NULL);DELETE FROM outbound_protection WHERE key='initial_guard'`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := s.DB().Exec(`INSERT INTO send_budget(at,known) VALUES(?,0)`, now.Add(-3*time.Hour-time.Duration(i)*time.Minute).UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetProtectionValue(context.Background(), "legacy_ledger_available", "1"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err = reopened.EnsureSendProtection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if ledgerCount(t, reopened, "outbound_attempts") != 20 {
		t.Fatal("retained historical legacy rows not imported")
	}
	d := recipientBudget(t, reopened, now.Add(90*time.Second), "new@s.whatsapp.net", false, true)
	if d.Allowed || d.Reason != "daily distinct non-contact cap reached" {
		t.Fatalf("migration lost unknown slots: %+v", d)
	}
	if err = reopened.EnsureSendProtection(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if ledgerCount(t, reopened, "outbound_attempts") != 20 {
		t.Fatal("legacy migration duplicated attempts")
	}
	var identities int
	if err = reopened.DB().QueryRow(`SELECT COUNT(*) FROM outbound_attempts WHERE recipient<>''`).Scan(&identities); err != nil || identities != 0 {
		t.Fatalf("invented historical identities: %d %v", identities, err)
	}
	// Upgrade an already initialized canonical ledger without recopying its mirror.
	if _, err = reopened.DB().Exec(`INSERT INTO send_budget VALUES(?,0)`, now.Add(-2*time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	again, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if err = again.EnsureSendProtection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if ledgerCount(t, again, "outbound_attempts") != 20 {
		t.Fatal("reopen recopied dual-write mirror")
	}
}
func TestRecipientBudgetReservationRollsBackOnLegacyFailure(t *testing.T) {
	s, now := readyRecipientStore(t)
	seedRecipientAttempt(t, s, now.Add(-25*time.Hour), "old@s.whatsapp.net", true)
	if _, err := s.DB().Exec(`CREATE TRIGGER refuse_legacy BEFORE INSERT ON send_budget BEGIN SELECT RAISE(ABORT,'synthetic ledger failure'); END`); err != nil {
		t.Fatal(err)
	}
	d, err := s.PersistentRecipientSendBudget(context.Background(), now, "a@s.whatsapp.net", true, true)
	if err == nil || d.Allowed {
		t.Fatalf("failed dual-write allowed: %+v %v", d, err)
	}
	if ledgerCount(t, s, "outbound_attempts") != 1 || ledgerCount(t, s, "send_budget") != 0 {
		t.Fatal("failed transaction left partial reservation or pruning")
	}
}
func TestRecipientBudgetParallelReservationSerializes(t *testing.T) {
	s, now := readyRecipientStore(t)
	var wg sync.WaitGroup
	results := make(chan ratelimit.Decision, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := s.PersistentRecipientSendBudget(context.Background(), now, "a@s.whatsapp.net", true, true)
			results <- d
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	allowed := 0
	for d := range results {
		if d.Allowed {
			allowed++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if allowed != 1 || ledgerCount(t, s, "outbound_attempts") != 1 || ledgerCount(t, s, "send_budget") != 1 {
		t.Fatalf("concurrent reservations allowed %d", allowed)
	}
}

func TestRecipientBudgetDistinctExpiryUsesLatestUnknownAttempt(t *testing.T) {
	s, now := readyRecipientStore(t)
	seedRecipientAttempt(t, s, now.Add(-23*time.Hour), "repeat@s.whatsapp.net", false)
	seedRecipientAttempt(t, s, now.Add(-2*time.Hour), "repeat@s.whatsapp.net", false)
	for i := 0; i < 19; i++ {
		seedRecipientAttempt(t, s, now.Add(-3*time.Hour), fmt.Sprintf("%d@s.whatsapp.net", i), false)
	}
	if d := recipientBudget(t, s, now, "new@s.whatsapp.net", false, false); d.Allowed || d.RetryAfter != 21*time.Hour {
		t.Fatalf("distinct slot expired before recipient's last attempt: %+v", d)
	}
}

func TestRecipientBudgetExistingCanonicalMigrationPreservesRows(t *testing.T) {
	s, now := readyRecipientStore(t)
	dir := s.Dir()
	if _, err := s.DB().Exec(`DROP TABLE outbound_attempts; CREATE TABLE outbound_attempts(at_ns INTEGER NOT NULL,known INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		at := now.Add(-3*time.Hour - time.Duration(i)*time.Minute).UnixNano()
		if _, err := s.DB().Exec(`INSERT INTO outbound_attempts VALUES(?,1)`, at); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB().Exec(`INSERT INTO send_budget VALUES(?,1)`, at); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	upgraded, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	if err = upgraded.EnsureSendProtection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if ledgerCount(t, upgraded, "outbound_attempts") != 20 {
		t.Fatal("schema migration lost or duplicated canonical attempts")
	}
	if d := recipientBudget(t, upgraded, now, "a@s.whatsapp.net", true, false); d.Allowed || d.Reason != "daily recipient send cap reached" {
		t.Fatalf("historical canonical attempts lost recipient protection: %+v", d)
	}
}

func TestRecipientBudgetHourlyAndSpacingFloors(t *testing.T) {
	for _, known := range []bool{true, false} {
		t.Run(fmt.Sprint(known), func(t *testing.T) {
			s, now := readyRecipientStore(t)
			cap, interval := 30, 45*time.Second
			if !known {
				cap, interval = 15, 90*time.Second
			}
			start := now
			for i := 0; i < cap; i++ {
				recipient := fmt.Sprintf("%d@s.whatsapp.net", i)
				if d := recipientBudget(t, s, now, recipient, known, true); !d.Allowed {
					t.Fatalf("reserve %d: %+v", i, d)
				}
				if d := recipientBudget(t, s, now.Add(interval-time.Nanosecond), recipient, known, true); d.Allowed || d.RetryAfter != time.Nanosecond {
					t.Fatalf("minimum spacing missing: %+v", d)
				}
				now = now.Add(interval)
			}
			reason := "hourly send cap reached"
			if !known {
				reason = "hourly non-contact send cap reached"
			}
			if d := recipientBudget(t, s, now, "fresh@s.whatsapp.net", known, true); d.Allowed || d.Reason != reason || !now.Add(d.RetryAfter).Equal(start.Add(time.Hour)) {
				t.Fatalf("hourly cap missing: %+v", d)
			}
			if d := recipientBudget(t, s, start.Add(time.Hour), "fresh@s.whatsapp.net", known, true); !d.Allowed {
				t.Fatalf("exact hourly expiry blocked: %+v", d)
			}
		})
	}
}
