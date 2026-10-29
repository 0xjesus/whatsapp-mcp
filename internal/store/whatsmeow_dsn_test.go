package store

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// A read-only lookup that is still iterating rows must not make whatsmeow's
// session writes fail. When they fail, whatsmeow skips encrypting for the
// recipient's devices and the recipient sees "Waiting for this message".
func TestWhatsmeowWriteSucceedsWhileReaderIsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "whatsapp.db")

	rw, err := sql.Open("sqlite3", WhatsmeowDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Close()
	if _, err := rw.Exec(`CREATE TABLE whatsmeow_identity_keys (their_id TEXT PRIMARY KEY, identity BLOB)`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, err := rw.Exec(`INSERT INTO whatsmeow_identity_keys VALUES (?, x'00')`, id); err != nil {
			t.Fatal(err)
		}
	}

	ro, err := sql.Open("sqlite3", WhatsmeowReadOnlyDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	rows, err := ro.Query(`SELECT their_id FROM whatsmeow_identity_keys`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() { // reader is now mid-iteration, holding its read snapshot
		t.Fatal("expected rows")
	}

	start := time.Now()
	_, err = rw.Exec(`INSERT OR REPLACE INTO whatsmeow_identity_keys VALUES ('4376739508437_1:0', x'01')`)
	if err != nil {
		t.Fatalf("session write failed while a reader was open (after %s): %v", time.Since(start).Round(time.Millisecond), err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("session write waited %s for a reader", d)
	}
}
