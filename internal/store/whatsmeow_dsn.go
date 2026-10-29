package store

// WhatsmeowDSN is the read-write DSN for whatsapp.db, the whatsmeow session
// store (Signal sessions, identities, sender keys, prekeys).
//
// WAL lets readers and the writer work at the same time; in the default
// rollback-journal mode any open reader blocks every session write, the write
// fails with "database is locked", and whatsmeow then silently skips the
// recipient's devices, so they only see "Waiting for this message".
// _txlock=immediate takes the write lock at BEGIN, so two transactions never
// deadlock upgrading from read to write (that case skips the busy timeout).
func WhatsmeowDSN(path string) string {
	return "file:" + path + "?_foreign_keys=on&_journal_mode=WAL&_busy_timeout=15000&_txlock=immediate"
}

// WhatsmeowReadOnlyDSN is the DSN for this daemon's read-only lookups
// (contacts, LID map) against whatsapp.db.
func WhatsmeowReadOnlyDSN(path string) string {
	return "file:" + path + "?mode=ro&_busy_timeout=5000"
}
