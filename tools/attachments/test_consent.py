import sqlite3
import unittest
from tools.attachments.consent import set_consent

class ConsentTests(unittest.TestCase):
    def test_grant_requires_explicit_confirmation_and_is_revocable(self):
        with sqlite3.connect(':memory:') as db:
            with self.assertRaises(ValueError): set_consent(db,'123@g.us',True,'explicit approval')
            with self.assertRaises(ValueError): set_consent(db,'123@g.us',True,'',True)
            set_consent(db,'123@g.us',True,'explicit account-owner approval',True)
            self.assertEqual(db.execute('SELECT allowed FROM group_monitoring_consent').fetchone()[0],1)
            set_consent(db,'123@g.us',False,'revoked')
            self.assertEqual(db.execute('SELECT allowed FROM group_monitoring_consent').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM group_monitoring_audit').fetchone()[0],2)

    def test_revocation_blocks_inflight_message_and_poll_writes_atomically(self):
        with sqlite3.connect(':memory:') as db:
            db.executescript('CREATE TABLE messages(id TEXT,chat_jid TEXT); CREATE TABLE poll_votes(poll_chat_jid TEXT);')
            set_consent(db,'123-456@g.us',True,'explicit legacy group approval',True)
            db.execute("INSERT INTO messages VALUES('before','123-456@g.us')")
            set_consent(db,'123-456@g.us',False,'revoked')
            db.execute("INSERT INTO messages VALUES('after','123-456@g.us')")
            db.execute("INSERT INTO poll_votes VALUES('123-456@g.us')")
            self.assertEqual(db.execute('SELECT COUNT(*) FROM messages').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM poll_votes').fetchone()[0],0)
