"""SQLite-only recovery transitions; no messaging accounts or network access."""
import sqlite3
import unittest

import test_sources


class ConsentTransitionTests(unittest.TestCase):
    setUp = test_sources.SourceTests.setUp
    source = test_sources.SourceTests.source
    insert = test_sources.SourceTests.insert
    write = test_sources.SourceTests.write

    def fixture(self, kind):
        source = self.source(kind)
        chat = '123@g.us' if kind == 'whatsapp' else -1
        for ident in range(1, 4):
            self.insert(source, chat=chat, mid=str(ident) if kind == 'whatsapp' else ident)
        source.install_capture()
        with sqlite3.connect(source.path) as db:
            db.execute("INSERT INTO group_monitoring_consent VALUES(?,1,'manual approval',datetime('now'))", (chat,))
            db.execute("UPDATE memory_group_rescan SET cursor='2'")
        return source, chat

    def test_evidence_changes_preserve_active_cursor_and_finished_recovery(self):
        for kind in ('whatsapp', 'telegram'):
            with self.subTest(kind=kind):
                source, chat = self.fixture(kind)
                with sqlite3.connect(source.path) as db:
                    before = db.execute('SELECT * FROM memory_changes').fetchall()
                    db.execute("UPDATE group_monitoring_consent SET evidence='dashboard:manual-toggle',updated_at=datetime('now')")
                    self.assertEqual(db.execute('SELECT cursor FROM memory_group_rescan').fetchone(), ('2',))
                    self.assertEqual(db.execute('SELECT * FROM memory_changes').fetchall(), before)
                    db.execute('DELETE FROM memory_group_rescan')
                    db.execute("UPDATE group_monitoring_consent SET evidence='dashboard:enable-all-explicit-user-request'")
                    self.assertEqual(db.execute('SELECT * FROM memory_group_rescan').fetchall(), [])

    def test_fresh_automatic_to_manual_keeps_active_cursor(self):
        for kind in ('whatsapp', 'telegram'):
            with self.subTest(kind=kind):
                source, chat = self.fixture(kind)
                with sqlite3.connect(source.path) as db:
                    db.execute("UPDATE group_monitoring_consent SET evidence='auto:max-members:10',updated_at=datetime('now')")
                    db.execute("UPDATE memory_group_rescan SET cursor='2'")
                    db.execute("UPDATE group_monitoring_consent SET evidence='dashboard:manual-toggle',updated_at=datetime('now')")
                    self.assertEqual(db.execute('SELECT cursor FROM memory_group_rescan').fetchone(), ('2',))

    def test_reenabled_or_expired_automatic_permissions_still_restart_recovery(self):
        for kind in ('whatsapp', 'telegram'):
            with self.subTest(kind=kind):
                source, chat = self.fixture(kind)
                with sqlite3.connect(source.path) as db:
                    for previous in ((0, 'manual off', '2000-01-01'),
                                     (1, 'auto:max-members:10', '2000-01-01'),
                                     (1, 'auto:max-members:10', 'invalid')):
                        db.execute('UPDATE group_monitoring_consent SET allowed=?,evidence=?,updated_at=?', previous)
                        db.execute("UPDATE memory_group_rescan SET cursor='2'")
                        db.execute("UPDATE group_monitoring_consent SET allowed=1,evidence='manual approval',updated_at=datetime('now')")
                        self.assertEqual(db.execute('SELECT cursor FROM memory_group_rescan').fetchone(), (None,))

    def test_reinstall_preserves_active_queue_and_explicit_recovery_still_replays(self):
        for kind in ('whatsapp', 'telegram'):
            with self.subTest(kind=kind):
                source, chat = self.fixture(kind)
                source.install_capture()
                with sqlite3.connect(source.path) as db:
                    self.assertEqual(db.execute('SELECT cursor FROM memory_group_rescan').fetchone(), ('2',))
                    # Dashboard Recover deliberately writes its own initial cursor.
                    db.execute("INSERT INTO memory_group_rescan(chat_id,cursor) VALUES(?,'') ON CONFLICT(chat_id) DO UPDATE SET cursor=''", (str(chat),))
                changes = source.changes(0, 100)
                self.assertEqual({row['message_id'] for row in changes}, {'1', '2', '3'})


if __name__ == '__main__':
    unittest.main()
