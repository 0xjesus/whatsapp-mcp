"""Read-only page resolution on synthetic WhatsApp and Telegram caches."""
from contextlib import contextmanager
import os
import sqlite3
import unittest
import uuid
from unittest.mock import patch

import test_sources as source_tests

module = source_tests.module


class SourceBatchTests(unittest.TestCase):
    setUp = source_tests.SourceTests.setUp
    source = source_tests.SourceTests.source
    write = source_tests.SourceTests.write
    insert = source_tests.SourceTests.insert

    def many(self, source, keys):
        self.assertTrue(callable(getattr(source, 'get_many', None)), 'batched source reader is missing')
        return source.get_many(keys)

    def test_five_hundred_keys_use_one_read_only_connection_and_one_projection(self):
        for kind in ('whatsapp', 'telegram'):
            with self.subTest(kind=kind):
                source = self.source(kind)
                chat = 'a' if kind == 'whatsapp' else '1'
                with sqlite3.connect(source.path) as c:
                    if kind == 'whatsapp':
                        c.executemany("INSERT INTO messages(id,chat_jid,sender,content,timestamp,media_type) VALUES(?,'a','s',?,'2026-01-01','')",
                                      [(str(i), 'synthetic '+str(i)) for i in range(500)])
                    else:
                        c.executemany("INSERT INTO messages(id,chat_id,sender_id,text,date,media_type) VALUES(?,1,1,?,'2026-01-01','')",
                                      [(i, 'synthetic '+str(i)) for i in range(500)])
                keys = [(chat, str(i)) for i in reversed(range(500))]
                expected = [source.get(*key) for key in keys]
                original_db = source._db
                opens, plans = [], []
                @contextmanager
                def counted(write=False):
                    opens.append(write)
                    with original_db(write=write) as c:
                        self.assertEqual(c.execute('PRAGMA query_only').fetchone()[0], 1)
                        with self.assertRaises(sqlite3.OperationalError):
                            c.execute('CREATE TABLE forbidden_write(id)')
                        plan = c.execute('EXPLAIN QUERY PLAN '+source._select(c)+
                            f" WHERE m.{source.fields['chat']}=? AND m.id=?", keys[0]).fetchall()
                        plans.extend(row['detail'] for row in plan)
                        yield c
                with patch.object(source, '_db', counted), patch.object(source, '_select', wraps=source._select) as projection:
                    actual = self.many(source, iter(keys))
                self.assertEqual(actual, expected)
                self.assertEqual(opens, [False])
                # One extra projection is the EXPLAIN performed by this test.
                self.assertEqual(projection.call_count, 2)
                self.assertTrue(any('SEARCH m USING INDEX' in plan for plan in plans), plans)
                self.assertFalse(any(plan.startswith('SCAN m') for plan in plans), plans)

    def test_order_duplicates_missing_and_literal_keys_match_get(self):
        for kind in ('whatsapp', 'telegram'):
            with self.subTest(kind=kind):
                source = self.source(kind)
                self.insert(source)
                self.insert(source, chat='b' if kind == 'whatsapp' else 2)
                keys = [('b','same'),('a','same'),('missing',"' OR 1=1 --"),('a','same')] if kind == 'whatsapp' else [('2','10'),('01','010'),('-3','10'),('1','10')]
                self.assertEqual(self.many(source, keys), [source.get(*key) for key in keys])

    def test_media_tombstones_and_revoked_groups_match_get(self):
        for kind in ('whatsapp', 'telegram'):
            with self.subTest(kind=kind):
                source = self.source(kind)
                group = '123@g.us' if kind == 'whatsapp' else -1
                chat, mid = ('a','same') if kind == 'whatsapp' else ('1','10')
                self.insert(source, text='', media='audio' if kind == 'whatsapp' else 'voice')
                self.insert(source, chat=group)
                self.insert(source, mid='gone' if kind == 'whatsapp' else 11)
                if kind == 'whatsapp':
                    self.write(source, 'ALTER TABLE messages ADD COLUMN file_sha256 BLOB')
                    self.write(source, 'CREATE TABLE attachment_analysis(message_id TEXT,chat_jid TEXT,text TEXT,status TEXT,media_hash TEXT,PRIMARY KEY(chat_jid,message_id,media_hash))')
                    self.write(source, "INSERT INTO transcripts(chat_jid,message_id,text,status) VALUES('a','same','voz ñ','done')")
                    self.write(source, "INSERT INTO attachment_analysis VALUES('same','a','attachment text','done','')")
                    self.write(source, "DELETE FROM messages WHERE id='gone'")
                else:
                    self.write(source, 'ALTER TABLE messages ADD COLUMN media_hash TEXT DEFAULT ""')
                    self.write(source, 'CREATE TABLE attachment_analysis(message_id INTEGER,chat_id INTEGER,text TEXT,status TEXT,media_hash TEXT,PRIMARY KEY(chat_id,message_id,media_hash))')
                    self.write(source, 'CREATE TABLE message_reactions(chat_id INTEGER,message_id INTEGER,text TEXT,PRIMARY KEY(chat_id,message_id))')
                    self.write(source, "INSERT INTO transcripts(chat_id,id,text,status) VALUES(1,10,'voz ñ','done')")
                    self.write(source, "UPDATE messages SET media_hash='synthetic' WHERE chat_id=1 AND id=10")
                    self.write(source, "INSERT INTO attachment_analysis VALUES(10,1,'attachment text','done','synthetic')")
                    self.write(source, "INSERT INTO message_reactions VALUES(1,10,'👍')")
                    self.write(source, 'UPDATE messages SET deleted=1 WHERE id=11')
                source.install_capture()
                self.write(source, "INSERT INTO group_monitoring_consent VALUES(?,1,'explicit test','now')", (group,))
                keys = [(chat,mid),(str(group),mid),(chat,'gone' if kind == 'whatsapp' else '11')]
                self.assertEqual(self.many(source, keys), [source.get(*key) for key in keys])
                self.assertIn('[Nota de voz] voz ñ', self.many(source, keys)[0]['text'])
                self.assertIn('attachment text', self.many(source, keys)[0]['text'])
                self.write(source, 'UPDATE group_monitoring_consent SET allowed=0')
                actual = self.many(source, keys)
                self.assertEqual(actual, [source.get(*key) for key in keys])
                self.assertIsNone(actual[1])
                self.assertIsNone(actual[2])

    def test_revocation_between_lookups_is_not_cached(self):
        source = self.source('whatsapp')
        self.insert(source, chat='123@g.us', mid='one')
        self.insert(source, chat='123@g.us', mid='two')
        source.install_capture()
        self.write(source, "INSERT INTO group_monitoring_consent VALUES('123@g.us',1,'explicit test','now')")
        normalize = source._normalize
        def revoke(row):
            result = normalize(row)
            self.write(source, 'UPDATE group_monitoring_consent SET allowed=0')
            return result
        with patch.object(source, '_normalize', side_effect=revoke):
            rows = self.many(source, [('123@g.us','one'),('123@g.us','two')])
        self.assertIsNotNone(rows[0])
        self.assertIsNone(rows[1])

    def test_telegram_channel_and_expired_automatic_consent_match_get(self):
        source = self.source('telegram')
        self.write(source, 'ALTER TABLE chats ADD COLUMN type TEXT')
        self.write(source, "INSERT INTO chats(id,title,type) VALUES(-1,'Group','supergroup'),(-2,'Broadcast','channel')")
        for chat in (1, -1, -2):
            self.insert(source, chat=chat)
        source.install_capture()
        self.write(source, "INSERT INTO group_monitoring_consent VALUES(-1,1,'auto:max-members:10','2000-01-01T00:00:00Z')")
        keys = [('1','10'),('-1','10'),('-2','10')]
        rows = self.many(source, keys)
        self.assertEqual(rows, [source.get(*key) for key in keys])
        self.assertIsNone(rows[1])
        self.assertIsNotNone(rows[2])

    def test_page_has_one_global_deadline_and_returns_no_partial_result(self):
        source = self.source('whatsapp')
        self.insert(source)
        clock = [100.0]
        normalize = source._normalize
        def slow(row):
            clock[0] += 3
            return normalize(row)
        with patch.object(module.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(source, '_normalize', side_effect=slow):
            with self.assertRaisesRegex(sqlite3.OperationalError, 'interrupted'):
                self.many(source, [('a','same')]*3)

    def test_empty_page_needs_no_database_and_oversized_page_is_rejected(self):
        source = self.source('whatsapp')
        with patch.object(source, '_db', side_effect=AssertionError('unexpected connection')):
            self.assertEqual(self.many(source, []), [])
            with self.assertRaises(ValueError):
                self.many(source, [('a','same')]*(module.MAX_PAGE+1))


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'), 'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class SourceBatchCheckpointTests(unittest.TestCase):
    setUp = source_tests.SourceTests.setUp
    source = source_tests.SourceTests.source
    write = source_tests.SourceTests.write
    insert = source_tests.SourceTests.insert

    def test_source_normalization_error_leaves_real_checkpoint_and_messages_unchanged(self):
        from store import Store
        from worker import ingest_step
        store = Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_source_batch_' + uuid.uuid4().hex)
        store.initialize()
        self.addCleanup(store.drop_test_schema)
        store.apply([], 'telegram', backfill_done=True, change_seq=0, change_token='before')
        source = self.source('telegram')
        source.install_capture()
        self.insert(source, mid=1)
        self.insert(source, mid=2)
        self.write(source, "UPDATE messages SET date='not a timestamp' WHERE id=2")
        before = store.state('telegram')
        with self.assertRaises(ValueError):
            ingest_step(store, source)
        self.assertEqual(store.state('telegram'), before)
        with store.connection() as db:
            self.assertEqual(db.execute('SELECT count(*) AS n FROM messages').fetchone()['n'], 0)
