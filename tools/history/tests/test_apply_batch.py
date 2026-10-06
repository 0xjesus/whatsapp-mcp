"""Synthetic PostgreSQL tests for bounded-round-trip ingestion and atomic replay."""
from contextlib import contextmanager
import hashlib
import json
import os
import time
import unittest
import uuid

from store import Store, chunks, digest


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'), 'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class BatchedApplyTests(unittest.TestCase):
    def setUp(self):
        self.store = Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_apply_' + uuid.uuid4().hex)
        self.store.initialize()

    def tearDown(self):
        self.store.drop_test_schema()

    def row(self, key='1', text='synthetic text', **fields):
        return dict(dict(source='whatsapp', chat_id='synthetic-chat', message_id=key,
                         timestamp=1750000000, sender='synthetic', chat_name='Synthetic', text=text, media_type=''), **fields)

    def read(self, sql, values=()):
        with self.store.connection() as db:
            return db.execute(sql, values).fetchall()

    @contextmanager
    def statements(self):
        execute = self.store.connection
        calls = []
        @contextmanager
        def counted(timeout='15s'):
            with execute(timeout) as connection:
                class Counter:
                    def execute(self, sql, params=()):
                        calls.append(sql)
                        return connection.execute(sql, params)
                yield Counter()
        self.store.connection = counted
        try:
            yield calls
        finally:
            self.store.connection = execute

    def test_five_hundred_new_messages_use_at_most_five_data_statements(self):
        records = [self.row(str(i), 'synthetic batch body '+str(i)) for i in range(500)]
        with self.statements() as calls:
            self.store.apply(records, 'whatsapp', cursor=500, change_seq=500, change_token='batch-token')
        self.assertLessEqual(len(calls), 5, 'remote ingestion must not perform SQL round trips per message')
        self.assertEqual(self.read('SELECT count(*) AS n FROM messages')[0]['n'], 500)
        self.assertEqual(self.read('SELECT count(*) AS n FROM message_chunks')[0]['n'], 500)
        self.assertEqual(self.store.state('whatsapp')['change_token'], 'batch-token')

    def test_identical_replay_does_not_rewrite_message_chunks_or_ready_vectors(self):
        records = [self.row(), self.row('2', 'second synthetic body')]
        self.store.apply(records, 'whatsapp', cursor=2)
        self.store.save_embeddings([(digest(records[0]['text']), [1.] + [0.] * 1023)])
        before_messages = self.read('SELECT id,ctid::text AS location,content_hash FROM messages ORDER BY id')
        before_chunks = self.read('SELECT *,ctid::text AS location FROM message_chunks ORDER BY message_pk,ordinal')
        before_embeddings = self.read('SELECT hash,ctid::text AS location,priority,embedded_at FROM embeddings ORDER BY hash')
        with self.statements() as calls:
            self.store.apply(iter(records), 'whatsapp', cursor=2, change_seq=10, change_token='replayed')
        self.assertEqual(self.read('SELECT id,ctid::text AS location,content_hash FROM messages ORDER BY id'), before_messages)
        self.assertEqual(self.read('SELECT *,ctid::text AS location FROM message_chunks ORDER BY message_pk,ordinal'), before_chunks)
        self.assertEqual(self.read('SELECT hash,ctid::text AS location,priority,embedded_at FROM embeddings ORDER BY hash'), before_embeddings)
        self.assertLessEqual(len(calls), 2)
        self.assertEqual(self.store.state('whatsapp')['change_seq'], 10)

    def test_duplicate_keys_keep_last_record_and_delete_precedes_reinsert(self):
        self.store.apply([self.row(), self.row('gone'), self.row(source='telegram')], 'whatsapp')
        records = [self.row(text='first edit'), self.row(source='telegram', text='telegram edit'), self.row(text='final edit')]
        self.store.apply(records, 'whatsapp', deleted=[('synthetic-chat','1'),('synthetic-chat','gone')], change_seq=4)
        actual = self.read('SELECT source,message_id,text FROM messages ORDER BY source')
        self.assertEqual(actual, [dict(source='telegram',message_id='1',text='telegram edit'),dict(source='whatsapp',message_id='1',text='final edit')])
        self.assertEqual(self.read('SELECT count(*) AS n FROM message_chunks')[0]['n'], 2)

    def test_mixed_page_replaces_only_changed_chunks_and_bulk_deletes(self):
        records = [self.row(str(i), 'original body '+str(i)) for i in range(500)]
        self.store.apply(records, 'whatsapp')
        before = self.read("SELECT c.*,c.ctid::text AS location FROM message_chunks c JOIN messages m ON m.id=c.message_pk WHERE m.message_id='0'")
        self.store.apply([records[0],self.row('1','edited body')], 'whatsapp')
        self.assertEqual(self.read("SELECT c.*,c.ctid::text AS location FROM message_chunks c JOIN messages m ON m.id=c.message_pk WHERE m.message_id='0'"), before)
        self.assertEqual(self.read("SELECT e.text FROM message_chunks c JOIN messages m ON m.id=c.message_pk JOIN embeddings e ON e.hash=c.hash WHERE m.message_id='1'"), [dict(text='edited body')])
        with self.statements() as calls:
            self.store.apply([], 'whatsapp', deleted=((r['chat_id'],r['message_id']) for r in records), change_seq=501)
        self.assertLessEqual(len(calls), 2)
        self.assertEqual(self.read('SELECT count(*) AS n FROM messages')[0]['n'], 0)
        self.assertEqual(self.read('SELECT count(*) AS n FROM message_chunks')[0]['n'], 0)
        self.assertEqual(self.store.state('whatsapp')['change_seq'], 501)

    def test_unicode_whitespace_and_content_fingerprint_are_preserved(self):
        text = '😃 reunión ñ\n' * 600
        row = self.row(text=text)
        self.store.apply([row, self.row('blank', ' \n\t'), self.row('empty', '')], 'whatsapp')
        expected = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
        self.assertEqual(self.read("SELECT content_hash FROM messages WHERE message_id='1'")[0]['content_hash'], expected)
        actual = self.read("SELECT e.text FROM message_chunks c JOIN embeddings e ON e.hash=c.hash JOIN messages m ON m.id=c.message_pk WHERE m.message_id='1' ORDER BY c.ordinal")
        self.assertEqual(''.join(r['text'] for r in actual), text)
        self.assertEqual([r['text'] for r in actual], chunks(text))
        self.assertEqual(self.read("SELECT count(*) AS n FROM message_chunks c JOIN messages m ON m.id=c.message_pk WHERE m.message_id IN ('blank','empty')")[0]['n'], 0)
        self.store.apply([self.row(text=' \t')], 'whatsapp')
        self.assertEqual(self.read('SELECT count(*) AS n FROM message_chunks')[0]['n'], 0)

    def test_shared_hash_uses_highest_priority_but_ready_vector_is_not_rewritten(self):
        now = int(time.time())
        rows = [self.row('old', 'shared text'), self.row('recent', 'shared text', timestamp=now), self.row('third', 'shared text')]
        self.store.apply(rows, 'whatsapp', change_seq=1)
        self.assertEqual(self.read('SELECT hash,priority FROM embeddings'), [dict(hash=digest('shared text'),priority=3)])
        self.assertEqual(self.read('SELECT count(*) AS n FROM message_chunks')[0]['n'], 3)
        self.store.apply([self.row('ready', 'cached vector')], 'whatsapp')
        self.store.save_embeddings([(digest('cached vector'), [1.] + [0.] * 1023)])
        ready = self.read('SELECT ctid::text AS location,priority,embedded_at FROM embeddings WHERE hash=%s', (digest('cached vector'),))
        self.store.apply([self.row('ready-new', 'cached vector', timestamp=now)], 'whatsapp', change_seq=2)
        self.assertEqual(self.read('SELECT ctid::text AS location,priority,embedded_at FROM embeddings WHERE hash=%s', (digest('cached vector'),)), ready)

    def test_checkpoint_failure_rolls_back_upserts_deletes_chunks_and_embeddings(self):
        self.store.apply([self.row(), self.row('delete')], 'whatsapp', cursor=2, change_seq=2, change_token='before')
        before = {table:self.read('SELECT * FROM '+table+' ORDER BY 1') for table in ('messages','message_chunks','embeddings','source_state')}
        with self.store.connection() as db:
            db.execute("""CREATE FUNCTION reject_synthetic_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN IF NEW.change_seq=999999 THEN RAISE EXCEPTION 'synthetic checkpoint failure'; END IF; RETURN NEW; END $$""")
            db.execute('CREATE TRIGGER reject_checkpoint BEFORE INSERT OR UPDATE ON source_state FOR EACH ROW EXECUTE FUNCTION reject_synthetic_checkpoint()')
        with self.assertRaises(Exception):
            self.store.apply([self.row(text='edited'),self.row('new','new synthetic body')], 'whatsapp', deleted=[('synthetic-chat','delete')], cursor=3, change_seq=999999)
        for table, rows in before.items():
            self.assertEqual(self.read('SELECT * FROM '+table+' ORDER BY 1'), rows, table)

    def test_empty_batch_checkpoint_preserves_unspecified_fields_and_updates_false(self):
        self.store.apply([], 'whatsapp', cursor=5, change_seq=8, change_token='token', backfill_done=True, source_stats={'count':5})
        self.store.apply([], 'whatsapp', backfill_done=False, source_stats={})
        state = self.store.state('whatsapp')
        self.assertEqual((state['cursor'],state['change_seq'],state['change_token'],state['backfill_done'],state['source_stats']), (5,8,'token',False,{}))

    def test_batching_does_not_grant_group_consent(self):
        self.store.apply([self.row(chat_id='unapproved@g.us'),self.row(source='telegram',chat_id='-42')], 'whatsapp')
        self.assertEqual(self.store.pending(10), [])
        self.assertEqual(self.read('SELECT * FROM group_monitoring_consent'), [])
        self.assertEqual(self.read('SELECT * FROM telegram_monitoring_allowed'), [])


if __name__ == '__main__':
    unittest.main()
