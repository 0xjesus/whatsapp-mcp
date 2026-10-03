"""Synthetic coverage and analytics checks against a disposable PostgreSQL database."""
import os
import unittest
import uuid
from contextlib import contextmanager


class CoverageQueryTests(unittest.TestCase):
    """The coverage SQL uses standard aggregates; exercise it on real SQLite too."""
    def test_grouped_coverage_single_scan_and_empty_source(self):
        import sqlite3
        from store import Store
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.row_factory = lambda cursor, row: dict(zip([c[0] for c in cursor.description], row))
        db.executescript('''CREATE TABLE messages(id INTEGER,source TEXT,timestamp INTEGER);
            CREATE TABLE message_chunks(message_pk INTEGER,ordinal INTEGER,hash TEXT);
            CREATE TABLE embeddings(hash TEXT,embedding TEXT);
            CREATE TABLE source_state(source TEXT,backfill_done BOOLEAN);
            CREATE TABLE worker_state(name TEXT);
            INSERT INTO messages VALUES(1,'whatsapp',1),(2,'whatsapp',2),(3,'telegram',3);
            INSERT INTO message_chunks VALUES(1,0,'shared'),(2,0,'pending'),(3,0,'shared');
            INSERT INTO embeddings VALUES('shared','vector'),('pending',NULL);
            INSERT INTO source_state VALUES('whatsapp',1),('telegram',1);''')
        queries = []
        db.set_trace_callback(queries.append)
        @contextmanager
        def connection(timeout='30s'):
            yield db
        store = Store('unused')
        store.connection = connection
        status = store.status()
        self.assertEqual(status['messages'], 3)
        self.assertEqual(status['per_source']['whatsapp']['messages'], 2)
        self.assertEqual(status['per_source']['telegram']['messages'], 1)
        self.assertFalse(status['per_source']['whatsapp']['semantic_complete'])
        self.assertTrue(status['per_source']['telegram']['semantic_complete'])
        self.assertEqual(sum('FROM messages m' in q for q in queries), 1)
        db.execute("DELETE FROM messages WHERE source='telegram'")
        db.execute("DELETE FROM source_state WHERE source='telegram'")
        empty = store.status()['per_source']['telegram']
        self.assertEqual(empty['messages'], 0)
        self.assertFalse(empty['semantic_complete'])


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'), 'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class StatusAnalyticsTests(unittest.TestCase):
    def setUp(self):
        from store import Store
        self.store = Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_metrics_' + uuid.uuid4().hex)
        self.store.initialize()

    def tearDown(self):
        self.store.drop_test_schema()

    def message(self, key, text, source='whatsapp'):
        return dict(source=source,chat_id='synthetic',message_id=str(key),timestamp=1700000000,
                    sender='synthetic',chat_name='Synthetic metrics',text=text,media_type='')

    def test_status_requires_text_chunks_and_every_repeated_chunk_ready(self):
        from store import digest
        texts=['','   ','\t\n','\u2003','single','A'*2401,'B'*2400]
        self.store.apply([self.message(i,text) for i,text in enumerate(texts)],'whatsapp',backfill_done=True)
        vector=[1.]+[0.]*1023
        self.store.save_embeddings([(digest(text),vector) for text in ['single','A'*1200,'B'*1200]])
        status=self.store.status('whatsapp')
        self.assertEqual(status['messages'],7)
        self.assertEqual(status['text_messages'],3, 'whitespace without chunks is not embeddable text')
        self.assertEqual(status['embedded_messages'],2, 'one pending final chunk keeps the entire message pending')
        self.assertFalse(status['semantic_complete'])
        self.store.save_embeddings([(digest('A'),vector)])
        self.assertEqual(self.store.status('whatsapp')['embedded_messages'],3)
        self.assertTrue(self.store.status('whatsapp')['semantic_complete'])
        self.assertEqual(self.store.status('telegram')['messages'],0)
        self.assertFalse(self.store.status('telegram')['semantic_complete'])

    def test_status_does_not_read_message_bodies(self):
        self.store.apply([self.message(1,'long synthetic body '*500)],'whatsapp')
        with self.store.connection() as db:
            db.execute('''ALTER TABLE messages RENAME TO message_storage;
                CREATE VIEW messages AS SELECT id,source,timestamp FROM message_storage;''')
        status=self.store.status('whatsapp')
        self.assertEqual((status['messages'],status['text_messages'],status['embedded_messages']),(1,1,0))

    def test_global_coverage_groups_sources_in_one_scan(self):
        from store import digest
        self.store.apply([self.message(1,'shared'), self.message(2,'pending')], 'whatsapp', backfill_done=True)
        self.store.apply([self.message(1,'shared','telegram')], 'telegram', backfill_done=True)
        self.store.save_embeddings([(digest('shared'), [1.]+[0.]*1023)])
        status = self.store.status()
        self.assertEqual((status['messages'], status['embedded_messages']), (3,2))
        self.assertEqual(status['per_source']['whatsapp']['messages'], 2)
        self.assertEqual(status['per_source']['telegram']['messages'], 1)
        self.assertFalse(status['per_source']['whatsapp']['semantic_complete'])
        self.assertTrue(status['per_source']['telegram']['semantic_complete'])
        for source in ('whatsapp', 'telegram'):
            self.assertEqual(status['per_source'][source]['states'][0]['source'], source)
        self.assertNotIn('workers', status['per_source']['whatsapp'])

    def test_empty_global_coverage_has_unknown_completion_for_missing_sources(self):
        status = self.store.status()
        self.assertEqual(status['messages'], 0)
        for source in ('whatsapp', 'telegram'):
            self.assertEqual(status['per_source'][source]['messages'], 0)
            self.assertFalse(status['per_source'][source]['semantic_complete'])

    def test_analytics_total_and_groups_share_one_snapshot_during_ingestion(self):
        from store import Store
        self.store.apply([self.message(1,'before concurrent insert')],'whatsapp')
        writer=Store(self.store.dsn,self.store.schema)
        original_connection=self.store.connection
        injected=False

        class InjectInsert:
            def __init__(self,db):
                self.db=db

            def execute(proxy,sql,params=None):
                nonlocal injected
                cursor=proxy.db.execute(sql,params)
                if not injected and 'FROM messages' in sql:
                    injected=True
                    writer.apply([self.message(2,'concurrent insert','telegram')],'telegram')
                return cursor

        @contextmanager
        def connection(timeout='15s'):
            with original_connection(timeout) as db:
                yield InjectInsert(db)

        self.store.connection=connection
        result=self.store.analytics(group_by='source')
        self.assertTrue(injected, 'concurrent insertion seam was not exercised')
        self.assertEqual(result['total'],sum(group['messages'] for group in result['groups']))
        self.assertEqual(result['total'],1, 'the aggregate snapshot predates the injected commit')

    def test_analytics_empty_results_and_limited_groups_keep_exact_total(self):
        self.assertEqual(self.store.analytics(source='telegram')['groups'],[])
        self.assertEqual(self.store.analytics(source='telegram')['total'],0)
        self.store.apply([self.message(1,'one'),self.message(2,'two','telegram')],'whatsapp')
        result=self.store.analytics(group_by='source',limit=1)
        self.assertEqual(result['total'],2)
        self.assertEqual(len(result['groups']),1)
        self.assertEqual(result['groups'][0]['messages'],1)


if __name__ == '__main__':
    unittest.main()


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'), 'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class SwitchModelTests(unittest.TestCase):
    def setUp(self):
        from store import Store
        self.store = Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_switch_' + uuid.uuid4().hex)
        self.store.initialize()

    def tearDown(self):
        self.store.drop_test_schema()

    def test_switch_model_resets_vectors_and_rebinds(self):
        from store import digest
        self.store.bind_model('old-identity')
        self.store.apply([dict(source='whatsapp',chat_id='c',message_id='1',timestamp=1700000000,sender='s',chat_name='n',text='hola',media_type='')],'whatsapp',backfill_done=True)
        self.store.save_embeddings([(digest('hola'), [1.]+[0.]*1023)])
        self.assertEqual(self.store.status('whatsapp')['embedded_messages'], 1)
        self.store.switch_model('openai:text-embedding-3-large:1024:chunks-utf8-1200:v1', 'new-identity')
        self.assertEqual(self.store.status('whatsapp')['embedded_messages'], 0)
        self.assertEqual(len(self.store.pending(10)), 1)
        self.store.check_model('new-identity')
        with self.assertRaises(RuntimeError):
            self.store.check_model('old-identity')


    def test_save_embeddings_by_ctid_and_bulk_mode(self):
        from store import digest
        self.store.apply([dict(source='whatsapp',chat_id='c',message_id='7',timestamp=1700000000,sender='s',chat_name='n',text='ctid path',media_type='')],'whatsapp')
        rows = self.store.pending(10)
        self.assertTrue(rows[0]['ctid'].startswith('('))
        self.store.save_embeddings([(rows[0]['hash'], [1.]+[0.]*1023)], ctids=[rows[0]['ctid']])
        self.assertEqual(self.store.pending(10), [])
        self.store.set_bulk_mode(True); self.store.set_bulk_mode(False)
