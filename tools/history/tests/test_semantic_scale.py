"""Synthetic PostgreSQL regression for shared chunks expanding into many messages."""
import importlib.util
import os
import unittest
import uuid


class SemanticStatementTests(unittest.TestCase):
    def test_query_builder_exists(self):
        self.assertIsNotNone(importlib.util.find_spec('perf_query'), 'bounded semantic query is missing')


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'), 'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class SemanticScaleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from store import Store
        cls.store = Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_scale_' + uuid.uuid4().hex)
        cls.store.initialize()
        with cls.store.connection('60s') as db:
            db.execute('''INSERT INTO embeddings(hash,text,embedding) VALUES
                ('common','common synthetic fragment',
                 (ARRAY[1::real] || array_fill(0::real,ARRAY[1023]))::halfvec(1024)),
                ('secondary','secondary synthetic fragment',
                 (ARRAY[0::real,1::real] || array_fill(0::real,ARRAY[1022]))::halfvec(1024));
                INSERT INTO messages(source,chat_id,message_id,timestamp,sender,chat_name,text,media_type,content_hash)
                SELECT CASE WHEN n%4=0 THEN 'telegram' ELSE 'whatsapp' END,
                    CASE WHEN n%101=0 THEN 'rare' ELSE 'bulk' END,n::text,1700000000+n,
                    CASE WHEN n%5=0 THEN 'alice' ELSE 'bob' END,'Synthetic scale fixture',
                    repeat('safe synthetic body ',320),'',md5(n::text)
                FROM generate_series(1,6000) n;
                INSERT INTO message_chunks SELECT id,0,'common' FROM messages;
                INSERT INTO message_chunks SELECT id,1,'secondary' FROM messages WHERE id>5500;
                INSERT INTO message_chunks SELECT 6000,ordinal,'common' FROM generate_series(2,20) ordinal;
                CREATE TABLE hydration_calls(n integer NOT NULL);
                INSERT INTO hydration_calls VALUES(0);
                CREATE FUNCTION count_hydration(body text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
                BEGIN UPDATE hydration_calls SET n=n+1; RETURN left(body,4000); END $$;
                ANALYZE messages; ANALYZE message_chunks; ANALYZE embeddings;''')

    @classmethod
    def tearDownClass(cls):
        cls.store.drop_test_schema()

    def execute_candidate(self, *, limit=6, count_hydration=False, **filters):
        self.assertIsNotNone(importlib.util.find_spec('perf_query'), 'bounded semantic query is missing')
        from perf_query import semantic_statement
        from store import vector_literal
        where, values = self.store.filters(**filters)
        sql, params = semantic_statement(where, values, vector_literal([1.] + [0.] * 1023), limit)
        if count_hydration:
            sql = sql.replace('left(m.text,4000)', 'count_hydration(m.text)')
        with self.store.connection('8s') as db:
            db.execute("SET LOCAL hnsw.iterative_scan='strict_order'")
            db.execute('SET LOCAL hnsw.ef_search=100')
            if count_hydration:
                db.execute('UPDATE hydration_calls SET n=0')
            rows = db.execute(sql, params).fetchall()
            hydration = db.execute('SELECT n FROM hydration_calls').fetchone()['n'] if count_hydration else None
        return rows, hydration

    def test_shared_chunk_hydrates_only_final_messages(self):
        rows, hydration = self.execute_candidate(source='telegram', count_hydration=True)
        self.assertEqual(len(rows), 6)
        self.assertEqual(hydration, 6, 'full text must be fetched only after the global result limit')
        self.assertEqual([int(row['message_id']) for row in rows], [6000,5996,5992,5988,5984,5980])
        self.assertTrue(all(len(row['text']) == 4000 for row in rows))

    def test_source_chat_date_sender_filters_match_existing_results(self):
        filters = dict(source='whatsapp', chat='rare', sender='alice',
                       after=1700005000, before=1700006000)
        rows, _ = self.execute_candidate(**filters)
        original = self.store.semantic([1.] + [0.] * 1023, limit=6, **filters)
        self.assertEqual(rows, original)
        self.assertEqual([int(row['message_id']) for row in rows], [5555,5050])

    def test_overlapping_chunks_do_not_underfill_or_duplicate_results(self):
        rows, _ = self.execute_candidate(source='whatsapp', limit=20)
        original = self.store.semantic([1.] + [0.] * 1023, source='whatsapp', limit=20)
        self.assertEqual(rows, original)
        self.assertEqual(len({row['id'] for row in rows}), 20)
        self.assertTrue(all(row['matched_fragment'] == 'common synthetic fragment' for row in rows))


if __name__ == '__main__':
    unittest.main()
