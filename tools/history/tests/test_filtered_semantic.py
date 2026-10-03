"""Synthetic PostgreSQL regressions for selective semantic retrieval."""
import os
import unittest
import uuid


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'), 'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class FilteredSemanticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from store import Store
        cls.store = Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_filtered_' + uuid.uuid4().hex)
        cls.store.initialize()
        cls.addClassCleanup(cls.store.drop_test_schema)
        with cls.store.connection('60s') as db:
            db.execute('SET LOCAL max_parallel_workers_per_gather=0')
            db.execute('''INSERT INTO embeddings(hash,text,embedding)
                SELECT 'unrelated-'||n,'unrelated synthetic fragment',
                    (ARRAY[0::real,1::real,n::real/5000] || array_fill(0::real,ARRAY[1021]))::halfvec(1024)
                FROM generate_series(1,4000) n;
                INSERT INTO embeddings(hash,text,embedding)
                SELECT 'shared-'||n,'shared synthetic fragment '||n,
                    (ARRAY[1::real,n::real/100] || array_fill(0::real,ARRAY[1022]))::halfvec(1024)
                FROM generate_series(1,8) n;
                INSERT INTO embeddings(hash,text)
                SELECT 'pending-'||n,'pending synthetic fragment' FROM generate_series(1,512) n;
                INSERT INTO messages(source,chat_id,message_id,timestamp,sender,chat_name,text,media_type,content_hash)
                SELECT 'whatsapp','bulk',n::text,1600000000+n,'bob','Synthetic bulk',
                    'unrelated synthetic message','',md5(n::text)
                FROM generate_series(1,4000) n;
                INSERT INTO message_chunks SELECT id,0,'unrelated-'||message_id FROM messages;
                INSERT INTO message_chunks
                SELECT m.id,n,'shared-'||n FROM messages m CROSS JOIN generate_series(1,8) n;
                INSERT INTO messages(source,chat_id,message_id,timestamp,sender,chat_name,text,media_type,content_hash)
                SELECT 'whatsapp','target',n::text,1700000000+n,
                    CASE WHEN n%2=0 THEN 'alice' ELSE 'bob' END,'Synthetic target',
                    repeat('synthetic target body ',250),'',md5('target-'||n)
                FROM generate_series(1,512) n;
                INSERT INTO message_chunks
                SELECT id,0,'pending-'||message_id FROM messages WHERE chat_id='target';
                INSERT INTO message_chunks
                SELECT id,1,'shared-'||message_id FROM messages
                WHERE chat_id='target' AND message_id::integer<=8;
                INSERT INTO message_chunks
                SELECT id,n,'shared-1' FROM messages CROSS JOIN generate_series(2,20) n
                WHERE chat_id='target' AND message_id='1';
                ANALYZE embeddings; ANALYZE messages; ANALYZE message_chunks;''')

    def execute(self, *, explain=False, limit=8, **filters):
        from perf_query import semantic_statement
        from store import vector_literal
        where, values = self.store.filters(**filters)
        sql, params = semantic_statement(where, values, vector_literal([1.] + [0.] * 1023), limit)
        with self.store.connection('8s') as db:
            db.execute('SET LOCAL max_parallel_workers_per_gather=0')
            db.execute("SET LOCAL hnsw.iterative_scan='strict_order'")
            db.execute('SET LOCAL hnsw.ef_search=100')
            if explain:
                return db.execute('EXPLAIN (ANALYZE, FORMAT JSON) ' + sql, params).fetchone()['QUERY PLAN'][0]
            return db.execute(sql, params).fetchall()

    def test_selective_chat_does_not_visit_messages_sharing_chunks_in_other_chats(self):
        plan = self.execute(source='whatsapp', chat='target', explain=True)
        self.assertEqual(plan['Plan']['Actual Rows'], 8, 'all eight embedded matches must be reachable')

        def message_visits(node):
            own = 0
            if node.get('Relation Name') == 'messages':
                own = (node['Actual Rows'] + node.get('Rows Removed by Filter', 0)) * node['Actual Loops']
            return own + sum(message_visits(child) for child in node.get('Plans', []))

        self.assertLess(message_visits(plan['Plan']), 2500,
                        'selective retrieval must not revisit unrelated messages for each shared fragment')

    def test_filters_ranking_repeated_chunks_and_context_ids(self):
        rows = self.execute(source='whatsapp', chat='target', after=1700000002,
                            before=1700000008, sender='alice')
        self.assertEqual([row['message_id'] for row in rows], ['2', '4', '6', '8'])
        self.assertEqual(len({row['id'] for row in rows}), 4)
        for row in rows:
            self.assertEqual(row['matched_fragment'], 'shared synthetic fragment ' + row['message_id'])
            self.assertEqual(row['id'], 4000 + int(row['message_id']))
            self.assertEqual(len(row['text']), 4000)
        self.assertEqual([row['message_id'] for row in self.execute(source='whatsapp', chat='target')],
                         [str(n) for n in range(1, 9)])

    def test_empty_source_and_unembedded_date_range_remain_empty(self):
        self.assertEqual(self.execute(source='telegram', chat='target'), [])
        self.assertEqual(self.execute(source='whatsapp', chat='target', after=1700000020), [])

    def test_date_filter_without_chat_excludes_nearer_old_messages(self):
        rows = self.execute(after=1700000004, before=1700000008, limit=3)
        self.assertEqual([row['message_id'] for row in rows], ['4', '5', '6'])
        self.assertTrue(all(row['chat_id'] == 'target' for row in rows))

    def test_large_filtered_set_falls_back_without_truncating_matches(self):
        from perf_query import FILTERED_CHUNK_LIMIT
        from store import Store
        store = Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_filtered_overflow_' + uuid.uuid4().hex)
        store.initialize()
        self.addCleanup(store.drop_test_schema)
        with store.connection('60s') as db:
            db.execute('SET LOCAL max_parallel_workers_per_gather=0')
            db.execute('''INSERT INTO embeddings(hash,text,embedding) VALUES
                ('common','repeated synthetic fragment',
                 (ARRAY[1::real,1::real] || array_fill(0::real,ARRAY[1022]))::halfvec(1024)),
                ('best','best synthetic fragment',
                 (ARRAY[1::real] || array_fill(0::real,ARRAY[1023]))::halfvec(1024));
                INSERT INTO messages(source,chat_id,message_id,timestamp,sender,chat_name,text,media_type,content_hash)
                SELECT CASE WHEN n=3 THEN 'telegram' ELSE 'whatsapp' END,'large',n::text,
                    1700000000+n,'alice','Synthetic overflow','synthetic message','',md5(n::text)
                FROM generate_series(1,3) n;''')
            db.execute("INSERT INTO message_chunks SELECT 1,n,'common' FROM generate_series(0,%s) n",
                       [FILTERED_CHUNK_LIMIT])
            db.execute('''INSERT INTO message_chunks VALUES(2,0,'best'),(3,0,'best');
                ANALYZE embeddings; ANALYZE messages; ANALYZE message_chunks;''')
        rows = store.semantic([1.] + [0.] * 1023, source='whatsapp', chat='large', limit=2)
        self.assertEqual([row['message_id'] for row in rows], ['2', '1'])
        self.assertEqual([row['matched_fragment'] for row in rows],
                         ['best synthetic fragment', 'repeated synthetic fragment'])


if __name__ == '__main__':
    unittest.main()
