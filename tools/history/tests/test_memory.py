"""Synthetic integration tests. MEMORY_TEST_DSN must point at a disposable test database."""
import importlib.util
import os
import unittest
import uuid


class ChunkTests(unittest.TestCase):
    def test_module_is_available(self):
        self.assertIsNotNone(importlib.util.find_spec('store'), 'history store is not implemented')

    def test_all_unicode_content_survives_chunking(self):
        from store import chunks
        text = 'Hola 🌎, presupuesto y reunión.\n' * 300
        result = chunks(text)
        self.assertEqual(''.join(result), text)
        self.assertTrue(all(len(t.encode('utf-8')) <= 1200 for t in result))


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'), 'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        from store import Store
        self.store = Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_' + uuid.uuid4().hex)
        self.store.initialize()

    def tearDown(self):
        self.store.drop_test_schema()

    def message(self, key='1', text='Presupuesto para el proyecto', source='whatsapp'):
        return dict(source=source, chat_id='synthetic-chat', message_id=key,
                    timestamp=1750000000, sender='test', chat_name='Pruebas',
                    text=text, media_type='')

    def test_denied_group_embeddings_are_not_scheduled_and_revoke_cleans_old_versions(self):
        group = dict(self.message(text='original private version'),chat_id='123@g.us')
        self.store.apply([group], 'whatsapp')
        self.assertEqual(self.store.pending(10), [])
        self.store.sync_group_consent(['123@g.us'])
        self.assertEqual(len(self.store.pending(10)), 1)
        self.store.apply([dict(group,text='edited private version')], 'whatsapp')
        self.store.sync_group_consent([])
        self.assertEqual(self.store.pending(10), [])
        with self.store.connection() as db:
            self.assertEqual(db.execute('SELECT count(*) AS n FROM embeddings').fetchone()['n'],0)
            self.assertEqual(db.execute('SELECT count(*) AS n FROM messages').fetchone()['n'],0)

    def test_group_cleanup_advances_across_bounded_message_batches(self):
        self.store.sync_group_consent(['123@g.us'])
        records=[dict(self.message(str(i),text='private group '+str(i)),chat_id='123@g.us') for i in range(450)]
        self.store.apply(records,'whatsapp')
        self.store.sync_group_consent([])
        self.assertEqual(self.store.pending(10),[])
        self.assertEqual(self.store.lexical('private',source='whatsapp'),[])
        with self.store.connection() as db:
            self.assertEqual(db.execute('SELECT count(*) AS n FROM messages').fetchone()['n'],250)
        for _ in range(5): self.store.sync_group_consent([])
        with self.store.connection() as db:
            self.assertEqual(db.execute('SELECT count(*) AS n FROM messages').fetchone()['n'],0)
            self.assertEqual(db.execute('SELECT count(*) AS n FROM embeddings').fetchone()['n'],0)

    def test_cleanup_timeout_cannot_roll_back_permission_revocation(self):
        from unittest.mock import patch
        from psycopg.errors import QueryCanceled
        self.store.sync_group_consent(['123@g.us'])
        self.store.apply([dict(self.message(text='private withheld'),chat_id='123@g.us')],'whatsapp')
        with patch.object(self.store,'cleanup_group_consent',side_effect=QueryCanceled('synthetic timeout')):
            self.store.sync_group_consent([])
        self.assertEqual(self.store.lexical('private',source='whatsapp'),[])
        self.assertEqual(self.store.pending(10),[])
        self.store.sync_group_consent([])

    def test_restart_deduplicates_and_checkpoint_is_atomic(self):
        from store import Store
        self.store.apply([self.message(), self.message('2')], 'whatsapp', cursor=2)
        other = Store(self.store.dsn, self.store.schema)
        other.apply([self.message(), self.message('2')], 'whatsapp', cursor=2)
        self.assertEqual(other.status('whatsapp')['messages'], 2)
        self.assertEqual(len(other.pending(20)), 1)
        self.assertEqual(other.state('whatsapp')['cursor'], 2)
        with self.assertRaises(Exception):
            other.apply([self.message('3'), dict(self.message('4'), timestamp='bad')], 'whatsapp', cursor=4)
        self.assertEqual(other.state('whatsapp')['cursor'], 2)
        self.assertEqual(other.status('whatsapp')['messages'], 2)

    def test_edit_delete_and_cross_platform_keys(self):
        self.store.apply([self.message(), self.message(source='telegram')], 'whatsapp')
        self.store.apply([self.message(text='Compra de flores')], 'whatsapp')
        self.assertEqual(len(self.store.lexical('flores', source='whatsapp')), 1)
        self.assertEqual(len(self.store.lexical('presupuesto', source='whatsapp')), 0)
        self.store.apply([], 'whatsapp', deleted=[('synthetic-chat', '1')], change_seq=3)
        self.assertEqual(self.store.status('whatsapp')['messages'], 0)
        self.assertEqual(self.store.status('telegram')['messages'], 1)

    def test_vectors_only_mark_success_after_commit(self):
        self.store.apply([self.message(), self.message('2', 'Flores para el jardín')], 'whatsapp')
        jobs = self.store.pending(20)
        self.store.embedding_failed([jobs[0]['hash']], 'synthetic timeout', delay=0)
        self.assertEqual(self.store.status('whatsapp')['embedded_messages'], 0)
        self.store.save_embeddings([(row['hash'], [1.0] + [0.0] * 1023) for row in jobs])
        self.assertEqual(self.store.status('whatsapp')['embedded_messages'], 2)
        self.assertEqual(len(self.store.semantic([1.0] + [0.0] * 1023, source='whatsapp')), 2)
        self.assertEqual(self.store.semantic([1.0] + [0.0] * 1023, source='telegram'), [])

    def test_model_binding_rejects_incompatible_vectors_after_restart(self):
        self.store.bind_model('synthetic-model-sha256-a')
        self.store.check_model('synthetic-model-sha256-a')
        with self.assertRaises(RuntimeError):
            self.store.bind_model('synthetic-model-sha256-b')
        with self.assertRaises(RuntimeError):
            self.store.check_model('synthetic-model-sha256-b')

    def test_source_recovery_rebuilds_only_affected_source(self):
        self.store.apply([self.message(),self.message(source='telegram')],'whatsapp',cursor=15,change_seq=9,change_token='old')
        self.store.reset_source('whatsapp','new-capture-identity')
        state=self.store.state('whatsapp')
        self.assertEqual(state['cursor'],0)
        self.assertEqual(state['change_seq'],0)
        self.assertIsNone(state['change_token'])
        self.assertFalse(state['backfill_done'])
        self.assertEqual(state['source_identity'],'new-capture-identity')
        self.assertEqual(self.store.status('telegram')['messages'],1)

    def test_new_change_gets_embedding_before_initial_backlog(self):
        self.store.apply([self.message('1','Texto histórico pendiente')],'whatsapp')
        self.store.apply([self.message('2','Mensaje entrante')],'whatsapp',change_seq=1,change_token='new')
        self.assertEqual(self.store.pending(1)[0]['text'],'Mensaje entrante')

    def test_media_without_text_remains_queryable(self):
        self.store.apply([self.message(text='')], 'whatsapp')
        result = self.store.status('whatsapp')
        self.assertEqual(result['messages'], 1)
        self.assertEqual(result['text_messages'], 0)
        self.assertEqual(result['embedded_messages'], 0)
        self.assertEqual(self.store.analytics(source='whatsapp')['total'], 1)

    def test_filters_and_literal_sql_input(self):
        self.store.apply([self.message()], 'whatsapp')
        self.assertEqual(self.store.lexical('presupuesto', source='whatsapp', chat="x' OR 1=1 --"), [])
        self.assertEqual(self.store.lexical('presupuesto', source='whatsapp', after=1800000000), [])
        with self.assertRaises(ValueError):
            self.store.analytics(source='whatsapp', group_by='pg_sleep(10)')


if __name__ == '__main__':
    unittest.main()
