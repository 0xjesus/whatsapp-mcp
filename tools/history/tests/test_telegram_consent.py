"""Synthetic, isolated Telegram PostgreSQL consent regressions."""
import os
import secrets
import subprocess
import tempfile
from pathlib import Path
import unittest
from test_integration import IMAGE, wait_until


@unittest.skipUnless(os.environ.get('HISTORY_DOCKER_TEST') == '1', 'requires isolated Docker')
class TelegramStoreConsentTests(unittest.TestCase):
    def test_policy_cleanup_and_all_read_paths(self):
        import psycopg
        from psycopg.conninfo import make_conninfo
        from store import Store
        from search_data import SearchData
        name = 'history-telegram-consent-' + secrets.token_hex(5)
        with tempfile.TemporaryDirectory() as temp:
            password = secrets.token_urlsafe(32)
            env = Path(temp) / 'pg.env'
            env.write_text('POSTGRES_PASSWORD=' + password + '\n')
            env.chmod(0o600)
            subprocess.run(['docker','run','--detach','--rm','--name',name,'--memory','768m','--cpus','2',
                            '--publish','127.0.0.1::5432','--env-file',str(env),IMAGE], check=True, capture_output=True)
            try:
                port = subprocess.check_output(['docker','port',name,'5432/tcp'], text=True).strip().rsplit(':',1)[1]
                dsn = make_conninfo(host='127.0.0.1', port=port, user='postgres',password=password,dbname='postgres')
                def ready():
                    with psycopg.connect(dsn,connect_timeout=1): return True
                wait_until(ready)
                with psycopg.connect(dsn) as db:
                    db.execute('CREATE EXTENSION vector')
                store = Store(dsn)
                store.initialize()
                record = lambda chat, mid: dict(source='telegram',chat_id=chat,message_id=str(mid),timestamp=1700000000,
                    sender='synthetic',chat_name='Synthetic '+chat,text='telescope '+chat+' '+str(mid),media_type='')
                records = [record('-1',n) for n in range(401)] + [record('-2',1),record('1',1),record('-3',1)]
                for row in records:
                    if row['chat_id'] in ('1','-2'): row['timestamp'] += 100
                store.apply(records,'telegram')
                # Suspend cleanup to verify that revoked rows disappear before physical deletion.
                cleanup = store.cleanup_telegram_consent
                store.cleanup_telegram_consent = lambda: None
                store.sync_telegram_consent(['-1'], ['-2'])
                self.assertEqual(store.analytics(source='telegram')['total'], 403)
                with store.connection() as db:
                    db.execute("UPDATE memory_meta SET value='0' WHERE key='telegram_consent_refreshed_at'")
                self.assertEqual(store.analytics(source='telegram')['total'], 1)
                self.assertEqual(len(store.pending(1024)), 1)
                self.assertEqual({r['chat_id'] for r in store.lexical('telescope',source='telegram')}, {'1'})
                store.sync_telegram_consent(['-1'], ['-2'], cleanup=False)
                self.assertEqual(store.analytics(source='telegram')['total'], 403)
                from server import search
                from unittest.mock import patch
                class RevokingModel:
                    def embed(inner, *args, **kwargs):
                        store.sync_telegram_consent([], ['-2'], cleanup=False)
                        return [[1.]+[0.]*1023]
                response = search(store, RevokingModel(), dict(source='telegram',query='telescope',mode='hybrid',limit=50,
                    plan=dict(keywords=['telescope'],semantic_queries=['telescope'],context='none')))
                self.assertNotIn('-1', {row['chat_id'] for row in response['results']})
                self.assertEqual(response['returned_message_embedding_coverage']['messages'], 2)
                store.sync_telegram_consent(['-1'], ['-2'], cleanup=False)
                original_coverage = SearchData.coverage
                def revoke_after_context(data, rows):
                    self.assertTrue(any(row['chat_id']=='-1' for row in rows))
                    coverage = original_coverage(data, rows)
                    store.sync_telegram_consent([], ['-2'], cleanup=False)
                    return coverage
                with patch.object(SearchData, 'coverage', revoke_after_context):
                    response = search(store, RevokingModel(), dict(source='telegram',query='telescope',mode='keyword',limit=50,
                        plan=dict(keywords=['telescope'],semantic_queries=[],context='neighbors')))
                self.assertNotIn('-1', {row['chat_id'] for row in response['results']})
                self.assertFalse(any(item['chat_id']=='-1' for row in response['results'] for item in row.get('context',[])))
                store.sync_telegram_consent(['-1'], ['-2'], cleanup=False)
                original_resolve = SearchData.resolve
                def revoke_after_candidates(data, *args, **kwargs):
                    candidates = original_resolve(data, *args, **kwargs)
                    self.assertTrue(any(row['chat_id']=='-1' for row in candidates))
                    store.sync_telegram_consent([], ['-2'], cleanup=False)
                    return candidates
                with patch.object(SearchData, 'resolve', revoke_after_candidates):
                    response = search(store, RevokingModel(), dict(source='telegram',query='telescope',mode='keyword',chat='Synthetic'))
                self.assertNotIn('-1', {row['chat_id'] for row in response['chat_candidates']})
                with patch.object(SearchData, 'authorized', side_effect=RuntimeError('synthetic unavailable')):
                    response = search(store, RevokingModel(), dict(source='telegram',query='telescope',mode='keyword'))
                self.assertEqual(response['results'], [])
                self.assertEqual(response['authorization_error'], 'RuntimeError')
                store.sync_telegram_consent([], ['-2'])
                self.assertEqual(store.analytics(source='telegram')['total'], 2)
                self.assertEqual({r['chat_id'] for r in store.lexical('telescope',source='telegram')}, {'1','-2'})
                self.assertEqual(len(store.pending(1024)), 2)
                directory = SearchData(store)
                self.assertEqual({r['chat_id'] for r in directory.directory('telegram')}, {'1','-2'})
                denied = dict(id=1,source='telegram',chat_id='-1',timestamp=1700000000)
                self.assertEqual(directory.context(denied,{},'general',[]), [])
                with store.connection() as db:
                    db.execute("UPDATE embeddings SET embedding=(ARRAY[1::real] || array_fill(0::real,ARRAY[1023]))::halfvec(1024)")
                self.assertEqual({r['chat_id'] for r in store.semantic([1.]+[0.]*1023,source='telegram')}, {'1','-2'})
                store.sync_telegram_consent(['-1'], ['-2'])
                self.assertEqual({r['chat_id'] for r in directory.directory('telegram')}, {'1','-1','-2'})
                store.sync_telegram_consent([], ['-2'])
                store.cleanup_telegram_consent = cleanup
                cleanup()
                with store.connection() as db:
                    self.assertEqual(db.execute("SELECT count(*) AS n FROM messages WHERE chat_id='-1'").fetchone()['n'],201)
                    self.assertEqual(db.execute("SELECT value FROM memory_meta WHERE key='telegram_message_cursor'").fetchone()['value'],'200')
                    self.assertIsNone(db.execute("SELECT value FROM memory_meta WHERE key='group_consent_cleanup'").fetchone())
                for _ in range(5): cleanup()
                with store.connection() as db:
                    self.assertEqual(db.execute('SELECT count(*) AS n FROM messages').fetchone()['n'],2)
                    self.assertEqual(db.execute('SELECT count(*) AS n FROM embeddings').fetchone()['n'],2)
                    self.assertEqual(db.execute("SELECT value FROM memory_meta WHERE key='telegram_consent_cleanup'").fetchone()['value'],'done')
                store.heartbeat('status',dict(private='snapshot'))
                store.heartbeat('status_error',dict(error='snapshot'))
                store.heartbeat('ingest_telegram',dict(ok=True))
                self.assertEqual([r['name'] for r in store.worker_status()], ['ingest_telegram'])
            finally:
                subprocess.run(['docker','rm','--force',name], capture_output=True)
