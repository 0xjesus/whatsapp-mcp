"""Bounded maintenance and reuse races on an isolated PostgreSQL schema."""
from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

import bulk_maint
from store import Store, digest


@unittest.skipUnless(os.environ.get('MEMORY_TEST_DSN'), 'set MEMORY_TEST_DSN to a disposable PostgreSQL database')
class PendingOrphanTests(unittest.TestCase):
    def setUp(self):
        self.store=Store(os.environ['MEMORY_TEST_DSN'], 'memory_test_gc_'+uuid.uuid4().hex)
        self.store.initialize()
        self.addCleanup(self.store.drop_test_schema)
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root=Path(self.directory.name)

    @contextmanager
    def connection(self):
        import psycopg
        from psycopg import sql
        with psycopg.connect(self.store.dsn,autocommit=True) as db:
            db.execute(sql.SQL('SET search_path TO {},public').format(sql.Identifier(self.store.schema)))
            yield db

    def row(self,key='1',text='synthetic message',**fields):
        return dict(dict(source='whatsapp',chat_id='chat',message_id=key,timestamp=1700000000,
                         sender='s',chat_name='synthetic',text=text,media_type=''),**fields)

    def seed(self,hashes):
        with self.connection() as db:
            with db.cursor() as c:
                c.executemany('INSERT INTO embeddings(hash,text) VALUES(%s,%s)',[(h,'synthetic cache') for h in hashes])

    def gc(self,db,**kwargs):
        function=getattr(bulk_maint,'gc_pending_orphans',None)
        self.assertTrue(callable(function),'bounded pending-orphan maintenance is missing')
        return function(db,root=self.root,**kwargs)

    def hashes(self):
        with self.connection() as db:
            return [r[0] for r in db.execute('SELECT hash FROM embeddings ORDER BY hash')]

    def test_removes_only_pending_orphans_preserving_references_and_complete_vectors(self):
        self.store.apply([self.row(),self.row('2')], 'whatsapp')
        self.seed(['orphan','ready'])
        self.store.save_embeddings([('ready',[1.]+[0.]*1023)])
        with self.connection() as db:
            result=self.gc(db)
        self.assertEqual(result['deleted'],1)
        self.assertEqual(self.hashes(),sorted([digest('synthetic message'),'ready']))

    def test_fixed_candidate_bound_and_wrap_revisit_earlier_new_orphan(self):
        self.seed(['a','b','c'])
        with self.connection() as db:
            first=self.gc(db,limit=2)
            self.assertEqual((first['scanned'],first['deleted']),(2,2))
            self.seed(['0'])
            self.gc(db,limit=2)
            self.assertIn('0',self.hashes())
            self.gc(db,limit=2)
        self.assertEqual(self.hashes(),[])

    def test_default_candidate_page_stops_at_five_hundred(self):
        self.seed([format(i,'064x') for i in range(501)])
        with self.connection() as db:
            result=self.gc(db)
        self.assertEqual((result['scanned'],result['deleted']),(500,500))
        self.assertEqual(len(self.hashes()),1)

    def test_second_collector_skips_instead_of_waiting(self):
        self.seed(['a'])
        with self.connection() as first, self.connection() as second:
            with first.transaction():
                first.execute('SELECT pg_advisory_xact_lock(%s,%s)',bulk_maint.GC_LOCK)
                self.assertEqual(self.gc(second)['skipped'],'maintenance_busy')
        self.assertEqual(self.hashes(),['a'])

    def test_frontend_priority_does_not_start_a_gc_transaction(self):
        self.seed(['a'])
        with self.connection() as db, patch.object(bulk_maint,'queries_waiting',return_value=True,create=True):
            self.assertEqual(self.gc(db)['skipped'],'foreground')
            self.assertEqual(db.execute("SELECT count(*) FROM memory_meta WHERE key='pending_orphan_cursor'").fetchone()[0],0)
        self.assertEqual(self.hashes(),['a'])

    def test_frontend_arriving_after_selection_rolls_back_the_page(self):
        self.seed(['a'])
        with self.connection() as db, patch.object(bulk_maint,'queries_waiting',side_effect=[False,True]):
            self.assertEqual(self.gc(db)['skipped'],'foreground')
            self.assertEqual(db.execute("SELECT count(*) FROM memory_meta WHERE key='pending_orphan_cursor'").fetchone()[0],0)
        self.assertEqual(self.hashes(),['a'])

    def test_timeout_rolls_back_cursor_and_deletion(self):
        self.assertTrue(callable(getattr(bulk_maint,'gc_pending_orphans',None)))
        self.seed(['a','b'])
        with self.connection() as db:
            db.execute("CREATE FUNCTION delay_gc() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1); RETURN OLD; END $$")
            db.execute('CREATE TRIGGER slow_gc BEFORE DELETE ON embeddings FOR EACH ROW EXECUTE FUNCTION delay_gc()')
            with self.assertRaises(Exception):
                self.gc(db,budget_seconds=.1)
            self.assertEqual(db.execute("SELECT count(*) FROM memory_meta WHERE key='pending_orphan_cursor'").fetchone()[0],0)
        self.assertEqual(self.hashes(),['a','b'])

    def test_uncommitted_reference_is_skipped_and_never_deleted(self):
        self.store.apply([self.row(text='')], 'whatsapp')
        self.seed(['a'])
        with self.connection() as writer, self.connection() as gc:
            with writer.transaction():
                ident=writer.execute('SELECT id FROM messages').fetchone()[0]
                writer.execute('INSERT INTO message_chunks VALUES(%s,0,%s)',(ident,'a'))
                self.assertEqual(self.gc(gc)['deleted'],0)
        self.assertEqual(self.hashes(),['a'])

    def test_worker_can_reuse_hash_while_gc_holds_candidate_lock(self):
        hash_=digest('reused body')
        self.seed([hash_])
        locked,release,finished=threading.Event(),threading.Event(),threading.Event()
        errors=[]
        class PausedConnection:
            def __init__(self,db):self.db=db
            def transaction(self):return self.db.transaction()
            def execute(self,sql,params=()):
                cursor=self.db.execute(sql,params)
                if 'FOR UPDATE SKIP LOCKED' in sql:
                    locked.set()
                    if not release.wait(1):raise TimeoutError('test release missing')
                return cursor
        def collect():
            try:
                with self.connection() as db:self.gc(PausedConnection(db))
            except BaseException as exc:errors.append(exc)
        def reuse():
            try:self.store.apply([self.row(text='reused body')], 'whatsapp',change_seq=1)
            except BaseException as exc:errors.append(exc)
            finally:finished.set()
        collector=threading.Thread(target=collect)
        writer=None
        collector.start()
        try:
            self.assertTrue(locked.wait(1))
            writer=threading.Thread(target=reuse)
            writer.start()
            self.assertFalse(finished.wait(.1))
        finally:release.set()
        collector.join(3)
        if writer is not None:writer.join(3)
        self.assertFalse(collector.is_alive() or writer.is_alive())
        self.assertEqual(errors,[])
        with self.connection() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM message_chunks c JOIN embeddings e ON e.hash=c.hash').fetchone()[0],1)
        self.assertEqual(self.hashes(),[hash_])

    def test_pending_threshold_counts_authorized_work_including_backoff_only(self):
        self.store.sync_group_consent(['yes@g.us'],cleanup=False)
        self.store.sync_telegram_consent(['-1'],[],cleanup=False)
        self.store.apply([self.row('yes','allowed',chat_id='yes@g.us'),
                          self.row('no','denied',chat_id='no@g.us'),
                          self.row('tg','telegram allowed',source='telegram',chat_id='-1')], 'whatsapp')
        self.seed(['orphan'])
        with self.connection() as db:
            db.execute("UPDATE embeddings SET retry_at=now()+interval '1 day'")
            self.assertEqual(bulk_maint.pending_estimate(db),2)
            db.execute("UPDATE memory_meta SET value=%s WHERE key='telegram_consent_refreshed_at'",(str(time.time()-61),))
            self.assertIsNone(bulk_maint.pending_estimate(db))

    def test_stale_policy_keeps_bulk_mode_and_timer_active(self):
        with self.connection() as db:
            db.execute('ALTER TABLE embeddings SET (autovacuum_enabled=false)')
            function=getattr(bulk_maint,'maintain',None)
            self.assertTrue(callable(function),'maintenance entry point is missing')
            result=function(db,root=self.root)
            self.assertEqual(result['bulk_decision'],'policy_stale')
            self.assertIn('autovacuum_enabled=false',db.execute("SELECT reloptions FROM pg_class WHERE oid='embeddings'::regclass").fetchone()[0])
            self.assertEqual(result['timer'],'kept_for_orphan_gc')


if __name__=='__main__':unittest.main()
