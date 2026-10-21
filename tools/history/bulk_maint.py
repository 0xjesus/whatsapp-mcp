"""Bounded pending-cache cleanup; bulk ANALYZE stops once autovacuum is restored.

The timer remains enabled for light orphan maintenance. This never removes ready
vectors or message references, and never treats stale consent as an empty queue.
"""
import json
from pathlib import Path
import time

from priority import queries_waiting
from store import Store

DONE_BELOW = 2000
ROOT = Path.home() / '.local/share/messaging-memory'
CONFIG = Path.home() / '.config/messaging-memory/database.json'
GC_CURSOR = 'pending_orphan_cursor'
GC_LOCK = (1296387407, 1)


class Budget:
    """Bound the whole small transaction, including each server-side statement."""
    def __init__(self, db, seconds):
        self.db = db
        self.deadline = time.monotonic() + seconds

    def execute(self, sql, params=()):
        remaining = int((self.deadline - time.monotonic()) * 1000)
        if remaining <= 0:
            raise TimeoutError('maintenance budget exhausted')
        self.db.execute("SELECT set_config('statement_timeout',%s,true)", (str(remaining)+'ms',))
        return self.db.execute(sql, params)


class ForegroundWaiting(Exception):
    pass


def gc_pending_orphans(db, *, root=ROOT, limit=500, budget_seconds=2):
    """One circular PK page on an autocommit connection; errors roll back its cursor."""
    if not isinstance(limit, int) or not 1 <= limit <= 500 or not 0 < budget_seconds <= 5:
        raise ValueError('invalid maintenance bound')
    result = dict(scanned=0, deleted=0, wrapped=False)
    if queries_waiting(root):
        return dict(result, skipped='foreground')
    try:
        with db.transaction():
            budget = Budget(db, budget_seconds)
            budget.execute("SET LOCAL lock_timeout='200ms'")
            if not budget.execute('SELECT pg_try_advisory_xact_lock(%s,%s)', GC_LOCK).fetchone()[0]:
                return dict(result, skipped='maintenance_busy')
            budget.execute('INSERT INTO memory_meta(key,value) VALUES(%s,%s) ON CONFLICT DO NOTHING', (GC_CURSOR,''))
            cursor = budget.execute('SELECT value FROM memory_meta WHERE key=%s', (GC_CURSOR,)).fetchone()[0]
            # Bound examined keys, rather than an unbounded search for 500 orphans.
            candidates = [r[0] for r in budget.execute('SELECT hash FROM embeddings WHERE hash>%s ORDER BY hash LIMIT %s', (cursor,limit))]
            result['scanned'] = len(candidates)
            if candidates:
                # Skip active embedding updates and FK references. A subsequent
                # worker UPSERT can recreate an orphan after this deletion commits.
                locked = [r[0] for r in budget.execute('''SELECT hash FROM embeddings
                    WHERE hash=ANY(%s) AND embedding IS NULL FOR UPDATE SKIP LOCKED''', (candidates,))]
                if queries_waiting(root):
                    raise ForegroundWaiting()
                if locked:
                    result['deleted'] = budget.execute('''DELETE FROM embeddings e WHERE e.hash=ANY(%s)
                        AND e.embedding IS NULL AND NOT EXISTS(SELECT 1 FROM message_chunks c WHERE c.hash=e.hash)''', (locked,)).rowcount
            result['wrapped'] = len(candidates) < limit
            next_cursor = '' if result['wrapped'] else candidates[-1]
            budget.execute('UPDATE memory_meta SET value=%s WHERE key=%s', (next_cursor,GC_CURSOR))
        return result
    except ForegroundWaiting:
        return dict(scanned=0, deleted=0, wrapped=False, skipped='foreground')


def consent_fresh(budget):
    return budget.execute("""SELECT count(*)=2 AND coalesce(bool_and(
        CAST(value AS double precision)>extract(epoch FROM clock_timestamp())-30
        AND CAST(value AS double precision)<=extract(epoch FROM clock_timestamp())+5),false)
        FROM memory_meta WHERE key IN ('whatsapp_consent_refreshed_at','telegram_consent_refreshed_at')""").fetchone()[0]


def pending_estimate(db):
    """Bounded authorized count including retries; None means policy is not fresh."""
    with db.transaction():
        budget = Budget(db, 5)
        budget.execute("SET LOCAL lock_timeout='200ms'")
        if not consent_fresh(budget):
            return None
        permission, params = Store('unused').filters()
        count = budget.execute('''SELECT count(*) FROM (SELECT 1 FROM embeddings e
            WHERE e.embedding IS NULL AND EXISTS(SELECT 1 FROM message_chunks c JOIN messages m ON m.id=c.message_pk
                WHERE c.hash=e.hash AND ''' + permission + ') LIMIT %s) eligible', [*params,DONE_BELOW+1]).fetchone()[0]
        return count if consent_fresh(budget) else None


def maintain(db, *, root=ROOT):
    from psycopg.errors import QueryCanceled
    out = dict(timer='kept_for_orphan_gc')
    out['orphan_gc'] = gc_pending_orphans(db, root=root)
    if out['orphan_gc'].get('skipped') or queries_waiting(root):
        return out
    db.execute("SET statement_timeout='5s'")
    db.execute("SET lock_timeout='200ms'")
    options = db.execute("SELECT reloptions FROM pg_class WHERE oid='embeddings'::regclass").fetchone()[0] or []
    out['autovacuum_off'] = 'autovacuum_enabled=false' in options
    if not out['autovacuum_off']:
        return out  # Subsequent timer runs only do bounded cache cleanup.
    pending = pending_estimate(db)
    out['authorized_pending_at_least'] = pending
    if pending is None:
        out['bulk_decision'] = 'policy_stale'
        return out
    if queries_waiting(root):
        out['bulk_decision'] = 'foreground'
        return out
    start = time.monotonic()
    if pending <= DONE_BELOW:
        db.execute('ALTER TABLE embeddings SET (autovacuum_enabled=true)')
        out['bulk_finished'] = True
        # Autovacuum can resume normal work even if this optional bounded pass times out.
        try:
            db.execute('VACUUM ANALYZE embeddings')
        except QueryCanceled:
            out['vacuum_deferred'] = True
    else:
        db.execute('ANALYZE embeddings')
    out['maintenance_seconds'] = round(time.monotonic()-start,3)
    return out


def main():
    import psycopg
    if queries_waiting(ROOT):
        print(json.dumps(dict(skipped='foreground')))
        return
    config = json.loads(CONFIG.read_text())
    try:
        with psycopg.connect(config['writer_dsn'],autocommit=True,connect_timeout=5) as db:
            result = maintain(db)
    except Exception as error:
        print(json.dumps(dict(ok=False,error=type(error).__name__)))
        raise SystemExit(1) from None
    print(json.dumps(dict(ok=True,**result)))


if __name__ == '__main__':
    main()
