"""Separate incremental ingestion and embedding loops, with durable checkpoints."""
import fcntl
import json
from datetime import datetime, timezone
import signal
import threading
import time
from pathlib import Path

from embeddings import Embedder, bounded_batch
from sources import Source
from recovery import recovery_plan
from priority import queries_waiting,EmbeddingBusy
from store import Store


ROOT=Path.home()/'.local/share/messaging-memory'
STOP=threading.Event()


def ingest_step(store, source):
    state=store.state(source.kind)
    if not state['backfill_done']:
        rows=source.backfill_page(state['cursor'],500)
        if rows:
            store.apply(rows,source.kind,cursor=max(row['rowid'] for row in rows))
        else:
            store.apply([],source.kind,backfill_done=True)
        return True
    events=source.changes(state['change_seq'],500)
    if not events:
        return False
    records,deleted=[],[]
    for chat_id,message_id in dict.fromkeys((e['chat_id'],e['message_id']) for e in events):
        row=source.get(chat_id,message_id)
        if row is None:
            deleted.append((chat_id,message_id))
        else:
            records.append(row)
    store.apply(records,source.kind,deleted=deleted,change_seq=events[-1]['seq'],change_token=events[-1]['event_token'])
    return True


def batch_limits(model):
    """Batch shape comes from the embedder's backend config; test doubles without it get the local defaults."""
    rows, size = getattr(model, 'batch_rows', 16), getattr(model, 'batch_bytes', 1600)
    return (rows if isinstance(rows, int) else 16), (size if isinstance(size, int) else 1600)


def embedding_step(store, model):
    max_rows,max_bytes=batch_limits(model)
    rows=bounded_batch(store.pending(max_rows), max_bytes, max_rows)
    if not rows:
        return False
    start=time.monotonic()
    try:
        vectors=model.embed([r['text'] for r in rows])
        store.save_embeddings([(row['hash'],v) for row,v in zip(rows,vectors,strict=True)], ctids=[row.get('ctid') for row in rows])
    except EmbeddingBusy:
        return False
    except Exception as error:
        # Error bodies may contain user input. Persist only the type, never the response.
        delay=min(3600,30*2**min(7,max(row['attempts'] for row in rows)))
        store.embedding_failed([r['hash'] for r in rows],type(error).__name__,delay=delay)
        store.heartbeat('embeddings',dict(ok=False,error=type(error).__name__,retry_seconds=delay))
        return False
    store.heartbeat('embeddings',dict(ok=True,last_batch=len(rows),seconds=round(time.monotonic()-start,3)))
    return True


def status_refresh(store):
    """Heavy coverage query, computed off the request path. Keeps the last good value on failure."""
    try:
        result = store.status(None, timeout='170s')
        # worker_state also holds the previous snapshot; never nest it inside the new one.
        result['workers'] = [w for w in result.get('workers', []) if w.get('name') not in ('status', 'status_error')]
        result['computed_at'] = datetime.now(timezone.utc).isoformat()
        store.heartbeat('status', json.loads(json.dumps(result, default=str)))
    except Exception as error:
        store.heartbeat('status_error', dict(error=type(error).__name__, at=datetime.now(timezone.utc).isoformat()))


BULK_PENDING = 50000  # above this the index is re-embedding; the coverage query only competes for disk


def status_due(store, now):
    """Skip the 170 s coverage query while nothing changed (fresh snapshot, no ingestion in the last hour)
    and during a bulk re-embed (hourly at most)."""
    try:
        snapshot = store.status_snapshot()
        if snapshot is None:
            return True
        age = (now - snapshot['updated_at']).total_seconds()
        if age >= 3600:
            return True
        cheap = store.cheap_status(None)
        if cheap.get('pending_embeddings', 0) > BULK_PENDING:
            return False
        return cheap.get('ingested_last_hour', 1) > 0
    except Exception:
        return True


def status_loop(store, stop, interval=300):
    while not stop.is_set():
        if not queries_waiting(ROOT) and status_due(store, datetime.now(timezone.utc)):
            status_refresh(store)
        stop.wait(interval)


def embedding_cycle(store, model, max_batches=4, step=None):
    """Up to max_batches consecutive embedding batches while nobody is waiting on an interactive query.
    The continuous worker uses one batch per cycle to keep interactive searches responsive."""
    step = step or embedding_step
    done = 0
    for _ in range(max_batches):
        if model.interactive_recent() or not step(store, model):
            break
        done += 1
    return done > 0


def refresh_source_stats(store,source,refresh):
    if source.kind in refresh and time.monotonic()-refresh[source.kind]<=300:
        return
    # Aggregate metadata is optional. A slow source must not stall its change stream.
    refresh[source.kind]=time.monotonic()
    try:
        store.apply([],source.kind,source_stats=source.stats())
    except Exception:
        pass


def ingestion_loop(store, sources):
    refresh,installed={},set()
    while not STOP.is_set():
        if queries_waiting(ROOT):
            STOP.wait(.2)
            continue
        worked=False
        for source in sources:
            if STOP.is_set():
                break
            try:
                if source.kind not in installed:
                    source.install_capture()
                    installed.add(source.kind)
                plan=recovery_plan(store.state(source.kind),source)
                if plan['reset_required']:
                    store.reset_source(source.kind,plan['source_identity'])
                worked=ingest_step(store,source) or worked
                refresh_source_stats(store,source,refresh)
                store.heartbeat('ingest_'+source.kind,dict(ok=True))
            except Exception as error:
                installed.discard(source.kind)
                try:
                    store.heartbeat('ingest_'+source.kind,dict(ok=False,error=type(error).__name__))
                except Exception:
                    pass
                STOP.wait(5)
        STOP.wait(.05 if worked else 5)


IO_PRESSURE = Path('/proc/pressure/io')


def io_pressure_some(path=IO_PRESSURE):
    """Percent of the last 10 s some task waited on disk (Linux PSI); 0 when unreadable."""
    try:
        for line in path.read_text().splitlines():
            if line.startswith('some'):
                return float(line.split('avg10=')[1].split()[0])
    except (OSError, ValueError, IndexError):
        pass
    return 0.


def pace_seconds(worked, pressure=None):
    """Pause between embedding batches. Idle: 5 s. Busy: 0.5 s, stretched when the disk is saturated so the
    MCP servers and their SQLite stores keep answering (a bulk re-embed drove PSI to 80 % and tripped
    the monitor's 3 s health probes). Cost: the bulk takes longer; the other services stay responsive."""
    pressure = io_pressure_some() if pressure is None else pressure
    if pressure >= 70:
        return 25
    if pressure >= 50:
        return 10
    if not worked:
        return 5
    if pressure >= 30:
        return 3
    return .5


def run(root=None, config_path=None, source_paths=None):
    """Worker entry point for launchers that supply their own storage and sources."""
    global ROOT
    if root is not None:
        ROOT = Path(root)
    ROOT.mkdir(mode=0o700,parents=True,exist_ok=True)
    lock=(ROOT/'worker.lock').open('a')
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit('Messaging memory worker already running')
    config=json.loads(Path(config_path or Path.home()/'.config/messaging-memory/database.json').read_text())
    store=Store(config['writer_dsn'])
    store.initialize()
    model=Embedder(ROOT)
    model.expected_identity=model.identity()
    store.bind_model(model.expected_identity)
    if source_paths is None:
        raise ValueError('Use history.py worker with configured source paths')
    sources=[Source(kind,Path(path)) for kind,path in source_paths.items()]
    for sig in (signal.SIGTERM,signal.SIGINT):
        signal.signal(sig,lambda *_:STOP.set())
    thread=threading.Thread(target=ingestion_loop,args=(store,sources),daemon=True)
    thread.start()
    threading.Thread(target=status_loop,args=(store,STOP),daemon=True).start()
    while not STOP.is_set():
        try:
            if model.interactive_recent():
                STOP.wait(2)
                continue
            worked=embedding_step(store,model)
            STOP.wait(pace_seconds(worked))
        except Exception as error:
            print(json.dumps(dict(component='worker',error=type(error).__name__)),flush=True)
            STOP.wait(max(10, pace_seconds(False)))
    thread.join(timeout=20)
    lock.close()


if __name__=='__main__':
    raise SystemExit('Use history.py worker')
