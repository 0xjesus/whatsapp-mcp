"""Derived, rebuildable history in PostgreSQL. Original SQLite stores remain authoritative."""
import hashlib
import json
import math
import re
import time
from contextlib import contextmanager
from perf_query import semantic_statement
import embed_config


MODEL = embed_config.label(embed_config.load())
SOURCES = ('whatsapp', 'telegram')


def chunks(text, byte_limit=1200):
    """No truncation, including emoji and long unbroken strings. Bound tokenizer worst case."""
    result, part, size = [], [], 0
    for char in text:
        count = len(char.encode('utf-8'))
        if size + count > byte_limit and part:
            result.append(''.join(part))
            part, size = [], 0
        part.append(char)
        size += count
    if part:
        result.append(''.join(part))
    return result


def digest(text):
    return hashlib.sha256((MODEL + '\0' + text).encode()).hexdigest()


def vector_literal(vector):
    if len(vector) != 1024 or not all(math.isfinite(float(x)) for x in vector):
        raise ValueError('Expected 1024 finite embedding dimensions')
    norm = math.sqrt(sum(float(x) ** 2 for x in vector))
    if norm == 0:
        raise ValueError('Zero embedding is not searchable')
    return '[' + ','.join(str(float(x) / norm) for x in vector) + ']'


class Store:
    def __init__(self, dsn, schema='public'):
        if not re.fullmatch(r'[a-z][a-z0-9_]*', schema):
            raise ValueError('Invalid schema')
        self.dsn, self.schema = dsn, schema

    @contextmanager
    def connection(self, timeout='15s'):
        import psycopg
        from psycopg.rows import dict_row
        with psycopg.connect(self.dsn, connect_timeout=5, row_factory=dict_row) as db:
            db.execute("SELECT set_config('search_path', %s, false)", (self.schema + ',public',))
            db.execute("SELECT set_config('statement_timeout', %s, false)", (timeout,))
            yield db

    def initialize(self):
        with self.connection('60s') as db:
            if self.schema != 'public':
                db.execute('CREATE SCHEMA IF NOT EXISTS ' + self.schema)
            db.execute('''
                CREATE TABLE IF NOT EXISTS memory_meta(key text PRIMARY KEY, value text NOT NULL);
                CREATE TABLE IF NOT EXISTS group_monitoring_consent(chat_jid text PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS telegram_monitoring_allowed(chat_id text PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS source_state(
                    source text PRIMARY KEY, cursor bigint NOT NULL DEFAULT 0,
                    change_seq bigint NOT NULL DEFAULT 0, backfill_done boolean NOT NULL DEFAULT false,
                    source_stats jsonb NOT NULL DEFAULT '{}', updated_at timestamptz DEFAULT now());
                ALTER TABLE source_state ADD COLUMN IF NOT EXISTS source_identity text;
                ALTER TABLE source_state ADD COLUMN IF NOT EXISTS change_token text;
                CREATE TABLE IF NOT EXISTS messages(
                    id bigserial PRIMARY KEY, source text NOT NULL, chat_id text NOT NULL,
                    message_id text NOT NULL, timestamp bigint NOT NULL, sender text NOT NULL,
                    chat_name text NOT NULL, text text NOT NULL, media_type text NOT NULL,
                    content_hash text NOT NULL,
                    search tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,
                    UNIQUE(source,chat_id,message_id));
                CREATE INDEX IF NOT EXISTS messages_chat_date ON messages(source,chat_id,timestamp);
                CREATE INDEX IF NOT EXISTS messages_source_date ON messages(source,timestamp);
                CREATE INDEX IF NOT EXISTS messages_search ON messages USING gin(search);
                CREATE TABLE IF NOT EXISTS embeddings(
                    hash text PRIMARY KEY, text text NOT NULL, embedding halfvec(1024),
                    attempts integer NOT NULL DEFAULT 0, retry_at timestamptz NOT NULL DEFAULT now(),
                    last_error text, created_at timestamptz NOT NULL DEFAULT now(), embedded_at timestamptz);
                ALTER TABLE embeddings ADD COLUMN IF NOT EXISTS priority integer NOT NULL DEFAULT 0;
                CREATE INDEX IF NOT EXISTS embeddings_pending ON embeddings(retry_at,created_at) WHERE embedding IS NULL;
                CREATE INDEX IF NOT EXISTS embeddings_priority ON embeddings(priority DESC,retry_at,created_at) WHERE embedding IS NULL;
                CREATE INDEX IF NOT EXISTS embeddings_hnsw ON embeddings USING hnsw(embedding halfvec_cosine_ops)
                    WITH (m=8,ef_construction=32);
                CREATE TABLE IF NOT EXISTS message_chunks(
                    message_pk bigint REFERENCES messages(id) ON DELETE CASCADE,
                    ordinal integer NOT NULL, hash text NOT NULL REFERENCES embeddings(hash),
                    PRIMARY KEY(message_pk,ordinal));
                CREATE INDEX IF NOT EXISTS message_chunks_hash ON message_chunks(hash);
                CREATE TABLE IF NOT EXISTS worker_state(
                    name text PRIMARY KEY, data jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
            ''')
            row = db.execute("SELECT value FROM memory_meta WHERE key='model'").fetchone()
            if row and row['value'] != MODEL:
                raise RuntimeError('Embedding model changed; use a separate index')
            db.execute("INSERT INTO memory_meta VALUES('model',%s) ON CONFLICT DO NOTHING", (MODEL,))

    def drop_test_schema(self):
        if not self.schema.startswith('memory_test_'):
            raise ValueError('Only synthetic test schemas can be dropped')
        with self.connection() as db:
            db.execute('DROP SCHEMA ' + self.schema + ' CASCADE')

    def bind_model(self, identity):
        with self.connection() as db:
            db.execute("INSERT INTO memory_meta VALUES('model_identity',%s) ON CONFLICT DO NOTHING", (identity,))
        self.check_model(identity)

    def check_model(self, identity):
        with self.connection() as db:
            row=db.execute("SELECT value FROM memory_meta WHERE key='model_identity'").fetchone()
            model=db.execute("SELECT value FROM memory_meta WHERE key='model'").fetchone()
        if not row or row['value']!=identity or not model or model['value']!=MODEL:
            raise RuntimeError('Model fingerprint differs from this index; rebuild with a separate model index')

    def state(self, source):
        with self.connection() as db:
            row = db.execute('SELECT * FROM source_state WHERE source=%s', (source,)).fetchone()
        return row or dict(source=source, cursor=0, change_seq=0, backfill_done=False, source_stats={},source_identity=None,change_token=None)

    def reset_source(self,source,identity):
        if source not in SOURCES:
            raise ValueError('Unknown source')
        with self.connection('120s') as db:
            db.execute('DELETE FROM messages WHERE source=%s',(source,))
            db.execute('''INSERT INTO source_state(source,source_identity) VALUES(%s,%s)
                ON CONFLICT(source) DO UPDATE SET cursor=0,change_seq=0,backfill_done=false,
                source_identity=excluded.source_identity,change_token=NULL,source_stats='{}',updated_at=now()''',(source,identity))

    def sync_group_consent(self, groups, *, cleanup=True):
        """Publish deny-by-default policy first; clean derived data in bounded batches."""
        groups = sorted(set(groups))
        with self.connection('15s') as db:
            db.execute("INSERT INTO memory_meta VALUES('whatsapp_consent_refreshed_at',%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(time.time()),))
            current = [r['chat_jid'] for r in db.execute('SELECT chat_jid FROM group_monitoring_consent ORDER BY chat_jid')]
            initialized = db.execute("SELECT 1 FROM memory_meta WHERE key='group_consent_initialized'").fetchone()
            if current != groups or not initialized:
                db.execute('DELETE FROM group_monitoring_consent')
                for group in groups:
                    db.execute('INSERT INTO group_monitoring_consent VALUES(%s)', (group,))
                db.execute("INSERT INTO memory_meta VALUES('group_consent_initialized','1') ON CONFLICT DO NOTHING")
                for key,value in [('group_consent_cleanup','pending'),('group_orphan_cursor',''),('group_message_cursor','0'),('group_cleanup_phase','messages')]:
                    db.execute('INSERT INTO memory_meta VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,value))
        if not cleanup:
            return
        # Policy is already committed. Timeout during cleanup cannot restore permission.
        try:
            self.cleanup_group_consent()
        except Exception as error:
            from psycopg.errors import QueryCanceled
            if not isinstance(error, QueryCanceled):
                raise

    def cleanup_group_consent(self):
        """At most 200 messages and 500 historical embedding candidates per cycle."""
        with self.connection('15s') as db:
            state = db.execute("SELECT value FROM memory_meta WHERE key='group_consent_cleanup'").fetchone()
            if not state or state['value'] != 'pending':
                return
            phase = db.execute("SELECT value FROM memory_meta WHERE key='group_cleanup_phase'").fetchone()
            if not phase or phase['value'] == 'messages':
                cursor = db.execute("SELECT value FROM memory_meta WHERE key='group_message_cursor'").fetchone()
                # Scan a fixed number of primary keys, including other sources. A
                # LIMIT on matching denied rows alone would still scan unbounded data.
                candidates = db.execute('SELECT id FROM messages WHERE id>%s ORDER BY id LIMIT 200',
                                        (int(cursor['value']) if cursor else 0,)).fetchall()
                if candidates:
                    db.execute("""DELETE FROM messages m WHERE m.id=ANY(%s) AND m.source='whatsapp'
                        AND m.chat_id LIKE '%%@g.us' AND NOT EXISTS(
                            SELECT 1 FROM group_monitoring_consent g WHERE g.chat_jid=m.chat_id)""",
                        ([r['id'] for r in candidates],))
                    db.execute("UPDATE memory_meta SET value=%s WHERE key='group_message_cursor'",(str(candidates[-1]['id']),))
                if len(candidates) == 200:
                    return
                db.execute("UPDATE memory_meta SET value='embeddings' WHERE key='group_cleanup_phase'")
            cursor = db.execute("SELECT value FROM memory_meta WHERE key='group_orphan_cursor'").fetchone()
            candidates = db.execute('SELECT hash FROM embeddings WHERE hash>%s ORDER BY hash LIMIT 500',
                                     (cursor['value'] if cursor else '',)).fetchall()
            if candidates:
                db.execute("""DELETE FROM embeddings e WHERE e.hash=ANY(%s)
                    AND NOT EXISTS(SELECT 1 FROM message_chunks c WHERE c.hash=e.hash)""",
                    ([r['hash'] for r in candidates],))
                db.execute("UPDATE memory_meta SET value=%s WHERE key='group_orphan_cursor'",(candidates[-1]['hash'],))
            if len(candidates) < 500:
                db.execute("UPDATE memory_meta SET value='done' WHERE key='group_consent_cleanup'")

    def sync_telegram_consent(self, allowed_groups, channels, *, cleanup=True):
        """Publish deny-by-default policy first; clean derived data in bounded batches."""
        groups = sorted(set(map(str, allowed_groups)) | set(map(str, channels)))
        with self.connection('15s') as db:
            current = [r['chat_id'] for r in db.execute('SELECT chat_id FROM telegram_monitoring_allowed ORDER BY chat_id')]
            initialized = db.execute("SELECT 1 FROM memory_meta WHERE key='telegram_consent_initialized'").fetchone()
            db.execute("INSERT INTO memory_meta VALUES('telegram_consent_refreshed_at',%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(time.time()),))
            if current != groups or not initialized:
                db.execute('DELETE FROM telegram_monitoring_allowed')
                for group in groups:
                    db.execute('INSERT INTO telegram_monitoring_allowed VALUES(%s)', (group,))
                db.execute("INSERT INTO memory_meta VALUES('telegram_consent_initialized','1') ON CONFLICT DO NOTHING")
                for key,value in [('telegram_consent_cleanup','pending'),('telegram_orphan_cursor',''),('telegram_message_cursor','0'),('telegram_cleanup_phase','messages')]:
                    db.execute('INSERT INTO memory_meta VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,value))
        if not cleanup:
            return
        # Policy is already committed. Timeout during cleanup cannot restore permission.
        try:
            self.cleanup_telegram_consent()
        except Exception as error:
            from psycopg.errors import QueryCanceled
            if not isinstance(error, QueryCanceled):
                raise

    def cleanup_telegram_consent(self):
        """At most 200 messages and 500 historical embedding candidates per cycle."""
        with self.connection('15s') as db:
            state = db.execute("SELECT value FROM memory_meta WHERE key='telegram_consent_cleanup'").fetchone()
            if not state or state['value'] != 'pending':
                return
            phase = db.execute("SELECT value FROM memory_meta WHERE key='telegram_cleanup_phase'").fetchone()
            if not phase or phase['value'] == 'messages':
                cursor = db.execute("SELECT value FROM memory_meta WHERE key='telegram_message_cursor'").fetchone()
                # Scan a fixed number of primary keys, including other sources. A
                # LIMIT on matching denied rows alone would still scan unbounded data.
                candidates = db.execute('SELECT id FROM messages WHERE id>%s ORDER BY id LIMIT 200',
                                        (int(cursor['value']) if cursor else 0,)).fetchall()
                if candidates:
                    db.execute("""DELETE FROM messages m WHERE m.id=ANY(%s) AND m.source='telegram'
                        AND m.chat_id LIKE '-%%' AND NOT EXISTS(
                            SELECT 1 FROM telegram_monitoring_allowed g WHERE g.chat_id=m.chat_id)""",
                        ([r['id'] for r in candidates],))
                    db.execute("UPDATE memory_meta SET value=%s WHERE key='telegram_message_cursor'",(str(candidates[-1]['id']),))
                if len(candidates) == 200:
                    return
                db.execute("UPDATE memory_meta SET value='embeddings' WHERE key='telegram_cleanup_phase'")
            cursor = db.execute("SELECT value FROM memory_meta WHERE key='telegram_orphan_cursor'").fetchone()
            candidates = db.execute('SELECT hash FROM embeddings WHERE hash>%s ORDER BY hash LIMIT 500',
                                     (cursor['value'] if cursor else '',)).fetchall()
            if candidates:
                db.execute("""DELETE FROM embeddings e WHERE e.hash=ANY(%s)
                    AND NOT EXISTS(SELECT 1 FROM message_chunks c WHERE c.hash=e.hash)""",
                    ([r['hash'] for r in candidates],))
                db.execute("UPDATE memory_meta SET value=%s WHERE key='telegram_orphan_cursor'",(candidates[-1]['hash'],))
            if len(candidates) < 500:
                db.execute("UPDATE memory_meta SET value='done' WHERE key='telegram_consent_cleanup'")

    def apply(self, records, source, *, deleted=(), cursor=None, change_seq=None, change_token=None, backfill_done=None, source_stats=None):
        """Messages and ingestion checkpoint commit together; replay is idempotent."""
        from psycopg.types.json import Jsonb
        with self.connection('120s') as db:
            for chat_id, message_id in deleted:
                db.execute('DELETE FROM messages WHERE source=%s AND chat_id=%s AND message_id=%s',
                           (source, chat_id, message_id))
            for record in records:
                content = {k: record[k] for k in ('source','chat_id','message_id','timestamp','sender','chat_name','text','media_type')}
                fingerprint = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()
                previous = db.execute('SELECT id,content_hash FROM messages WHERE source=%s AND chat_id=%s AND message_id=%s',
                                      (content['source'], content['chat_id'], content['message_id'])).fetchone()
                if previous and previous['content_hash'] == fingerprint:
                    continue
                values = [content[k] for k in ('source','chat_id','message_id','timestamp','sender','chat_name','text','media_type')]
                row = db.execute('''INSERT INTO messages(source,chat_id,message_id,timestamp,sender,chat_name,text,media_type,content_hash)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source,chat_id,message_id) DO UPDATE SET
                    timestamp=excluded.timestamp,sender=excluded.sender,chat_name=excluded.chat_name,text=excluded.text,
                    media_type=excluded.media_type,content_hash=excluded.content_hash RETURNING id''', values + [fingerprint]).fetchone()
                message_pk = row['id']
                db.execute('DELETE FROM message_chunks WHERE message_pk=%s', (message_pk,))
                for ordinal, text in enumerate(chunks(content['text']) if content['text'].strip() else []):
                    hash_ = digest(text)
                    priority=(3 if int(content['timestamp'])>=time.time()-7*86400 else 1) if change_seq is not None else 0
                    db.execute('''INSERT INTO embeddings(hash,text,priority) VALUES(%s,%s,%s)
                        ON CONFLICT(hash) DO UPDATE SET priority=greatest(embeddings.priority,excluded.priority)
                        WHERE embeddings.embedding IS NULL AND embeddings.priority<excluded.priority''', (hash_, text, priority))
                    db.execute('INSERT INTO message_chunks VALUES(%s,%s,%s)', (message_pk,ordinal,hash_))
            db.execute('INSERT INTO source_state(source) VALUES(%s) ON CONFLICT DO NOTHING', (source,))
            updates, values = ['updated_at=now()'], []
            for key, value in [('cursor',cursor),('change_seq',change_seq),('change_token',change_token),('backfill_done',backfill_done),('source_stats',source_stats)]:
                if value is not None:
                    updates.append(key + '=%s')
                    values.append(Jsonb(value) if key == 'source_stats' else value)
            db.execute('UPDATE source_state SET ' + ','.join(updates) + ' WHERE source=%s', values + [source])

    def heartbeat(self, name, data):
        from psycopg.types.json import Jsonb
        with self.connection() as db:
            db.execute('''INSERT INTO worker_state VALUES(%s,%s,now()) ON CONFLICT(name)
                DO UPDATE SET data=excluded.data,updated_at=now()''', (name,Jsonb(data)))

    def pending(self, limit=16):
        freshness = self.telegram_consent_freshness()
        whatsapp_freshness = self.whatsapp_consent_freshness()
        with self.connection() as db:
            return db.execute('''SELECT e.hash,e.text,e.attempts,e.ctid::text AS ctid FROM embeddings e
                WHERE embedding IS NULL AND retry_at<=now()
                AND EXISTS(SELECT 1 FROM message_chunks c JOIN messages m ON m.id=c.message_pk WHERE c.hash=e.hash
                    AND (m.source!='whatsapp' OR m.chat_id NOT LIKE '%%@g.us' OR EXISTS(SELECT 1 FROM group_monitoring_consent gc WHERE gc.chat_jid=m.chat_id) AND ''' + whatsapp_freshness + ''')
                    AND (m.source!='telegram' OR m.chat_id NOT LIKE '-%%' OR (EXISTS(SELECT 1 FROM telegram_monitoring_allowed tc WHERE tc.chat_id=m.chat_id) AND ''' + freshness + ''')))
                ORDER BY priority DESC,retry_at,created_at LIMIT %s''', (min(limit,1024),)).fetchall()

    def switch_model(self, label, identity):
        """Move this index to another embedding model: same schema, every vector recomputed. One transaction.
        Dropping and re-adding the vector column avoids rewriting every row.
        Dependent indexes go with the column and are rebuilt."""
        with self.connection('0') as db:  # no statement timeout: deliberate one-shot maintenance
            db.execute("INSERT INTO memory_meta VALUES('model',%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (label,))
            db.execute("INSERT INTO memory_meta VALUES('model_identity',%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (identity,))
            db.execute('ALTER TABLE embeddings DROP COLUMN embedding')
            db.execute('ALTER TABLE embeddings ADD COLUMN embedding halfvec(1024)')
            # Only rows in backoff need resetting; embedded_at is informational.
            db.execute('UPDATE embeddings SET attempts=0,retry_at=now(),last_error=NULL WHERE attempts>0 OR retry_at>now()')
            db.execute('CREATE INDEX IF NOT EXISTS embeddings_pending ON embeddings(retry_at,created_at) WHERE embedding IS NULL')
            db.execute('CREATE INDEX IF NOT EXISTS embeddings_priority ON embeddings(priority DESC,retry_at,created_at) WHERE embedding IS NULL')
            db.execute('CREATE INDEX IF NOT EXISTS embeddings_hnsw ON embeddings USING hnsw(embedding halfvec_cosine_ops) WITH (m=8,ef_construction=32)')
        with self.connection('0') as db:
            db.execute('ANALYZE embeddings')  # fresh indexes without statistics made pending() plan a 15 s+ sort
        global MODEL
        MODEL = label

    def save_embeddings(self, rows, ctids=None):
        """One statement per batch. With ctids (from pending()) the rows are addressed by physical
        position, avoiding a separate hash index lookup.
        The hash is still matched, so a row rewritten in between is simply left pending."""
        rows = list(rows)
        if not rows:
            return
        hashes = [h for h, _ in rows]
        vectors = [vector_literal(v) for _, v in rows]
        with self.connection('120s') as db:
            # A lost commit only means those rows are embedded again; the fsync per batch is not worth it on this disk.
            db.execute('SET LOCAL synchronous_commit=off')
            if ctids and len(ctids) == len(rows):
                db.execute('''UPDATE embeddings e SET embedding=v.emb::halfvec,embedded_at=now(),last_error=NULL
                    FROM unnest(%s::text[],%s::text[],%s::text[]) AS v(hash,emb,tid)
                    WHERE e.ctid=v.tid::tid AND e.hash=v.hash''', (hashes, vectors, list(ctids)))
            else:
                db.execute('''UPDATE embeddings e SET embedding=v.emb::halfvec,embedded_at=now(),last_error=NULL
                    FROM unnest(%s::text[],%s::text[]) AS v(hash,emb) WHERE e.hash=v.hash''', (hashes, vectors))

    def set_bulk_mode(self, enabled):
        """During a full re-embed autovacuum on the table only competes for the disk; vacuum once afterwards."""
        with self.connection('60s') as db:
            db.execute('ALTER TABLE embeddings SET (autovacuum_enabled=%s)' % ('false' if enabled else 'true'))

    def embedding_failed(self, hashes, reason, delay=60):
        with self.connection() as db:
            db.execute('''UPDATE embeddings SET attempts=attempts+1,last_error=%s,
                retry_at=now()+(%s * interval '1 second') WHERE hash=ANY(%s)''', (reason[:160],delay,list(hashes)))

    @staticmethod
    def whatsapp_consent_freshness():
        cutoff = time.time() - 60
        return f"EXISTS(SELECT 1 FROM memory_meta WHERE key='whatsapp_consent_refreshed_at' AND CAST(value AS double precision)>{cutoff})"

    @staticmethod
    def telegram_consent_freshness():
        # Successful source synchronization grants a 60-second lease. A failed
        # worker cannot leave group approval valid indefinitely.
        cutoff = time.time() - 60
        return f"EXISTS(SELECT 1 FROM memory_meta WHERE key='telegram_consent_refreshed_at' AND CAST(value AS double precision)>{cutoff})"

    def filters(self, source=None, chat=None, after=None, before=None, sender=None, alias='m'):
        if source is not None and source not in SOURCES:
            raise ValueError('source must be whatsapp or telegram')
        clauses = [f"({alias}.source!='whatsapp' OR {alias}.chat_id NOT LIKE '%%@g.us' OR EXISTS(SELECT 1 FROM group_monitoring_consent gc WHERE gc.chat_jid={alias}.chat_id) AND {self.whatsapp_consent_freshness()})"]
        clauses.append(f"({alias}.source!='telegram' OR {alias}.chat_id NOT LIKE '-%%' OR (EXISTS(SELECT 1 FROM telegram_monitoring_allowed tc WHERE tc.chat_id={alias}.chat_id) AND {self.telegram_consent_freshness()}))")
        values = []
        for key, value, op in [('source',source,'='),('chat_id',chat,'='),('timestamp',after,'>='),('timestamp',before,'<='),('sender',sender,'=')]:
            if value is not None:
                clauses.append(alias + '.' + key + op + '%s')
                values.append(value)
        return ' AND '.join(clauses) or 'true', values

    def lexical(self, query, *, limit=20, timeout='8s', **filters):
        where, values = self.filters(**filters)
        with self.connection(timeout) as db:
            return db.execute('''SELECT m.id,m.source,m.chat_id,m.message_id,m.timestamp,m.sender,m.chat_name,
                    left(m.text,4000) AS text,m.media_type,ts_rank_cd(m.search,q) AS score
                FROM messages m,websearch_to_tsquery('simple',%s) q WHERE m.search @@ q AND ''' + where +
                ' ORDER BY score DESC,m.timestamp DESC LIMIT %s', [query] + values + [min(limit,100)]).fetchall()

    def semantic(self, vector, *, limit=20, timeout='8s', **filters):
        where, values = self.filters(**filters)
        literal = vector_literal(vector)
        with self.connection(timeout) as db:
            db.execute("SET LOCAL hnsw.iterative_scan='strict_order'")
            db.execute('SET LOCAL hnsw.ef_search=100')
            sql,params=semantic_statement(where,values,literal,limit)
            return db.execute(sql,params).fetchall()

    def status(self, source=None, timeout='30s'):
        where, values = self.filters(source=source)
        with self.connection(timeout) as db:
            # Ordinal zero identifies embeddable text without reading/decompressing
            # bodies. Hash joins keep coverage linear as the ready-vector set grows.
            rows = db.execute('''WITH ready AS MATERIALIZED (
                    SELECT hash FROM embeddings WHERE embedding IS NOT NULL),
                extra_pending AS MATERIALIZED (
                    SELECT DISTINCT c.message_pk FROM message_chunks c
                    LEFT JOIN ready r ON r.hash=c.hash
                    WHERE c.ordinal>0 AND r.hash IS NULL)
                SELECT m.source,count(*) AS messages,count(c.message_pk) AS text_messages,
                    count(*) FILTER(WHERE r.hash IS NOT NULL AND p.message_pk IS NULL) AS embedded_messages,
                    min(m.timestamp) AS first_timestamp,max(m.timestamp) AS last_timestamp
                FROM messages m LEFT JOIN message_chunks c ON c.message_pk=m.id AND c.ordinal=0
                LEFT JOIN ready r ON r.hash=c.hash LEFT JOIN extra_pending p ON p.message_pk=m.id
                WHERE ''' + where + ' GROUP BY m.source', values).fetchall()
            states = db.execute('SELECT * FROM source_state' + (' WHERE source=%s' if source else ''), [source] if source else []).fetchall()
            workers = db.execute('SELECT * FROM worker_state').fetchall()
            # Group during the existing coverage scan; requests select a cached
            # source without repeating the expensive chunk/embedding joins.
            by_source = {row['source']: dict(row) for row in rows}
            parts = {}
            for kind in ([source] if source else SOURCES):
                part = by_source.get(kind, dict(messages=0, text_messages=0, embedded_messages=0,
                                                first_timestamp=None, last_timestamp=None))
                scoped_states = [s for s in states if s['source'] == kind]
                part.update(source=kind, states=scoped_states, source_history_complete=False,
                            semantic_complete=bool(scoped_states) and all(s['backfill_done'] for s in scoped_states)
                            and part['text_messages'] == part['embedded_messages'])
                parts[kind] = part
            if source:
                result = dict(parts[source])
            else:
                result = {key: sum(p[key] for p in parts.values())
                          for key in ('messages', 'text_messages', 'embedded_messages')}
                first = [p['first_timestamp'] for p in parts.values() if p['first_timestamp'] is not None]
                last = [p['last_timestamp'] for p in parts.values() if p['last_timestamp'] is not None]
                result.update(source='all', states=states, per_source=parts,
                              first_timestamp=min(first) if first else None, last_timestamp=max(last) if last else None,
                              semantic_complete=bool(states) and all(s['backfill_done'] for s in states)
                              and result['text_messages'] == result['embedded_messages'], source_history_complete=False)
            result.update(model=MODEL, workers=workers,
                          coverage_note='Index covers messages delivered by source clients. Initial embedding and upstream history sync may still be running. Media without text needs a transcript or extraction.')
            return result

    def cheap_status(self, source=None):
        """Millisecond-class figures for the live status line: lag, pending embeddings, recent ingestion.
        Without a source the per-source figures are combined so every query stays on (source,timestamp)."""
        if source is None:
            parts = [self.cheap_status(s) for s in SOURCES]
            lags = [p['lag_s'] for p in parts if p['lag_s'] is not None]
            latest = [p['last_indexed_at'] for p in parts if p['last_indexed_at']]
            return dict(lag_s=max(lags) if lags else None, pending_embeddings=parts[0]['pending_embeddings'],
                        ingested_last_hour=sum(p['ingested_last_hour'] for p in parts),
                        last_indexed_at=max(latest) if latest else None)
        where, values = self.filters(source=source)
        now = int(time.time())
        with self.connection('3s') as db:
            latest = db.execute('SELECT max(timestamp) AS ts FROM messages m WHERE ' + where + ' AND m.timestamp > %s',
                                values + [now - 86400]).fetchone()['ts']
            hour = db.execute('SELECT count(*) AS n FROM messages m WHERE ' + where + ' AND m.timestamp > %s',
                              values + [now - 3600]).fetchone()['n']
            bounded = db.execute("""SELECT count(*) AS n FROM (SELECT 1 FROM embeddings WHERE embedding IS NULL
                AND (retry_at IS NULL OR retry_at <= now()) LIMIT 5001) s""").fetchone()['n']
            estimate = int(db.execute("SELECT reltuples FROM pg_class WHERE relname='embeddings_priority'").fetchone()['reltuples'] or 0)
            states = db.execute('SELECT source, source_stats FROM source_state' + (' WHERE source=%s' if source else ''),
                                [source] if source else []).fetchall()
        source_latest = max((int((s['source_stats'] or {}).get('last_timestamp') or 0) for s in states), default=0)
        lag = max(0, source_latest - int(latest)) if latest and source_latest else None
        return dict(lag_s=lag, pending_embeddings=bounded if bounded <= 5000 else max(estimate, bounded),
                    ingested_last_hour=hour, last_indexed_at=int(latest) if latest else None)

    def worker_status(self):
        with self.connection('3s') as db:
            return db.execute("SELECT * FROM worker_state WHERE name NOT IN ('status','status_error') ORDER BY name").fetchall()

    def status_error(self):
        with self.connection('3s') as db:
            row = db.execute("SELECT data FROM worker_state WHERE name='status_error'").fetchone()
        return row['data'] if row else None

    def status_snapshot(self):
        """The heavy coverage figures the worker computed last, or None before its first pass."""
        with self.connection('3s') as db:
            row = db.execute("SELECT data, updated_at FROM worker_state WHERE name='status'").fetchone()
        return dict(row) if row else None

    def analytics(self, *, source=None, chat=None, after=None, before=None, sender=None, group_by='month', limit=50):
        groups = {'month':"to_char(to_timestamp(m.timestamp) AT TIME ZONE 'UTC','YYYY-MM')",
                  'day':"to_char(to_timestamp(m.timestamp) AT TIME ZONE 'UTC','YYYY-MM-DD')",
                  'chat':'m.chat_id','sender':'m.sender','media_type':'m.media_type','source':'m.source'}
        if group_by not in groups:
            raise ValueError('group_by must be month, day, chat, sender, media_type or source')
        where, values = self.filters(source=source,chat=chat,after=after,before=before,sender=sender)
        with self.connection('15s') as db:
            # Total and limited groups use one scan and one MVCC snapshot while
            # the ingestion worker continues inserting messages.
            result = db.execute('''WITH grouped AS MATERIALIZED (
                    SELECT ''' + groups[group_by] + ''' AS group_key,count(*) AS messages
                    FROM messages m WHERE ''' + where + ''' GROUP BY 1),
                limited AS (
                    SELECT * FROM grouped ORDER BY messages DESC,group_key LIMIT %s)
                SELECT COALESCE((SELECT sum(messages) FROM grouped),0)::bigint AS total,
                    COALESCE((SELECT jsonb_agg(to_jsonb(limited) ORDER BY messages DESC,group_key)
                              FROM limited),'[]'::jsonb) AS groups''', values + [min(limit,100)]).fetchone()
        return dict(source=source or 'all',group_by=group_by,total=result['total'],groups=result['groups'],
                    note='Counts reflect imported original messages; embedding completion does not affect counts.')
