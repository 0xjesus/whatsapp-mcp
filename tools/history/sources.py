"""Read messaging caches and capture committed changes without account clients.

Only install_capture writes: it adds its own queue and triggers. The consumer
must persist its cursor after committing the corresponding index transaction.
Capture a watermark before paginating the initial snapshot, then replay changes
after that watermark. Resolve each event with get; events contain identities,
not historical bodies. This makes retries and WhatsApp REPLACE writes harmless.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import time
import uuid


PREFIX = '[Nota de voz] '
MAX_PAGE = 1000
FIELDS = {
    'whatsapp': dict(chat='chat_jid', chat_key='jid', title='name', body='content',
                     date='timestamp', sender='sender', transcript_id='message_id',
                     voice='audio'),
    'telegram': dict(chat='chat_id', chat_key='id', title='title', body='text',
                     date='date', sender='sender_id', transcript_id='id',
                     voice='voice'),
}


def _epoch(value):
    if value is None or value == '':
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _limit(value):
    return max(1, min(int(value), MAX_PAGE))


class Source:
    def __init__(self, kind: str, path: Path):
        self.kind = {'wa': 'whatsapp', 'tg': 'telegram'}.get(kind, kind)
        if self.kind not in FIELDS:
            raise ValueError('unknown messaging source')
        self.path = Path(path).expanduser().absolute()
        self.fields = FIELDS[self.kind]

    @contextmanager
    def _db(self, write=False):
        c = sqlite3.connect(self.path.as_uri() + ('?mode=rw' if write else '?mode=ro'),
                            uri=True, timeout=5)
        c.row_factory = sqlite3.Row
        try:
            if write:
                c.execute('BEGIN IMMEDIATE')
            else:
                c.execute('PRAGMA query_only=ON')
                deadline = time.monotonic() + 5
                c.set_progress_handler(lambda: time.monotonic() > deadline, 10000)
            yield c
            if write:
                c.commit()
        finally:
            c.close()

    @staticmethod
    def _has(c, table):
        return c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                         (table,)).fetchone() is not None

    def install_capture(self):
        """Atomically add capture to this cache; never creates a missing database."""
        f = self.fields
        chat = f['chat']
        with self._db(write=True) as c:
            required = {chat, 'id', f['body'], f['date'], f['sender'], 'media_type'}
            columns = {r['name'] for r in c.execute('PRAGMA table_info(messages)')}
            if not required <= columns or self.kind == 'telegram' and 'deleted' not in columns:
                raise ValueError('unsupported messaging cache schema')
            c.execute('''CREATE TABLE IF NOT EXISTS memory_capture_meta(
                key TEXT PRIMARY KEY,value TEXT NOT NULL)''')
            c.execute("INSERT OR IGNORE INTO memory_capture_meta VALUES('capture_uuid',?)",
                      (str(uuid.uuid4()),))
            c.execute('''CREATE TABLE IF NOT EXISTS memory_changes(
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL, message_id TEXT NOT NULL, op TEXT NOT NULL,
                event_token TEXT NOT NULL DEFAULT (lower(hex(randomblob(16)))))''')
            queue_columns = {r['name'] for r in c.execute('PRAGMA table_info(memory_changes)')}
            if 'event_token' not in queue_columns:
                c.execute('ALTER TABLE memory_changes ADD COLUMN event_token TEXT')
                c.execute('UPDATE memory_changes SET event_token=lower(hex(randomblob(16)))')
            # ALTER TABLE cannot add a nonconstant default. This trigger also covers
            # legacy message/transcript triggers whose INSERTs omit the new column.
            c.execute('''CREATE TRIGGER IF NOT EXISTS memory_changes_token
                AFTER INSERT ON memory_changes WHEN new.event_token IS NULL BEGIN
                UPDATE memory_changes SET event_token=lower(hex(randomblob(16)))
                WHERE seq=new.seq; END''')
            if self.kind == 'whatsapp':
                c.execute("""CREATE TABLE IF NOT EXISTS group_monitoring_consent(
                    chat_jid TEXT PRIMARY KEY,allowed INTEGER NOT NULL DEFAULT 0,
                    evidence TEXT NOT NULL,updated_at TEXT NOT NULL)""")
                for table in ('messages','reactions','poll_votes','message_mutations','view_once_media','transcripts','attachment_analysis'):
                    guard_columns = {row['name'] for row in c.execute('PRAGMA table_info('+table+')')}
                    chat_column = 'poll_chat_jid' if table == 'poll_votes' else 'chat_jid'
                    if chat_column not in guard_columns:
                        continue
                    for event in ('INSERT','UPDATE'):
                        c.execute(f"DROP TRIGGER IF EXISTS monitoring_guard_{table}_{event.lower()}")
                        c.execute(f"""CREATE TRIGGER IF NOT EXISTS monitoring_guard_{table}_{event.lower()}
                            BEFORE {event} ON {table} WHEN new.{chat_column} LIKE '%@g.us'
                            AND NOT EXISTS(SELECT 1 FROM group_monitoring_consent g WHERE g.chat_jid=new.{chat_column} AND g.allowed=1 AND (g.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',g.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900))
                            BEGIN SELECT RAISE(IGNORE); END""")
                self._install_group_rescan(c, 'chat_jid', 'memory_consent')
            if self.kind == 'telegram':
                c.execute("""CREATE TABLE IF NOT EXISTS group_monitoring_consent(
                    chat_id INTEGER PRIMARY KEY,allowed INTEGER NOT NULL DEFAULT 0,
                    evidence TEXT NOT NULL,updated_at TEXT NOT NULL)""")
                typed = 'type' in {r['name'] for r in c.execute('PRAGMA table_info(chats)')}
                channel = " OR EXISTS(SELECT 1 FROM chats WHERE id=new.chat_id AND type='channel')" if typed else ''
                for table in ('messages', 'transcripts', 'attachment_analysis', 'message_reactions'):
                    if not self._has(c, table):
                        continue
                    for event in ('INSERT', 'UPDATE'):
                        c.execute(f"DROP TRIGGER IF EXISTS memory_telegram_guard_{table}_{event.lower()}")
                        c.execute(f"""CREATE TRIGGER IF NOT EXISTS memory_telegram_guard_{table}_{event.lower()}
                            BEFORE {event} ON {table} WHEN NOT (new.chat_id>0{channel}
                            OR EXISTS(SELECT 1 FROM group_monitoring_consent g WHERE g.chat_id=new.chat_id AND g.allowed=1 AND (g.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',g.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)))
                            BEGIN SELECT RAISE(IGNORE); END""")
                self._install_group_rescan(c, 'chat_id', 'memory_telegram_consent')
                if typed:
                    for event in ('INSERT','UPDATE','DELETE'):
                        prefix = 'old' if event == 'DELETE' else 'new'
                        c.execute(f'DROP TRIGGER IF EXISTS memory_telegram_chat_{event.lower()}')
                        condition = ' WHEN old.type IS NOT new.type OR old.id IS NOT new.id' if event == 'UPDATE' else ''
                        affected = 'chat_id IN (old.id,new.id)' if event == 'UPDATE' else f'chat_id={prefix}.id'
                        c.execute(f"""CREATE TRIGGER memory_telegram_chat_{event.lower()}
                            AFTER {event} ON chats{condition} BEGIN
                            INSERT INTO memory_changes(chat_id,message_id,op)
                            SELECT CAST(chat_id AS TEXT),CAST(id AS TEXT),'upsert' FROM messages WHERE {affected};
                            END""")
            watched = [chat, 'id', f['body'], f['date'], f['sender'], 'media_type']
            if self.kind == 'whatsapp' and 'file_sha256' in columns:
                watched += ['file_sha256']
            if self.kind == 'telegram':
                watched += ['deleted']
                if 'media_hash' in columns: watched += ['media_hash']
            changed = ' OR '.join(f'old.{x} IS NOT new.{x}' for x in watched)
            new_op = "CASE WHEN new.deleted=1 THEN 'delete' ELSE 'upsert' END" if self.kind == 'telegram' else "'upsert'"
            enqueue_new = f'''INSERT INTO memory_changes(chat_id,message_id,op)
                VALUES(CAST(new.{chat} AS TEXT),CAST(new.id AS TEXT),{new_op});'''
            enqueue_old = f'''INSERT INTO memory_changes(chat_id,message_id,op)
                VALUES(CAST(old.{chat} AS TEXT),CAST(old.id AS TEXT),'delete');'''
            c.execute(f'''CREATE TRIGGER IF NOT EXISTS memory_messages_insert
                AFTER INSERT ON messages BEGIN {enqueue_new} END''')
            c.execute(f'''CREATE TRIGGER IF NOT EXISTS memory_messages_delete
                AFTER DELETE ON messages BEGIN {enqueue_old} END''')
            c.execute('DROP TRIGGER IF EXISTS memory_messages_update')
            c.execute(f'''CREATE TRIGGER IF NOT EXISTS memory_messages_update
                AFTER UPDATE OF {','.join(watched)} ON messages WHEN {changed} BEGIN
                INSERT INTO memory_changes(chat_id,message_id,op)
                SELECT CAST(old.{chat} AS TEXT),CAST(old.id AS TEXT),'delete'
                WHERE old.{chat} IS NOT new.{chat} OR old.id IS NOT new.id;
                {enqueue_new} END''')
            if self._has(c, 'attachment_analysis'):
                for event, prefix in [('INSERT', 'new'), ('UPDATE', 'new'), ('DELETE', 'old')]:
                    c.execute(f'''CREATE TRIGGER IF NOT EXISTS memory_attachment_{event.lower()}
                        AFTER {event} ON attachment_analysis BEGIN
                        INSERT INTO memory_changes(chat_id,message_id,op)
                        VALUES({prefix}.{chat},{prefix}.message_id,'upsert'); END''')
            if self.kind == 'telegram' and self._has(c, 'message_reactions'):
                for event, prefix in [('INSERT','new'),('UPDATE','new'),('DELETE','old')]:
                    c.execute(f'''CREATE TRIGGER IF NOT EXISTS memory_reactions_{event.lower()}
                        AFTER {event} ON message_reactions BEGIN
                        INSERT INTO memory_changes(chat_id,message_id,op)
                        VALUES({prefix}.chat_id,{prefix}.message_id,'upsert'); END''')
            if self._has(c, 'transcripts'):
                ident = f['transcript_id']
                def transcript_event(prefix):
                    return f'''INSERT INTO memory_changes(chat_id,message_id,op)
                        VALUES(CAST({prefix}.{chat} AS TEXT),CAST({prefix}.{ident} AS TEXT),'upsert');'''
                for event, prefix in [('INSERT', 'new'), ('DELETE', 'old')]:
                    c.execute(f'''CREATE TRIGGER IF NOT EXISTS memory_transcripts_{event.lower()}
                        AFTER {event} ON transcripts BEGIN {transcript_event(prefix)} END''')
                watched = [chat, ident, 'text', 'status']
                changed = ' OR '.join(f'old.{x} IS NOT new.{x}' for x in watched)
                c.execute(f'''CREATE TRIGGER IF NOT EXISTS memory_transcripts_update
                    AFTER UPDATE OF {','.join(watched)} ON transcripts WHEN {changed} BEGIN
                    INSERT INTO memory_changes(chat_id,message_id,op)
                    SELECT CAST(old.{chat} AS TEXT),CAST(old.{ident} AS TEXT),'upsert'
                    WHERE old.{chat} IS NOT new.{chat} OR old.{ident} IS NOT new.{ident};
                    {transcript_event('new')} END''')

    def _select(self, c):
        f = self.fields
        projection = f'''SELECT m.rowid AS rowid,CAST(m.{f['chat']} AS TEXT) AS chat_id,
            CAST(m.id AS TEXT) AS message_id,m.{f['date']} AS timestamp,
            CAST(m.{f['sender']} AS TEXT) AS sender,c.{f['title']} AS chat_name,
            m.{f['body']} AS text,m.media_type'''
        join = f" FROM messages m LEFT JOIN chats c ON c.{f['chat_key']}=m.{f['chat']}"
        if self._has(c, 'transcripts'):
            projection += ',t.text AS transcript_text,t.status AS transcript_status'
            join += f" LEFT JOIN transcripts t ON t.{f['chat']}=m.{f['chat']} AND t.{f['transcript_id']}=m.id"
        else:
            projection += ',NULL AS transcript_text,NULL AS transcript_status'
        if self.kind == 'whatsapp' and self._has(c, 'attachment_analysis'):
            projection += ',a.text AS attachment_text,a.status AS attachment_status'
            join += " LEFT JOIN attachment_analysis a ON a.chat_jid=m.chat_jid AND a.message_id=m.id AND a.media_hash=hex(COALESCE(m.file_sha256,X''))"
        elif (self.kind == 'telegram' and self._has(c, 'attachment_analysis')
              and 'media_hash' in {r['name'] for r in c.execute('PRAGMA table_info(messages)')}):
            projection += ',a.text AS attachment_text,a.status AS attachment_status'
            join += " LEFT JOIN attachment_analysis a ON a.chat_id=m.chat_id AND a.message_id=m.id AND m.media_hash!='' AND a.media_hash=m.media_hash AND m.deleted=0"
        else:
            projection += ',NULL AS attachment_text,NULL AS attachment_status'
        if self.kind == 'telegram' and self._has(c, 'message_reactions'):
            projection += ',r.text AS reactions_text'
            join += ' LEFT JOIN message_reactions r ON r.chat_id=m.chat_id AND r.message_id=m.id'
        else:
            projection += ',NULL AS reactions_text'
        return projection + join

    def _normalize(self, row):
        text = row['text'] or ''
        placeholder = not text.strip() or text.startswith('[Nota de voz]') or text.strip().lower() in {
            '[audio]', '[voice]', '[voice note]', '(inaudible)', '[nota de voz]',
        }
        if (row['media_type'] == self.fields['voice'] and placeholder
                and row['transcript_status'] == 'done'):
            text = PREFIX + ((row['transcript_text'] or '').strip() or '(inaudible)')
        if (row['media_type'] in ('video','video_note')
                and row['transcript_status'] == 'done' and (row['transcript_text'] or '').strip()
                and not any(line.startswith('[Audio del video] ') for line in text.split('\n'))):
            spoken = '[Audio del video] ' + row['transcript_text'].strip()
            text = (text.strip() + '\n' + spoken) if text.strip() else spoken
        if (self.kind == 'telegram' and row['media_type'] == 'audio'
                and row['transcript_status'] == 'done' and (row['transcript_text'] or '').strip()
                and not any(line.startswith('[Audio] ') for line in text.split('\n'))):
            text = text.rstrip() + '\n[Audio] ' + row['transcript_text'].strip()
        if row['reactions_text']:
            text = text.rstrip() + '\n[Reacciones] ' + row['reactions_text']
        if row['attachment_status'] in ('done','partial') and row['attachment_text']:
            label = '[Contenido del archivo]' if row['attachment_status'] == 'done' else '[Contenido del archivo: extracción parcial]'
            text = text + '\n' + label + '\n' + row['attachment_text']
        return dict(rowid=row['rowid'], source=self.kind, chat_id=row['chat_id'],
                    message_id=row['message_id'], timestamp=_epoch(row['timestamp']),
                    sender=row['sender'] or '', chat_name=row['chat_name'] or '',
                    text=text, media_type=row['media_type'] or '')

    def _consent_filter(self, c):
        if self.kind == 'telegram':
            typed = 'type' in {r['name'] for r in c.execute('PRAGMA table_info(chats)')}
            channel = " OR c.type='channel'" if typed else ''
            consent = " OR EXISTS(SELECT 1 FROM group_monitoring_consent g WHERE g.chat_id=m.chat_id AND g.allowed=1 AND (g.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',g.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900))" if self._has(c, 'group_monitoring_consent') else ''
            return f" AND (m.chat_id>0{channel}{consent})"

        if not self._has(c, 'group_monitoring_consent'):
            return " AND m.chat_jid NOT LIKE '%@g.us'"
        return " AND (m.chat_jid NOT LIKE '%@g.us' OR EXISTS(SELECT 1 FROM group_monitoring_consent g WHERE g.chat_jid=m.chat_jid AND g.allowed=1 AND (g.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',g.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)))"

    def allowed_groups(self):
        with self._db() as c:
            if not self._has(c, 'group_monitoring_consent'):
                return []
            column = 'chat_jid' if self.kind == 'whatsapp' else 'chat_id'
            return [str(r[0]) for r in c.execute(f"SELECT {column} FROM group_monitoring_consent WHERE allowed=1 AND (evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900) ORDER BY {column}")]

    def telegram_channels(self):
        if self.kind != 'telegram':
            return []
        with self._db() as c:
            if 'type' not in {r['name'] for r in c.execute('PRAGMA table_info(chats)')}:
                return []
            return [str(r[0]) for r in c.execute("SELECT id FROM chats WHERE type='channel' ORDER BY id")]

    def backfill_page(self, after_rowid: int, limit: int):
        """Page cached rows, including empty text, excluding Telegram tombstones."""
        with self._db() as c:
            alive = self._consent_filter(c) + (' AND COALESCE(m.deleted,0)=0' if self.kind == 'telegram' else '')
            rows = c.execute(self._select(c) + f' WHERE m.rowid>?{alive} ORDER BY m.rowid LIMIT ?',
                             (int(after_rowid), _limit(limit))).fetchall()
            return [self._normalize(row) for row in rows]

    def get(self, chat_id, message_id):
        with self._db() as c:
            alive = self._consent_filter(c) + (' AND COALESCE(m.deleted,0)=0' if self.kind == 'telegram' else '')
            row = c.execute(self._select(c) + f" WHERE m.{self.fields['chat']}=? AND m.id=?{alive}",
                            (str(chat_id), str(message_id))).fetchone()
            return self._normalize(row) if row else None

    def _install_group_rescan(self, c, chat_column, prefix):
        c.execute("""CREATE TABLE IF NOT EXISTS memory_group_rescan(
            chat_id TEXT PRIMARY KEY,cursor TEXT)""")
        # Telegram already has (chat_id,id); reuse it rather than indexing its cache again.
        indexes = [row['name'] for row in c.execute('PRAGMA index_list(messages)')]
        indexed = any([r['name'] for r in c.execute('PRAGMA index_info("'+name.replace('"','""')+'")')][:2]
                      == [chat_column, 'id'] for name in indexes)
        if not indexed:
            c.execute(f'CREATE INDEX IF NOT EXISTS memory_group_rescan_messages ON messages({chat_column},id)')
        fresh = "(new.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',new.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)"
        renewed = "(old.evidence LIKE 'auto:max-members:%' AND (CAST(strftime('%s',old.updated_at) AS INTEGER) IS NULL OR CAST(strftime('%s',old.updated_at) AS INTEGER)<=CAST(strftime('%s','now') AS INTEGER)-900))"
        for event in ('INSERT','UPDATE','DELETE'):
            c.execute(f'DROP TRIGGER IF EXISTS {prefix}_{event.lower()}')
            if event == 'DELETE':
                c.execute(f"""CREATE TRIGGER {prefix}_delete AFTER DELETE ON group_monitoring_consent BEGIN
                    DELETE FROM memory_group_rescan WHERE chat_id=CAST(old.{chat_column} AS TEXT); END""")
                continue
            transition = '' if event == 'INSERT' else f' AND (old.allowed!=1 OR old.evidence IS NOT new.evidence OR {renewed})'
            c.execute(f"""CREATE TRIGGER {prefix}_{event.lower()} AFTER {event} ON group_monitoring_consent
                WHEN new.allowed=1 AND {fresh}{transition} BEGIN
                INSERT INTO memory_group_rescan(chat_id,cursor) VALUES(CAST(new.{chat_column} AS TEXT),NULL)
                ON CONFLICT(chat_id) DO UPDATE SET cursor=NULL; END""")

    def _drain_group_rescan(self, c, capacity):
        if capacity <= 0 or not self._has(c, 'memory_group_rescan'):
            return
        pending = c.execute('SELECT chat_id,cursor FROM memory_group_rescan ORDER BY rowid LIMIT 1').fetchone()
        if pending is None:
            return
        column = self.fields['chat']
        peer = int(pending['chat_id']) if self.kind == 'telegram' else pending['chat_id']
        allowed = c.execute(f"""SELECT 1 FROM group_monitoring_consent WHERE {column}=? AND allowed=1
            AND (evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)""", (peer,)).fetchone()
        if not allowed:
            c.execute('DELETE FROM memory_group_rescan WHERE chat_id=?', (pending['chat_id'],))
            return
        predicate = f'{column}=?'
        args = [peer]
        # Older queues use an empty string for the initial keyset position.
        if pending['cursor'] not in (None, ''):
            predicate += ' AND id>?'
            args.append(int(pending['cursor']) if self.kind == 'telegram' else pending['cursor'])
        rows = c.execute(f'SELECT id FROM messages WHERE {predicate} ORDER BY id LIMIT ?',
                         (*args, capacity + 1)).fetchall()
        batch = rows[:capacity]
        c.executemany("INSERT INTO memory_changes(chat_id,message_id,op) VALUES(?,?,'upsert')",
                      [(pending['chat_id'], str(row['id'])) for row in batch])
        if len(rows) <= capacity:
            c.execute('DELETE FROM memory_group_rescan WHERE chat_id=?', (pending['chat_id'],))
        else:
            c.execute('UPDATE memory_group_rescan SET cursor=? WHERE chat_id=?',
                      (str(batch[-1]['id']), pending['chat_id']))

    def changes(self, after_seq, limit):
        limit = _limit(limit)
        with self._db(write=True) as c:
            rows = c.execute("""SELECT seq,chat_id,message_id,op,event_token FROM memory_changes
                WHERE seq>? ORDER BY seq LIMIT ?""", (int(after_seq), limit)).fetchall()
            # Existing edits/deletes have priority. Never outrun downstream consumption.
            capacity = min(200, limit - len(rows))
            if capacity > 0:
                self._drain_group_rescan(c, capacity)
                rows = c.execute("""SELECT seq,chat_id,message_id,op,event_token FROM memory_changes
                    WHERE seq>? ORDER BY seq LIMIT ?""", (int(after_seq), limit)).fetchall()
            return [dict(row) for row in rows]

    def identity(self):
        """Return this capture installation's UUID without inspecting message bodies."""
        with self._db() as c:
            row = c.execute("SELECT value FROM memory_capture_meta WHERE key='capture_uuid'").fetchone()
            if row is None or not row['value']:
                raise RuntimeError('Source capture identity is missing; install capture first')
            return row['value']

    def checkpoint_token(self, seq):
        """Return an event identity; a missing/reused checkpoint requires reconciliation."""
        if int(seq) <= 0:
            return None
        with self._db() as c:
            row = c.execute('SELECT event_token FROM memory_changes WHERE seq=?', (int(seq),)).fetchone()
            return row['event_token'] if row else None

    def watermark(self):
        with self._db() as c:
            if not self._has(c, 'memory_changes'):
                return 0
            return c.execute('SELECT COALESCE(MAX(seq),0) FROM memory_changes').fetchone()[0]

    def stats(self):
        """Bounded aggregate inspection; callers should cache this off hot paths."""
        f = self.fields
        with self._db() as c:
            deleted = 'COALESCE(m.deleted,0)' if self.kind == 'telegram' else '0'
            row = c.execute(f'''SELECT COUNT(*) AS messages,COALESCE(SUM({deleted}),0) AS deleted_messages,
                COALESCE(SUM({deleted}=0 AND length(trim(COALESCE(m.{f['body']},'')))>0),0) AS text_messages,
                MIN(CAST(strftime('%s',m.{f['date']}) AS INTEGER)) AS first_timestamp,
                MAX(CAST(strftime('%s',m.{f['date']}) AS INTEGER)) AS last_timestamp
                FROM messages m''').fetchone()
            result = dict(row)
            result['source'] = self.kind
            result['chats'] = c.execute('SELECT COUNT(*) FROM chats').fetchone()[0]
            result['transcripts_done'] = (c.execute("SELECT COUNT(*) FROM transcripts WHERE status='done'").fetchone()[0]
                                          if self._has(c, 'transcripts') else 0)
            result['change_watermark'] = (c.execute('SELECT COALESCE(MAX(seq),0) FROM memory_changes').fetchone()[0]
                                           if self._has(c, 'memory_changes') else 0)
            return result
