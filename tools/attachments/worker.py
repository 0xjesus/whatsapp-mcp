"""Durable attachment analysis. Run on the daemon host with the same WA_STORE."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time

SCHEMA = '''CREATE TABLE IF NOT EXISTS group_monitoring_consent(
    chat_jid TEXT PRIMARY KEY, allowed INTEGER NOT NULL DEFAULT 0,
    evidence TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS attachment_analysis(
    message_id TEXT NOT NULL, chat_jid TEXT NOT NULL, media_hash TEXT NOT NULL,
    text TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT, metadata TEXT NOT NULL DEFAULT '{}', updated_at TEXT NOT NULL,
    retry_at REAL NOT NULL DEFAULT 0, PRIMARY KEY(message_id,chat_jid));
CREATE TRIGGER IF NOT EXISTS attachment_message_delete AFTER DELETE ON messages BEGIN
    DELETE FROM attachment_analysis WHERE message_id=old.id AND chat_jid=old.chat_jid;
END;'''


def initialize(db):
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    from tools.attachments.consent import install_guards
    install_guards(db)
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='memory_changes'").fetchone():
        for event,prefix in [('INSERT','new'),('UPDATE','new'),('DELETE','old')]:
            db.execute(f"""CREATE TRIGGER IF NOT EXISTS memory_attachment_{event.lower()}
                AFTER {event} ON attachment_analysis BEGIN
                INSERT INTO memory_changes(chat_id,message_id,op)
                VALUES({prefix}.chat_jid,{prefix}.message_id,'upsert'); END""")
    db.commit()


def allowed(db, jid):
    if not jid.endswith('@g.us'):
        return jid != 'status@broadcast'
    return db.execute("SELECT 1 FROM group_monitoring_consent WHERE chat_jid=? AND allowed=1 AND (evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)", (jid,)).fetchone() is not None


def pending(db, limit=4):
    vision_enabled = bool(os.environ.get('WA_VISION_MODEL'))
    cloud_enabled = os.environ.get('WA_ATTACHMENT_BACKEND') == 'openai'
    return [dict(row) for row in db.execute('''SELECT m.id AS message_id,m.chat_jid,m.filename,m.media_type,
        hex(COALESCE(m.file_sha256,X'')) AS media_hash,
        CASE WHEN a.media_hash=hex(COALESCE(m.file_sha256,X'')) THEN COALESCE(a.attempts,0) ELSE 0 END AS attempts
        FROM messages m LEFT JOIN attachment_analysis a ON a.message_id=m.id AND a.chat_jid=m.chat_jid
        WHERE m.media_type IN ('image','sticker','document') AND m.chat_jid!='status@broadcast'
        AND (m.chat_jid NOT LIKE '%@g.us' OR EXISTS(SELECT 1 FROM group_monitoring_consent g WHERE g.chat_jid=m.chat_jid AND g.allowed=1 AND (g.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',g.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900)))
        AND (a.message_id IS NULL OR a.media_hash!=hex(COALESCE(m.file_sha256,X''))
             OR (a.status='failed' AND a.retry_at<=?)
             OR (? AND a.status='partial' AND a.error='vision_not_configured')
             OR (? AND a.status IN ('done','partial') AND a.retry_at<=?
                 AND (COALESCE(json_extract(a.metadata,'$.cloud_version'),'')!='openai-v1'
                      OR json_extract(a.metadata,'$.cloud_pending')=1)))
        ORDER BY CASE WHEN m.timestamp>=datetime('now','-1 day') THEN 0 ELSE 1 END,m.timestamp ASC,m.rowid ASC LIMIT ?''', (time.time(), vision_enabled and not cloud_enabled, cloud_enabled, time.time(), limit))]


def save(db, row, result):
    # Recheck current media and consent in the write transaction; never resurrect revoked messages.
    if not db.in_transaction:
        db.execute('BEGIN IMMEDIATE')
    current = db.execute("SELECT hex(COALESCE(file_sha256,X'')) FROM messages WHERE id=? AND chat_jid=?",
                         (row['message_id'], row['chat_jid'])).fetchone()
    if not current or current[0] != row['media_hash'] or not allowed(db, row['chat_jid']):
        db.commit()
        return False
    status = result['status']
    attempts = row['attempts'] + (0 if result.get('cloud_pending') else 1)
    if status == 'failed':
        prior = db.execute('SELECT text,status,metadata FROM attachment_analysis WHERE message_id=? AND chat_jid=? AND media_hash=?', (row['message_id'],row['chat_jid'],row['media_hash'])).fetchone()
        if prior and prior['text']:
            result = dict(result, text=prior['text'])
            if os.environ.get('WA_ATTACHMENT_BACKEND') == 'openai':
                merged = json.loads(prior['metadata'])
                merged.update(result)
                result = dict(merged, cloud_pending=True, retry_at=time.time()+3600)
                status = 'partial'
    if status == 'failed' and attempts >= 3:
        status = 'failed_permanent'
    metadata = {k:v for k,v in result.items() if k not in ('text','status')}
    db.execute('''INSERT INTO attachment_analysis(message_id,chat_jid,media_hash,text,status,attempts,error,metadata,updated_at,retry_at)
        VALUES(?,?,?,?,?,?,?,?,datetime('now'),?) ON CONFLICT(message_id,chat_jid) DO UPDATE SET
        media_hash=excluded.media_hash,text=excluded.text,status=excluded.status,attempts=excluded.attempts,
        error=excluded.error,metadata=excluded.metadata,updated_at=excluded.updated_at,retry_at=excluded.retry_at''',
        (row['message_id'],row['chat_jid'],row['media_hash'],result.get('text',''),status,attempts,
         result.get('reason'),json.dumps(metadata),result.get('retry_at',time.time()+min(3600,60*2**min(attempts,8)))))
    db.commit()
    return True


def analyze(path, filename, media_type):
    child = subprocess.run([sys.executable, '-m', 'tools.attachments.worker', '--extract', str(path),
                            '--filename', filename or '', '--media-type', media_type],
                           capture_output=True, timeout=600, env=dict(os.environ, OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', TOKENIZERS_PARALLELISM='false'))
    if child.returncode:
        raise RuntimeError('extractor_failed')
    return json.loads(child.stdout)


def run_once(db, mcp, temp_root):
    for row in pending(db):
        if not allowed(db, row['chat_jid']):
            continue
        try:
            columns = {r[1] for r in db.execute('PRAGMA table_info(messages)')}
            if 'file_length' in columns:
                size = db.execute('SELECT file_length FROM messages WHERE id=? AND chat_jid=?', (row['message_id'],row['chat_jid'])).fetchone()
                if size and (size[0] or 0) > 50 * 1024 * 1024:
                    save(db,row,{'text':'','status':'failed_permanent','reason':'file_size_limit'})
                    continue
            with tempfile.TemporaryDirectory(dir=temp_root) as tmp:
                path = Path(tmp) / 'attachment'
                response = mcp.call('download_media', {'chat_jid':row['chat_jid'],
                    'message_id':row['message_id'], 'output_path':str(path)})
                if response.get('error') or response.get('result',{}).get('isError') or not path.is_file():
                    raise RuntimeError('download_unavailable')
                if row['media_hash'] and hashlib.sha256(path.read_bytes()).hexdigest().upper() != row['media_hash']:
                    raise RuntimeError('media_hash_mismatch')
                if not allowed(db, row['chat_jid']):
                    continue
                outcome = analyze(path, row['filename'], row['media_type'])
                # Persist local evidence before cloud configuration or rendering can fail.
                if os.environ.get('WA_ATTACHMENT_BACKEND') == 'openai':
                    existing = db.execute('SELECT metadata FROM attachment_analysis WHERE message_id=? AND chat_jid=? AND media_hash=?', (row['message_id'],row['chat_jid'],row['media_hash'])).fetchone()
                    prior_metadata = json.loads(existing[0]) if existing else {}
                    local_checkpoint = dict(outcome, status='partial', cloud_pending=True, retry_at=time.time()+60)
                    if prior_metadata.get('cloud_text'):
                        from tools.attachments.cloud import SEPARATOR
                        local_checkpoint['text'] += SEPARATOR + prior_metadata['cloud_text'].strip()
                    save(db,row,dict(prior_metadata, **local_checkpoint))
                if os.environ.get('WA_ATTACHMENT_BACKEND') == 'openai':
                    from tools.attachments.cloud_client import CloudClient
                    from tools.attachments.cloud import enrich
                    def authorized():
                        current = db.execute("SELECT hex(COALESCE(file_sha256,X'')) FROM messages WHERE id=? AND chat_jid=?", (row['message_id'],row['chat_jid'])).fetchone()
                        return bool(current and current[0]==row['media_hash'] and allowed(db,row['chat_jid']))
                    client = CloudClient(Path(os.environ['WA_STORE'])/'attachment-cloud.db',
                        Path(os.environ['WA_OPENAI_KEY_FILE']),
                        monthly_budget=float(os.environ.get('WA_ATTACHMENT_MONTHLY_USD','10')),
                        model=os.environ.get('WA_OPENAI_MODEL','gpt-5.4-mini-2026-03-17'),
                        authorize=authorized)
                    outcome = enrich(path,row['filename'],row['media_type'],outcome,client,
                                     previous=prior_metadata)
            save(db, row, outcome)
            print(json.dumps({'component':'attachments','status':outcome['status']}), flush=True)
        except Exception as error:
            save(db, row, {'text':'','status':'failed','reason':type(error).__name__})
            print(json.dumps({'component':'attachments','status':'failed','reason':type(error).__name__}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--extract', type=Path)
    parser.add_argument('--filename', default='')
    parser.add_argument('--media-type', default='')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    if args.extract:
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (6 * 1024**3, 6 * 1024**3))
        resource.setrlimit(resource.RLIMIT_CPU, (540, 540))
        from tools.attachments.extract import extract
        vision = None
        if os.environ.get('WA_VISION_MODEL') and os.environ.get('WA_ATTACHMENT_BACKEND') != 'openai':
            from tools.attachments.vision import describe
            vision = describe
        print(json.dumps(extract(args.extract, args.filename, args.media_type, vision=vision)))
        return
    from tools.transcriber.transcribe_daemon import Mcp
    store = Path(os.environ['WA_STORE']).resolve()
    db_path = store / 'messages.db'
    temp_root = store / 'uploads' / '_attachments'
    temp_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp_root.chmod(0o700)
    with (temp_root / 'worker.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = sqlite3.connect(db_path.as_uri()+'?mode=rw', uri=True, timeout=30)
        initialize(db)
        mcp = Mcp()
        try:
            while True:
                run_once(db, mcp, temp_root)
                if args.once:
                    break
                time.sleep(30)
        finally:
            db.close()


if __name__ == '__main__': main()
