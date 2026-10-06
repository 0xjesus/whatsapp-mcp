"""Explicit local group authorization. Never infer consent from incoming messages."""
import argparse
import datetime
import json
import os
import re
from pathlib import Path
import sqlite3

SCHEMA = '''CREATE TABLE IF NOT EXISTS group_monitoring_consent(
 chat_jid TEXT PRIMARY KEY,allowed INTEGER NOT NULL DEFAULT 0,
 evidence TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS group_monitoring_policy(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),enabled INTEGER NOT NULL DEFAULT 0,
 max_members INTEGER NOT NULL DEFAULT 10 CHECK(max_members>0));
INSERT OR IGNORE INTO group_monitoring_policy VALUES(1,0,10);
CREATE TABLE IF NOT EXISTS group_monitoring_inventory(
 chat_jid TEXT PRIMARY KEY,name TEXT NOT NULL,member_count INTEGER,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS group_monitoring_audit(
 seq INTEGER PRIMARY KEY AUTOINCREMENT,chat_jid TEXT NOT NULL,allowed INTEGER NOT NULL,
 evidence TEXT NOT NULL,updated_at TEXT NOT NULL);'''


def install_guards(db):
    """SQLite checks permission in the same transaction as each cached write."""
    for table in ('messages','reactions','poll_votes','message_mutations','view_once_media','transcripts','attachment_analysis'):
        columns = {row[1] for row in db.execute('PRAGMA table_info('+table+')')}
        chat_column = 'poll_chat_jid' if table == 'poll_votes' else 'chat_jid'
        if chat_column not in columns:
            continue
        for event in ('INSERT','UPDATE'):
            db.execute(f"DROP TRIGGER IF EXISTS monitoring_guard_{table}_{event.lower()}")
            db.execute(f"""CREATE TRIGGER IF NOT EXISTS monitoring_guard_{table}_{event.lower()}
                BEFORE {event} ON {table} WHEN new.{chat_column} LIKE '%@g.us'
                AND NOT EXISTS(SELECT 1 FROM group_monitoring_consent g WHERE g.chat_jid=new.{chat_column} AND g.allowed=1 AND (g.evidence NOT LIKE 'auto:max-members:%' OR CAST(strftime('%s',g.updated_at) AS INTEGER)>CAST(strftime('%s','now') AS INTEGER)-900))
                BEGIN SELECT RAISE(IGNORE); END""")


def set_consent(db, jid, allowed, evidence, confirmed=False):
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?@g\.us', jid):
        raise ValueError('exact_group_jid_required')
    if evidence.startswith('auto:max-members:'):
        raise ValueError('reserved_automatic_evidence_prefix')
    if allowed and (not confirmed or not evidence.strip()):
        raise ValueError('explicit_confirmation_and_evidence_required')
    db.executescript(SCHEMA)
    install_guards(db)
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with db:
        db.execute('''INSERT INTO group_monitoring_consent VALUES(?,?,?,?)
            ON CONFLICT(chat_jid) DO UPDATE SET allowed=excluded.allowed,evidence=excluded.evidence,updated_at=excluded.updated_at''',
            (jid,int(allowed),evidence.strip(),now))
        db.execute('INSERT INTO group_monitoring_audit(chat_jid,allowed,evidence,updated_at) VALUES(?,?,?,?)',
                   (jid,int(allowed),evidence.strip(),now))
        if not allowed and db.execute("SELECT 1 FROM sqlite_master WHERE name='attachment_analysis'").fetchone():
            db.execute('DELETE FROM attachment_analysis WHERE chat_jid=?',(jid,))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['allow','revoke','list','auto-enable','auto-disable','inventory'])
    parser.add_argument('--group')
    parser.add_argument('--evidence',default='')
    parser.add_argument('--confirm',action='store_true')
    args=parser.parse_args()
    path=Path(os.environ['WA_STORE']).resolve()/'messages.db'
    with sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=30) as db:
        if args.action in ('auto-enable','auto-disable'):
            db.executescript(SCHEMA)
            with db:
                db.execute('UPDATE group_monitoring_policy SET enabled=? WHERE singleton=1',(int(args.action=='auto-enable'),))
                db.execute("UPDATE group_monitoring_consent SET allowed=0 WHERE evidence LIKE 'auto:max-members:%'")
            print(json.dumps({'automatic':args.action=='auto-enable','max_members':10}))
        elif args.action=='inventory':
            db.executescript(SCHEMA)
            db.row_factory=sqlite3.Row
            print(json.dumps([dict(r) for r in db.execute('SELECT * FROM group_monitoring_inventory')]))
        elif args.action=='list':
            db.executescript(SCHEMA)
            db.row_factory=sqlite3.Row
            print(json.dumps([dict(r) for r in db.execute('SELECT * FROM group_monitoring_consent')]))
        else:
            set_consent(db,args.group or '',args.action=='allow',args.evidence,args.confirm)
            print(json.dumps({'group':args.group,'monitoring':args.action=='allow'}))

if __name__=='__main__': main()
