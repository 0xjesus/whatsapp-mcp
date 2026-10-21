"""Synthetic source databases only. No account or network access."""
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest

SOURCE_PATH = Path(__file__).resolve().parents[1] / 'sources.py'
Source = None
if SOURCE_PATH.exists():
    spec = importlib.util.spec_from_file_location('memory_sources', SOURCE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    Source = module.Source


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def source(self, kind):
        self.assertIsNotNone(Source, 'Source adapter has not been implemented')
        path = Path(self.temp.name) / (kind + '.db')
        c = sqlite3.connect(path)
        if kind == 'whatsapp':
            c.executescript('''
                CREATE TABLE chats(jid TEXT PRIMARY KEY,name TEXT,last_message_time TEXT);
                CREATE TABLE messages(id TEXT,chat_jid TEXT,sender TEXT,content TEXT,
                    timestamp TEXT,is_from_me BOOLEAN,media_type TEXT,filename TEXT,
                    PRIMARY KEY(id,chat_jid));
                CREATE TABLE transcripts(message_id TEXT,chat_jid TEXT,text TEXT,
                    status TEXT,attempts INTEGER DEFAULT 0,updated_at TEXT,
                    PRIMARY KEY(message_id,chat_jid));
                INSERT INTO chats VALUES('a','Alpha',NULL),('b','Beta',NULL);
            ''')
        else:
            c.executescript('''
                CREATE TABLE chats(id INTEGER PRIMARY KEY,title TEXT,
                    first_pass_done INTEGER DEFAULT 0,backfill_done INTEGER DEFAULT 0);
                CREATE TABLE messages(chat_id INTEGER,id INTEGER,date TEXT,sender_id INTEGER,
                    sender_name TEXT,out INTEGER DEFAULT 0,text TEXT,media_type TEXT,
                    deleted INTEGER DEFAULT 0,edited INTEGER DEFAULT 0,
                    PRIMARY KEY(chat_id,id));
                CREATE TABLE transcripts(chat_id INTEGER,id INTEGER,text TEXT,status TEXT,
                    attempts INTEGER DEFAULT 0,updated_at TEXT,PRIMARY KEY(chat_id,id));
                INSERT INTO chats(id,title) VALUES(1,'Alpha'),(2,'Beta');
            ''')
        c.close()
        return Source(kind, path)

    def write(self, source, sql, args=()):
        with sqlite3.connect(source.path) as c:
            c.execute(sql, args)

    def insert(self, source, chat=None, mid=None, text='synthetic text', media=''):
        if source.kind == 'whatsapp':
            self.write(source, '''INSERT OR REPLACE INTO messages
                (chat_jid,id,sender,content,timestamp,is_from_me,media_type)
                VALUES(?,?,'sender',?,'2026-09-14 09:00:00-06:00',0,?)''',
                (chat or 'a', mid or 'same', text, media))
        else:
            self.write(source, '''INSERT OR REPLACE INTO messages
                (chat_id,id,sender_id,sender_name,text,date,media_type)
                VALUES(?,?,123,'Sender',?,'2026-09-14T15:00:00Z',?)''',
                (chat or 1, mid or 10, text, media))

    def test_backfill_pages_and_normalizes_composite_identity(self):
        for kind in ['whatsapp', 'telegram']:
            with self.subTest(kind=kind):
                source = self.source(kind)
                self.insert(source)
                self.insert(source, chat='b' if kind == 'whatsapp' else 2, text='')
                source.install_capture()
                first = source.backfill_page(0, 1)
                second = source.backfill_page(first[-1]['rowid'], 1)
                self.assertEqual(len(first), 1)
                self.assertEqual(len(second), 1)
                self.assertEqual(first[0]['timestamp'], 1789398000)
                self.assertEqual(first[0]['source'], kind)
                self.assertEqual(first[0]['chat_name'], 'Alpha')
                self.assertIsInstance(first[0]['chat_id'], str)
                self.assertIsInstance(first[0]['message_id'], str)
                self.assertNotEqual(first[0]['chat_id'], second[0]['chat_id'])
                self.assertEqual(first[0]['message_id'], second[0]['message_id'])
                self.assertEqual(second[0]['text'], '')
                self.assertEqual(source.backfill_page(second[-1]['rowid'], 1), [])
                self.assertEqual(source.watermark(), 0)

    def test_capture_is_transactional_and_idempotent(self):
        source = self.source('whatsapp')
        source.install_capture()
        source.install_capture()
        self.insert(source)
        with sqlite3.connect(source.path) as c:
            c.execute("UPDATE messages SET content='rolled back'")
            c.rollback()
        changes = source.changes(0, 20)
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]['op'], 'upsert')
        self.assertNotIn('text', changes[0])
        self.assertEqual(source.get('a', 'same')['text'], 'synthetic text')

    def test_whatsapp_replace_edit_delete_and_reinsert(self):
        source = self.source('whatsapp')
        source.install_capture()
        self.insert(source)
        old_rowid = source.get('a', 'same')['rowid']
        self.insert(source, chat='b')
        self.insert(source, text='replacement')
        self.assertNotEqual(source.get('a', 'same')['rowid'], old_rowid)
        self.write(source, "UPDATE messages SET content='edited' WHERE chat_jid='a'")
        cursor = source.watermark()
        self.write(source, "DELETE FROM messages WHERE chat_jid='a'")
        self.assertIsNone(source.get('a', 'same'))
        self.assertEqual(source.changes(cursor, 10)[0]['op'], 'delete')
        self.insert(source, text='restored')
        self.assertEqual(source.get('a', 'same')['text'], 'restored')
        self.assertEqual(source.get('b', 'same')['text'], 'synthetic text')

    def test_telegram_softdelete_is_a_tombstone_even_with_transcript(self):
        source = self.source('telegram')
        source.install_capture()
        self.insert(source, media='voice')
        self.write(source, "INSERT INTO transcripts(chat_id,id,text,status) VALUES(1,10,'voice','done')")
        cursor = source.watermark()
        self.write(source, 'UPDATE messages SET deleted=1 WHERE chat_id=1 AND id=10')
        self.assertIsNone(source.get('1', '10'))
        self.assertEqual(source.backfill_page(0, 20), [])
        self.assertEqual(source.changes(cursor, 20)[0]['op'], 'delete')

    def test_late_transcripts_are_canonical_and_captured_without_message_update(self):
        for kind in ['whatsapp', 'telegram']:
            with self.subTest(kind=kind):
                source = self.source(kind)
                source.install_capture()
                self.insert(source, text='', media='audio' if kind == 'whatsapp' else 'voice')
                key = ('a', 'same') if kind == 'whatsapp' else ('1', '10')
                columns = 'chat_jid,message_id' if kind == 'whatsapp' else 'chat_id,id'
                cursor = source.watermark()
                self.write(source, f'INSERT INTO transcripts({columns},text,status) VALUES(?,?,?,?)',
                           (*key, 'spoken synthetic text', 'done'))
                self.assertEqual(source.get(*key)['text'], '[Nota de voz] spoken synthetic text')
                self.assertEqual(source.changes(cursor, 10)[0]['op'], 'upsert')
                self.write(source, "UPDATE transcripts SET text='corrected'")
                self.assertEqual(source.get(*key)['text'], '[Nota de voz] corrected')
                self.write(source, 'DELETE FROM transcripts')
                self.assertEqual(source.get(*key)['text'], '')

    def test_caption_wins_over_transcript_but_voice_prefix_is_repaired(self):
        source = self.source('whatsapp')
        source.install_capture()
        self.insert(source, text='real caption', media='audio')
        self.write(source, "INSERT INTO transcripts(chat_jid,message_id,text,status) VALUES('a','same','voice','done')")
        self.assertEqual(source.get('a', 'same')['text'], 'real caption')
        self.write(source, "UPDATE messages SET content='[Nota de voz] old'")
        self.assertEqual(source.get('a', 'same')['text'], '[Nota de voz] voice')

    def test_video_transcript_appends_to_caption(self):
        source = self.source('whatsapp')
        source.install_capture()
        self.insert(source, mid='vid1', text='mira esto', media='video')
        self.write(source, "INSERT INTO transcripts(chat_jid,message_id,text,status) VALUES('a','vid1','hola banda, este es el pitch','done')")
        self.assertEqual(source.get('a', 'vid1')['text'], 'mira esto\n[Audio del video] hola banda, este es el pitch')
        self.insert(source, mid='vid2', text='', media='video')
        self.write(source, "INSERT INTO transcripts(chat_jid,message_id,text,status) VALUES('a','vid2','sin caption','done')")
        self.assertEqual(source.get('a', 'vid2')['text'], '[Audio del video] sin caption')
        self.insert(source, mid='vid4', text='[Audio del video] ya escrito por el bridge', media='video')
        self.write(source, "INSERT INTO transcripts(chat_jid,message_id,text,status) VALUES('a','vid4','ya escrito por el bridge','done')")
        self.assertEqual(source.get('a', 'vid4')['text'], '[Audio del video] ya escrito por el bridge')
        self.insert(source, mid='vid5', text='edited caption\n[Audio del video] manual correction', media='video')
        self.write(source, "INSERT INTO transcripts(chat_jid,message_id,text,status) VALUES('a','vid5','original machine transcript','done')")
        self.assertEqual(source.get('a', 'vid5')['text'], 'edited caption\n[Audio del video] manual correction')
        self.insert(source, mid='vid3', text='pendiente', media='video')
        self.write(source, "INSERT INTO transcripts(chat_jid,message_id,text,status) VALUES('a','vid3',NULL,'failed')")
        self.assertEqual(source.get('a', 'vid3')['text'], 'pendiente')

    def test_resume_replays_all_changes_after_snapshot_watermark(self):
        source = self.source('telegram')
        self.insert(source)
        source.install_capture()
        watermark = source.watermark()
        snapshot = source.backfill_page(0, 20)
        self.insert(source, chat=2, text='other chat')
        self.write(source, "UPDATE messages SET text='updated' WHERE chat_id=1")
        self.write(source, 'DELETE FROM messages WHERE chat_id=2')
        index = {(r['chat_id'], r['message_id']): r for r in snapshot}
        resumed = Source('telegram', source.path)
        while events := resumed.changes(watermark, 1):
            event = events[0]
            key = event['chat_id'], event['message_id']
            row = resumed.get(*key)
            if row is None:
                index.pop(key, None)
            else:
                index[key] = row
            watermark = event['seq']
        self.assertEqual(list(index), [('1', '10')])
        self.assertEqual(index[('1', '10')]['text'], 'updated')

    def test_primary_key_update_captures_old_key_removal(self):
        source = self.source('telegram')
        source.install_capture()
        self.insert(source)
        cursor = source.watermark()
        self.write(source, 'UPDATE messages SET chat_id=2 WHERE chat_id=1')
        events = source.changes(cursor, 20)
        self.assertEqual([(r['chat_id'], r['op']) for r in events], [('1', 'delete'), ('2', 'upsert')])

    def test_stats_have_aggregate_counts_and_no_bodies(self):
        source = self.source('telegram')
        source.install_capture()
        self.insert(source, text='private synthetic sentinel')
        self.insert(source, chat=2, text='deleted sentinel')
        self.write(source, 'UPDATE messages SET deleted=1 WHERE chat_id=2')
        stats = source.stats()
        self.assertEqual(stats['messages'], 2)
        self.assertEqual(stats['deleted_messages'], 1)
        self.assertEqual(stats['text_messages'], 1)
        self.assertEqual(stats['chats'], 2)
        self.assertEqual(stats['first_timestamp'], 1789398000)
        self.assertNotIn('sentinel', str(stats))

    def test_absent_transcripts_are_supported_without_creating_source_tables(self):
        source = self.source('whatsapp')
        self.write(source, 'DROP TABLE transcripts')
        self.insert(source)
        source.install_capture()
        self.assertEqual(source.get('a', 'same')['text'], 'synthetic text')
        with sqlite3.connect(source.path) as c:
            self.assertIsNone(c.execute("SELECT name FROM sqlite_master WHERE name='transcripts'").fetchone())

    def test_capture_ignores_noop_updates(self):
        source = self.source('telegram')
        source.install_capture()
        self.insert(source)
        watermark = source.watermark()
        self.write(source, 'UPDATE messages SET text=text')
        self.assertEqual(source.watermark(), watermark)

    def test_attachment_analysis_is_searchable_without_overwriting_caption(self):
        source = self.source('whatsapp')
        self.write(source, 'ALTER TABLE messages ADD COLUMN file_sha256 BLOB')
        self.write(source, "CREATE TABLE attachment_analysis(message_id TEXT,chat_jid TEXT,text TEXT,status TEXT,media_hash TEXT)")
        self.insert(source, text='original caption', media='document')
        source.install_capture()
        watermark = source.watermark()
        self.write(source, "INSERT INTO attachment_analysis VALUES('same','a','last page evidence','done','')")
        self.assertIn('last page evidence', source.get('a', 'same')['text'])
        self.assertIn('original caption', source.get('a', 'same')['text'])
        self.assertEqual(len(source.changes(watermark, 10)), 1)
        self.write(source, "UPDATE attachment_analysis SET text='corrected last page'")
        self.assertIn('corrected last page', source.get('a', 'same')['text'])
        self.write(source, "UPDATE messages SET file_sha256=X'1234'")
        self.assertNotIn('last page', source.get('a', 'same')['text'])

    def test_groups_require_individual_consent_and_revocation_enqueues_removal(self):
        source = self.source('whatsapp')
        self.insert(source, chat='123@g.us', text='private group')
        self.insert(source, chat='a', mid='direct')
        source.install_capture()
        self.assertIsNone(source.get('123@g.us', 'same'))
        self.assertEqual(len(source.backfill_page(0, 20)), 1)
        self.write(source, "INSERT INTO group_monitoring_consent VALUES('123@g.us',1,'explicit test approval','now')")
        self.assertIsNotNone(source.get('123@g.us', 'same'))
        cursor = source.watermark()
        self.write(source, "UPDATE group_monitoring_consent SET allowed=0")
        self.assertIsNone(source.get('123@g.us', 'same'))
        self.assertEqual(source.changes(cursor, 20), [])

    def test_missing_source_is_never_created(self):
        self.assertIsNotNone(Source, 'Source adapter has not been implemented')
        path = Path(self.temp.name) / 'missing.db'
        source = Source('whatsapp', path)
        with self.assertRaises(sqlite3.OperationalError):
            source.install_capture()
        self.assertFalse(path.exists())

    def test_capture_identity_and_event_tokens_survive_reinstallation(self):
        source = self.source('telegram')
        source.install_capture()
        self.assertTrue(callable(getattr(source, 'identity', None)), 'capture identity is missing')
        identity = source.identity()
        self.assertTrue(identity)
        self.insert(source)
        self.insert(source, chat=2)
        events = source.changes(0, 20)
        self.assertTrue(all(len(event['event_token']) == 32 for event in events))
        self.assertNotEqual(events[0]['event_token'], events[1]['event_token'])
        source.install_capture()
        resumed = Source('telegram', source.path)
        self.assertEqual(resumed.identity(), identity)
        self.assertEqual(resumed.changes(0, 20), events)
        self.assertEqual(resumed.checkpoint_token(events[-1]['seq']), events[-1]['event_token'])
        self.assertIsNone(resumed.checkpoint_token(0))
        self.assertIsNone(resumed.checkpoint_token(events[-1]['seq'] + 1))

    def test_legacy_queue_tokens_are_migrated_without_rewriting_events(self):
        source = self.source('whatsapp')
        self.write(source, '''CREATE TABLE memory_changes(seq INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,message_id TEXT NOT NULL,op TEXT NOT NULL)''')
        self.write(source, "INSERT INTO memory_changes VALUES(7,'a','legacy','upsert')")
        source.install_capture()
        legacy = source.changes(0, 20)[0]
        self.assertIn('event_token', legacy, 'legacy queue token migration is missing')
        self.assertEqual((legacy['seq'], legacy['chat_id'], legacy['message_id'], legacy['op']),
                         (7, 'a', 'legacy', 'upsert'))
        self.assertEqual(len(legacy['event_token']), 32)
        self.insert(source)
        new = source.changes(7, 20)[0]
        self.assertEqual(len(new['event_token']), 32)
        self.assertNotEqual(new['event_token'], legacy['event_token'])
        source.install_capture()
        self.assertEqual(source.checkpoint_token(7), legacy['event_token'])
        self.assertEqual(source.get('a', 'same')['text'], 'synthetic text')


if __name__ == '__main__':
    unittest.main()

class TelegramConsentTests(unittest.TestCase):
    setUp = SourceTests.setUp
    source = SourceTests.source
    write = SourceTests.write
    insert = SourceTests.insert
    def test_telegram_deny_grant_revoke_and_channels(self):
        source = self.source('telegram')
        self.write(source, 'ALTER TABLE chats ADD COLUMN type TEXT')
        self.write(source, "INSERT INTO chats(id,title,type) VALUES(-1,'Group','supergroup'),(-2,'Broadcast','channel')")
        self.insert(source, chat=-1)
        self.insert(source, chat=-2)
        self.insert(source, chat=-3)
        self.insert(source, chat=1)
        self.assertEqual({r['chat_id'] for r in source.backfill_page(0, 100)}, {'1','-2'})
        source.install_capture()
        self.assertEqual(source.allowed_groups(), [])
        self.assertEqual(source.telegram_channels(), ['-2'])
        self.assertIsNone(source.get('-1', '10'))
        self.write(source, "INSERT INTO group_monitoring_consent VALUES(-1,1,'synthetic approval','now')")
        self.assertEqual(source.allowed_groups(), ['-1'])
        self.assertIsNotNone(source.get('-1', '10'))
        watermark = source.watermark()
        self.write(source, "UPDATE group_monitoring_consent SET allowed=0 WHERE chat_id=-1")
        self.assertIsNone(source.get('-1', '10'))
        self.assertEqual(source.changes(watermark, 100), [])
        self.insert(source, chat=-1, mid=11)
        self.write(source, "INSERT INTO transcripts(chat_id,id,text,status) VALUES(-1,10,'secret','done')")
        with sqlite3.connect(source.path) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM messages WHERE chat_id=-1 AND id=11').fetchone()[0], 0)
            self.assertEqual(c.execute('SELECT count(*) FROM transcripts WHERE chat_id=-1').fetchone()[0], 0)
        self.assertIsNotNone(source.get('-2', '10'))
        self.assertIsNotNone(source.get('1', '10'))
        watermark = source.watermark()
        self.write(source, "UPDATE chats SET title='Renamed broadcast' WHERE id=-2")
        self.assertEqual(source.watermark(), watermark)
        self.write(source, "UPDATE chats SET type='supergroup' WHERE id=-2")
        self.assertGreater(source.watermark(), watermark)
        self.assertEqual(source.telegram_channels(), [])
        self.assertIsNone(source.get('-2', '10'))

    def test_telegram_legacy_missing_type_denies_unknown_negative(self):
        source = self.source('telegram')
        self.insert(source, chat=-99)
        self.assertEqual(source.backfill_page(0, 100), [])
        source.install_capture()
        self.assertEqual(source.telegram_channels(), [])

class ConsentHeartbeatTests(unittest.TestCase):
    setUp = SourceTests.setUp
    source = SourceTests.source
    write = SourceTests.write
    insert = SourceTests.insert

    def test_fresh_automatic_heartbeats_do_not_requeue_messages(self):
        for kind, chat, column in [('whatsapp', '123@g.us', 'chat_jid'), ('telegram', -1, 'chat_id')]:
            with self.subTest(kind=kind):
                source = self.source(kind)
                self.insert(source, chat=chat)
                source.install_capture()
                with sqlite3.connect(source.path) as db:
                    db.execute("INSERT INTO group_monitoring_consent VALUES(?,1,'auto:max-members:10',datetime('now'))", (chat,))
                    before = db.execute('SELECT count(*) FROM memory_changes').fetchone()[0]
                    db.execute("UPDATE group_monitoring_consent SET updated_at=datetime('now','+1 second')")
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_changes').fetchone()[0], before)
                    db.execute("UPDATE group_monitoring_consent SET updated_at='2000-01-01T00:00:00Z'")
                    db.execute("UPDATE group_monitoring_consent SET updated_at=datetime('now')")
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_changes').fetchone()[0], before)
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_group_rescan').fetchone()[0], 1)

class GroupRescanTests(unittest.TestCase):
    setUp = SourceTests.setUp
    source = SourceTests.source
    write = SourceTests.write
    insert = SourceTests.insert

    def test_legacy_empty_cursor_cannot_block_live_changes_or_group_recovery(self):
        source = self.source('telegram')
        for mid in range(1, 451):
            self.insert(source, chat=-1, mid=mid)
        source.install_capture()
        self.write(source, "INSERT INTO group_monitoring_consent VALUES(-1,1,'explicit approval','now')")
        self.write(source, "UPDATE memory_group_rescan SET cursor='' WHERE chat_id='-1'")
        self.insert(source, chat=-1, mid=451, text='synthetic recent message')

        batch = source.changes(0, 1000)
        self.assertEqual(batch[0]['message_id'], '451', 'Live changes must keep priority')
        self.assertEqual(len(batch), 201, 'Recovery must remain bounded to 200 rows')
        seen = {row['message_id'] for row in batch}
        checkpoint = batch[-1]['seq']
        source = Source('telegram', source.path)
        source.install_capture()
        for _ in range(4):
            batch = source.changes(checkpoint, 1000)
            if not batch:
                break
            self.assertLessEqual(len(batch), 200)
            seen.update(row['message_id'] for row in batch)
            checkpoint = batch[-1]['seq']
        self.assertEqual(seen, {str(mid) for mid in range(1, 452)})
        with sqlite3.connect(source.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM memory_group_rescan').fetchone()[0], 0)

    def test_approval_is_constant_size_and_rescan_is_bounded_durable(self):
        for kind, chat in [('whatsapp', '123@g.us'), ('telegram', -1)]:
            with self.subTest(kind=kind):
                source = self.source(kind)
                for mid in range(1, 451):
                    self.insert(source, chat=chat, mid=str(mid) if kind == 'whatsapp' else mid)
                source.install_capture()
                with sqlite3.connect(source.path) as db:
                    db.execute("INSERT INTO group_monitoring_consent VALUES(?,0,'auto:max-members:10',datetime('now'))", (chat,))
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_group_rescan').fetchone()[0], 0)
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_changes').fetchone()[0], 0)
                    db.execute('UPDATE group_monitoring_consent SET allowed=1')
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_group_rescan').fetchone()[0], 1)
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_changes').fetchone()[0], 0)
                batch = source.changes(0, 1000)
                self.assertEqual(len(batch), 200)
                seen = {r['message_id'] for r in batch}
                cursor = batch[-1]['seq']
                # Restart retains the per-group keyset cursor and never resets it.
                source = Source(kind, source.path)
                source.install_capture()
                for expected in (200, 50):
                    batch = source.changes(cursor, 1000)
                    self.assertEqual(len(batch), expected)
                    self.assertFalse(seen & {r['message_id'] for r in batch})
                    seen.update(r['message_id'] for r in batch)
                    cursor = batch[-1]['seq']
                self.assertEqual(len(seen), 450)
                self.assertEqual(source.changes(cursor, 1000), [])
                with sqlite3.connect(source.path) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_group_rescan').fetchone()[0], 0)

    def test_revoked_or_expired_rescans_are_dropped_without_message_reads(self):
        for kind, chat in [('whatsapp', '123@g.us'), ('telegram', -1)]:
            with self.subTest(kind=kind):
                source = self.source(kind)
                self.insert(source, chat=chat)
                source.install_capture()
                with sqlite3.connect(source.path) as db:
                    db.execute("INSERT INTO group_monitoring_consent VALUES(?,1,'auto:max-members:10',datetime('now'))", (chat,))
                    db.execute("UPDATE group_monitoring_consent SET updated_at='2000-01-01T00:00:00Z'")
                self.assertEqual(source.changes(0, 100), [])
                with sqlite3.connect(source.path) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM memory_group_rescan').fetchone()[0], 0)
                    db.execute("UPDATE group_monitoring_consent SET updated_at=datetime('now')")
                    db.execute('UPDATE group_monitoring_consent SET allowed=0')
                self.assertEqual(source.changes(0, 100), [])
