"""Synthetic Telegram attachment capture and identity boundaries."""
import sqlite3
import unittest
from tools.history.tests import test_sources

class TelegramAttachments(unittest.TestCase):
    setUp=test_sources.SourceTests.setUp
    source=test_sources.SourceTests.source
    write=test_sources.SourceTests.write
    insert=test_sources.SourceTests.insert
    def ready(self):
        source=self.source('telegram')
        with sqlite3.connect(source.path) as c:
            c.executescript('''ALTER TABLE messages ADD COLUMN media_hash TEXT DEFAULT '';
                CREATE TABLE attachment_analysis(chat_id INTEGER,message_id INTEGER,media_hash TEXT,text TEXT,status TEXT,PRIMARY KEY(chat_id,message_id));
                CREATE TABLE message_reactions(chat_id INTEGER,message_id INTEGER,text TEXT,PRIMARY KEY(chat_id,message_id));''')
        source.install_capture();self.insert(source,media='document')
        self.write(source,"UPDATE messages SET media_hash='document:42'")
        return source
    def test_attachment_join_identity_and_capture(self):
        source=self.ready();watermark=source.watermark()
        self.write(source,"INSERT INTO attachment_analysis VALUES(1,10,'document:42','private text','partial')")
        self.assertIn('private text',source.get(1,10)['text'])
        self.assertGreater(source.watermark(),watermark)
        watermark=source.watermark()
        self.write(source,"UPDATE attachment_analysis SET text='updated private text'")
        self.assertGreater(source.watermark(),watermark)
        self.assertIn('updated private text',source.get(1,10)['text'])
        watermark=source.watermark()
        self.write(source,'DELETE FROM attachment_analysis')
        self.assertGreater(source.watermark(),watermark)
        self.assertNotIn('private text',source.get(1,10)['text'])
        self.write(source,"INSERT INTO attachment_analysis VALUES(1,10,'document:42','private text','done')")
        self.write(source,"UPDATE messages SET media_hash='document:43'")
        self.assertNotIn('private text',source.get(1,10)['text'])
        self.write(source,"UPDATE messages SET deleted=1")
        self.assertIsNone(source.get(1,10))
    def test_unknown_identity_and_revoked_group_not_indexed(self):
        source=self.ready()
        self.write(source,"UPDATE messages SET media_hash=''")
        self.write(source,"INSERT INTO attachment_analysis VALUES(1,10,'','stale','done')")
        self.assertNotIn('stale',source.get(1,10)['text'])
        self.write(source,"INSERT INTO chats(id,title) VALUES(-7,'group')")
        self.write(source,"INSERT INTO group_monitoring_consent VALUES(-7,1,'manual',datetime('now'))")
        self.insert(source,chat=-7,mid=11,media='document')
        self.write(source,"UPDATE messages SET media_hash='document:42' WHERE chat_id=-7")
        self.write(source,"INSERT INTO attachment_analysis VALUES(-7,11,'document:42','group secret','done')")
        self.assertIn('group secret',source.get(-7,11)['text'])
        self.write(source,"UPDATE group_monitoring_consent SET allowed=0")
        self.assertIsNone(source.get(-7,11))
        self.write(source,"UPDATE attachment_analysis SET text='new secret' WHERE chat_id=-7")
        with sqlite3.connect(source.path) as c:self.assertEqual(c.execute('SELECT text FROM attachment_analysis WHERE chat_id=-7').fetchone()[0],'group secret')
    def test_audio_video_and_reactions(self):
        source=self.ready();self.write(source,"UPDATE messages SET media_type='video'")
        self.write(source,"INSERT INTO transcripts(chat_id,id,text,status) VALUES(1,10,'spoken','done')")
        self.assertIn('[Audio del video] spoken',source.get(1,10)['text'])
        start=source.watermark();self.write(source,"INSERT INTO message_reactions VALUES(1,10,'👍 ×2')")
        self.assertIn('[Reacciones] 👍 ×2',source.get(1,10)['text']);self.assertGreater(source.watermark(),start)
        for kind in ('audio','video_note'):
            self.write(source,'UPDATE messages SET media_type=?',(kind,))
            self.assertIn('spoken',source.get(1,10)['text'])

if __name__=='__main__':unittest.main()
