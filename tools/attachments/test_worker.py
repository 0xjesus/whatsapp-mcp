import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).with_name('worker.py')

class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(MODULE.exists(), 'durable attachment worker is missing')
        spec = importlib.util.spec_from_file_location('attachment_worker', MODULE)
        self.worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.worker)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'messages.db'
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.db.executescript('''CREATE TABLE messages(id TEXT,chat_jid TEXT,content TEXT,media_type TEXT,filename TEXT,file_sha256 BLOB,timestamp TEXT,PRIMARY KEY(id,chat_jid));
            INSERT INTO messages VALUES('one','direct','caption','document','test.txt',X'1234','2026-01-01');
            INSERT INTO messages VALUES('two','unapproved@g.us','private group caption','image','test.png',X'5678','2026-01-01');''')
        self.worker.initialize(self.db)

    def test_group_default_denied(self):
        self.assertEqual([r['message_id'] for r in self.worker.pending(self.db)], ['one'])

    def test_result_keeps_caption_and_is_idempotent(self):
        row = self.worker.pending(self.db)[0]
        self.worker.save(self.db, row, {'text':'entire attachment evidence','status':'done'})
        self.assertEqual(self.db.execute('SELECT content FROM messages WHERE id="one"').fetchone()[0], 'caption')
        self.assertEqual(self.worker.pending(self.db), [])
        self.assertEqual(self.db.execute('SELECT text FROM attachment_analysis').fetchone()[0], 'entire attachment evidence')

    def test_replaced_media_requeues(self):
        row = self.worker.pending(self.db)[0]
        self.worker.save(self.db, row, {'text':'old content','status':'done'})
        self.db.execute("UPDATE messages SET file_sha256=X'9999' WHERE id='one'")
        self.assertEqual(len(self.worker.pending(self.db)), 1)

    def test_deleted_message_cannot_publish_result(self):
        row = self.worker.pending(self.db)[0]
        self.db.execute("DELETE FROM messages WHERE id='one'")
        self.worker.save(self.db, row, {'text':'revoked content','status':'done'})
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM attachment_analysis').fetchone()[0], 0)

    def test_new_media_arriving_during_extraction_discards_old_result(self):
        row = self.worker.pending(self.db)[0]
        self.db.execute("UPDATE messages SET file_sha256=X'9999' WHERE id='one'")
        self.worker.save(self.db, row, {'text':'stale content','status':'done'})
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM attachment_analysis').fetchone()[0], 0)

    def test_ocr_only_is_upgraded_when_vision_is_enabled(self):
        row = self.worker.pending(self.db)[0]
        self.worker.save(self.db,row,{'text':'OCR text','status':'partial','reason':'vision_not_configured'})
        with patch.dict('os.environ',{'WA_VISION_MODEL':'/synthetic/local/model'}):
            self.assertEqual(len(self.worker.pending(self.db)),1)

    def test_cloud_upgrade_and_budget_deferral(self):
        import time
        row = self.worker.pending(self.db)[0]
        self.worker.save(self.db,row,{'text':'evidence','status':'done'})
        self.db.execute('UPDATE attachment_analysis SET retry_at=0'); self.db.commit()
        with patch.dict('os.environ',{'WA_ATTACHMENT_BACKEND':'openai'}):
            self.assertEqual(len(self.worker.pending(self.db)),1)
            self.worker.save(self.db,row,{'text':'evidence','status':'partial',
                'cloud_version':'openai-v1','cloud_pending':True,'retry_at':time.time()+1000})
            self.assertEqual(self.worker.pending(self.db),[])
            self.assertEqual(self.db.execute('SELECT attempts FROM attachment_analysis').fetchone()[0],0)

    def test_upgrade_failure_keeps_previous_searchable_evidence(self):
        row = self.worker.pending(self.db)[0]
        self.worker.save(self.db,row,{'text':'existing evidence','status':'done'})
        with patch.dict('os.environ',{'WA_ATTACHMENT_BACKEND':'openai'}):
            self.worker.save(self.db,row,{'text':'','status':'failed','reason':'download_unavailable'})
        got=self.db.execute('SELECT text,status FROM attachment_analysis').fetchone()
        self.assertEqual(tuple(got),('existing evidence','partial'))

    def test_first_cloud_configuration_failure_keeps_fresh_extraction(self):
        import os
        self.db.execute("UPDATE messages SET file_sha256=X'' WHERE id='one'")
        self.db.commit()
        class Mcp:
            def call(self, name, args):
                Path(args['output_path']).write_text('synthetic document')
                return {}
        with patch.dict(os.environ,{'WA_ATTACHMENT_BACKEND':'openai','WA_STORE':self.temp.name},clear=True), patch.object(self.worker,'analyze',return_value={'text':'fresh evidence','status':'done'}):
            self.worker.run_once(self.db,Mcp(),self.temp.name)
        got=self.db.execute('SELECT text,status FROM attachment_analysis').fetchone()
        self.assertEqual(tuple(got),('fresh evidence','partial'))

if __name__ == '__main__': unittest.main()
