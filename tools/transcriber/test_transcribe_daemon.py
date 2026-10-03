import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import transcribe_daemon as daemon


class McpTests(unittest.TestCase):
    def test_expired_session_is_renewed_only_once(self):
        from urllib.error import HTTPError
        for status in (400,404):
            with self.subTest(status=status):
                client = daemon.Mcp()
                client.session = 'old-session'
                error = HTTPError('http://localhost/mcp', status, 'synthetic', {}, None)
                def initialize():
                    client.session = 'new-session'
                with patch.object(client, '_init', side_effect=initialize) as init, \
                        patch.object(daemon.urllib.request, 'urlopen', side_effect=[error,error,RuntimeError('unbounded retry')]) as request:
                    with self.assertRaises(HTTPError):
                        client.call('download_media', {})
                    self.assertEqual(request.call_count, 2)
                    init.assert_called_once()

    def test_main_creates_private_temporary_directory(self):
        fake_whisper = SimpleNamespace(WhisperModel=Mock(side_effect=SystemExit('stop before model load')))
        with patch.dict('sys.modules', {'faster_whisper':fake_whisper}), \
                patch.object(daemon.os, 'makedirs') as mkdir, patch.object(daemon.os, 'umask') as umask, \
                patch.object(daemon.os, 'chmod') as chmod:
            with self.assertRaises(SystemExit):
                daemon.main()
            umask.assert_called_once_with(0o077)
            mkdir.assert_called_once_with(daemon.TMP, mode=0o700, exist_ok=True)
            chmod.assert_called_once_with(daemon.TMP, 0o700)


class PendingTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript('''CREATE TABLE messages(id TEXT,chat_jid TEXT,timestamp TEXT,content TEXT,media_type TEXT);
            CREATE TABLE transcripts(message_id TEXT,chat_jid TEXT,text TEXT,status TEXT,attempts INTEGER,
                model TEXT,error TEXT,created_at TEXT,updated_at TEXT,PRIMARY KEY(message_id,chat_jid));''')

    def message(self, mid, content='', status=None, text=None, age='-1 minute'):
        self.db.execute("INSERT INTO messages VALUES(?,'chat',datetime('now',?),?,'audio')", (mid,age,content))
        if status:
            self.db.execute("INSERT INTO transcripts(message_id,chat_jid,text,status,attempts) VALUES(?,'chat',?,?,1)", (mid,text,status))

    def test_video_publishes_caption_and_audio_and_repairs_without_inference(self):
        self.message('video', 'Caption')
        self.db.execute("UPDATE messages SET media_type='video'")
        with tempfile.TemporaryDirectory() as directory, patch.object(daemon, 'TMP', directory):
            def download(name, args):
                Path(args['output_path']).write_bytes(b'video')
                return {}
            def extract(src, dst):
                Path(dst).write_bytes(b'audio')
            model = Mock()
            model.transcribe.return_value = ([SimpleNamespace(text='Spoken words')], SimpleNamespace(duration=3,language='en'))
            mcp = Mock()
            mcp.call.side_effect = download
            with patch.object(daemon, 'extract_audio', side_effect=extract):
                daemon.run_once(self.db, model, mcp)
            self.assertEqual(self.db.execute('SELECT content FROM messages').fetchone()[0],
                             'Caption\n[Audio del video] Spoken words')
            self.db.execute("UPDATE messages SET content='Edited caption'")
            daemon.run_once(self.db, model, mcp)
            self.assertEqual(self.db.execute('SELECT content FROM messages').fetchone()[0],
                             'Edited caption\n[Audio del video] Spoken words')
            self.db.execute("UPDATE messages SET content='Edited caption\n[Audio del video] Manual correction'")
            daemon.run_once(self.db, model, mcp)
            self.assertEqual(model.transcribe.call_count, 1)
            self.assertEqual(mcp.call.call_count, 1)
            self.assertEqual(self.db.execute('SELECT content FROM messages').fetchone()[0],
                             'Edited caption\n[Audio del video] Manual correction')

    def test_video_without_audio_becomes_terminal_without_transcription(self):
        self.message('silent', 'Keep this caption')
        self.db.execute("UPDATE messages SET media_type='video'")
        with tempfile.TemporaryDirectory() as directory, patch.object(daemon, 'TMP', directory):
            mcp, model = Mock(), Mock()
            mcp.call.side_effect = lambda name, args: Path(args['output_path']).write_bytes(b'video')
            with patch.object(daemon, 'extract_audio', side_effect=ValueError('no_audio')):
                daemon.run_once(self.db, model, mcp)
                daemon.run_once(self.db, model, mcp)
            self.assertEqual(self.db.execute('SELECT status,error FROM transcripts').fetchone(), ('failed_permanent','no_audio'))
            model.transcribe.assert_not_called()
            self.assertEqual(mcp.call.call_count, 1)
            self.assertEqual(self.db.execute('SELECT content FROM messages').fetchone()[0], 'Keep this caption')

    def test_oversized_video_is_not_published_as_a_complete_transcript(self):
        self.message('long', 'Keep caption')
        self.db.execute("UPDATE messages SET media_type='video'")
        with tempfile.TemporaryDirectory() as directory, patch.object(daemon, 'TMP', directory):
            mcp, model = Mock(), Mock()
            mcp.call.side_effect = lambda name, args: Path(args['output_path']).write_bytes(b'video')
            model.transcribe.return_value = ([SimpleNamespace(text='Partial transcript')], SimpleNamespace(duration=601))
            with patch.object(daemon, 'extract_audio', side_effect=lambda src,dst: Path(dst).write_bytes(b'audio')):
                daemon.run_once(self.db, model, mcp)
            self.assertEqual(self.db.execute('SELECT status,error FROM transcripts').fetchone(), ('failed_permanent','video_too_long'))
            self.assertEqual(self.db.execute('SELECT content FROM messages').fetchone()[0], 'Keep caption')
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_stories_never_enter_transcription_queue(self):
        self.message('story')
        self.db.execute("UPDATE messages SET chat_jid='status@broadcast',media_type='video'")
        self.assertEqual(daemon.pending(self.db), [])
    def test_extraction_is_bounded_and_atomic(self):
        with tempfile.TemporaryDirectory() as directory:
            src, dst = Path(directory)/'video.mp4', Path(directory)/'audio.ogg'
            def convert(command, **kwargs):
                self.assertEqual(command[command.index('-t')+1], '601')
                self.assertEqual(kwargs['timeout'], 120)
                Path(command[-1]).write_bytes(b'audio')
                self.assertFalse(dst.exists())
                return SimpleNamespace(returncode=0, stderr=b'')
            with patch.object(daemon.subprocess, 'run', side_effect=convert):
                daemon.extract_audio(src, dst)
            self.assertEqual(dst.read_bytes(), b'audio')
            self.assertFalse(dst.with_suffix('.ogg.part').exists())

    def test_no_audio_does_not_leave_cached_partial_audio(self):
        with tempfile.TemporaryDirectory() as directory:
            dst = Path(directory)/'audio.ogg'
            part = dst.with_suffix('.ogg.part')
            part.write_bytes(b'partial')
            with patch.object(daemon.subprocess, 'run', return_value=SimpleNamespace(returncode=1,stderr=b'Output file does not contain any stream')):
                with self.assertRaisesRegex(ValueError, 'no_audio'):
                    daemon.extract_audio(Path(directory)/'silent.mp4', dst)
            self.assertFalse(part.exists())

    def test_manual_correction_does_not_starve_untranscribed_audio(self):
        self.message('done', 'Manually corrected text', 'done', 'Machine text')
        self.message('pending', age='-2 minutes')
        with patch.object(daemon, 'BATCH', 1):
            self.assertEqual([r[0] for r in daemon.pending(self.db)], ['pending'])
        daemon.apply_text(self.db, 'done', 'chat', 'Machine text')
        self.assertEqual(self.db.execute("SELECT content FROM messages WHERE id='done'").fetchone()[0], 'Manually corrected text')

    def test_empty_done_is_terminal_and_does_not_starve_next_audio(self):
        for i, text in enumerate(('', None, '   ')):
            self.message(str(i), '', 'done', text)
        self.message('pending', age='-2 minutes')
        with patch.object(daemon, 'BATCH', 1):
            self.assertEqual([r[0] for r in daemon.pending(self.db)], ['pending'])

    def test_done_repair_restores_empty_body_once_without_inference(self):
        for content in ('', None):
            with self.subTest(content=content):
                self.db.execute('DELETE FROM messages')
                self.db.execute('DELETE FROM transcripts')
                self.message('repair', content, 'done', 'Saved words')
                self.message('pending', age='-2 minutes')
                with patch.object(daemon, 'BATCH', 1):
                    row = daemon.pending(self.db)[0]
                    self.assertEqual((row[0], row[4], row[6]), ('repair', 'done', 'Saved words'))
                    daemon.apply_text(self.db, row[0], row[1], row[6])
                    self.assertEqual([r[0] for r in daemon.pending(self.db)], ['pending'])
                self.assertEqual(self.db.execute("SELECT content FROM messages WHERE id='repair'").fetchone()[0],
                                 daemon.PREFIX + 'Saved words')


if __name__ == '__main__':
    unittest.main()
