"""Opt-in Docker test with synthetic messages only: HISTORY_DOCKER_TEST=1."""
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'pgvector/pgvector:pg16@sha256:ccc6e83d6e35e931dc7c5def2022729d5a6c370318d099181995567ff1fb4d6b'


class SyntheticEmbeddings(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        assert self.path == '/v1/embeddings'
        assert body['model'] == 'synthetic-test-model'
        vectors = []
        for i, text in enumerate(body['input']):
            axis = 0 if 'telescope' in text.lower() else 1
            vector = [0.] * 1024
            vector[axis] = 1.
            vectors.append(dict(index=i, embedding=vector))
        raw = json.dumps(dict(data=vectors, usage=dict(total_tokens=len(vectors)))).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def wait_until(predicate, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception:
            pass
        time.sleep(.2)
    raise AssertionError('Timed out waiting for test service')


@unittest.skipUnless(os.environ.get('HISTORY_DOCKER_TEST') == '1', 'set HISTORY_DOCKER_TEST=1 to run isolated Docker integration')
class IntegrationTests(unittest.TestCase):
    def test_install_worker_vectors_reader_api_and_incremental_changes(self):
        import psycopg
        from psycopg.conninfo import make_conninfo
        container = 'history-package-test-' + secrets.token_hex(6)
        processes = []
        embedding = ThreadingHTTPServer(('127.0.0.1', 0), SyntheticEmbeddings)
        threading.Thread(target=embedding.serve_forever, daemon=True).start()
        try:
            with tempfile.TemporaryDirectory(prefix='history-public-test-') as temp:
                root = Path(temp)
                config, data = root / 'config', root / 'data'
                password = secrets.token_urlsafe(32)
                env_file = root / 'postgres.env'
                env_file.write_text('POSTGRES_PASSWORD=' + password + '\n')
                env_file.chmod(0o600)
                subprocess.run(['docker', 'run', '--detach', '--rm', '--name', container,
                                '--memory', '768m', '--cpus', '2', '--publish', '127.0.0.1::5432',
                                '--env-file', str(env_file), IMAGE], check=True, capture_output=True)
                port = subprocess.check_output(['docker', 'port', container, '5432/tcp'], text=True).strip().rsplit(':', 1)[1]
                admin = make_conninfo(host='127.0.0.1', port=port, user='postgres', password=password, dbname='postgres')
                def ready():
                    with psycopg.connect(admin, connect_timeout=1) as db:
                        return db.execute('SELECT 1').fetchone()[0]
                wait_until(ready)
                admin_file = root / 'admin.dsn'
                admin_file.write_text(admin)
                admin_file.chmod(0o600)
                source = root / 'messages.db'
                with sqlite3.connect(source) as db:
                    db.executescript('''CREATE TABLE chats(jid TEXT PRIMARY KEY,name TEXT);
                        CREATE TABLE messages(id TEXT,chat_jid TEXT,sender TEXT,content TEXT,
                            timestamp TEXT,media_type TEXT,PRIMARY KEY(chat_jid,id));
                        INSERT INTO chats VALUES('test-chat','Synthetic astronomy');
                        INSERT INTO messages VALUES('m1','test-chat','test-author','Bring the telescope',
                            '2026-01-02T10:00:00Z','');
                        INSERT INTO messages VALUES('m2','test-chat','test-author','Buy bread',
                            '2026-01-02T11:00:00Z','');
                        ALTER TABLE messages ADD COLUMN file_sha256 BLOB;
                        CREATE TABLE attachment_analysis(message_id TEXT,chat_jid TEXT,text TEXT,status TEXT,media_hash TEXT);''')
                command = [sys.executable, str(ROOT / 'history.py'), '--config-dir', str(config)]
                installed = subprocess.run(command + ['install', '--admin-dsn-file', str(admin_file),
                    '--whatsapp-db', str(source), '--data-dir', str(data), '--database', 'history_test',
                    '--embedding-url', f'http://127.0.0.1:{embedding.server_port}/v1', '--model', 'synthetic-test-model'],
                    capture_output=True, text=True)
                self.assertEqual(installed.returncode, 0, installed.stderr)
                self.assertNotIn(password, installed.stdout + installed.stderr)
                installed_info = json.loads(installed.stdout)
                self.assertTrue((Path(installed_info['backup']) / 'whatsapp-messages.db').is_file())
                credentials = json.loads((config / 'database.json').read_text())
                self.assertNotEqual(credentials['writer_dsn'], credentials['reader_dsn'])
                self.assertEqual(json.loads((config / 'runtime.json').read_text())['sources'], {'whatsapp': str(source)})
                for path in config.iterdir():
                    self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                self.assertEqual(config.stat().st_mode & 0o777, 0o700)
                again = subprocess.run(command + ['install', '--admin-dsn-file', str(admin_file),
                    '--whatsapp-db', str(source)], capture_output=True, text=True)
                self.assertNotEqual(again.returncode, 0)
                self.assertEqual(json.loads((config / 'database.json').read_text()), credentials)
                with psycopg.connect(credentials['reader_dsn']) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM messages').fetchone()[0], 0)
                    with self.assertRaises(psycopg.errors.ReadOnlySqlTransaction):
                        db.execute("DELETE FROM memory_meta")
                # SQL grants still reject writes if a reader disables its session read-only default.
                with psycopg.connect(credentials['reader_dsn'], autocommit=True) as db:
                    db.execute('SET default_transaction_read_only=off')
                    with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                        db.execute('DELETE FROM memory_meta')
                log = (root / 'process.log').open('w+')
                processes.append(subprocess.Popen(command + ['worker'], stdout=log, stderr=log))
                def indexed(count):
                    with psycopg.connect(credentials['reader_dsn']) as db:
                        return db.execute('SELECT count(*) FROM embeddings WHERE embedding IS NOT NULL').fetchone()[0] == count
                wait_until(lambda: indexed(2))
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0))
                    api_port = sock.getsockname()[1]
                processes.append(subprocess.Popen(command + ['api', '--port', str(api_port)], stdout=log, stderr=log))
                base = f'http://127.0.0.1:{api_port}'
                def request(path, body=None):
                    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                                 headers={} if body is None else {'Content-Type': 'application/json'})
                    with urllib.request.urlopen(req, timeout=15) as response:
                        return json.load(response)
                self.assertTrue(wait_until(lambda: request('/health'))['ok'])
                status = request('/status?source=whatsapp')
                self.assertEqual(status['source'], 'whatsapp')
                self.assertEqual(status['pending_embeddings'], 0)
                result = request('/search', dict(source='whatsapp', query='telescope', mode='semantic', limit=5))
                self.assertEqual(result['semantic_status'], 'searched')
                self.assertFalse(result['degraded'])
                self.assertTrue(any(row['message_id'] == 'm1' for row in result['results']))
                result = request('/search', dict(source='whatsapp', query='bread', mode='keyword', limit=5))
                self.assertTrue(any(row['message_id'] == 'm2' for row in result['results']))
                with sqlite3.connect(source) as db:
                    db.execute("UPDATE messages SET content='Repair the telescope' WHERE id='m1'")
                    db.execute("DELETE FROM messages WHERE id='m2'")
                def changed():
                    with psycopg.connect(credentials['reader_dsn']) as db:
                        return db.execute('SELECT text FROM messages ORDER BY message_id').fetchall() == [('Repair the telescope',)]
                wait_until(changed)
                # Late attachment extraction must become searchable, including its final page.
                with sqlite3.connect(source) as db:
                    db.execute("INSERT INTO attachment_analysis VALUES('m1','test-chat','Final page orchid 7429','done','')")
                def attachment_found():
                    response=request('/search',dict(source='whatsapp',query='orchid',mode='keyword'))
                    return any('7429' in row['text'] for row in response['results'])
                wait_until(attachment_found)
                # Group membership does not authorize indexing; an explicit grant does.
                with sqlite3.connect(source) as db:
                    db.execute("INSERT INTO messages VALUES('g1','123@g.us','author','private telescope violet','2026-01-03T00:00:00Z','',NULL)")
                time.sleep(6)
                self.assertFalse(request('/search',dict(source='whatsapp',query='violet',mode='keyword'))['results'])
                with sqlite3.connect(source) as db:
                    db.execute("INSERT INTO group_monitoring_consent VALUES('123@g.us',1,'explicit synthetic consent','now')")
                    db.execute("INSERT INTO messages VALUES('g1','123@g.us','author','private telescope violet','2026-01-03T00:00:00Z','',NULL)")
                wait_until(lambda: bool(request('/search',dict(source='whatsapp',query='violet',mode='keyword'))['results']))
                with sqlite3.connect(source) as db:
                    db.execute("UPDATE group_monitoring_consent SET allowed=0 WHERE chat_jid='123@g.us'")
                wait_until(lambda: not request('/search',dict(source='whatsapp',query='violet',mode='keyword'))['results'])
                self.assertTrue((data / 'embedding-usage.json').is_file())
                for process in processes:
                    process.terminate()
                for process in processes:
                    process.wait(timeout=25)
                processes.clear()
                # Supply the real pg_dump from our container where a host PostgreSQL
                # client is not installed. Credentials travel in the environment.
                client_bin = root / 'client-bin'
                client_bin.mkdir()
                dump = client_bin / 'pg_dump'
                dump.write_text('#!' + sys.executable + '\n' +
                    'import subprocess, sys\n' +
                    'destination = sys.argv[sys.argv.index("--file") + 1]\n' +
                    'with open(destination, "wb") as output:\n' +
                    '    subprocess.run(' + repr(['docker', 'exec', '--env', 'PGUSER', '--env', 'PGPASSWORD',
                    '--env', 'PGDATABASE', '--env', 'PGHOST=127.0.0.1', '--env', 'PGPORT=5432', container,
                    'pg_dump', '--format=custom']) + ', stdout=output, check=True)\n')
                dump.chmod(0o700)
                archive = root / 'archive'
                archived = subprocess.run(command + ['backup', '--destination', str(archive)],
                    env=dict(os.environ, PATH=str(client_bin) + os.pathsep + os.environ['PATH']),
                    capture_output=True, text=True)
                self.assertEqual(archived.returncode, 0, archived.stderr)
                self.assertIn('model_identity', json.loads((archive / 'metadata.json').read_text())['model'])
                self.assertEqual(json.loads((archive / 'database.json').read_text()), credentials)
                with (archive / 'index.dump').open('rb') as content:
                    listing = subprocess.run(['docker', 'exec', '-i', container, 'pg_restore', '--list'],
                                              stdin=content, capture_output=True, text=True)
                self.assertEqual(listing.returncode, 0, listing.stderr)
                self.assertIn('memory_meta', listing.stdout)
                embedding_config = config / 'embeddings.json'
                new_model = json.loads(embedding_config.read_text())
                new_model['provider'] = 'local_reindexed'
                embedding_config.write_text(json.dumps(new_model))
                migrated = subprocess.run(command + ['migrate-model', '--confirm-reembed'], capture_output=True, text=True)
                self.assertEqual(migrated.returncode, 0, migrated.stderr)
                self.assertTrue(indexed(0))
                with psycopg.connect(credentials['reader_dsn']) as db:
                    self.assertTrue(db.execute("SELECT value FROM memory_meta WHERE key='model'").fetchone()[0].startswith('local_reindexed:'))
                    self.assertEqual(db.execute('SELECT count(*) FROM messages').fetchone()[0], 1)
                # Run all store/query regressions in disposable schemas in this same
                # fresh database, never against an installed user index.
                test_env = dict(os.environ, MEMORY_TEST_DSN=credentials['writer_dsn'],
                                PYTHONPATH=str(ROOT), MESSAGING_MEMORY_CONFIG_DIR=str(config),
                                MESSAGING_MEMORY_DATA_DIR=str(data))
                test_env.pop('HISTORY_DOCKER_TEST', None)
                regressions = subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s',
                                              str(ROOT / 'tests')], env=test_env, capture_output=True, text=True)
                self.assertEqual(regressions.returncode, 0, regressions.stderr)
                log.close()
        finally:
            for process in processes:
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=25)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            embedding.shutdown()
            embedding.server_close()
            subprocess.run(['docker', 'rm', '--force', container], capture_output=True)


if __name__ == '__main__':
    unittest.main()
