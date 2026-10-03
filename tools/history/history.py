#!/usr/bin/env python3
"""Portable, opt-in history index setup and process launcher (Python 3.10+)."""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sqlite3
import subprocess
import threading
from datetime import datetime, timezone


def private_json(path, value):
    """Create only; do not follow or replace existing credential files."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as out:
        json.dump(value, out, indent=2)
        out.write('\n')


def private_dir(path):
    if path.is_symlink():
        raise ValueError('Private directory must not be a symlink')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)


def source_backup(sources, destination):
    private_dir(destination)
    for kind, path in sources.items():
        target = destination / (kind + '-messages.db')
        with sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True) as origin:
            with sqlite3.connect(target) as copy:
                origin.backup(copy)
                if copy.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise RuntimeError('Source backup failed its integrity check')
        target.chmod(0o600)


def install(args):
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    if not re.fullmatch(r'[a-z][a-z0-9_]{0,39}', args.database):
        raise ValueError('Database name must be a lowercase identifier, at most 40 characters')
    sources = {'whatsapp': str(args.whatsapp_db.resolve(strict=True))}
    if args.telegram_db:
        sources['telegram'] = str(args.telegram_db.resolve(strict=True))
    for path in sources.values():
        if not Path(path).is_file():
            raise ValueError('Source must be an existing SQLite file')
    for name in ('database.json', 'reader.json', 'embeddings.json', 'runtime.json'):
        if (args.config_dir / name).exists() or (args.config_dir / name).is_symlink():
            raise FileExistsError('Config already exists; reuse this installation or choose a new config directory')
    admin = args.admin_dsn_file.read_text().strip()
    connection = conninfo_to_dict(admin)
    database, writer, reader = args.database, args.database + '_writer', args.database + '_reader'
    passwords = {writer: secrets.token_urlsafe(32), reader: secrets.token_urlsafe(32)}
    private_dir(args.config_dir)
    private_dir(args.data_dir)
    backup = args.data_dir / 'backups' / ('install-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    source_backup(sources, backup)
    # Each install provisions a fresh dedicated database. Never take over existing objects.
    with psycopg.connect(admin, autocommit=True) as db:
        if db.execute('SELECT 1 FROM pg_database WHERE datname=%s', (database,)).fetchone():
            raise ValueError('Database already exists; use a fresh dedicated database')
        if db.execute('SELECT 1 FROM pg_roles WHERE rolname=ANY(%s)', ([writer, reader],)).fetchone():
            raise ValueError('Database roles already exist; use a fresh database name')
        for role in (writer, reader):
            db.execute(sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}').format(
                sql.Identifier(role), sql.Literal(passwords[role])))
        db.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(database), sql.Identifier(writer)))
        db.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(database)))
        db.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}, {}').format(
            sql.Identifier(database), sql.Identifier(writer), sql.Identifier(reader)))
    target_admin = make_conninfo(**dict(connection, dbname=database))
    with psycopg.connect(target_admin) as db:
        db.execute('CREATE EXTENSION IF NOT EXISTS vector')
        version = db.execute("SELECT extversion FROM pg_extension WHERE extname='vector'").fetchone()[0]
        if tuple(int(p) for p in version.split('.')[:2]) < (0, 8):
            raise RuntimeError('pgvector >= 0.8 is required')
        db.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC')
        db.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(reader)))
        db.execute(sql.SQL('ALTER ROLE {} SET default_transaction_read_only=on').format(sql.Identifier(reader)))
    writer_dsn = make_conninfo(**dict(connection, dbname=database, user=writer, password=passwords[writer]))
    reader_dsn = make_conninfo(**dict(connection, dbname=database, user=reader, password=passwords[reader]))
    # Local mode is the default. No message leaves this machine unless explicitly configured.
    embedding = dict(provider='local', model=args.model, dimensions=1024,
                     base_url=args.embedding_url, key_file='', send_dimensions=False,
                     batch_rows=16, batch_bytes=1600, usd_per_million_tokens=0)
    private_json(args.config_dir / 'embeddings.json', embedding)
    private_json(args.config_dir / 'database.json', dict(writer_dsn=writer_dsn, reader_dsn=reader_dsn))
    private_json(args.config_dir / 'reader.json', dict(reader_dsn=reader_dsn))
    private_json(args.config_dir / 'runtime.json', dict(data_dir=str(args.data_dir), sources=sources))
    from store import Store
    from embeddings import Embedder
    store = Store(writer_dsn)
    store.initialize()
    store.bind_model(Embedder(args.data_dir).identity())
    with psycopg.connect(writer_dsn) as db:
        db.execute(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}').format(sql.Identifier(reader)))
        db.execute(sql.SQL('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {}').format(sql.Identifier(reader)))
    private_json(backup / 'install-metadata.json', dict(database=database, pgvector=version, sources=sources,
                 model=args.model, dimensions=1024))
    print(json.dumps(dict(installed=True, config_dir=str(args.config_dir), data_dir=str(args.data_dir), backup=str(backup))))


def backup(args, config, runtime):
    import psycopg
    from psycopg.conninfo import conninfo_to_dict
    destination = args.destination.resolve()
    destination.mkdir(parents=True, exist_ok=False, mode=0o700)
    # Keep authentication out of argv, shell history and command output.
    values = conninfo_to_dict(config['writer_dsn'])
    env = dict(os.environ)
    for key, variable in [('host', 'PGHOST'), ('port', 'PGPORT'), ('user', 'PGUSER'),
                          ('password', 'PGPASSWORD'), ('dbname', 'PGDATABASE'), ('sslmode', 'PGSSLMODE')]:
        if key in values:
            env[variable] = values[key]
    subprocess.run(['pg_dump', '--format=custom', '--file', str(destination / 'index.dump')], env=env, check=True)
    for name in ('database.json', 'reader.json', 'embeddings.json', 'runtime.json'):
        shutil.copyfile(args.config_dir / name, destination / name)
        (destination / name).chmod(0o600)
    source_backup(runtime['sources'], destination / 'sources')
    with psycopg.connect(config['writer_dsn']) as db:
        metadata = dict(db.execute('SELECT key,value FROM memory_meta').fetchall())
    private_json(destination / 'metadata.json', dict(model=metadata, created_at=datetime.now(timezone.utc).isoformat()))
    print(json.dumps(dict(backup=str(destination))))


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config-dir', type=Path, default=Path.home() / '.config/messaging-memory')
    sub = parser.add_subparsers(dest='command', required=True)
    setup = sub.add_parser('install', help='provision a fresh PostgreSQL index and private configuration')
    setup.add_argument('--admin-dsn-file', type=Path, required=True)
    setup.add_argument('--database', default='messaging_memory')
    setup.add_argument('--data-dir', type=Path, default=Path.home() / '.local/share/messaging-memory')
    setup.add_argument('--whatsapp-db', type=Path, required=True)
    setup.add_argument('--telegram-db', type=Path)
    setup.add_argument('--embedding-url', default='http://127.0.0.1:8083/v1')
    setup.add_argument('--model', default='bge-m3')
    sub.add_parser('worker', help='ingest and embed continuously')
    api = sub.add_parser('api', help='serve the read-only loopback API')
    api.add_argument('--port', type=int, default=7256)
    sub.add_parser('usage', help='show local token accounting')
    migration = sub.add_parser('migrate-model', help='reset vectors after a model change; stop both services first')
    migration.add_argument('--confirm-reembed', action='store_true', required=True)
    archive = sub.add_parser('backup', help='back up index, metadata, credentials and source SQLite databases')
    archive.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    args.config_dir = args.config_dir.expanduser().resolve()
    os.environ['MESSAGING_MEMORY_CONFIG_DIR'] = str(args.config_dir)
    if args.command == 'install':
        args.data_dir = args.data_dir.expanduser().resolve()
        os.environ['MESSAGING_MEMORY_DATA_DIR'] = str(args.data_dir)
        install(args)
        return
    runtime = json.loads((args.config_dir / 'runtime.json').read_text())
    root = Path(runtime['data_dir'])
    os.environ['MESSAGING_MEMORY_DATA_DIR'] = str(root)
    if args.command == 'api':
        config = json.loads((args.config_dir / 'reader.json').read_text())
        from http.server import ThreadingHTTPServer
        from server import Handler, StatusCache
        from store import Store
        from embeddings import Embedder
        server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
        server.daemon_threads = True
        server.store = Store(config['reader_dsn'])
        server.model = Embedder(root)
        server.model.expected_identity = server.model.identity()
        server.store.check_model(server.model.expected_identity)
        server.cache = StatusCache(server.store)
        server.slots = threading.BoundedSemaphore(3)
        try:
            server.serve_forever()
        finally:
            server.server_close()
    elif args.command == 'worker':
        from worker import run
        run(root=root, config_path=args.config_dir / 'database.json', source_paths=runtime['sources'])
    elif args.command == 'usage':
        import embed_config
        import usage
        cfg = embed_config.load()
        print(json.dumps(usage.summary(cfg['usd_per_million_tokens'])))
    elif args.command == 'migrate-model':
        import migrate_model
        migrate_model.main()
    elif args.command == 'backup':
        config = json.loads((args.config_dir / 'database.json').read_text())
        backup(args, config, runtime)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Driver exceptions can include connection parameters: never print credentials.
        raise SystemExit('History command failed (' + type(error).__name__ + '); check configuration and service availability.') from None
