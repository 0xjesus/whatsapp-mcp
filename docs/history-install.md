# Optional semantic history index

The Go daemon works without this package. The optional Python worker copies cached message text into a dedicated PostgreSQL database, creates 1024-dimensional embeddings, and serves a local read-only query API on `127.0.0.1:7256`. SQLite remains the authoritative source. This does not retrieve messages that WhatsApp never supplied to the local cache.

The worker adds `memory_capture_meta`, `memory_changes`, and `memory_*` triggers to each configured source SQLite database. Inserts, edits, deletes, and completed audio/video transcripts then update the derived index. The installer takes consistent SQLite backups before the worker installs these triggers. Allow enough disk space for the source backups, PostgreSQL text and vectors, and its indexes.

## Requirements and example paths

Use Linux or macOS, Python 3.10 or newer, `uv`, PostgreSQL 16 or newer with pgvector **0.8 or newer**, and an existing WhatsApp message cache. Windows users need a Linux environment because the worker uses POSIX file locks. Install a matching PostgreSQL client (`pg_dump`/`pg_restore`) for backups.

All paths below are illustrative absolute paths: the checkout is `/opt/whatsapp-mcp`, the service account is `alice`, and the existing cache is `/home/alice/.local/share/whatsapp-mcp/store/messages.db`. Substitute the actual absolute paths on your machine. No messages, tokens, credentials, databases, or generated configuration belong in the checkout.

Bootstrap the pinned Python dependencies:

```sh
sh /opt/whatsapp-mcp/tools/history/bootstrap.sh /home/alice/.local/share/messaging-memory/venv
```

Keep the checkout in place: the launcher imports the bundled Python modules directly. An upgrade replaces these source files; it does not require reinstalling PostgreSQL or recreating the index. Run the dependency bootstrap again when the lock file changes.

## PostgreSQL

An existing PostgreSQL installation is suitable if it has pgvector installed and allows a dedicated database. The installer needs an administrator connection capable of creating roles, databases, and the `vector` extension. It creates a fresh database and refuses to take over an existing database or roles.

For a standalone Docker installation, this example uses a persistent volume, loopback binding, and a pinned image. Keep the container and volume; deleting the volume destroys the derived index.

```sh
install -d -m 700 /home/alice/.config/messaging-memory-bootstrap
python3 - <<'PY'
import json, os, secrets
from pathlib import Path
root = Path('/home/alice/.config/messaging-memory-bootstrap')
password = secrets.token_urlsafe(32)
for name, value in {
    'postgres.env': 'POSTGRES_PASSWORD=' + password + '\n',
    'admin.dsn': 'host=127.0.0.1 port=5439 dbname=postgres user=postgres password=' + password + '\n',
}.items():
    fd = os.open(root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as out:
        out.write(value)
PY
docker volume create messaging-history-postgres
docker run --detach --name messaging-history-postgres --restart unless-stopped \
  --memory 2g --cpus 2 --publish 127.0.0.1:5439:5432 \
  --env-file /home/alice/.config/messaging-memory-bootstrap/postgres.env \
  --volume messaging-history-postgres:/var/lib/postgresql/data \
  pgvector/pgvector:pg16@sha256:ccc6e83d6e35e931dc7c5def2022729d5a6c370318d099181995567ff1fb4d6b
docker exec messaging-history-postgres pg_isready -U postgres
```

Wait until the readiness command succeeds. For an existing PostgreSQL server, instead put its administrator connection string in a mode-0600 file and pass that absolute file path below. Prefer password/SCRAM authentication for TCP connections. The installer preserves connection options such as TLS requirements in the generated DSNs. It never prints the DSNs or generated passwords.

## Install and run

Start an OpenAI-compatible local embedding server separately. It must return **1024 nonzero finite floats per input** from `POST /v1/embeddings`. The example assumes a model advertised as `bge-m3` at `http://127.0.0.1:8083/v1`. The package does not download a model or start that server. Set the actual advertised model name and endpoint explicitly.

```sh
/home/alice/.local/share/messaging-memory/venv/bin/python \
  /opt/whatsapp-mcp/tools/history/history.py \
  --config-dir /home/alice/.config/messaging-memory install \
  --admin-dsn-file /home/alice/.config/messaging-memory-bootstrap/admin.dsn \
  --whatsapp-db /home/alice/.local/share/whatsapp-mcp/store/messages.db \
  --data-dir /home/alice/.local/share/messaging-memory \
  --database messaging_memory \
  --embedding-url http://127.0.0.1:8083/v1 --model bge-m3
```

The installer creates random, separate writer and reader credentials. The writer owns the dedicated index. The reader gets `SELECT`, no write grants, and a read-only transaction default. Generated files have mode 0600 and their configuration directory has mode 0700:

* `/home/alice/.config/messaging-memory/database.json`: worker and backup credentials.
* `/home/alice/.config/messaging-memory/reader.json`: reader credential only; the API reads this file.
* `/home/alice/.config/messaging-memory/embeddings.json`: model and endpoint configuration.
* `/home/alice/.config/messaging-memory/runtime.json`: absolute source paths and runtime directory.

Without `--config-dir`, the launcher uses the current account's `.config/messaging-memory` directory under its home directory. Without `--data-dir`, installation uses `.local/share/messaging-memory` under that same home directory. The install result prints the resolved absolute paths and backup location.

Only WhatsApp is enabled by default. Add `--telegram-db /home/alice/.local/share/telegram-mcp/messages.db` during installation to opt into a compatible Telegram SQLite cache. Telegram is not required. The worker must have write access to the configured SQLite files to install capture and observe future commits. It never opens account sessions or sends messages.

Run these as two long-lived processes, in separate terminals or through your process supervisor:

```sh
/home/alice/.local/share/messaging-memory/venv/bin/python /opt/whatsapp-mcp/tools/history/history.py \
  --config-dir /home/alice/.config/messaging-memory worker
```

```sh
/home/alice/.local/share/messaging-memory/venv/bin/python /opt/whatsapp-mcp/tools/history/history.py \
  --config-dir /home/alice/.config/messaging-memory api
```

For systemd, use those complete command lines as separate `ExecStart` values, the same Unix user, `UMask=0077`, `Restart=on-failure`, and a dependency on PostgreSQL/network availability. For launchd, use the same executable and arguments in `ProgramArguments`. Run one worker per runtime directory; its file lock rejects duplicates. SIGTERM stops the worker cleanly. The API always binds loopback; `api --port` changes only its port. The endpoint has no remote authentication and must not be exposed through a public proxy or tunnel. Other local processes under the same account can query it.

```sh
curl --fail http://127.0.0.1:7256/health
curl --fail 'http://127.0.0.1:7256/status?source=whatsapp'
curl --fail http://127.0.0.1:7256/search \
  -H 'Content-Type: application/json' \
  --data '{"source":"whatsapp","query":"meeting arrangements","mode":"hybrid","limit":5}'
```

The Go daemon's `semantic_search`, `index_status`, and `history_analytics` tools connect to the fixed address `http://127.0.0.1:7256`. Keep the default API port for those tools. `keyword` search works while embeddings are pending or unavailable. `hybrid` reports degraded semantic retrieval explicitly. Coverage totals are snapshots computed by the worker; check `stale`, `status_age_s`, `semantic_complete`, and the live global `pending_embeddings` before treating a result as complete. A successful health check proves database connectivity, not embedding completion.

## Optional OpenAI backend

Local embeddings are the installer's default. Choosing a hosted backend sends message chunks and search queries to that provider. To use OpenAI, place your API key in `/home/alice/.config/messaging-memory/openai.key` with mode 0600, then replace `/home/alice/.config/messaging-memory/embeddings.json` with:

```json
{
  "provider": "openai",
  "model": "text-embedding-3-large",
  "dimensions": 1024,
  "base_url": "https://api.openai.com/v1",
  "key_file": "/home/alice/.config/messaging-memory/openai.key",
  "send_dimensions": true,
  "batch_rows": 512,
  "batch_bytes": 120000,
  "usd_per_million_tokens": 0.13
}
```

The price field is a configurable estimate, not a billing guarantee; update it to your provider's applicable rate. Local servers commonly require `send_dimensions: false`. A local server without authentication uses an empty `key_file`. Plain HTTP is accepted only on loopback. HTTPS may target a hosted provider. Requests do not follow redirects or use environment proxies. Switching endpoints does not implicitly forward the default OpenAI key to another endpoint.

Usage counters contain token and batch counts, not message bodies. Read them with:

```sh
/home/alice/.local/share/messaging-memory/venv/bin/python /opt/whatsapp-mcp/tools/history/history.py \
  --config-dir /home/alice/.config/messaging-memory usage
```

## Backup, model changes, and recovery

Stop the worker and API before maintenance. For a mutually consistent backup with SQLite, also stop processes writing the source caches. With a compatible `pg_dump` on `PATH`, create a new private backup directory:

```sh
/home/alice/.local/share/messaging-memory/venv/bin/python /opt/whatsapp-mcp/tools/history/history.py \
  --config-dir /home/alice/.config/messaging-memory backup \
  --destination /home/alice/backups/messaging-memory-before-model-change
```

The backup contains a PostgreSQL custom-format dump, `memory_meta` model identity, complete private configuration, and consistent SQLite copies. It contains personal messages and credentials: protect the whole directory. It does not copy an external embedding key or model files; preserve those separately. PostgreSQL role definitions are not in the dump. Restore into a separately provisioned compatible database, recreate writer/reader grants, and update the restored DSNs and runtime paths before starting services. Do not overwrite live source databases while their daemons are running.

After backing up and editing the embedding configuration, explicitly migrate the stopped index:

```sh
/home/alice/.local/share/messaging-memory/venv/bin/python /opt/whatsapp-mcp/tools/history/history.py \
  --config-dir /home/alice/.config/messaging-memory migrate-model --confirm-reembed
```

Migration probes the configured provider with a short synthetic string, then resets **all vectors** and rebuilds their indexes in a transaction. It preserves message rows and ingestion checkpoints. Restart the worker and API and allow the full embedding backlog to finish. This consumes compute or provider tokens and can take substantial time. A change of model/provider requires migration even when both models output 1024 dimensions; their vector spaces are incompatible. The model label is the identity, not a weight-file checksum: if a server silently replaces weights under the same name, use a new model label and migrate. Dimensions other than 1024 require a separately designed schema; this command does not resize vectors.

Restoring an older SQLite snapshot or replacing a source database is detected using source identity and event tokens, and triggers source reconciliation. A restored PostgreSQL dump must retain its matching model metadata and configuration. Never mix backups from different models. To rebuild from scratch, provision a **new** dedicated database and new configuration directory, preserving the old index until the replacement has been checked. Failed fresh installations may leave new database roles or a partially provisioned database; inspect those specific objects with the database administrator before removing them or choosing a new installation name. Existing installation files are never overwritten by `install`.

Stopping these processes disables the optional index without affecting ordinary WhatsApp MCP operations. The SQLite change queue is retained and can grow while the worker is stopped. No automatic queue pruning or secure deletion of unreferenced embedding text is provided. Deletes remove messages from search results, but old chunks and backups can retain text. If you need deletion from retained storage, plan explicit index rebuilding and backup retention.

## Verify the package

The unit tests use synthetic fixtures. The opt-in Docker integration starts its own container with a random password, a unique name, a loopback dynamic port, and resource limits. It runs the actual installer and worker against synthetic SQLite, supplies a local fake embedding server, checks keyword/semantic API results and incremental edits/deletes, verifies reader privileges, runs PostgreSQL regressions in disposable schemas, and removes only its own container. It makes no LLM API calls.

```sh
PYTHONPATH=/opt/whatsapp-mcp/tools/history \
  /home/alice/.local/share/messaging-memory/venv/bin/python -m unittest discover \
  -s /opt/whatsapp-mcp/tools/history/tests
HISTORY_DOCKER_TEST=1 PYTHONPATH=/opt/whatsapp-mcp/tools/history \
  /home/alice/.local/share/messaging-memory/venv/bin/python -m unittest discover \
  -s /opt/whatsapp-mcp/tools/history/tests -p test_integration.py
```

The Docker image digest above is the tested package fixture. If that digest is unavailable for your architecture, use a compatible PostgreSQL 16/pgvector image in your deployment and verify pgvector's version; the checked-in integration fixture deliberately remains pinned.

Telegram groups and supergroups require explicit per-group monitoring approval. The index preserves private chats and known broadcast channels, and denies unknown negative peers. See [Telegram consent](../tools/history/telegram-consent.md) for the source schema, policy refresh and derived-data cleanup. Existing installations must grant the read-only database role SELECT on `telegram_monitoring_allowed` when upgrading.
