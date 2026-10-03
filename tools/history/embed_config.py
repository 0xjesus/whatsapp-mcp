"""Embedding backend for this index: any server that speaks the OpenAI embeddings API.
Default is OpenAI itself; a local OpenAI-compatible server (llama.cpp / llamafile, vLLM, TEI) is the
offline fallback. Shared by the store (model label) and the Embedder (requests).
~/.config/messaging-memory/embeddings.json overrides any field:
  provider   short label that becomes part of the index identity ("openai", "local", ...)
  model      model name sent in the request
  base_url   https://api.openai.com/v1 or http://127.0.0.1:<port>/v1 (loopback only for plain http)
  key_file   bearer token file; may be absent for a local server without auth
  send_dimensions  include "dimensions": 1024 in the request (OpenAI yes; most local servers ignore or reject it)
  batch_rows / batch_bytes  request shape
  usd_per_million_tokens    price used by usage_report.py (0 for a local server)
Changing provider or model changes the label: run migrate_model.py (full re-embed)."""
import json
import os
from pathlib import Path
from urllib.parse import urlparse

CONFIG = Path(os.environ.get('MESSAGING_MEMORY_CONFIG_DIR', str(Path.home() / '.config/messaging-memory'))) / 'embeddings.json'
DIMENSIONS = 1024  # the halfvec(1024) schema and HNSW index are fixed; the model must emit this size
OPENAI_URL = 'https://api.openai.com/v1'
DEFAULT = dict(provider='openai', model='text-embedding-3-large', dimensions=DIMENSIONS, base_url=OPENAI_URL,
               key_file=str(CONFIG.parent / 'openai.key'), send_dimensions=True,
               batch_rows=512, batch_bytes=120000, usd_per_million_tokens=0.13)
LOCAL_EXAMPLE = dict(provider='local', model='bge-m3', dimensions=DIMENSIONS, base_url='http://127.0.0.1:8083/v1',
                     key_file=str(CONFIG.parent / 'local-model.key'), send_dimensions=False,
                     batch_rows=16, batch_bytes=1600, usd_per_million_tokens=0)


def load(path=CONFIG):
    cfg = dict(DEFAULT)
    try:
        overrides = json.loads(Path(path).read_text())
    except FileNotFoundError:
        overrides = {}
    if not isinstance(overrides, dict):
        raise ValueError('embeddings.json must contain an object')
    cfg.update(overrides)
    # Changing servers must never forward the default cloud credential implicitly.
    if 'key_file' not in overrides and (cfg['provider'] != 'openai' or
                                       str(cfg['base_url']).rstrip('/') != OPENAI_URL):
        cfg['key_file'] = None
    if not str(cfg.get('provider', '')).strip() or ':' in str(cfg['provider']):
        raise ValueError('embeddings.json needs a provider label without ":"')
    if int(cfg['dimensions']) != DIMENSIONS:
        raise ValueError('embedding dimensions must stay 1024 for this index')
    if not cfg.get('model') or ':' in str(cfg['model']):
        raise ValueError('embeddings.json needs a model name without ":"')
    url = urlparse(str(cfg.get('base_url', '')))
    loopback = url.hostname in ('127.0.0.1', 'localhost', '::1')
    if url.scheme == 'https' and url.hostname:
        pass
    elif url.scheme == 'http' and loopback:
        pass
    else:
        raise ValueError('base_url must be https:// or a loopback http:// server')
    cfg['base_url'] = str(cfg['base_url']).rstrip('/')
    return cfg


def label(cfg):
    return f"{cfg['provider']}:{cfg['model']}:{DIMENSIONS}:chunks-utf8-1200:v1"
