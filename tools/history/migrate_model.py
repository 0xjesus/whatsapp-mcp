"""One-shot: point the live index at the backend in ~/.config/messaging-memory/embeddings.json.
Run with the worker and API stopped. Every vector is recomputed afterwards by the worker, newest first."""
import json
import sys
from pathlib import Path

import embed_config
from embeddings import Embedder
from store import Store


def main():
    cfg = embed_config.load()
    label = embed_config.label(cfg)
    model = Embedder(config=cfg)
    model.expected_identity = model.identity()
    model.request_vectors(['prueba de conexión'])  # fail here, before touching the index, if the key is bad
    db = json.loads((embed_config.CONFIG.parent / 'database.json').read_text())
    store = Store(db['writer_dsn'])
    store.switch_model(label, model.expected_identity)
    store.check_model(model.expected_identity)
    print(json.dumps(dict(model=label, identity=model.expected_identity[:12])))


if __name__ == '__main__':
    sys.exit(main())
