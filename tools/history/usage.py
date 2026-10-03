"""Token accounting for the embedding backend: one JSON file, one row per UTC day, updated under a lock.
Source of truth for billing is the provider's dashboard; this is the local running estimate."""
import fcntl
import json
import os
import datetime
from pathlib import Path

DEFAULT_PATH = Path(os.environ.get('MESSAGING_MEMORY_DATA_DIR', str(Path.home() / '.local/share/messaging-memory'))) / 'embedding-usage.json'


def record(tokens, rows, path=DEFAULT_PATH, day=None):
    day = day or datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_suffix('.lock')
    with open(lock, 'a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        data = _read(path)
        entry = data.setdefault(day, {'requests': 0, 'tokens': 0, 'rows': 0})
        entry['requests'] += 1
        entry['tokens'] += int(tokens or 0)
        entry['rows'] += int(rows or 0)
        tmp = path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(data, sort_keys=True))
        os.replace(tmp, path)
    return entry


def _read(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def summary(usd_per_million, path=DEFAULT_PATH, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    data = _read(path)
    today = now.strftime('%Y-%m-%d')
    month = now.strftime('%Y-%m')
    def total(days):
        rows = [data[d] for d in days if d in data]
        return {'requests': sum(r['requests'] for r in rows), 'tokens': sum(r['tokens'] for r in rows),
                'rows': sum(r['rows'] for r in rows)}
    out = {'today': total([today]), 'month': total([d for d in data if d.startswith(month)]), 'all_time': total(list(data))}
    for block in out.values():
        block['usd'] = round(block['tokens'] / 1e6 * float(usd_per_million), 4)
    return out
