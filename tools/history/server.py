"""Private loopback query API consumed by WhatsApp and Telegram MCP servers."""
import json
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from embeddings import Embedder
from store import Store, SOURCES
from priority import query_priority


def date_value(value,end=False):
    if value is None or value=='':
        return None
    if isinstance(value,(int,float)):
        return int(value)
    date=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if date.tzinfo is None:
        date=date.replace(tzinfo=timezone.utc)
    return int(date.timestamp())+(86399 if end and len(str(value))==10 else 0)


def request_filters(body):
    source=body.get('source')
    if source not in SOURCES:
        raise ValueError('source must be whatsapp or telegram')
    result=dict(source=source,chat=str(body['chat']) if body.get('chat') else None,
                sender=str(body['sender']) if body.get('sender') else None,
                after=date_value(body.get('after')),before=date_value(body.get('before'),end=True))
    if result['after'] is not None and result['before'] is not None and result['after']>result['before']:
        raise ValueError('after must not exceed before')
    return result


def search(store,model,body):
    from search_engine import execute
    return execute(store,model,body,request_filters(body))


def compose_status(snapshot, cheap, now, source=None):
    """Worker-computed coverage (may be minutes old) plus live millisecond figures; never the heavy query."""
    out = dict(messages=None, text_messages=None, embedded_messages=None, semantic_complete=None)
    age = None
    if snapshot:
        data = snapshot['data']
        scoped = data.get('per_source', {}).get(source) if source else data
        if source and data.get('source') == source:
            scoped = data
        if scoped is not None:
            out.update({k: v for k, v in data.items() if k in ('model', 'workers', 'computed_at', 'coverage_note')})
            out.update({k: v for k, v in scoped.items() if k != 'per_source'})
            age = int((now - snapshot['updated_at']).total_seconds())
    # Live values must never inherit an old cached value after a failed read.
    for key in ('lag_s', 'pending_embeddings', 'ingested_last_hour', 'last_indexed_at'):
        out.pop(key, None)
    out.update(cheap)
    out.update(source=source or 'all', pending_embeddings_scope='all',
               status_age_s=age, stale=age is None or age > 900, measured_at=now.isoformat())
    return out


class StatusCache:
    """Kept for the handler's call shape; the status is cheap now, so there is nothing to cache."""
    def __init__(self,store):
        self.store=store

    def get(self,source):
        """Never 503: the heavy snapshot and the cheap figures degrade independently."""
        now=datetime.now(timezone.utc)
        snapshot=None
        try:
            snapshot=self.store.status_snapshot()
        except Exception as error:
            snapshot_error=type(error).__name__
        else:
            snapshot_error=None
        try:
            cheap=self.store.cheap_status(source)
        except Exception as error:
            cheap=dict(cheap_error=type(error).__name__)
        out=compose_status(snapshot,cheap,now,source)
        if snapshot_error:
            out['snapshot_error']=snapshot_error
        try:
            out['status_error']=self.store.status_error()
        except Exception:
            out['status_error']=None
        return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*_):
        pass

    def send_json(self,code,data):
        raw=json.dumps(data,default=str).encode()
        self.send_response(code)
        self.send_header('Content-Type','application/json')
        self.send_header('Cache-Control','no-store')
        self.send_header('Content-Length',str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        self.handle_request(False)

    def do_POST(self):
        self.handle_request(True)

    def handle_request(self,post):
        if self.headers.get('Origin') or self.headers.get('Host','').split(':')[0] not in ('127.0.0.1','localhost'):
            self.send_json(403,dict(error='Private loopback API'))
            return
        if not self.server.slots.acquire(blocking=False):
            self.send_json(503,dict(error='Query capacity busy; retry shortly'))
            return
        try:
            priority=query_priority(self.server.model.root)
            priority.__enter__()
            path=urlparse(self.path)
            body={}
            if post:
                size=int(self.headers.get('Content-Length','0'))
                if size<1 or size>16384 or self.headers.get('Content-Type','').split(';')[0]!='application/json':
                    raise ValueError('Expected bounded application/json request')
                self.connection.settimeout(10)
                body=json.loads(self.rfile.read(size))
            if not post and path.path=='/health':
                with self.server.store.connection() as db:
                    db.execute('SELECT 1 FROM memory_meta LIMIT 1')
                result=dict(ok=True,backend='postgresql-pgvector',compute='local')
            elif not post and path.path=='/status':
                source=parse_qs(path.query).get('source',[None])[0]
                if source is not None and source not in SOURCES:
                    raise ValueError('Invalid source')
                result=self.server.cache.get(source)
            elif post and path.path=='/search':
                result=search(self.server.store,self.server.model,body)
            elif post and path.path=='/analytics':
                result=self.server.store.analytics(**request_filters(body),group_by=body.get('group_by','month'),
                                                   limit=max(1,min(int(body.get('limit',50)),100)))
            else:
                self.send_json(404,dict(error='Unknown endpoint'))
                return
            self.send_json(200,result)
        except (ValueError,TypeError,KeyError):
            self.send_json(400,dict(error='Invalid request fields; verify source, dates, mode and limits'))
        except Exception as error:
            self.send_json(503,dict(error='History query temporarily unavailable',kind=type(error).__name__))
        finally:
            if 'priority' in locals():
                priority.__exit__(None,None,None)
            self.server.slots.release()


if __name__ == '__main__':
    raise SystemExit('Use history.py api')
