"""Embedding client for any OpenAI-compatible server (OpenAI by default) with bounded batches, no redirects,
a per-call size cap and token accounting."""
import hashlib
import json
import math
import threading
import time
import urllib.request
from pathlib import Path
from collections import OrderedDict
from priority import model_slot,queries_waiting
import embed_config
import usage


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        return None


def bounded_batch(rows, limit=1600, max_rows=None):
    batch, size = [], 0
    for row in rows:
        if max_rows is not None and len(batch) >= max_rows:
            break
        count = len(row['text'].encode('utf-8')) + 4
        if batch and size + count > limit:
            break
        if count > limit:
            raise ValueError('Embedding chunk exceeds context bound')
        batch.append(row)
        size += count
    return batch


def decode_vectors(data, expected):
    rows = data.get('data', [])
    if len(rows) != expected or sorted(r.get('index',-1) for r in rows) != list(range(expected)):
        raise ValueError('Embedding response count or indices mismatch')
    vectors = [r['embedding'] for r in sorted(rows,key=lambda r:r['index'])]
    if any(len(v) != 1024 or not all(math.isfinite(float(x)) for x in v) or not any(v) for v in vectors):
        raise ValueError('Embedding dimensions or values invalid')
    return vectors


class Embedder:
    def __init__(self, root=None, config=None):
        self.root = root or Path.home()/'.local/share/messaging-memory'
        self.config = config or embed_config.load()
        self.provider = self.config['provider']
        self.batch_rows = int(self.config['batch_rows'])
        self.batch_bytes = int(self.config['batch_bytes'])
        self.keyfile = Path(self.config['key_file']) if self.config.get('key_file') else None
        self.usage_path = usage.DEFAULT_PATH
        self.expected_identity=None
        self.cache=OrderedDict()
        self.cache_lock=threading.Lock()

    def identity(self):
        """A hosted model has no file to fingerprint: the label (provider, model, dimensions) is the identity."""
        return hashlib.sha256((embed_config.label(self.config)+':'+str(self.config['dimensions'])).encode()).hexdigest()

    def interactive_recent(self):
        return queries_waiting(self.root)

    def request_vectors(self,texts,timeout=30):
        url=self.config['base_url']+'/embeddings'
        body={'model':self.config['model'],'input':texts}
        if self.config.get('send_dimensions'):
            body['dimensions']=int(self.config['dimensions'])
        cap=32*1024*1024  # 512 vectors × 1024 floats as JSON text is ~10 MiB
        headers={'Content-Type':'application/json'}
        if self.keyfile is not None and self.keyfile.exists():
            headers['Authorization']='Bearer '+self.keyfile.read_text().strip()
        request=urllib.request.Request(url,data=json.dumps(body).encode(),headers=headers)
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
        with opener.open(request,timeout=timeout) as response:
            raw=response.read(cap+1)
            if len(raw)>cap:
                raise ValueError('Embedding response exceeds bound')
            data=json.loads(raw)
            vectors=decode_vectors(data,len(texts))
        self.account(data,len(texts))
        return vectors

    def account(self,data,rows):
        """Best effort: a usage write must never fail an embedding call."""
        try:
            tokens=int(((data or {}).get('usage') or {}).get('total_tokens') or 0)
            usage.record(tokens,rows,path=self.usage_path)
        except Exception:
            pass

    def embed(self, texts, interactive=False, budget_seconds=None):
        deadline=None if budget_seconds is None else time.monotonic()+float(budget_seconds)
        if budget_seconds is not None and (not interactive or not 0<float(budget_seconds)<=30):
            raise ValueError('Invalid interactive embedding budget')
        if self.expected_identity is None or self.identity()!=self.expected_identity:
            raise RuntimeError('Model identity is not bound to the history index')
        if not texts or len(texts)>self.batch_rows or sum(len(t.encode())+4 for t in texts)>self.batch_bytes:
            raise ValueError('Embedding input exceeds bounded batch')
        key=tuple(texts)
        with self.cache_lock:
            cached=self.cache.get(key) if interactive else None
            if cached and time.monotonic()-cached[0]<600:
                self.cache.move_to_end(key)
                return cached[1]
        remaining=35 if deadline is None else max(0,deadline-time.monotonic())
        with model_slot(self.root,interactive,wait_seconds=remaining):
            if deadline is None:
                vectors=self.request_vectors(texts)
            else:
                remaining=deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('Interactive embedding budget exhausted')
                vectors=self.request_vectors(texts,timeout=remaining)
        if interactive:
            with self.cache_lock:
                self.cache[key]=(time.monotonic(),vectors)
                while len(self.cache)>64:
                    self.cache.popitem(last=False)
        return vectors
