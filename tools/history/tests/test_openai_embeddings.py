"""OpenAI embedding backend: config, identity, request shape, batching. No network, no DB."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import embed_config
import embeddings
from embeddings import Embedder, bounded_batch


def openai_config(tmp):
    key = Path(tmp) / 'openai.key'; key.write_text('sk-test-not-real\n')
    cfg = Path(tmp) / 'embeddings.json'
    cfg.write_text(json.dumps({'provider': 'openai', 'model': 'text-embedding-3-large', 'dimensions': 1024,
                               'key_file': str(key), 'batch_rows': 256, 'batch_bytes': 60000}))
    return cfg


class ConfigTests(unittest.TestCase):
    def test_bad_config_cannot_silently_switch_to_cloud(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'embeddings.json'
            p.write_text('{broken local configuration')
            with self.assertRaises(ValueError):
                embed_config.load(p)

    def test_custom_endpoint_does_not_inherit_openai_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'embeddings.json'
            p.write_text(json.dumps({'provider': 'local', 'model': 'bge-m3',
                                     'base_url': 'http://127.0.0.1:8083/v1'}))
            self.assertIsNone(embed_config.load(p)['key_file'])
            p.write_text(json.dumps({'base_url': 'https://example.com/v1'}))
            self.assertIsNone(embed_config.load(p)['key_file'])

    def test_default_is_openai_large(self):
        cfg = embed_config.load(Path('/nonexistent/embeddings.json'))
        self.assertEqual((cfg['provider'], cfg['model']), ('openai', 'text-embedding-3-large'))
        self.assertEqual(embed_config.label(cfg), 'openai:text-embedding-3-large:1024:chunks-utf8-1200:v1')
        self.assertEqual((cfg['batch_rows'], cfg['batch_bytes']), (512, 120000))
        self.assertTrue(cfg['key_file'].endswith('openai.key'))
        self.assertEqual(cfg['base_url'], 'https://api.openai.com/v1')
        self.assertTrue(cfg['send_dimensions'])

    def test_openai_config_and_label(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = embed_config.load(openai_config(tmp))
        self.assertEqual(cfg['provider'], 'openai')
        self.assertEqual(embed_config.label(cfg), 'openai:text-embedding-3-large:1024:chunks-utf8-1200:v1')
        self.assertEqual(cfg['batch_rows'], 256)

    def test_rejects_other_dimensions_and_providers(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'e.json'
            p.write_text(json.dumps({'provider': 'openai', 'model': 'x', 'dimensions': 1536, 'key_file': 'k'}))
            with self.assertRaises(ValueError):
                embed_config.load(p)
            p.write_text(json.dumps({'provider': 'local', 'model': 'bge-m3', 'base_url': 'http://example.com/v1'}))
            with self.assertRaises(ValueError):
                embed_config.load(p)


class OpenAIEmbedderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.model = Embedder(Path(self.tmp.name), config=embed_config.load(openai_config(self.tmp.name)))

    def test_identity_is_the_model_label(self):
        ident = self.model.identity()
        self.assertFalse(hasattr(embeddings, 'subprocess'), 'no local model fingerprinting left')
        self.assertEqual(len(ident), 64)
        self.assertEqual(ident, self.model.identity())

    def test_request_shape(self):
        seen = {}

        class FakeResponse:
            def __init__(self, n): self.n = n
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self, limit=None):
                return json.dumps({'data': [{'index': i, 'embedding': [0.5] * 1024} for i in range(self.n)]}).encode()

        class FakeOpener:
            def open(self, request, timeout=None):
                seen['url'] = request.full_url; seen['auth'] = request.get_header('Authorization')
                seen['body'] = json.loads(request.data); seen['timeout'] = timeout
                return FakeResponse(len(seen['body']['input']))
        with patch.object(embeddings.urllib.request, 'build_opener', return_value=FakeOpener()):
            vectors = self.model.request_vectors(['hola', 'mundo'])
        self.assertEqual(seen['url'], 'https://api.openai.com/v1/embeddings')
        self.assertEqual(seen['auth'], 'Bearer sk-test-not-real')
        self.assertEqual(seen['body'], {'model': 'text-embedding-3-large', 'input': ['hola', 'mundo'], 'dimensions': 1024})
        self.assertEqual(len(vectors), 2); self.assertEqual(len(vectors[0]), 1024)

    def test_batch_bounds_follow_config(self):
        rows = [dict(text='x' * 1000) for _ in range(300)]
        self.assertEqual(len(bounded_batch(rows, 1600)), 1)
        self.assertEqual(len(bounded_batch(rows, 60000)), 59)
        self.assertEqual(len(bounded_batch(rows, self.model.batch_bytes, self.model.batch_rows)), 59)
        self.assertEqual(len(bounded_batch([dict(text='x')] * 300, 60000, 256)), 256)
        self.model.expected_identity = self.model.identity()
        with self.assertRaises(ValueError):
            self.model.embed(['x' * 1000] * 61)


class CompatibleBackendTests(unittest.TestCase):
    def test_local_compatible_server_config_and_label(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'embeddings.json'
            p.write_text(json.dumps(embed_config.LOCAL_EXAMPLE))
            cfg = embed_config.load(p)
        self.assertEqual(embed_config.label(cfg), 'local:bge-m3:1024:chunks-utf8-1200:v1')
        self.assertEqual(cfg['base_url'], 'http://127.0.0.1:8083/v1')
        self.assertFalse(cfg['send_dimensions'])
        self.assertEqual(cfg['usd_per_million_tokens'], 0)

    def test_request_shape_for_local_server_and_usage_accounting(self):
        import usage
        with tempfile.TemporaryDirectory() as d:
            cfg = dict(embed_config.LOCAL_EXAMPLE, key_file=str(Path(d) / 'missing.key'))
            model = embeddings.Embedder(Path(d), config=cfg)
            model.usage_path = Path(d) / 'usage.json'
            seen = {}

            class Resp:
                def __init__(self, payload): self.payload = payload
                def read(self, n=-1): return json.dumps(self.payload).encode()
                def __enter__(self): return self
                def __exit__(self, *a): return False

            def fake_open(request, timeout=None):
                seen['url'] = request.full_url
                seen['body'] = json.loads(request.data)
                seen['auth'] = request.get_header('Authorization')
                return Resp({'data': [{'index': 0, 'embedding': [0.5] * 1024}], 'usage': {'total_tokens': 7}})

            with patch.object(embeddings.urllib.request, 'build_opener', return_value=type('O', (), {'open': staticmethod(fake_open)})()):
                vectors = model.request_vectors(['hola'])
            self.assertEqual(seen['url'], 'http://127.0.0.1:8083/v1/embeddings')
            self.assertNotIn('dimensions', seen['body'], 'local servers do not get the dimensions field')
            self.assertIsNone(seen['auth'], 'no key file: no Authorization header')
            self.assertEqual(len(vectors[0]), 1024)
            report = usage.summary(0.13, path=model.usage_path)
            self.assertEqual((report['today']['requests'], report['today']['tokens'], report['today']['rows']), (1, 7, 1))
            usage.record(1_000_000, 10, path=model.usage_path)
            self.assertAlmostEqual(usage.summary(0.13, path=model.usage_path)['month']['usd'], 0.13, places=3)
