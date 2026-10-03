"""Fast /status path: worker snapshot + cheap live figures. No PostgreSQL needed (fakes)."""
import unittest
from datetime import datetime, timezone, timedelta

import server
import worker


class FakeStore:
    def __init__(self, snapshot=None, cheap=None, fail_heavy=False):
        self.snapshot, self.cheap_value, self.fail_heavy = snapshot, cheap or {}, fail_heavy
        self.heartbeats = []

    def status_snapshot(self):
        return self.snapshot

    def cheap_status(self, source):
        return dict(self.cheap_value)

    def status(self, source=None, timeout='30s'):
        if self.fail_heavy:
            raise TimeoutError('canceling statement due to statement timeout')
        return {'messages': 10, 'text_messages': 8, 'embedded_messages': 6, 'semantic_complete': False,
                'states': [{'source': 'whatsapp', 'updated_at': datetime.now(timezone.utc)}],
                'workers': [{'name': 'embeddings', 'data': {'ok': True}}, {'name': 'status', 'data': {'messages': 9, 'workers': []}}]}

    def heartbeat(self, name, data):
        self.heartbeats.append((name, data))
        if name == 'status':
            self.snapshot = {'data': data, 'updated_at': datetime.now(timezone.utc)}


class ComposeStatusTests(unittest.TestCase):
    def test_scoped_request_uses_scoped_coverage_from_single_snapshot(self):
        now = datetime.now(timezone.utc)
        snap = {'updated_at': now, 'data': {'source': 'all', 'messages': 12,
                'per_source': {'whatsapp': {'source': 'whatsapp', 'messages': 10,
                    'states': [{'source': 'whatsapp'}]},
                    'telegram': {'source': 'telegram', 'messages': 2}}}}
        for source, count in [('whatsapp', 10), ('telegram', 2), (None, 12)]:
            out = server.StatusCache(FakeStore(snapshot=snap, cheap={'lag_s': 4, 'pending_embeddings': 7})).get(source)
            self.assertEqual(out['source'], source or 'all')
            self.assertEqual(out['messages'], count)
            self.assertEqual(out['pending_embeddings_scope'], 'all')
            self.assertNotIn('per_source', out)
        self.assertEqual(server.StatusCache(FakeStore(snapshot=snap)).get('whatsapp')['states'], [{'source': 'whatsapp'}])

    def test_legacy_global_snapshot_never_claims_fresh_scoped_counts(self):
        snap = {'updated_at': datetime.now(timezone.utc), 'data': {'source': 'all', 'messages': 12}}
        out = server.StatusCache(FakeStore(snapshot=snap, cheap={'lag_s': 4})).get('whatsapp')
        self.assertEqual(out['source'], 'whatsapp')
        self.assertIsNone(out['messages'])
        self.assertIsNone(out['status_age_s'])
        self.assertTrue(out['stale'])
        self.assertEqual(out['lag_s'], 4)

    def test_failed_live_metrics_cannot_reuse_cached_live_values(self):
        snap = {'updated_at': datetime.now(timezone.utc), 'data': {'source': 'all', 'messages': 12,
                'lag_s': 0, 'pending_embeddings': 0}}
        out = server.StatusCache(FailingCheapStore(snapshot=snap)).get(None)
        self.assertIsNone(out.get('lag_s'))
        self.assertIsNone(out.get('pending_embeddings'))

    def test_status_without_cached_heavy_row(self):
        out = server.compose_status(None, {'lag_s': 12, 'pending_embeddings': 5, 'ingested_last_hour': 3, 'last_indexed_at': 1}, now=datetime.now(timezone.utc))
        self.assertIsNone(out['status_age_s'])
        self.assertEqual((out['lag_s'], out['pending_embeddings']), (12, 5))
        self.assertTrue(out['stale'])
        self.assertIsNone(out['messages'])

    def test_status_merges_snapshot_and_marks_fresh(self):
        now = datetime.now(timezone.utc)
        snap = {'data': {'messages': 10, 'embedded_messages': 6, 'semantic_complete': False}, 'updated_at': now - timedelta(seconds=120)}
        out = server.compose_status(snap, {'lag_s': 0, 'pending_embeddings': 0, 'ingested_last_hour': 9, 'last_indexed_at': 1}, now=now)
        self.assertEqual(out['messages'], 10)
        self.assertEqual(out['status_age_s'], 120)
        self.assertFalse(out['stale'])


class StatusLoopTests(unittest.TestCase):
    def test_loop_writes_status_heartbeat_once(self):
        store = FakeStore()
        worker.status_refresh(store)
        names = [n for n, _ in store.heartbeats]
        self.assertEqual(names, ['status'])
        data = store.heartbeats[0][1]
        self.assertIn('computed_at', data)
        self.assertEqual(data['messages'], 10)
        self.assertIsInstance(data['states'][0]['updated_at'], str, 'datetimes must be JSON-safe before heartbeat')
        self.assertEqual([w['name'] for w in data['workers']], ['embeddings'], 'the snapshot must not nest previous snapshots')

    def test_status_keeps_last_value_when_refresh_fails(self):
        store = FakeStore(snapshot={'data': {'messages': 1}, 'updated_at': datetime.now(timezone.utc)}, fail_heavy=True)
        worker.status_refresh(store)
        self.assertEqual([n for n, _ in store.heartbeats], ['status_error'])
        self.assertEqual(store.snapshot['data'], {'messages': 1})


class FakeModel:
    def __init__(self, interactive_after):
        self.calls, self.interactive_after = 0, interactive_after

    def interactive_recent(self):
        return self.calls >= self.interactive_after


class EmbeddingCycleTests(unittest.TestCase):
    def test_embedding_cycle_stops_on_interactive(self):
        model = FakeModel(interactive_after=2)
        steps = []

        def step(store, m):
            model.calls += 1
            steps.append(model.calls)
            return True
        worked = worker.embedding_cycle(None, model, max_batches=4, step=step)
        self.assertTrue(worked)
        self.assertEqual(steps, [1, 2])

    def test_embedding_cycle_reports_idle(self):
        self.assertFalse(worker.embedding_cycle(None, FakeModel(99), max_batches=4, step=lambda s, m: False))


class FailingCheapStore(FakeStore):
    def cheap_status(self, source):
        raise TimeoutError('canceling statement')

    def status_error(self):
        return {'error': 'QueryCanceled', 'at': '2026-10-02T07:00:00+00:00'}


class DegradedStatusTests(unittest.TestCase):
    def test_cheap_failure_degrades_instead_of_raising(self):
        now = datetime.now(timezone.utc)
        store = FailingCheapStore(snapshot={'data': {'messages': 5}, 'updated_at': now})
        out = server.StatusCache(store).get(None)
        self.assertEqual(out['messages'], 5)
        self.assertEqual(out['cheap_error'], 'TimeoutError')
        self.assertIsNone(out.get('lag_s'))
        self.assertEqual(out['status_error']['error'], 'QueryCanceled')

    def test_refresh_skipped_when_fresh_and_idle(self):
        now = datetime.now(timezone.utc)
        store = FakeStore(snapshot={'data': {'messages': 1}, 'updated_at': now - timedelta(seconds=100)}, cheap={'ingested_last_hour': 0})
        self.assertFalse(worker.status_due(store, now))
        store = FakeStore(snapshot={'data': {'messages': 1}, 'updated_at': now - timedelta(seconds=100)}, cheap={'ingested_last_hour': 3})
        self.assertTrue(worker.status_due(store, now))
        store = FakeStore(snapshot={'data': {'messages': 1}, 'updated_at': now - timedelta(seconds=4000)}, cheap={'ingested_last_hour': 0})
        self.assertTrue(worker.status_due(store, now))
        self.assertTrue(worker.status_due(FakeStore(), now))
        bulk = FakeStore(snapshot={'data': {'messages': 1}, 'updated_at': now - timedelta(seconds=600)}, cheap={'ingested_last_hour': 9, 'pending_embeddings': 1_200_000})
        self.assertFalse(worker.status_due(bulk, now), 'bulk re-embed: do not add a 170 s scan every 5 min')


class PacingTests(unittest.TestCase):
    def test_failed_batch_keeps_disk_pressure_cooldown(self):
        self.assertEqual(worker.pace_seconds(False, pressure=75), 25)
        self.assertEqual(worker.pace_seconds(False, pressure=55), 10)
        self.assertEqual(worker.pace_seconds(False, pressure=35), 5)

    def test_pace_follows_io_pressure(self):
        self.assertEqual(worker.pace_seconds(False, pressure=0), 5)
        self.assertEqual(worker.pace_seconds(True, pressure=10), .5)
        self.assertEqual(worker.pace_seconds(True, pressure=35), 3)
        self.assertEqual(worker.pace_seconds(True, pressure=55), 10)
        self.assertEqual(worker.pace_seconds(True, pressure=75), 25)

    def test_io_pressure_parses_psi_and_tolerates_absence(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'io'
            p.write_text('some avg10=73.54 avg60=79.97 avg300=79.01 total=1\nfull avg10=60.00 avg60=1 avg300=1 total=1\n')
            self.assertEqual(worker.io_pressure_some(p), 73.54)
            self.assertEqual(worker.io_pressure_some(Path(d) / 'missing'), 0.)
