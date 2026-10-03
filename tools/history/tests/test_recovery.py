"""Source restoration detection with synthetic SQLite backups and no PostgreSQL."""
import importlib.util
from pathlib import Path
import shutil
import sqlite3
import unittest

import test_sources as fixtures


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SourceTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.source = self.fixture.source('telegram')
        self.source.install_capture()

    def state(self):
        self.assertTrue(callable(getattr(self.source, 'identity', None)), 'capture identity is missing')
        seq = self.source.watermark()
        return dict(source_identity=self.source.identity(), change_seq=seq,
                    change_token=self.source.checkpoint_token(seq), cursor=500, backfill_done=True)

    def plan(self, state):
        self.assertIsNotNone(importlib.util.find_spec('recovery'), 'recovery helper is missing')
        from recovery import recovery_plan
        return recovery_plan(state, self.source)

    def snapshot(self):
        path = Path(self.fixture.temp.name) / 'snapshot.db'
        with sqlite3.connect(self.source.path) as origin, sqlite3.connect(path) as dest:
            origin.backup(dest)
        return path

    def test_matching_checkpoint_resumes_without_reset(self):
        self.fixture.insert(self.source)
        state = self.state()
        self.fixture.insert(self.source, chat=2)
        plan = self.plan(state)
        self.assertFalse(plan['reset_required'])
        self.assertIsNone(plan['reason'])
        self.assertEqual(plan['source_identity'], state['source_identity'])

    def test_new_index_records_source_identity_before_backfill(self):
        plan = self.plan(dict(change_seq=0, cursor=0, backfill_done=False))
        self.assertTrue(plan['reset_required'])
        self.assertEqual(plan['reason'], 'source_identity_missing')

    def test_new_database_identity_requires_source_reset(self):
        state = self.state()
        self.source.path.unlink()
        self.source = self.fixture.source('telegram')
        self.source.install_capture()
        plan = self.plan(state)
        self.assertTrue(plan['reset_required'])
        self.assertEqual(plan['reason'], 'source_identity_changed')

    def test_older_snapshot_with_same_identity_and_reused_sequence_is_detected(self):
        self.fixture.insert(self.source)
        snapshot = self.snapshot()
        self.fixture.insert(self.source, chat=2, mid=20)
        state = self.state()
        shutil.copy2(snapshot, self.source.path)
        self.source.install_capture()
        self.fixture.insert(self.source, chat=2, mid=30)
        self.assertEqual(self.source.identity(), state['source_identity'])
        self.assertEqual(self.source.watermark(), state['change_seq'])
        self.assertEqual(self.source.changes(state['change_seq'], 20), [])
        plan = self.plan(state)
        self.assertTrue(plan['reset_required'])
        self.assertEqual(plan['reason'], 'checkpoint_reused')

    def test_older_snapshot_without_checkpoint_is_detected(self):
        snapshot = self.snapshot()
        self.fixture.insert(self.source)
        state = self.state()
        shutil.copy2(snapshot, self.source.path)
        plan = self.plan(state)
        self.assertTrue(plan['reset_required'])
        self.assertEqual(plan['reason'], 'checkpoint_missing')

    def test_legacy_checkpoint_without_token_requires_reconciliation(self):
        self.fixture.insert(self.source)
        state = self.state()
        state['change_token'] = None
        self.assertEqual(self.plan(state)['reason'], 'checkpoint_token_missing')

    def test_zero_checkpoint_with_registered_identity_is_safe(self):
        state = self.state()
        state['backfill_done'] = False
        self.fixture.insert(self.source)
        self.assertFalse(self.plan(state)['reset_required'])


if __name__ == '__main__':
    unittest.main()
