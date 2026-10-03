import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

import embed_config
import migrate_model


class MigrationTests(unittest.TestCase):
    def test_local_backend_is_probed_before_switching_index(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            config = home / '.config/messaging-memory/database.json'
            config.parent.mkdir(parents=True)
            config.write_text(json.dumps({'writer_dsn': 'synthetic-dsn'}))
            calls = []
            model = MagicMock()
            model.identity.return_value = 'synthetic-identity'
            model.request_vectors.side_effect = lambda _: calls.append('probe')
            store = MagicMock()
            store.switch_model.side_effect = lambda *_: calls.append('switch')
            with patch.object(migrate_model.embed_config, 'load', return_value=dict(embed_config.LOCAL_EXAMPLE)), \
                    patch.object(migrate_model, 'Embedder', return_value=model), \
                    patch.object(migrate_model, 'Store', return_value=store), \
                    patch.object(migrate_model.embed_config, 'CONFIG', config.parent / 'embeddings.json'), \
                    patch('builtins.print'):
                migrate_model.main()
            self.assertEqual(calls, ['probe', 'switch'])
            store.switch_model.assert_called_once_with(
                embed_config.label(embed_config.LOCAL_EXAMPLE), 'synthetic-identity')

    def test_failed_backend_probe_cannot_modify_database(self):
        model = MagicMock()
        model.request_vectors.side_effect = TimeoutError('unavailable')
        with patch.object(migrate_model.embed_config, 'load', return_value=dict(embed_config.DEFAULT)), \
                patch.object(migrate_model, 'Embedder', return_value=model), \
                patch.object(migrate_model, 'Store') as store:
            with self.assertRaises(TimeoutError):
                migrate_model.main()
            store.assert_not_called()
