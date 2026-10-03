"""Installer contracts; no server or network needed."""
import importlib.util
import pathlib
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


class InstallerTests(unittest.TestCase):
    def test_portable_installer_is_shipped(self):
        self.assertTrue((ROOT / 'history.py').is_file(), 'portable history launcher is missing')

    def test_private_json_refuses_overwrite_and_symlinks(self):
        spec = importlib.util.spec_from_file_location('history', ROOT / 'history.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / 'credentials.json'
            module.private_json(path, {'secret': 'synthetic'})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                module.private_json(path, {})
            link = pathlib.Path(tmp) / 'link.json'
            link.symlink_to(path)
            with self.assertRaises(FileExistsError):
                module.private_json(link, {})
            self.assertIn('synthetic', path.read_text())


if __name__ == '__main__':
    unittest.main()
