import importlib.util
from pathlib import Path
import unittest


_PATH = Path(__file__).resolve().parents[1] / 'docs/benchmarks/registry-pulls-2026-09-30/verify-python-source.py'
_SPEC = importlib.util.spec_from_file_location('verify_python_source', _PATH)
verify = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(verify)


class SourceConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.identity = 'registry-pull-canary-123456789abc-045'
        self.original = {'Env': ['PATH=/bin'], 'Labels': {verify.IMAGE_ID_LABEL: verify.ORIGINAL_ID, 'owned': '1'}}
        self.published = {**self.original, 'Labels': {**self.original['Labels'], verify.IMAGE_ID_LABEL: self.identity}}

    def test_only_owned_name_binding_may_change(self):
        verify.compare_runtime_config(self.original, self.published, self.identity)
        self.assertEqual(self.published['Labels'][verify.IMAGE_ID_LABEL], self.identity)

    def test_other_config_and_labels_fail_closed(self):
        for changed in ({**self.published, 'Env': ['PATH=/bad']},
                        {**self.published, 'Labels': {**self.published['Labels'], 'owned': '2'}}):
            with self.assertRaises(ValueError):
                verify.compare_runtime_config(self.original, changed, self.identity)

    def test_identity_must_bind_both_sources(self):
        with self.assertRaises(ValueError):
            verify.compare_runtime_config(self.original, self.published, self.identity + '-wrong')
        with self.assertRaises(ValueError):
            verify.compare_runtime_config(self.published, self.published, self.identity)


if __name__ == '__main__':
    unittest.main()
