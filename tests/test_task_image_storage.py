import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from probe_task_image_storage import blob_accounting


class TaskStorageTests(unittest.TestCase):
    def test_counts_missing_union_once_and_requires_exact_retained_size(self):
        a, b, c = ('sha256:' + letter * 64 for letter in 'abc')
        seen = set()
        def retained(digest):
            return {a: 10, b: 19}.get(digest)
        first = blob_accounting([{'digest': a, 'size': 10}, {'digest': b, 'size': 20}], retained, seen)
        second = blob_accounting([{'digest': b, 'size': 20}, {'digest': c, 'size': 30}], retained, seen)
        self.assertEqual(first['retained_oci_bytes'], 10)
        self.assertEqual(first['additional_union_oci_bytes'], 20)
        self.assertEqual(second['missing_oci_bytes'], 50)
        self.assertEqual(second['additional_union_oci_bytes'], 30)

    def test_rejects_unsafe_paths_and_conflicting_sizes(self):
        with self.assertRaises(ValueError):
            blob_accounting([{'digest': '../escape', 'size': 10}], lambda d: None, set())
        with self.assertRaises(ValueError):
            blob_accounting([{'digest': 'sha256:' + 'a' * 64, 'size': n} for n in (1, 2)], lambda d: None, set())
