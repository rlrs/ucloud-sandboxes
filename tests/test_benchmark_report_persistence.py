import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import live_relay_load_benchmark as benchmark


class ReportWriterTests(unittest.TestCase):
    def report(self):
        return {
            'cycles': [{'sandbox': 'x', 'cycle': 0, 'timing': .12345678901234567,
                        'message': 'μ agent', 'none': None, 'value': 2**62,
                        'flags': [True, False]}],
            'counts': {'http_429': 5},
        }

    @unittest.skipIf(benchmark._report_orjson is None, 'optional orjson is unavailable')
    def test_orjson_and_fallback_are_structurally_equivalent(self):
        report = self.report()
        self.assertEqual(json.loads(benchmark.encode_report(report)), report)
        with patch.object(benchmark, '_report_orjson', None):
            self.assertEqual(json.loads(benchmark.encode_report(report)), report)

    def test_atomic_checkpoint_reports_prior_completed_cost_without_self_reference(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'report.json'
            report, samples = self.report(), []
            benchmark.persist_report(path, report, samples)
            first = json.loads(path.read_bytes())
            self.assertEqual(first['cycles'], report['cycles'])
            self.assertEqual(first['report_persistence']['completed_checkpoints'], 0)
            expected = 'orjson' if benchmark._report_orjson is not None else 'stdlib-json'
            self.assertEqual(first['report_serializer']['name'], expected)
            self.assertEqual(len(samples), 1)
            self.assertGreater(samples[0]['serialization_seconds'], 0)
            self.assertEqual(samples[0]['bytes'], path.stat().st_size)
            benchmark.persist_report(path, report, samples)
            second = json.loads(path.read_bytes())
            self.assertEqual(second['report_persistence']['completed_checkpoints'], 1)
            self.assertEqual(second['report_persistence']['samples'][0], samples[0])
            self.assertFalse(path.with_suffix('.json.tmp').exists())


if __name__ == '__main__':
    unittest.main()
