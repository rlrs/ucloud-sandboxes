import unittest
from scripts.analyze_relay_load import phases, summarize


class LoadAttributionTests(unittest.TestCase):
    def test_phases_partition_continuation_without_double_counting(self):
        cycle = dict(response_ready_unix=100, response_ready_to_submit_seconds=.1,
            guest_response_received_unix=101.1, guest_tool_finished_unix=101.2,
            guest_receipt_started_unix=101.3, observer_request_created_unix=101.4,
            guest_continuation_observed_unix=101.5, post_continuation_exec_seconds=.2)
        result = phases(cycle, {'start': 100.3, 'end': 101},
                        {'completed_at': 100.2, 'delivery_released_at': 101.05, 'response_wait_finished_at': 101.08})
        self.assertAlmostEqual(sum(v for k, v in result.items() if k != 'observed_to_exec_ms' and v is not None), 1500)
        # Clock skew or overlap is evidence to inspect, never silently clamped.
        cycle['observer_request_created_unix'] = 101.6
        self.assertLess(phases(cycle, None, {})['observer_queue_and_poll_ms'], 0)
        self.assertIsNone(phases(cycle, None, {})['worker_wake_ms'])

    def test_missing_evidence_is_not_zero_latency(self):
        result = summarize([None, -2, 4])
        self.assertEqual(result['count'], 2)
        self.assertEqual(result['negative_count'], 1)
        self.assertIsNone(summarize([])['mean'])
