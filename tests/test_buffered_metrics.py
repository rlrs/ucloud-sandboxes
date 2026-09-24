import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import patch

from ucloud_sandboxes.metrics import (
    BufferedMetricsStore, MetricEvent, MetricsStore, _EncodedMetricEvent,
)


class BufferedMetricsTests(unittest.TestCase):
    def test_encoded_snapshot_preserves_accounting_and_json_values(self):
        event = MetricEvent('2026-09-24T16:00:00+00:00', 'wake\"é', {
            'nested': {'values': [None, True, 1.25, 'é\n\"']},
            'tuple': (1, 2),
        })
        encoded = _EncodedMetricEvent.from_event(event)
        self.assertEqual(encoded.queue_bytes, len(json.dumps(
            event.to_dict(), sort_keys=True, separators=(',', ':'),
        ).encode('utf-8')))
        self.assertEqual(encoded.payload_bytes, len((json.dumps(
            event.to_dict(), sort_keys=True,
        ) + '\n').encode('utf-8')))
        snapshot = json.loads(encoded.data_json)
        event.data['nested']['values'].append('later')
        self.assertEqual(json.loads(encoded.data_json), snapshot)
        self.assertEqual(snapshot['tuple'], [1, 2])

    def test_truncation_and_stored_byte_accounting_are_preserved(self):
        with TemporaryDirectory() as directory:
            store = BufferedMetricsStore(
                Path(directory) / 'metrics.sqlite', max_event_bytes=200,
            )
            try:
                data = {'padding': 'é' * 200}
                timestamp = '2026-09-24T16:00:00+00:00'
                original = MetricEvent(timestamp, 'large', data)
                result = store.append('large', data, timestamp=timestamp)
                self.assertTrue(store.flush())
                self.assertEqual(result.data, {
                    'metrics_payload_truncated': True,
                    'original_bytes': len(json.dumps(
                        original.to_dict(), sort_keys=True, separators=(',', ':'),
                    ).encode('utf-8')),
                })
                self.assertEqual(store.load_events()[0], result)
                row = store._sqlite_connection.execute(
                    'SELECT payload_bytes FROM metric_events',
                ).fetchone()
                self.assertEqual(row[0], len((json.dumps(
                    result.to_dict(), sort_keys=True,
                ) + '\n').encode('utf-8')))
            finally:
                self.assertTrue(store.close())

    def test_blocked_database_does_not_block_callers_and_drain_preserves_snapshots(self):
        with TemporaryDirectory() as directory:
            store = BufferedMetricsStore(Path(directory) / 'metrics.sqlite')
            entered, release = Event(), Event()
            original = store._append_events
            def blocked(events):
                entered.set()
                self.assertTrue(release.wait(5))
                original(events)
            try:
                with patch.object(store, '_append_events', side_effect=blocked):
                    store.append('first')
                    self.assertTrue(entered.wait(2))
                    payload = {'values': [1]}
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        pool.submit(store.append, 'second', payload, timestamp='2026-09-22T00:00:00+00:00').result(timeout=1)
                    payload['values'][0] = 2
                    self.assertFalse(store.flush(timeout=0.01))
                    release.set()
                    self.assertTrue(store.flush())
                events = store.load_events()
                self.assertEqual([event.kind for event in events], ['first', 'second'])
                self.assertEqual(events[1].data, {'values': [1]})
                self.assertEqual(events[1].timestamp, '2026-09-22T00:00:00+00:00')
            finally:
                release.set()
                self.assertTrue(store.close())

    def test_overload_is_bounded_and_reported_without_rejecting_sandbox_work(self):
        with TemporaryDirectory() as directory:
            store = BufferedMetricsStore(Path(directory) / 'metrics.sqlite', queue_events=2)
            entered, release = Event(), Event()
            original = store._append_events
            def blocked(events):
                entered.set()
                self.assertTrue(release.wait(5))
                original(events)
            try:
                with patch.object(store, '_append_events', side_effect=blocked):
                    store.append('writing')
                    self.assertTrue(entered.wait(2))
                    for i in range(20):
                        store.append('queued', {'i': i})
                    self.assertEqual(len(store._queue), 2)
                    self.assertLessEqual(store._queue_bytes, store._queue_byte_limit)
                    release.set()
                    self.assertTrue(store.flush())
                events = store.load_events()
                dropped = [e for e in events if e.kind == 'metrics_dropped_events']
                self.assertEqual(sum(e.data['count'] for e in dropped), 18)
                self.assertEqual([e.data['i'] for e in events if e.kind == 'queued'], [0, 1])
            finally:
                release.set()
                self.assertTrue(store.close())

    def test_failure_rolls_back_and_writer_recovers(self):
        with TemporaryDirectory() as directory:
            store = BufferedMetricsStore(Path(directory) / 'metrics.sqlite')
            try:
                def fail(events):
                    store._sqlite_connection.execute("INSERT INTO metric_events(timestamp,timestamp_epoch,kind,data_json,payload_bytes) VALUES ('bad',0,'uncommitted','{}',2)")
                    raise RuntimeError('injected')
                with patch.object(store, '_append_events', side_effect=fail), self.assertLogs('ucloud_sandboxes.metrics', level='ERROR'):
                    store.append('lost')
                    self.assertTrue(store.flush())
                store.append('recovered')
                self.assertTrue(store.flush())
                events = store.load_events()
                self.assertEqual([e.kind for e in events], ['metrics_dropped_events', 'recovered'])
                self.assertEqual(events[0].data['count'], 1)
            finally:
                self.assertTrue(store.close())

    def test_close_drains_and_byte_bound_reports_oversized_event(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'metrics.sqlite'
            store = BufferedMetricsStore(path, queue_bytes=200)
            store.append('too-large', {'padding': 'x' * 1000})
            self.assertTrue(store.close())
            events = MetricsStore(path).load_events()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].kind, 'metrics_dropped_events')
            self.assertEqual(events[0].data, {'count': 1, 'reason': 'queue_full'})
            self.assertFalse(store._writer.is_alive())
            with self.assertRaisesRegex(RuntimeError, 'closed'):
                store.append('late')
