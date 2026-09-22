from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import patch

from ucloud_sandboxes.metrics import BufferedMetricsStore, MetricsStore


class BufferedMetricsTests(unittest.TestCase):
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
