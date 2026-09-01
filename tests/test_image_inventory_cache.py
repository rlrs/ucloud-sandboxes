from __future__ import annotations

from threading import Barrier, Event, Lock, Thread
import unittest

from ucloud_sandboxes.image_inventory_cache import (
    ImageInventoryCache,
    ImageInventorySnapshot,
)


class ImageInventoryCacheTests(unittest.TestCase):
    def test_concurrent_cold_readers_share_one_load(self) -> None:
        worker_count = 8
        start = Barrier(worker_count + 1)
        load_started = Event()
        release_load = Event()
        guard = Lock()
        calls = 0
        results: list[ImageInventorySnapshot] = []
        errors: list[BaseException] = []
        cache = ImageInventoryCache(ttl_seconds=5.0, clock=lambda: 0.0)

        def loader() -> ImageInventorySnapshot:
            nonlocal calls
            with guard:
                calls += 1
            load_started.set()
            release_load.wait(timeout=2.0)
            return ImageInventorySnapshot.from_records(
                [{"id": "image-1", "tag": "registry/image:v1"}],
                complete=True,
            )

        def read() -> None:
            start.wait()
            try:
                snapshot = cache.get_or_load(loader)
            except BaseException as exc:
                with guard:
                    errors.append(exc)
            else:
                with guard:
                    results.append(snapshot)

        threads = [Thread(target=read) for _ in range(worker_count)]
        for thread in threads:
            thread.start()
        start.wait()
        self.assertTrue(load_started.wait(timeout=1.0))
        release_load.set()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(calls, 1)
        self.assertEqual(len(results), worker_count)
        self.assertTrue(
            all(
                snapshot.records
                == ({"id": "image-1", "tag": "registry/image:v1"},)
                and snapshot.complete
                for snapshot in results
            )
        )

        results[0].records[0]["tag"] = "changed"
        self.assertEqual(
            cache.get_or_load(loader).records,
            ({"id": "image-1", "tag": "registry/image:v1"},),
        )

    def test_invalidation_does_not_wait_and_fences_stale_publish(self) -> None:
        load_started = Event()
        release_load = Event()
        invalidated = Event()
        calls = 0
        cache = ImageInventoryCache(ttl_seconds=5.0, clock=lambda: 0.0)

        def loader() -> ImageInventorySnapshot:
            nonlocal calls
            calls += 1
            if calls == 1:
                load_started.set()
                release_load.wait(timeout=2.0)
                return ImageInventorySnapshot.from_records(
                    [{"id": "stale"}], complete=False
                )
            return ImageInventorySnapshot.from_records(
                [{"id": "fresh"}], complete=True
            )

        first_result: list[ImageInventorySnapshot] = []
        first = Thread(target=lambda: first_result.append(cache.get_or_load(loader)))
        first.start()
        self.assertTrue(load_started.wait(timeout=1.0))

        def invalidate() -> None:
            cache.invalidate()
            invalidated.set()

        invalidator = Thread(target=invalidate)
        invalidator.start()
        completed_while_loading = invalidated.wait(timeout=1.0)
        release_load.set()
        invalidator.join(timeout=2.0)
        first.join(timeout=2.0)

        self.assertTrue(completed_while_loading)
        self.assertFalse(first.is_alive())
        self.assertEqual(first_result[0].records, ({"id": "stale"},))
        self.assertFalse(first_result[0].complete)
        self.assertEqual(cache.get_or_load(loader).records, ({"id": "fresh"},))
        self.assertEqual(cache.get_or_load(loader).records, ({"id": "fresh"},))
        self.assertEqual(calls, 2)

    def test_post_invalidation_reader_starts_fresh_generation_immediately(self) -> None:
        stale_started = Event()
        release_stale = Event()
        fresh_finished = Event()
        cache = ImageInventoryCache(ttl_seconds=5.0, clock=lambda: 0.0)
        stale_results: list[ImageInventorySnapshot] = []
        fresh_results: list[ImageInventorySnapshot] = []

        def stale_loader() -> ImageInventorySnapshot:
            stale_started.set()
            release_stale.wait(timeout=2.0)
            return ImageInventorySnapshot.from_records(
                [{"id": "stale"}], complete=False
            )

        def fresh_loader() -> ImageInventorySnapshot:
            return ImageInventorySnapshot.from_records(
                [{"id": "fresh"}], complete=True
            )

        stale_reader = Thread(
            target=lambda: stale_results.append(cache.get_or_load(stale_loader))
        )
        stale_reader.start()
        self.assertTrue(stale_started.wait(timeout=1.0))

        cache.invalidate()

        def read_fresh() -> None:
            fresh_results.append(cache.get_or_load(fresh_loader))
            fresh_finished.set()

        fresh_reader = Thread(target=read_fresh)
        fresh_reader.start()
        completed_before_stale = fresh_finished.wait(timeout=1.0)
        release_stale.set()
        fresh_reader.join(timeout=2.0)
        stale_reader.join(timeout=2.0)

        self.assertTrue(completed_before_stale)
        self.assertFalse(fresh_reader.is_alive())
        self.assertFalse(stale_reader.is_alive())
        self.assertEqual(fresh_results[0].records, ({"id": "fresh"},))
        self.assertTrue(fresh_results[0].complete)
        self.assertEqual(stale_results[0].records, ({"id": "stale"},))
        self.assertFalse(stale_results[0].complete)
        self.assertEqual(
            cache.get_or_load(
                lambda: ImageInventorySnapshot.from_records(
                    [{"id": "unexpected"}], complete=False
                )
            ).records,
            ({"id": "fresh"},),
        )

    def test_loader_exception_wakes_waiter_and_allows_retry(self) -> None:
        start = Barrier(3)
        first_started = Event()
        release_failure = Event()
        guard = Lock()
        calls = 0
        results: list[ImageInventorySnapshot] = []
        errors: list[BaseException] = []
        cache = ImageInventoryCache(ttl_seconds=5.0, clock=lambda: 0.0)

        def loader() -> ImageInventorySnapshot:
            nonlocal calls
            with guard:
                calls += 1
                attempt = calls
            if attempt == 1:
                first_started.set()
                release_failure.wait(timeout=2.0)
                raise RuntimeError("inventory unavailable")
            return ImageInventorySnapshot.from_records(
                [{"id": "recovered"}], complete=True
            )

        def read() -> None:
            start.wait()
            try:
                snapshot = cache.get_or_load(loader)
            except BaseException as exc:
                with guard:
                    errors.append(exc)
            else:
                with guard:
                    results.append(snapshot)

        threads = [Thread(target=read) for _ in range(2)]
        for thread in threads:
            thread.start()
        start.wait()
        self.assertTrue(first_started.wait(timeout=1.0))
        release_failure.set()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(calls, 2)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual(str(errors[0]), "inventory unavailable")
        self.assertEqual(results[0].records, ({"id": "recovered"},))
        self.assertTrue(results[0].complete)
        self.assertEqual(cache.get_or_load(loader).records, ({"id": "recovered"},))
        self.assertEqual(calls, 2)

    def test_empty_inventory_is_cached_until_ttl_expires(self) -> None:
        now = [10.0]
        calls = 0
        cache = ImageInventoryCache(ttl_seconds=5.0, clock=lambda: now[0])

        def loader() -> ImageInventorySnapshot:
            nonlocal calls
            calls += 1
            return ImageInventorySnapshot.from_records(
                [] if calls == 1 else [{"id": "new-image"}],
                complete=True,
            )

        self.assertEqual(cache.get_or_load(loader).records, ())
        self.assertTrue(cache.get_or_load(loader).complete)
        now[0] += 4.999
        self.assertEqual(cache.get_or_load(loader).records, ())
        self.assertEqual(calls, 1)

        now[0] += 0.002
        refreshed = cache.get_or_load(loader)
        self.assertEqual(refreshed.records, ({"id": "new-image"},))
        self.assertTrue(refreshed.complete)
        self.assertEqual(calls, 2)

    def test_incomplete_inventory_uses_short_retry_ttl(self) -> None:
        now = [10.0]
        calls = 0
        cache = ImageInventoryCache(ttl_seconds=5.0, clock=lambda: now[0])

        def loader() -> ImageInventorySnapshot:
            nonlocal calls
            calls += 1
            return ImageInventorySnapshot.from_records(
                [] if calls == 1 else [{"id": "recovered"}],
                complete=calls != 1,
            )

        first = cache.get_or_load(loader)
        self.assertFalse(first.complete)
        now[0] += 0.499
        self.assertFalse(cache.get_or_load(loader).complete)
        self.assertEqual(calls, 1)

        now[0] += 0.002
        recovered = cache.get_or_load(loader)
        self.assertTrue(recovered.complete)
        self.assertEqual(recovered.records, ({"id": "recovered"},))
        self.assertEqual(calls, 2)

    def test_invalid_ttl_is_rejected(self) -> None:
        for ttl_seconds in (-1.0, float("nan"), float("inf")):
            with self.subTest(ttl_seconds=ttl_seconds), self.assertRaises(ValueError):
                ImageInventoryCache(ttl_seconds=ttl_seconds)


if __name__ == "__main__":
    unittest.main()
