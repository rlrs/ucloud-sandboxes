from pathlib import Path
from types import SimpleNamespace
import unittest

from ucloud_sandboxes.image_eviction import ImageCacheEvictor

GIB = 1024**3


def image(n: int) -> str:
    return "sha256:" + f"{n:x}" * 64


class FakeStore:
    """A filesystem of `total` GiB holding images of known sizes."""

    def __init__(self, sizes: dict[str, int], tag_times: dict[str, float], total: int = 100):
        self.root = Path("/images")
        self.sizes = dict(sizes)
        self.tag_times = tag_times
        self.total = total
        self.busy: set[str] = set()
        self.evicted: list[str] = []

    def statvfs(self, path):
        assert path == self.root
        used = sum(self.sizes.values())
        return SimpleNamespace(f_blocks=self.total * GIB, f_bavail=(self.total - used) * GIB)

    def list_image_ids(self):
        return tuple(self.sizes)

    def image_tag_times(self, image_ids):
        return {item: self.tag_times.get(item, 0.0) for item in image_ids}

    def evict_image(self, image_id, *, is_referenced):
        if image_id in self.busy or is_referenced(image_id):
            return False
        del self.sizes[image_id]
        self.evicted.append(image_id)
        return True


def evictor(store: FakeStore, *, referenced=(), now=10_000.0) -> ImageCacheEvictor:
    return ImageCacheEvictor(
        store,
        is_referenced=lambda image_id: image_id in referenced,
        clock=lambda: now,
        statvfs=store.statvfs,
    )


class ImageCacheEvictorTests(unittest.TestCase):
    def test_below_high_watermark_nothing_is_evicted(self):
        store = FakeStore({image(1): 40, image(2): 40}, {image(1): 1.0, image(2): 2.0})
        self.assertEqual(evictor(store).evict_if_needed(), ())
        self.assertEqual(store.evicted, [])

    def test_evicts_least_recently_used_until_below_low_watermark(self):
        sizes = {image(n): 10 for n in range(1, 10)}  # 90% full
        times = {image(n): float(n) for n in range(1, 10)}  # image(1) oldest
        store = FakeStore(sizes, times)
        evicted = evictor(store).evict_if_needed()
        # 90% -> below 70% takes three 10 GiB images, oldest first.
        self.assertEqual(evicted, (image(1), image(2), image(3)))
        self.assertLess(sum(store.sizes.values()), 70)

    def test_referenced_busy_and_recently_used_images_are_kept(self):
        sizes = {image(n): 10 for n in range(1, 10)}
        times = {image(n): float(n) for n in range(1, 10)}
        times[image(4)] = 9_900.0  # pulled 100 s ago, inside the grace period
        store = FakeStore(sizes, times)
        store.busy.add(image(2))
        subject = evictor(store, referenced={image(1)})
        subject.note_used(image(3))  # its last sandbox was just deleted
        evicted = subject.evict_if_needed()
        self.assertEqual(evicted, (image(5), image(6), image(7)))

    def test_follow_up_runs_only_after_an_eviction(self):
        store = FakeStore({image(n): 10 for n in range(1, 10)},
                          {image(n): float(n) for n in range(1, 10)})
        subject = evictor(store)
        calls = []
        subject.after_eviction.append(calls.append)
        subject.evict_if_needed()
        self.assertEqual(calls, [(image(1), image(2), image(3))])
        subject.evict_if_needed()  # now below the high watermark
        self.assertEqual(len(calls), 1)

    def test_a_failing_image_does_not_stop_the_sweep(self):
        store = FakeStore({image(n): 10 for n in range(1, 10)},
                          {image(n): float(n) for n in range(1, 10)})
        original = store.evict_image

        def evict(image_id, *, is_referenced):
            if image_id == image(1):
                raise RuntimeError("docker rm failed")
            return original(image_id, is_referenced=is_referenced)

        store.evict_image = evict
        self.assertEqual(evictor(store).evict_if_needed(), (image(2), image(3), image(4)))

    def test_rejects_inverted_watermarks(self):
        store = FakeStore({}, {})
        with self.assertRaises(ValueError):
            ImageCacheEvictor(store, is_referenced=lambda _: False,
                              high_watermark=0.5, low_watermark=0.6)


if __name__ == "__main__":
    unittest.main()
