from hashlib import sha256
from io import BytesIO
import multiprocessing
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.build_context_store import BuildContextBlobStore


def _digest(payload: bytes) -> str:
    return f"sha256:{sha256(payload).hexdigest()}"


class _CoordinatedReader:
    def __init__(self, payload: bytes, started: object, proceed: object) -> None:
        self.payload = payload
        self.started = started
        self.proceed = proceed
        self.offset = 0
        self.blocked = False

    def read(self, size: int = -1) -> bytes:
        if not self.blocked:
            self.blocked = True
            self.started.set()  # type: ignore[attr-defined]
            if not self.proceed.wait(timeout=10):  # type: ignore[attr-defined]
                raise AssertionError("test did not release the blob writer")
        if self.offset == len(self.payload):
            return b""
        end = len(self.payload) if size < 0 else self.offset + size
        chunk = self.payload[self.offset : end]
        self.offset += len(chunk)
        return chunk


def _put_blob(
    root: str,
    payload: bytes,
    digest: str,
    started: object,
    proceed: object,
    deduplicated: object,
) -> None:
    store = BuildContextBlobStore(
        Path(root),
        max_blob_bytes=len(payload),
        max_age_seconds=0,
    )
    result = store.put_with_status(
        digest,
        _CoordinatedReader(payload, started, proceed),
        content_length=len(payload),
    )
    deduplicated.value = int(result.deduplicated)  # type: ignore[attr-defined]


def _collect_blob(
    root: str,
    payload_size: int,
    writer_started: object,
    collected: object,
    finished: object,
) -> None:
    if not writer_started.wait(timeout=10):  # type: ignore[attr-defined]
        raise AssertionError("blob writer did not reach its stream boundary")
    result = BuildContextBlobStore(
        Path(root),
        max_blob_bytes=payload_size,
        max_age_seconds=0,
    ).gc(now=10**10)
    collected.value = result.removed_entries  # type: ignore[attr-defined]
    finished.set()  # type: ignore[attr-defined]


class BuildContextBlobStoreProcessTests(unittest.TestCase):
    def test_put_republishes_complete_blob_when_process_gc_removes_old_digest(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            payload = b"process-safe build context" * 1024
            digest = _digest(payload)
            store = BuildContextBlobStore(
                root,
                max_blob_bytes=len(payload),
                max_age_seconds=0,
            )
            store.put_with_status(
                digest,
                BytesIO(payload),
                content_length=len(payload),
            )

            context = multiprocessing.get_context("spawn")
            writer_started = context.Event()
            release_writer = context.Event()
            gc_finished = context.Event()
            deduplicated = context.Value("i", -1)
            collected = context.Value("i", -1)
            writer = context.Process(
                target=_put_blob,
                args=(
                    raw_dir,
                    payload,
                    digest,
                    writer_started,
                    release_writer,
                    deduplicated,
                ),
            )
            collector = context.Process(
                target=_collect_blob,
                args=(
                    raw_dir,
                    len(payload),
                    writer_started,
                    collected,
                    gc_finished,
                ),
            )

            writer.start()
            collector.start()
            writer_reached_boundary = writer_started.wait(timeout=10)
            gc_completed_while_writer_waited = gc_finished.wait(timeout=10)
            release_writer.set()
            for process in (writer, collector):
                process.join(timeout=10)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2)

            self.assertTrue(writer_reached_boundary)
            self.assertTrue(gc_completed_while_writer_waited)
            self.assertEqual([writer.exitcode, collector.exitcode], [0, 0])
            self.assertEqual(collected.value, 1)
            self.assertEqual(deduplicated.value, 0)
            self.assertEqual(store.path(digest).read_bytes(), payload)
            self.assertEqual(
                [path.name for path in store.blob_dir.iterdir()],
                [digest.removeprefix("sha256:")],
            )


if __name__ == "__main__":
    unittest.main()
