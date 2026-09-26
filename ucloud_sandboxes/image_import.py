"""Import external images for immutable-environment (EROFS) workers.

An EROFS worker reads image content on demand from signed components in the
managed registry (docs/immutable-environments.md), so it can only run images
the trusted builder has published with an environment attachment. A sandbox
that names an external image (for example one on Docker Hub) is imported
first: the gateway submits an ordinary managed build of ``FROM <image>`` under
a deterministic image ID, the builder pushes it and publishes its EROFS
components, and the create then uses the pinned managed reference.

Creates for an image that is still importing receive a retryable 503; the SDK
retries creates until their deadline.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import logging
import tarfile
import threading
import time
from typing import Callable

_LOG = logging.getLogger(__name__)

IMPORT_RETRY_AFTER_SECONDS = 5
# Resubmit an import at most this often per image; the build path itself is
# single-flight, this only bounds the requests creates generate while waiting.
RESUBMIT_INTERVAL_SECONDS = 30.0


def import_image_id(image: str) -> str:
    """The managed image ID an external reference is imported under."""

    return "import-" + hashlib.sha256(image.strip().encode("utf-8")).hexdigest()[:40]


def import_build_context(image: str) -> bytes:
    """A deterministic build context whose Dockerfile only names the image."""

    dockerfile = f"FROM {image.strip()}\n".encode("utf-8")
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
            info = tarfile.TarInfo("Dockerfile")
            info.size = len(dockerfile)
            info.mtime = 0
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(dockerfile))
    return buffer.getvalue()


class ImageImportSubmitter:
    """Submit each import at most once per interval, off the request thread."""

    def __init__(
        self,
        submit: Callable[[str, str], None],
        *,
        interval_seconds: float = RESUBMIT_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        background: bool = True,
    ) -> None:
        self._submit = submit
        self.interval_seconds = interval_seconds
        self._clock = clock
        self._background = background
        self._submitted: dict[str, float] = {}
        self._guard = threading.Lock()

    def ensure_submitted(self, import_id: str, image: str) -> bool:
        now = self._clock()
        with self._guard:
            last = self._submitted.get(import_id)
            if last is not None and now - last < self.interval_seconds:
                return False
            self._submitted[import_id] = now
            if len(self._submitted) > 4096:
                oldest = sorted(self._submitted, key=self._submitted.get)[:1024]
                for key in oldest:
                    del self._submitted[key]
        if self._background:
            threading.Thread(
                target=self._run, args=(import_id, image),
                name=f"image-import-{import_id[:16]}", daemon=True,
            ).start()
        else:
            self._run(import_id, image)
        return True

    def _run(self, import_id: str, image: str) -> None:
        try:
            self._submit(import_id, image)
        except Exception as exc:  # The next create retry resubmits.
            _LOG.warning("could not submit the import of %s: %s", image, exc)
