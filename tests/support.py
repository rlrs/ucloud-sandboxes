"""Fixtures that must not depend on the developer's environment or test order."""

from __future__ import annotations

import errno
from functools import lru_cache
import os
from pathlib import Path
import re
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
# The scripts under test insert this checkout themselves. Inserting it here as
# well makes SDK availability independent of which module was imported first.
_SDK_CHECKOUT = REPO_ROOT / "ucloud-sandboxes-sdk" / "src"
if _SDK_CHECKOUT.is_dir() and str(_SDK_CHECKOUT) not in sys.path:
    sys.path.insert(0, str(_SDK_CHECKOUT))

# A node's state roots are created under systemd's default UMask=0022.
NODE_DIRECTORY_MODE = 0o755
_VERSION = re.compile(r"(\d+)\.(\d+)\.(\d+)")


def make_dirs(path: Path | str, *, mode: int = NODE_DIRECTORY_MODE) -> Path:
    """Create path and every missing parent with exactly mode.

    Product stores reject group- or world-writable directories. Path.mkdir
    applies the process umask (0002 on many developer machines) and
    os.makedirs applies mode only to the leaf, so fixtures use this instead.
    Like Path.mkdir(parents=True), an existing leaf is an error.
    """
    path = Path(path)
    missing = []
    current = path
    while not os.path.lexists(current):
        missing.append(current)
        current = current.parent
    if not missing:
        raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), str(path))
    for directory in reversed(missing):
        directory.mkdir(mode=mode)
        os.chmod(directory, mode)
    return path


def _parse_version(text: str) -> tuple[int, int, int]:
    match = _VERSION.match(text)
    if match is None:
        raise ValueError(f"unparseable SDK version {text!r}")
    return tuple(int(part) for part in match.groups())


@lru_cache(maxsize=None)
def sdk_version() -> tuple[int, int, int] | None:
    try:
        import ucloud_sandboxes_sdk
    except ImportError:
        return None
    return _parse_version(ucloud_sandboxes_sdk.__version__)


def sdk_skip_reason(minimum: str | None = None) -> str | None:
    """Why SDK tests cannot run here, or None when they can."""
    version = sdk_version()
    if version is None:
        return (
            "requires ucloud_sandboxes_sdk (an ucloud-sandboxes-sdk/src checkout "
            "or PYTHONPATH)"
        )
    if minimum is not None and version < _parse_version(minimum):
        found = ".".join(map(str, version))
        return f"requires ucloud_sandboxes_sdk >= {minimum}; found {found}"
    return None


def requires_sdk(minimum: str | None = None):
    reason = sdk_skip_reason(minimum)
    return unittest.skipIf(reason is not None, reason or "")


def skip_module(module: str, reason: str):
    """A load_tests hook that reports a whole module as one skipped test.

    Raising SkipTest at import works under discovery but is an error under
    ``python -m unittest tests.test_x``; load_tests works under both.
    """

    def load_tests(loader, tests, pattern):
        del tests, pattern

        @unittest.skip(reason)
        class ModuleSkipped(unittest.TestCase):
            def test_module(self):
                pass

        ModuleSkipped.__module__ = module
        ModuleSkipped.__qualname__ = "ModuleSkipped"
        return loader.suiteClass([ModuleSkipped("test_module")])

    return load_tests

