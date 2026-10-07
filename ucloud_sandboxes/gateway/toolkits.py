"""Toolkit layers (C2.5, docs/toolkit-layers.md).

A toolkit is a signed environment whose one whole-image component holds only
``/opt/ucloud/toolkits/<name>/`` (``publish-environment`` with that allow path
builds it). ``toolkits.sqlite3``, beside ``image-roots.sqlite3``, maps
``name:tag`` to the toolkit's root and remembers every composition. A create
that asks for toolkits gets the image's root with the toolkits' components
appended to ``EnvironmentManifest.toolkits`` (the slot the signed-artifact
rules reserve for them), signed by the gateway and dispatched as the sandbox's
``environment_root``. Nodes lease, mount and share it like any other root.
"""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import sqlite3
import threading
import time

from ..environment_artifact import (CommitEnvironmentComponent, EnvironmentComponent, LayerEnvironmentComponent,
                                    RafsEnvironmentComponent, load_environment, publish_environment, require_digest)
from ..environment_manifest import EnvironmentManifest
from ..sandbox import TOOLKIT_REF_RE

MAX_COMPONENTS = 33  # The base and at most 32 more: the mount option bound.


class ToolkitError(ValueError):
    """A toolkit request this deployment cannot satisfy (unknown, malformed)."""


def toolkits_path(image_file):
    return Path(image_file).with_name("toolkits.sqlite3")


class ToolkitStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        os.close(os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600))
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS toolkits (
                    name TEXT NOT NULL, tag TEXT NOT NULL, root TEXT NOT NULL, recorded REAL NOT NULL,
                    PRIMARY KEY (name, tag));
                CREATE TABLE IF NOT EXISTS toolkit_roots (
                    name TEXT NOT NULL, root TEXT NOT NULL, recorded REAL NOT NULL, PRIMARY KEY (name, root));
                CREATE TABLE IF NOT EXISTS toolkit_compositions (
                    image_root TEXT NOT NULL, toolkits TEXT NOT NULL, root TEXT NOT NULL, recorded REAL NOT NULL,
                    PRIMARY KEY (image_root, toolkits));
            """)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def register(self, name, tag, root):
        """Point ``name:tag`` at ``root``; every root a name ever had stays pinnable."""
        if not TOOLKIT_REF_RE.fullmatch(f"{name}:{tag}"):
            raise ToolkitError("toolkit name or tag is invalid")
        require_digest(root)
        now = time.time()
        with self._db() as db:
            db.execute("INSERT OR REPLACE INTO toolkits VALUES (?, ?, ?, ?)", (name, tag, root, now))
            db.execute("INSERT OR IGNORE INTO toolkit_roots VALUES (?, ?, ?)", (name, root, now))

    def resolve(self, name, tag):
        with self._db() as db:
            row = db.execute("SELECT root FROM toolkits WHERE name = ? AND tag = ?", (name, tag)).fetchone()
        return None if row is None else row[0]

    def known(self, name, root):
        with self._db() as db:
            return db.execute("SELECT 1 FROM toolkit_roots WHERE name = ? AND root = ?",
                              (name, root)).fetchone() is not None

    def listing(self):
        with self._db() as db:
            return [{"name": name, "tag": tag, "root": root, "recorded": recorded} for name, tag, root, recorded in
                    db.execute("SELECT name, tag, root, recorded FROM toolkits ORDER BY name, tag")]

    def composition(self, image_root, toolkits):
        with self._db() as db:
            row = db.execute("SELECT root FROM toolkit_compositions WHERE image_root = ? AND toolkits = ?",
                             (image_root, toolkits)).fetchone()
        return None if row is None else row[0]

    def record_composition(self, image_root, toolkits, root):
        with self._db() as db:
            db.execute("INSERT OR REPLACE INTO toolkit_compositions VALUES (?, ?, ?, ?)",
                       (image_root, toolkits, root, time.time()))

    def live_roots(self):
        """Roots retention keeps for their rows alone: every registered toolkit
        root and every composition (a route may pin one at any time)."""
        with self._db() as db:
            return ({row[0] for row in db.execute("SELECT root FROM toolkit_roots")}
                    | {row[0] for row in db.execute("SELECT root FROM toolkit_compositions")})


class ToolkitComposer:
    """Pins toolkit references and composes them onto image roots."""

    def __init__(self, registry, store, signing_key):
        self.registry, self.store, self.signing_key = registry, store, signing_key
        self._guard = threading.Lock()
        self._composing = {}

    def pin(self, refs):
        """``name:tag`` becomes ``name@<root>``; a pinned ref must be a root that name had."""
        pinned = []
        for ref in refs:
            match = TOOLKIT_REF_RE.fullmatch(ref)
            if match is None:
                raise ToolkitError(f"toolkit reference {ref!r} is invalid")
            name, tag, root = match[1], match[2], match[3]
            if root is None:
                root = self.store.resolve(name, tag)
                if root is None:
                    raise ToolkitError(f"toolkit {name}:{tag} is not registered")
            elif not self.store.known(name, root):
                raise ToolkitError(f"toolkit {name}@{root} is not a registered root of {name}")
            pinned.append(f"{name}@{root}")
        return tuple(pinned)

    def validate_toolkit(self, name, root):
        """A toolkit root is one signed whole-image component; returns its digest."""
        toolkit = load_environment(self.registry, root)
        if toolkit.environment.workspace is not None or toolkit.environment.toolkits:
            raise ToolkitError(f"toolkit {name} must be one whole-image component")
        if type(self.registry.load(toolkit.environment.base)) is not EnvironmentComponent:
            raise ToolkitError(f"toolkit {name} must be one whole-image component")
        return toolkit.environment.base

    def compose(self, image_root, pinned):
        """The signed root of ``image_root`` with ``pinned`` toolkits on top."""
        require_digest(image_root)
        key = ",".join(pinned)
        existing = self.store.composition(image_root, key)
        if existing is not None:
            return existing
        with self._guard:  # One composition per key at a time; others wait for it.
            event = self._composing.get((image_root, key))
            owner = event is None
            if owner:
                event = self._composing[(image_root, key)] = threading.Event()
        if not owner:
            event.wait(120)
            existing = self.store.composition(image_root, key)
            if existing is None:
                raise ToolkitError("toolkit composition did not finish")
            return existing
        try:
            root = self._compose(image_root, pinned, key)
            self.store.record_composition(image_root, key, root)
            return root
        finally:
            with self._guard:
                self._composing.pop((image_root, key), None)
            event.set()

    def _compose(self, image_root, pinned, key):
        image = load_environment(self.registry, image_root)
        components = [self.registry.load(digest) for digest in image.components]
        if any(isinstance(component, CommitEnvironmentComponent) for component in components):
            raise ToolkitError("toolkits on committed images are not supported yet")
        toolkit_components = [self.validate_toolkit(*ref.split("@", 1)) for ref in pinned]
        added = [digest for digest in toolkit_components if digest not in image.components]
        if len(image.components) + len(added) > MAX_COMPONENTS:
            raise ToolkitError("the image and its toolkits exceed the component bound")
        # The image's own layer components already rebuilt its layers when its
        # root was published; they say which, also for a released image.
        diff_ids = [layer for component in components
                    if isinstance(component, (RafsEnvironmentComponent, LayerEnvironmentComponent))
                    for layer in component.source_layers] or None
        manifest = EnvironmentManifest(image.environment.base, workspace=image.environment.workspace,
                                       toolkits=(*image.environment.toolkits, *added))
        tag = "toolkit-composition-" + hashlib.sha256(f"{image_root}|{key}".encode()).hexdigest()[:40]
        return publish_environment(self.registry, source_image=image.source_image, environment=manifest,
                                   image_config=image.image_config, signing_key=self.signing_key, tag=tag,
                                   source_diff_ids=diff_ids)
