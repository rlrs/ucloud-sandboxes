"""Chunk store M2: the environment root the gateway dispatches for an image.

``image-roots.sqlite3`` sits beside images.sqlite. Like the prepared catalog
it is created with ``IF NOT EXISTS`` and no strict schema check, so an older
release simply ignores it. A row is keyed by the pinned (annotated) manifest
digest and moves converted → switched → released, or back to reverted; every
change is journaled so a wave can be reverted from the journal
(docs/chunk-store-m2-plan.md §3.2).
"""
from contextlib import contextmanager
import os
from pathlib import Path
import sqlite3
import time

from ..environment_artifact import require_digest

TRANSITIONS = {"converted": {"switched", "reverted"}, "switched": {"released", "reverted"},
               "reverted": {"switched"}, "released": set()}
DISPATCHED = ("switched", "released")
# Retention keeps these new roots; a reverted one too, so the switch can follow
# again without reconversion.
LIVE = ("converted", "switched", "released", "reverted")
COLUMNS = ("repository", "manifest_digest", "config_digest", "old_root", "new_root", "wave", "state",
           "build_input", "updated")


def roots_path(image_file):
    return Path(image_file).with_name("image-roots.sqlite3")


def retention_view(image_file):
    """(roots retention keeps for their rows alone, images whose annotation no
    longer counts) (plan §3.3). A dispatched image's old root is kept only by
    references (routes, owners), so a build input, whose manifest stays,
    still releases its EROFS root. Empty before the gateway opens the table."""
    path = roots_path(image_file)
    if not path.exists():
        return set(), set()
    store = ImageRootsStore(path)
    return store.live_roots(), {(row["repository"], row["manifest_digest"])
                                for state in DISPATCHED for row in store.rows(state=state)}


class ImageRootsStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        os.close(os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600))
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS image_roots (
                    repository TEXT NOT NULL, manifest_digest TEXT NOT NULL, config_digest TEXT NOT NULL,
                    old_root TEXT NOT NULL, new_root TEXT NOT NULL, wave TEXT NOT NULL, state TEXT NOT NULL,
                    build_input INTEGER NOT NULL, updated REAL NOT NULL,
                    PRIMARY KEY (repository, manifest_digest));
                CREATE INDEX IF NOT EXISTS image_roots_wave ON image_roots(wave, state);
                CREATE TABLE IF NOT EXISTS image_roots_journal (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, repository TEXT NOT NULL,
                    manifest_digest TEXT NOT NULL, from_state TEXT, to_state TEXT NOT NULL,
                    new_root TEXT NOT NULL, detail TEXT NOT NULL);
            """)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def get(self, repository, manifest_digest):
        with self._db() as db:
            row = db.execute(f"SELECT {', '.join(COLUMNS)} FROM image_roots WHERE repository = ? AND "
                             "manifest_digest = ?", (repository, manifest_digest)).fetchone()
        return None if row is None else dict(zip(COLUMNS, row))

    def dispatch_root(self, repository, manifest_digest):
        """The root to dispatch, or None while the annotation still decides."""
        row = self.get(repository, manifest_digest)
        return row["new_root"] if row is not None and row["state"] in DISPATCHED else None

    def live_roots(self):
        with self._db() as db:
            return {root for (root,) in db.execute(
                f"SELECT new_root FROM image_roots WHERE state IN ({','.join('?' * len(LIVE))})", LIVE)}

    def rows(self, *, wave=None, state=None):
        query, args = f"SELECT {', '.join(COLUMNS)} FROM image_roots WHERE 1", []
        for column, value in (("wave", wave), ("state", state)):
            if value is not None:
                query, args = query + f" AND {column} = ?", args + [value]
        with self._db() as db:
            return [dict(zip(COLUMNS, row)) for row in db.execute(query + " ORDER BY repository", args)]

    def record_converted(self, repository, manifest_digest, *, config_digest, old_root, new_root, wave,
                         build_input, detail=""):
        """A verified conversion. A converted or reverted row may be replaced
        (a reconversion); a dispatched one may not."""
        for digest in (manifest_digest, config_digest, old_root, new_root):
            require_digest(digest)
        with self._db() as db:
            current = db.execute("SELECT state FROM image_roots WHERE repository = ? AND manifest_digest = ?",
                                 (repository, manifest_digest)).fetchone()
            if current is not None and current[0] in DISPATCHED:
                raise ValueError(f"{repository}@{manifest_digest} is {current[0]}; revert it first")
            db.execute("INSERT OR REPLACE INTO image_roots VALUES (?, ?, ?, ?, ?, ?, 'converted', ?, ?)",
                       (repository, manifest_digest, config_digest, old_root, new_root, str(wave),
                        int(bool(build_input)), time.time()))
            self._journal(db, repository, manifest_digest, current and current[0], "converted", new_root, detail)

    def transition(self, repository, manifest_digest, to_state, *, detail=""):
        with self._db() as db:
            row = db.execute("SELECT state, new_root FROM image_roots WHERE repository = ? AND manifest_digest = ?",
                             (repository, manifest_digest)).fetchone()
            if row is None:
                raise KeyError(f"{repository}@{manifest_digest} has no image_roots row")
            if to_state not in TRANSITIONS[row[0]]:
                raise ValueError(f"{repository}@{manifest_digest}: {row[0]} cannot become {to_state}")
            db.execute("UPDATE image_roots SET state = ?, updated = ? WHERE repository = ? AND manifest_digest = ?",
                       (to_state, time.time(), repository, manifest_digest))
            self._journal(db, repository, manifest_digest, row[0], to_state, row[1], detail)

    def journal(self, repository=None):
        query = "SELECT seq, at, repository, manifest_digest, from_state, to_state, new_root, detail FROM " \
                "image_roots_journal" + (" WHERE repository = ?" if repository else "") + " ORDER BY seq"
        with self._db() as db:
            return db.execute(query, (repository,) if repository else ()).fetchall()

    @staticmethod
    def _journal(db, repository, manifest_digest, from_state, to_state, new_root, detail):
        db.execute("INSERT INTO image_roots_journal (at, repository, manifest_digest, from_state, to_state, "
                   "new_root, detail) VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (time.time(), repository, manifest_digest, from_state, to_state, new_root, detail))
