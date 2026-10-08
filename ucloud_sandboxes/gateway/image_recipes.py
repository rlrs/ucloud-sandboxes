"""Image recipes: names a trainer asks for, built on demand or ahead (C2.7).

Training names images (``prime/primeintellect/tmax:task_…``) and never sends
recipes. ``image-recipes.sqlite3``, beside ``image-roots.sqlite3``, maps each
registered name to a recipe: an uploaded build context, its Dockerfile path and
build arguments. Identical recipes share one gateway-managed image id,
``recipe-<sha40>``, so a recipe is built once whatever names it carries.

``ensure`` reports each name as ``ready`` (with the reference a sandbox uses),
``building``, ``queued`` (no builder slot yet), ``failed`` or ``unknown``, and
submits builds for the missing ones through the gateway's ordinary build path,
where prepared foundations and regenerated bases apply. It is idempotent and
cheap, so a trainer calls it for the next step's tasks and polls it; a create
naming a recipe that is not built calls it too and answers retryable 503.

Build records live on builders and vanish with them, so the store keeps its own:
one row per image id, claimed atomically before a submission, so the gateway's
processes never submit one image twice.

Retention: ``pinned`` images stay registry-leased (a corpus built ahead);
``cached`` ones age out as any managed image does and are rebuilt on demand.

With builder_format "rafs" a build ends in the chunk store; ensure then
releases the image's OCI copy, as the M2 waves did for the corpus, but per
image: it can always be rebuilt from its recipe, so no regeneration receipt.

The store is also the gateway's index of training image names
(docs/image-index.md). A name is either a recipe (built as above) or
``prepared``: an image already in the chunk store, ready from registration.
Each name carries its environment, the dataset tasks that use it and where it
came from, so the index answers what a trainer may sample: ``task_ids``.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import time

_LOG = logging.getLogger(__name__)

RETENTIONS = ("pinned", "cached")
MAX_RECIPES_PER_REQUEST = 1000
MAX_ENSURE_NAMES = 1000
MAX_NAME_BYTES = 512
# Submissions per ensure call: each costs a context sync and a builder round
# trip; the rest report queued and go out on the next call.
MAX_SUBMITS_PER_ENSURE = 32
# A failed build is retried after this long, at most FAILURE_ATTEMPTS times in
# all: network failures pass, recipe rot does not.
FAILURE_BACKOFF_SECONDS = 600
FAILURE_ATTEMPTS = 3
# A submitted build no builder knows about (its builder went away) is
# resubmitted after this long.
LOST_BUILD_SECONDS = 180
# OCI releases per ensure call; the rest wait for a later call.
MAX_RELEASES = 64
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/:@+-]*")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
# A prepared image: a managed repository at a manifest digest, without the
# registry host (hosts move; the gateway adds its own).
_PREPARED = re.compile(r"[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}")
_ENVIRONMENT = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")
MAX_TASKS_PER_NAME = 100_000
MAX_TASK_ID_BYTES = 512
MAX_SOURCE_BYTES = 4096
# What a name's image is doing, as the index reports it.
NAME_STATES = ("ready", "building", "not_built", "retrying", "failed")


class RecipeError(ValueError):
    """A recipe registration or ensure request this gateway refuses."""


def recipes_path(image_file):
    return Path(image_file).with_name("image-recipes.sqlite3")


def recipe_identity(recipe):
    """(sha256 hex, image id) of a recipe's build inputs; names are not inputs."""
    if recipe.get("prepared_reference"):
        digest = hashlib.sha256(json.dumps({"prepared_reference": recipe["prepared_reference"]}).encode()).hexdigest()
        return digest, f"prepared-{digest[:40]}"
    document = {key: recipe[key] for key in ("context_archive_digest", "dockerfile", "build_args")}
    digest = hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return digest, f"recipe-{digest[:40]}"


def name_state(kind, state, attempts):
    """The index's state of a name from its image's row (NAME_STATES)."""
    if kind == "prepared":  # Ready from registration; failed once the chunk store lost it.
        return "failed" if state == "failed" else "ready"
    if state == "ready":
        return "ready"
    if state in ("submitting", "building"):
        return "building"
    if state == "failed":
        return "failed" if attempts >= FAILURE_ATTEMPTS else "retrying"
    return "not_built"


def _index_fields(raw, name):
    """environment, tasks and source: what the index knows about a name."""
    environment = raw.get("environment")
    if environment is not None and (not isinstance(environment, str) or not _ENVIRONMENT.fullmatch(environment)):
        raise RecipeError(f"{name}: environment must be a short lowercase name")
    tasks = raw.get("tasks", [])
    if (not isinstance(tasks, list) or len(tasks) > MAX_TASKS_PER_NAME
            or not all(isinstance(t, str) and t and len(t.encode()) <= MAX_TASK_ID_BYTES for t in tasks)):
        raise RecipeError(f"{name}: tasks must be a list of task ids")
    if tasks and environment is None:
        raise RecipeError(f"{name}: tasks need an environment")
    source = raw.get("source", {})
    if not isinstance(source, dict) or len(json.dumps(source)) > MAX_SOURCE_BYTES:
        raise RecipeError(f"{name}: source must be a small object")
    return {"environment": environment, "tasks": sorted(set(tasks)), "source": source}


def validate_recipe(raw):
    """A registration row: {name, context_archive_digest, context_archive_size,
    dockerfile?, build_args?, retention?}."""
    if not isinstance(raw, dict):
        raise RecipeError("each recipe must be an object")
    unknown = set(raw) - {"name", "context_archive_digest", "context_archive_size", "dockerfile", "build_args",
                          "retention", "prepared_reference", "environment", "tasks", "source"}
    if unknown:
        raise RecipeError(f"unknown recipe fields: {sorted(unknown)}")
    name = raw.get("name")
    if not isinstance(name, str) or len(name.encode()) > MAX_NAME_BYTES or not _NAME.fullmatch(name):
        raise RecipeError(f"recipe name {name!r} is invalid")
    index = _index_fields(raw, name)
    if "prepared_reference" in raw:
        reference = raw["prepared_reference"]
        if set(raw) & {"context_archive_digest", "context_archive_size", "dockerfile", "build_args"}:
            raise RecipeError(f"{name}: a prepared image has no build inputs")
        if not isinstance(reference, str) or not _PREPARED.fullmatch(reference):
            raise RecipeError(f"{name}: prepared_reference must be <repository>@sha256:<64 hex>, without a host")
        retention = raw.get("retention", "cached")
        if retention not in RETENTIONS:
            raise RecipeError(f"{name}: retention must be one of {RETENTIONS}")
        return {"name": name, "prepared_reference": reference, "retention": retention, **index}
    digest = raw.get("context_archive_digest")
    if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
        raise RecipeError(f"{name}: context_archive_digest must be sha256:<64 hex>")
    size = raw.get("context_archive_size")
    if type(size) is not int or size <= 0:
        raise RecipeError(f"{name}: context_archive_size must be a positive integer")
    dockerfile = raw.get("dockerfile", "Dockerfile")
    if not isinstance(dockerfile, str) or not dockerfile or dockerfile.startswith("/") or ".." in dockerfile.split("/"):
        raise RecipeError(f"{name}: dockerfile must be a relative path inside the context")
    build_args = raw.get("build_args", {})
    if not isinstance(build_args, dict) or not all(isinstance(k, str) and isinstance(v, str)
                                                   for k, v in build_args.items()):
        raise RecipeError(f"{name}: build_args must map strings to strings")
    retention = raw.get("retention", "cached")
    if retention not in RETENTIONS:
        raise RecipeError(f"{name}: retention must be one of {RETENTIONS}")
    return {"name": name, "context_archive_digest": digest, "context_archive_size": size,
            "dockerfile": dockerfile, "build_args": dict(sorted(build_args.items())), "retention": retention, **index}


def keep_context(uploads, kept, digest, size):
    """Copy an uploaded build context into the recipes' own store, which never
    expires (uploads age out after a day; a recipe may wait much longer)."""
    try:
        if kept.size(digest) == size:
            return
    except (FileNotFoundError, ValueError):
        pass
    with uploads.open(digest) as reader:
        kept.put_with_status(digest, reader, content_length=size)


def restore_context(kept, uploads, digest, size):
    """Put a recipe's context back where builds read it, when it aged out."""
    try:
        if uploads.size_and_touch(digest) == size:
            return
    except (FileNotFoundError, ValueError):
        pass
    with kept.open(digest) as reader:
        uploads.put_with_status(digest, reader, content_length=size)


def build_payload(recipe):
    """The /v1/images/build payload that builds ``recipe`` (gateway-managed tag)."""
    return {"id": recipe["image_id"], "context_path": ".", "dockerfile": recipe["dockerfile"],
            "context_archive_digest": recipe["context_archive_digest"],
            "context_archive_size": recipe["context_archive_size"], "context_archive_format": "tar.gz",
            "build_args": dict(recipe["build_args"]), "labels": {"ucloud-sandboxes.recipe": recipe["recipe_sha256"]},
            "push": True, "wait": False}  # Submit and return: ensure polls; never hold a request for a build.


class ImageRecipeStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        os.close(os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600))
        db = sqlite3.connect(self.path, timeout=30)
        try:
            db.executescript("""
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS recipes (
                    name TEXT PRIMARY KEY, image_id TEXT NOT NULL, retention TEXT NOT NULL,
                    registered REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS recipe_images (
                    image_id TEXT PRIMARY KEY, recipe_sha256 TEXT NOT NULL, recipe TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'absent', build_id TEXT, error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0, changed REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS recipes_by_image ON recipes (image_id);
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(recipe_images)")}
            if "oci" not in columns:  # present | released (chunk store only) | kept (an EROFS build)
                db.execute("ALTER TABLE recipe_images ADD COLUMN oci TEXT NOT NULL DEFAULT 'present'")
            if "kind" not in columns:  # build (a recipe) | prepared (already in the chunk store)
                db.execute("ALTER TABLE recipe_images ADD COLUMN kind TEXT NOT NULL DEFAULT 'build'")
            columns = {row[1] for row in db.execute("PRAGMA table_info(recipes)")}
            if "environment" not in columns:
                db.execute("ALTER TABLE recipes ADD COLUMN environment TEXT")
                db.execute("ALTER TABLE recipes ADD COLUMN source TEXT NOT NULL DEFAULT '{}'")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS name_tasks (
                    environment TEXT NOT NULL, task_id TEXT NOT NULL, name TEXT NOT NULL,
                    PRIMARY KEY (environment, task_id));
                CREATE INDEX IF NOT EXISTS name_tasks_by_name ON name_tasks (name);
                CREATE INDEX IF NOT EXISTS recipes_by_environment ON recipes (environment, name);
            """)
        finally:
            db.close()

    @contextmanager
    def _db(self, *, write=True):
        """One transaction; writers take the lock up front, readers never block (WAL)."""
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                yield db
            except BaseException:
                db.execute("ROLLBACK")
                raise
            db.execute("COMMIT")
        finally:
            db.close()

    def register(self, recipes, *, now=None):
        """Upsert validated recipes; returns [{name, image_id, changed}]. A name
        whose recipe changes points at the new image id; the old image stays.
        A name's environment, source and tasks are replaced by the new ones;
        a task moves to the name that registered it last."""
        now = time.time() if now is None else now
        out = []
        with self._db() as db:
            for recipe in recipes:
                sha, image_id = recipe_identity(recipe)
                if recipe.get("prepared_reference"):
                    kind, stored, state = "prepared", {"reference": recipe["prepared_reference"]}, "ready"
                else:
                    kind, state = "build", "absent"
                    stored = {key: recipe[key] for key in ("context_archive_digest", "context_archive_size",
                                                             "dockerfile", "build_args")}
                db.execute("INSERT OR IGNORE INTO recipe_images (image_id, recipe_sha256, recipe, state, changed, kind) "
                           "VALUES (?, ?, ?, ?, ?, ?)",
                           (image_id, sha, json.dumps(stored, sort_keys=True), state, now, kind))
                environment, source = recipe.get("environment"), json.dumps(recipe.get("source") or {}, sort_keys=True)
                row = db.execute("SELECT image_id, retention, environment, source, registered FROM recipes "
                                 "WHERE name = ?", (recipe["name"],)).fetchone()
                changed = row is None or row[:2] != (image_id, recipe["retention"])
                if changed or row[2:4] != (environment, source):
                    db.execute("INSERT OR REPLACE INTO recipes (name, image_id, retention, registered, environment, "
                               "source) VALUES (?, ?, ?, ?, ?, ?)",
                               (recipe["name"], image_id, recipe["retention"], now if changed else row[4],
                                environment, source))
                tasks = recipe.get("tasks") or []
                if tasks or row is not None:
                    db.execute("DELETE FROM name_tasks WHERE name = ?", (recipe["name"],))
                    db.executemany("INSERT OR REPLACE INTO name_tasks VALUES (?, ?, ?)",
                                   [(environment, task, recipe["name"]) for task in tasks])
                out.append({"name": recipe["name"], "image_id": image_id, "changed": changed})
        return out

    def lookup(self, names):
        """{name: recipe} for the registered names among ``names``."""
        names = list(dict.fromkeys(names))
        found = {}
        with self._db(write=False) as db:
            for start in range(0, len(names), 500):
                chunk = names[start:start + 500]
                rows = db.execute(
                    "SELECT r.name, r.image_id, r.retention, i.recipe_sha256, i.recipe, i.state, i.build_id, "
                    "i.error, i.attempts, i.changed, i.oci, i.kind, r.environment, r.source "
                    "FROM recipes r JOIN recipe_images i USING (image_id) "
                    f"WHERE r.name IN ({','.join('?' * len(chunk))})", chunk)
                for (name, image_id, retention, sha, recipe, state, build_id, error, attempts, changed, oci, kind,
                     environment, source) in rows:
                    found[name] = {**json.loads(recipe), "name": name, "image_id": image_id, "retention": retention,
                                   "recipe_sha256": sha, "state": state, "build_id": build_id, "error": error,
                                   "attempts": attempts, "changed": changed, "oci": oci, "kind": kind,
                                   "environment": environment, "source": json.loads(source or "{}")}
        return found

    def has(self, name):
        with self._db(write=False) as db:
            return db.execute("SELECT 1 FROM recipes WHERE name = ?", (name,)).fetchone() is not None

    def claim(self, image_id, *, now=None):
        """Take the right to submit ``image_id``'s build; False when another
        process holds it, it is still backing off, or it failed for good."""
        now = time.time() if now is None else now
        with self._db() as db:
            row = db.execute("SELECT state, attempts, changed FROM recipe_images WHERE image_id = ?",
                             (image_id,)).fetchone()
            if row is None:
                return False
            state, attempts, changed = row
            if state == "submitting" and now - changed < LOST_BUILD_SECONDS:
                return False
            if state == "building" and now - changed < LOST_BUILD_SECONDS:
                return False
            if state == "failed" and (attempts >= FAILURE_ATTEMPTS or now - changed < FAILURE_BACKOFF_SECONDS):
                return False
            db.execute("UPDATE recipe_images SET state = 'submitting', changed = ? WHERE image_id = ?",
                       (now, image_id))
            return True

    def record(self, image_id, state, *, build_id=None, error=None, attempt=False, now=None):
        now = time.time() if now is None else now
        with self._db() as db:
            db.execute("UPDATE recipe_images SET state = ?, build_id = COALESCE(?, build_id), error = ?, "
                       "attempts = attempts + ?, changed = ? WHERE image_id = ?",
                       (state, build_id, error, int(attempt), now, image_id))

    def touch(self, image_id, *, now=None):
        """A build seen alive on its builder: not lost."""
        with self._db() as db:
            db.execute("UPDATE recipe_images SET changed = ? WHERE image_id = ? AND state = 'building'",
                       (time.time() if now is None else now, image_id))

    def mark_oci(self, image_id, oci):
        with self._db() as db:
            db.execute("UPDATE recipe_images SET oci = ? WHERE image_id = ?", (oci, image_id))

    def pinned_image_ids(self):
        with self._db(write=False) as db:
            return {row[0] for row in db.execute("SELECT DISTINCT image_id FROM recipes WHERE retention = 'pinned'")}

    def counts(self):
        with self._db(write=False) as db:
            return {"names": db.execute("SELECT COUNT(*) FROM recipes").fetchone()[0],
                    "images": dict(db.execute("SELECT state, COUNT(*) FROM recipe_images GROUP BY state").fetchall())}

    # --- The index's views (docs/image-index.md) ---

    def summary(self):
        """{environment: {names, tasks, prepared, recipes, states: {state: names}}};
        names registered without an environment count under "(none)"."""
        out = {}
        with self._db(write=False) as db:
            for environment, kind, state, attempts, names in db.execute(
                    "SELECT r.environment, i.kind, i.state, i.attempts, COUNT(*) FROM recipes r "
                    "JOIN recipe_images i USING (image_id) GROUP BY 1, 2, 3, 4"):
                row = out.setdefault(environment or "(none)", {"names": 0, "tasks": 0, "prepared": 0, "recipes": 0,
                                                               "states": dict.fromkeys(NAME_STATES, 0)})
                row["names"] += names
                row["prepared" if kind == "prepared" else "recipes"] += names
                row["states"][name_state(kind, state, attempts)] += names
            for environment, tasks in db.execute("SELECT environment, COUNT(*) FROM name_tasks GROUP BY 1"):
                if environment in out:
                    out[environment]["tasks"] = tasks
        return dict(sorted(out.items()))

    def names(self, *, environment=None, state=None, after="", limit=500):
        """Names in order after ``after``, filtered by environment and NAME_STATES
        state: [{name, environment, kind, state}] and the next cursor (None at the end)."""
        if state is not None and state not in NAME_STATES:
            raise RecipeError(f"state must be one of {NAME_STATES}")
        limit = max(1, min(int(limit), 5000))
        query = ("SELECT r.name, r.environment, i.kind, i.state, i.attempts FROM recipes r "
                 "JOIN recipe_images i USING (image_id) WHERE r.name > ?")
        args = [after or ""]
        if environment is not None:
            query += " AND r.environment = ?"
            args.append(environment)
        query += " ORDER BY r.name"
        rows, cursor = [], None
        with self._db(write=False) as db:
            for name, env, kind, image_state, attempts in db.execute(query, args):
                current = name_state(kind, image_state, attempts)
                if state is not None and current != state:
                    continue
                if len(rows) == limit:
                    cursor = rows[-1]["name"]
                    break
                rows.append({"name": name, "environment": env, "kind": kind, "state": current})
        return rows, cursor

    def detail(self, name):
        """Everything the index knows about one name, or None."""
        recipe = self.lookup([name]).get(name)
        if recipe is None:
            return None
        with self._db(write=False) as db:
            tasks = [row[0] for row in db.execute("SELECT task_id FROM name_tasks WHERE name = ? ORDER BY task_id "
                                                  "LIMIT 101", (name,))]
            total = db.execute("SELECT COUNT(*) FROM name_tasks WHERE name = ?", (name,)).fetchone()[0]
            aliases = [row[0] for row in db.execute("SELECT name FROM recipes WHERE image_id = ? AND name != ? "
                                                    "ORDER BY name LIMIT 20", (recipe["image_id"], name))]
        made = ({"prepared_reference": recipe["reference"]} if recipe["kind"] == "prepared" else
                {key: recipe[key] for key in ("context_archive_digest", "context_archive_size", "dockerfile",
                                              "build_args")})
        return {"name": name, "environment": recipe["environment"], "source": recipe["source"],
                "kind": recipe["kind"], "state": name_state(recipe["kind"], recipe["state"], recipe["attempts"]),
                "image_id": recipe["image_id"], **made, "build_id": recipe["build_id"], "error": recipe["error"],
                "attempts": recipe["attempts"], "retention": recipe["retention"], "tasks": total,
                "task_ids": tasks[:100], "other_names": aliases}

    def task_ids(self, environment):
        """The environment's task ids a trainer may sample: every task whose
        name is prepared or buildable, i.e. not failed for good. Returns
        (task ids, {excluded state: tasks})."""
        keep, excluded = [], {}
        with self._db(write=False) as db:
            for task, kind, state, attempts in db.execute(
                    "SELECT t.task_id, i.kind, i.state, i.attempts FROM name_tasks t JOIN recipes r USING (name) "
                    "JOIN recipe_images i USING (image_id) WHERE t.environment = ? ORDER BY t.task_id",
                    (environment,)):
                current = name_state(kind, state, attempts)
                if current == "failed":
                    excluded[current] = excluded.get(current, 0) + 1
                else:
                    keep.append(task)
        return keep, excluded


@dataclass
class Submission:
    """What dispatching one build produced."""
    state: str  # building | queued | failed
    build_id: str | None = None
    error: str | None = None


class RecipeEnsurer:
    """Status and on-demand builds for registered names.

    ``resolve(image_id)``: the sandbox reference of a built image, or None.
    ``build_status(build_id)``: (status, build) from the builders, status None
    when no builder knows it. ``adopt(build)``: record a succeeded build's image
    with the gateway, so it resolves after its builder is gone.
    ``submit(payload)``: a Submission. ``protect(image_id)``: lease a pinned image.
    """

    def __init__(self, store, *, resolve, build_status, adopt, submit, protect=None, release=None, now=time.time,
                 max_submits=MAX_SUBMITS_PER_ENSURE, resolve_prepared=None):
        self.store, self.resolve, self.build_status, self.adopt = store, resolve, build_status, adopt
        self.submit, self.protect, self.release, self.now, self.max_submits = submit, protect, release, now, max_submits
        # ``resolve_prepared(repository@digest)``: the sandbox reference of a
        # prepared image while the chunk store holds it, else None.
        self.resolve_prepared = resolve_prepared

    def ensure(self, names):
        names = list(dict.fromkeys(names))
        if len(names) > MAX_ENSURE_NAMES:
            raise RecipeError(f"ensure takes at most {MAX_ENSURE_NAMES} names")
        recipes = self.store.lookup(names)
        by_image, out, submits = {}, {}, [self.max_submits]
        for name in names:
            recipe = recipes.get(name)
            if recipe is None:
                out[name] = {"state": "unknown"}
                continue
            if recipe["image_id"] not in by_image:
                by_image[recipe["image_id"]] = self._one(recipe, submits)
            out[name] = {"image_id": recipe["image_id"], **by_image[recipe["image_id"]]}
        self._release(recipes, by_image)
        return out

    def _release(self, recipes, by_image):
        """Ready images built into the chunk store lose their OCI copy (a recipe
        rebuilds it): ``release({image_id: reference})`` answers each released,
        kept (no chunk-store root) or pending (held; retried on a later call)."""
        if self.release is None:
            return
        candidates = {}
        for recipe in recipes.values():
            status = by_image.get(recipe["image_id"], {})
            if recipe["kind"] == "prepared":
                continue
            if status.get("state") == "ready" and recipe["oci"] == "present" and len(candidates) < MAX_RELEASES:
                candidates[recipe["image_id"]] = status["reference"]
        if not candidates:
            return
        for image_id, outcome in self.release(candidates).items():
            if outcome in ("released", "kept"):
                self.store.mark_oci(image_id, outcome)

    def _ready(self, recipe):
        reference = self.resolve(recipe["image_id"])
        if reference is None:
            return None
        if recipe["state"] != "ready":  # Once, as it becomes ready: a durable reference for a pinned image.
            if recipe["retention"] == "pinned" and self.protect is not None:
                try:
                    self.protect(recipe["image_id"])
                except Exception:  # noqa: BLE001 - usable now; pinned on a later call, never blocking others.
                    _LOG.warning("image recipe %s: pinning failed; retried on the next ensure", recipe["image_id"],
                                 exc_info=True)
                    return {"state": "ready", "reference": reference}
            self.store.record(recipe["image_id"], "ready", error=None, now=self.now())
        return {"state": "ready", "reference": reference}

    def _one(self, recipe, submits):
        image_id, state = recipe["image_id"], recipe["state"]
        if recipe["kind"] == "prepared":  # Never built here: ready while the chunk store holds it.
            reference = self.resolve_prepared(recipe["reference"]) if self.resolve_prepared is not None else None
            if reference is None:
                error = f"prepared image {recipe['reference']} is not in the chunk store"
                if recipe["state"] != "failed":
                    self.store.record(image_id, "failed", error=error, now=self.now())
                return {"state": "failed", "error": error}
            if recipe["state"] != "ready":
                self.store.record(image_id, "ready", error=None, now=self.now())
            return {"state": "ready", "reference": reference}
        if state == "ready":  # Only a built image resolves; a cached one may since have aged out.
            ready = self._ready(recipe)
            if ready is not None:
                return ready
            self.store.record(image_id, "absent", error="the built image is gone; rebuilding", now=self.now())
            recipe = {**recipe, "state": "absent"}
            state = "absent"
        if state == "building" and recipe["build_id"]:
            status, build = self.build_status(recipe["build_id"])
            if status == "succeeded":
                self.adopt(build)
                ready = self._ready(recipe)
                if ready is not None:
                    return ready
                self.store.record(image_id, "failed", error="built image is not available to sandboxes",
                                  now=self.now())
                return {"state": "failed", "error": "built image is not available to sandboxes"}
            if status == "failed":
                error = str(build.get("error") or "build failed")[:2000]
                self.store.record(image_id, "failed", error=error, now=self.now())
                return {"state": "failed", "error": error, "attempts": recipe["attempts"]}
            if status is not None:
                self.store.touch(image_id, now=self.now())
                return {"state": "building", "build_id": recipe["build_id"]}
        if submits[0] <= 0 or not self.store.claim(image_id, now=self.now()):
            current = self.store.lookup([recipe["name"]])[recipe["name"]]
            if current["state"] == "failed":
                return {"state": "failed", "error": current["error"], "attempts": current["attempts"]}
            if current["state"] in ("building", "submitting"):
                return {"state": "building", "build_id": current["build_id"]}
            return {"state": "queued"}
        submits[0] -= 1
        try:
            submission = self.submit(build_payload(recipe))
        except Exception as exc:  # noqa: BLE001 - report, release the claim, retry on a later call.
            submission = Submission("queued", error=f"{type(exc).__name__}: {exc}"[:2000])
        if submission.state == "building":
            self.store.record(image_id, "building", build_id=submission.build_id, error=None, attempt=True,
                              now=self.now())
            self.store.mark_oci(image_id, "present")
            return {"state": "building", "build_id": submission.build_id}
        if submission.state == "failed":
            self.store.record(image_id, "failed", error=submission.error, attempt=True, now=self.now())
            return {"state": "failed", "error": submission.error, "attempts": recipe["attempts"] + 1}
        self.store.record(image_id, "absent", error=submission.error, now=self.now())
        return {"state": "queued", **({"reason": submission.error} if submission.error else {})}
