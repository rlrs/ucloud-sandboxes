"""Image recipes (C2.7): registration, ensure, and the build lifecycle around it."""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.gateway.image_recipes import (FAILURE_ATTEMPTS, FAILURE_BACKOFF_SECONDS, LOST_BUILD_SECONDS,
                                                    ImageRecipeStore, RecipeEnsurer, RecipeError, Submission,
                                                    build_payload, validate_recipe)

DIGEST = "sha256:" + "a" * 64


def recipe(name, digest=DIGEST, **extra):
    return validate_recipe({"name": name, "context_archive_digest": digest, "context_archive_size": 10, **extra})


class Builders:
    """Fake builders: submissions, their statuses, adopted images."""

    def __init__(self):
        self.submitted, self.status, self.adopted, self.next_state, self.protected = [], {}, set(), "building", []

    def submit(self, payload):
        self.submitted.append(payload)
        if self.next_state != "building":
            return Submission(self.next_state, error="no ready builder" if self.next_state == "queued" else "boom")
        build_id = f"b{len(self.submitted)}"
        self.status[build_id] = ("running", {"build_id": build_id, "image_id": payload["id"]})
        return Submission("building", build_id=build_id)

    def finish(self, build_id, status, error=None):
        self.status[build_id] = (status, {**self.status[build_id][1], "error": error})

    def build_status(self, build_id):
        return self.status.get(build_id, (None, None))

    def adopt(self, build):
        self.adopted.add(build["image_id"])

    def resolve(self, image_id):
        return f"registry/{image_id}@sha256:{'b' * 64}" if image_id in self.adopted else None


class ImageRecipeTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = ImageRecipeStore(Path(self.temp.name) / "image-recipes.sqlite3")
        self.builders, self.clock = Builders(), [1000.0]
        self.ensurer = RecipeEnsurer(self.store, resolve=self.builders.resolve, build_status=self.builders.build_status,
                                     adopt=self.builders.adopt, submit=self.builders.submit,
                                     protect=self.builders.protected.append, now=lambda: self.clock[0])

    def test_validation_refuses_what_a_build_could_not_use(self):
        for bad in ({"name": "x"}, {"name": "x", "context_archive_digest": "nope", "context_archive_size": 1},
                    {"name": "../x", "context_archive_digest": DIGEST, "context_archive_size": 1},
                    {"name": "x", "context_archive_digest": DIGEST, "context_archive_size": 1, "dockerfile": "/etc/x"},
                    {"name": "x", "context_archive_digest": DIGEST, "context_archive_size": 1, "retention": "forever"},
                    {"name": "x", "context_archive_digest": DIGEST, "context_archive_size": 1, "extra": 1}):
            with self.assertRaises(RecipeError):
                validate_recipe(bad)

    def test_identical_recipes_share_one_image_and_one_build(self):
        registered = self.store.register([recipe("prime/tmax:task_1"), recipe("alias/tmax:task_1"),
                                          recipe("prime/tmax:task_2", "sha256:" + "c" * 64)])
        self.assertEqual(registered[0]["image_id"], registered[1]["image_id"])
        self.assertNotEqual(registered[0]["image_id"], registered[2]["image_id"])
        self.assertTrue(registered[0]["image_id"].startswith("recipe-"))
        status = self.ensurer.ensure(["prime/tmax:task_1", "alias/tmax:task_1", "prime/tmax:task_2", "nope:1"])
        self.assertEqual([s["state"] for s in status.values()], ["building", "building", "building", "unknown"])
        self.assertEqual(len(self.builders.submitted), 2)
        self.assertEqual(self.builders.submitted[0], build_payload(self.store.lookup(["prime/tmax:task_1"])
                                                                   ["prime/tmax:task_1"]))
        self.assertTrue(self.builders.submitted[0]["push"])
        self.assertIs(self.builders.submitted[0]["wait"], False)  # A synchronous build would hold the ensure call.

    def test_a_build_goes_from_building_to_ready_and_stays_ready_without_its_builder(self):
        self.store.register([recipe("t:1", retention="pinned")])
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "building")
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "building")  # Polled, not resubmitted.
        self.assertEqual(len(self.builders.submitted), 1)
        self.builders.finish("b1", "succeeded")
        ready = self.ensurer.ensure(["t:1"])["t:1"]
        self.assertEqual(ready["state"], "ready")
        self.assertIn("@sha256:", ready["reference"])
        self.assertEqual(self.builders.protected, [ready["image_id"]])  # Pinned: leased.
        self.builders.status.clear()  # The builder scaled down.
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "ready")

    def test_a_cached_image_that_ages_out_is_rebuilt(self):
        self.store.register([recipe("t:1")])
        self.ensurer.ensure(["t:1"])
        self.builders.finish("b1", "succeeded")
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "ready")
        self.assertEqual(self.builders.protected, [])
        self.builders.adopted.clear()  # Pruned from the registry.
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "building")
        self.assertEqual(len(self.builders.submitted), 2)

    def test_failures_back_off_then_give_up(self):
        self.store.register([recipe("t:1")])
        for attempt in range(1, FAILURE_ATTEMPTS + 1):
            self.ensurer.ensure(["t:1"])
            self.builders.finish(f"b{attempt}", "failed", error="pip: no matching distribution")
            failed = self.ensurer.ensure(["t:1"])["t:1"]
            self.assertEqual((failed["state"], failed["error"]), ("failed", "pip: no matching distribution"))
            self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "failed")  # Backing off.
            self.assertEqual(len(self.builders.submitted), attempt)
            self.clock[0] += FAILURE_BACKOFF_SECONDS + 1
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "failed")  # For good.
        self.assertEqual(len(self.builders.submitted), FAILURE_ATTEMPTS)

    def test_no_builder_slot_is_queued_and_resubmitted_next_time(self):
        self.store.register([recipe("t:1")])
        self.builders.next_state = "queued"
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "queued")
        self.builders.next_state = "building"
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "building")
        self.assertEqual(len(self.builders.submitted), 2)

    def test_a_lost_build_is_resubmitted_after_a_grace(self):
        self.store.register([recipe("t:1")])
        self.ensurer.ensure(["t:1"])
        self.builders.status.clear()  # Its builder died before reporting.
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "building")
        self.assertEqual(len(self.builders.submitted), 1)
        self.clock[0] += LOST_BUILD_SECONDS + 1
        self.assertEqual(self.ensurer.ensure(["t:1"])["t:1"]["state"], "building")
        self.assertEqual(len(self.builders.submitted), 2)

    def test_submissions_per_call_are_bounded(self):
        self.store.register([recipe(f"t:{i}", "sha256:" + f"{i:064x}") for i in range(5)])
        small = RecipeEnsurer(self.store, resolve=self.builders.resolve, build_status=self.builders.build_status,
                              adopt=self.builders.adopt, submit=self.builders.submit, now=lambda: self.clock[0],
                              max_submits=2)
        states = [s["state"] for s in small.ensure([f"t:{i}" for i in range(5)]).values()]
        self.assertEqual(states, ["building", "building", "queued", "queued", "queued"])

    def test_a_claim_keeps_two_processes_from_submitting_one_image(self):
        image_id = self.store.register([recipe("t:1")])[0]["image_id"]
        self.assertTrue(self.store.claim(image_id, now=1000))
        self.assertFalse(self.store.claim(image_id, now=1001))
        self.assertTrue(self.store.claim(image_id, now=1000 + LOST_BUILD_SECONDS + 1))  # A crashed claimant.

    def test_reregistering_a_name_with_a_new_recipe_moves_it(self):
        first = self.store.register([recipe("t:1")])[0]
        again = self.store.register([recipe("t:1")])[0]
        moved = self.store.register([recipe("t:1", "sha256:" + "d" * 64)])[0]
        self.assertFalse(again["changed"])
        self.assertTrue(moved["changed"])
        self.assertNotEqual(first["image_id"], moved["image_id"])
        self.assertEqual(self.store.lookup(["t:1"])["t:1"]["image_id"], moved["image_id"])


if __name__ == "__main__":
    unittest.main()


class ImageRecipeReleaseTests(unittest.TestCase):
    """Ready images built into the chunk store lose their OCI copy, once."""

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = ImageRecipeStore(Path(self.temp.name) / "image-recipes.sqlite3")
        self.builders, self.calls, self.answers = Builders(), [], {}
        self.ensurer = RecipeEnsurer(self.store, resolve=self.builders.resolve, build_status=self.builders.build_status,
                                     adopt=self.builders.adopt, submit=self.builders.submit, release=self.release)

    def release(self, references):
        self.calls.append(dict(references))
        return {image_id: self.answers.get(image_id, "released") for image_id in references}

    def build(self, *names):
        self.store.register([recipe(name, "sha256:" + f"{i:064x}") for i, name in enumerate(names)])
        self.ensurer.ensure(list(names))
        for build_id in list(self.builders.status):
            self.builders.finish(build_id, "succeeded")

    def test_released_once_and_kept_images_are_not_retried(self):
        self.build("t:1", "t:2")
        ids = {name: self.store.lookup([name])[name]["image_id"] for name in ("t:1", "t:2")}
        self.answers[ids["t:2"]] = "kept"  # An EROFS build.
        statuses = self.ensurer.ensure(["t:1", "t:2"])
        self.assertEqual(set(self.calls[0]), set(ids.values()))
        self.assertEqual(self.calls[0][ids["t:1"]], statuses["t:1"]["reference"])
        self.assertEqual(self.store.lookup(["t:1"])["t:1"]["oci"], "released")
        self.assertEqual(self.store.lookup(["t:2"])["t:2"]["oci"], "kept")
        self.ensurer.ensure(["t:1", "t:2"])
        self.assertEqual(len(self.calls), 1)  # Neither is offered again.

    def test_a_held_image_is_retried_and_a_rebuild_brings_its_copy_back(self):
        self.build("t:1")
        image_id = self.store.lookup(["t:1"])["t:1"]["image_id"]
        self.answers[image_id] = "pending"  # A lease or a route still reads the manifest.
        self.ensurer.ensure(["t:1"])
        self.answers.pop(image_id)
        self.ensurer.ensure(["t:1"])
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.store.lookup(["t:1"])["t:1"]["oci"], "released")
        self.builders.adopted.clear()  # Gone (evicted); rebuilt.
        self.ensurer.ensure(["t:1"])
        self.assertEqual(self.store.lookup(["t:1"])["t:1"]["oci"], "present")


PREPARED = "ucloud-managed/precomputed-abc@sha256:" + "d" * 64


class ImageIndexTests(unittest.TestCase):
    """The index of training names: prepared images, recipes, their tasks and states."""

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = ImageRecipeStore(Path(self.temp.name) / "image-recipes.sqlite3")
        self.builders, self.present = Builders(), {PREPARED}
        self.ensurer = RecipeEnsurer(self.store, resolve=self.builders.resolve, build_status=self.builders.build_status,
                                     adopt=self.builders.adopt, submit=self.builders.submit,
                                     resolve_prepared=lambda ref: f"registry/{ref}" if ref in self.present else None)

    def prepared(self, name, reference=PREPARED, **extra):
        return validate_recipe({"name": name, "prepared_reference": reference, **extra})

    def test_a_prepared_name_is_ready_without_a_build(self):
        self.store.register([self.prepared("aweaiteam/scaleswe:x_pr1", environment="scaleswe",
                                           tasks=["x__1", "x__2"], source={"dataset": "PrimeIntellect/Scale-SWE"})])
        status = self.ensurer.ensure(["aweaiteam/scaleswe:x_pr1"])["aweaiteam/scaleswe:x_pr1"]
        self.assertEqual(status["state"], "ready")
        self.assertEqual(status["reference"], f"registry/{PREPARED}")
        self.assertEqual(self.builders.submitted, [])
        detail = self.store.detail("aweaiteam/scaleswe:x_pr1")
        self.assertEqual((detail["kind"], detail["state"], detail["prepared_reference"], detail["tasks"]),
                         ("prepared", "ready", PREPARED, 2))
        self.assertEqual(detail["source"], {"dataset": "PrimeIntellect/Scale-SWE"})

    def test_a_prepared_image_the_chunk_store_lost_fails_and_leaves_the_allowlist(self):
        self.store.register([self.prepared("a:1", environment="scaleswe", tasks=["t1"])])
        self.present.clear()
        self.assertEqual(self.ensurer.ensure(["a:1"])["a:1"]["state"], "failed")
        self.assertEqual(self.store.task_ids("scaleswe"), ([], {"failed": 1}))
        self.present.add(PREPARED)  # Back (restored): ready again.
        self.assertEqual(self.ensurer.ensure(["a:1"])["a:1"]["state"], "ready")
        self.assertEqual(self.store.task_ids("scaleswe"), (["t1"], {}))

    def test_validation_of_prepared_names_and_index_fields(self):
        for bad in ({"name": "x", "prepared_reference": "10.0.0.1:5000/" + PREPARED},  # A host moves; never stored.
                    {"name": "x", "prepared_reference": PREPARED, "dockerfile": "Dockerfile"},
                    {"name": "x", "prepared_reference": "ucloud-managed/x:latest"},
                    {"name": "x", "prepared_reference": PREPARED, "tasks": ["t"]},  # Tasks need an environment.
                    {"name": "x", "prepared_reference": PREPARED, "environment": "Has Spaces"},
                    {"name": "x", "prepared_reference": PREPARED, "environment": "e", "tasks": [""]}):
            with self.subTest(bad=bad), self.assertRaises(RecipeError):
                validate_recipe(bad)

    def test_summary_names_and_task_ids_follow_each_names_state(self):
        self.store.register([
            self.prepared("p:1", environment="scaleswe", tasks=["s1"]),
            recipe("tmax:1", "sha256:" + "1" * 64, environment="tmax", tasks=["task_1"]),
            recipe("tmax:2", "sha256:" + "2" * 64, environment="tmax", tasks=["task_2"]),
            recipe("tmax:3", "sha256:" + "3" * 64, environment="tmax", tasks=["task_3"]),
        ])
        broken = self.store.lookup(["tmax:3"])["tmax:3"]["image_id"]
        for _ in range(FAILURE_ATTEMPTS):  # Recipe rot: failed for good.
            self.store.record(broken, "failed", error="step 2 exited 1", attempt=True)
        retrying = self.store.lookup(["tmax:2"])["tmax:2"]["image_id"]
        self.store.record(retrying, "failed", error="network", attempt=True)
        summary = self.store.summary()
        self.assertEqual(summary["scaleswe"]["states"]["ready"], 1)
        self.assertEqual((summary["tmax"]["names"], summary["tmax"]["recipes"], summary["tmax"]["tasks"]), (3, 3, 3))
        self.assertEqual({k: v for k, v in summary["tmax"]["states"].items() if v},
                         {"not_built": 1, "retrying": 1, "failed": 1})
        self.assertEqual(self.store.task_ids("tmax"), (["task_1", "task_2"], {"failed": 1}))
        self.assertEqual([row["name"] for row in self.store.names(environment="tmax", state="failed")[0]], ["tmax:3"])
        page, cursor = self.store.names(limit=2)
        self.assertEqual(([row["name"] for row in page], cursor), (["p:1", "tmax:1"], "tmax:1"))
        self.assertEqual([row["name"] for row in self.store.names(after=cursor)[0]], ["tmax:2", "tmax:3"])
        self.assertEqual(self.store.detail("tmax:3")["error"], "step 2 exited 1")

    def test_reregistering_replaces_a_names_tasks_and_one_image_lists_its_other_names(self):
        self.store.register([self.prepared("a:1", environment="swesmith", tasks=["t1", "t2"]),
                             self.prepared("b:1", environment="swesmith", tasks=["t3"])])
        self.store.register([self.prepared("a:1", environment="swesmith", tasks=["t2", "t4"])])
        self.assertEqual(self.store.task_ids("swesmith")[0], ["t2", "t3", "t4"])
        self.assertEqual(self.store.detail("a:1")["other_names"], ["b:1"])
        self.assertIsNone(self.store.detail("nobody:1"))


class PinnedReferenceTests(unittest.TestCase):
    def test_a_rebuilt_pinned_image_moves_its_reference(self):
        # Rebuilding a pinned recipe names another digest under the same tag:
        # the reference moves (it refused, "digest is immutable", and ensure
        # answered 503 for every name in the call).
        from tempfile import TemporaryDirectory
        from ucloud_sandboxes.gateway.registry_refs import RegistryReferences
        from ucloud_sandboxes.managed_registry import RegistryUsageStore
        with TemporaryDirectory() as directory:
            store = RegistryUsageStore(Path(directory) / "usage.sqlite")
            refs = RegistryReferences(registry_url="http://10.0.0.1:5000", registry_worker_url="http://10.0.0.1:5000",
                                      usage_store=store, deployment_id="d", dependency_resolver=None)
            tag = "10.0.0.1:5000/ucloud-managed/recipe-x-abc:latest"
            old, new = "sha256:" + "a" * 64, "sha256:" + "b" * 64
            refs.ensure_image_reference(f"{tag}@{old}", "image-recipe-pinned:recipe-x")
            refs.ensure_image_reference(f"{tag}@{new}", "image-recipe-pinned:recipe-x")
            lease = store.get_lease("ucloud-managed/recipe-x-abc", "latest", "image-recipe-pinned:recipe-x")
            self.assertEqual((lease.digest, lease.expires_at), (new, ""))
            refs.ensure_image_reference(f"{tag}@{new}", "image-recipe-pinned:recipe-x")  # Unchanged: a no-op.
