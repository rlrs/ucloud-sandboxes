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
