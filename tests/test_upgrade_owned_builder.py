import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "owned_builder_upgrade", Path(__file__).parents[1] / "scripts/upgrade_owned_builder.py",
)
upgrade = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(upgrade)


class OwnedBuilderUpgradeTests(unittest.TestCase):
    def test_literal_systemd_arguments_and_existing_flag(self):
        self.assertEqual(upgrade.finishing_args(["python", "--max-finishing-image-builds", "0"]),
                         ["python", "--max-finishing-image-builds", "2"])
        self.assertEqual(upgrade.systemd_argument("a $b %i \\\""), '"a $$b %%i \\\\\\\""')
        with self.assertRaises(ValueError):
            upgrade.systemd_argument("bad\nargument")

    def exercise_failure(self, kind):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            override = root / "unit.d/99-owned.conf"
            source, target = root / "old", root / "candidate"
            state = {"job_id": "123", "node_epoch": "epoch", "source": source,
                     "unit": root / "unit", "argv": ["python", "-m", "ucloud_sandboxes.cli", "serve-builder-agent"]}
            before = {"draining": False, "admission_open": True, "unit_sha256": "a" * 64}
            token = None

            def request(_, path, payload=None):
                nonlocal token
                if path == "/v1/drain" and payload["draining"]:
                    token = payload["token"]
                    return {"drain": {"ready": kind != "race", "active_image_builds": int(kind == "race"),
                                      "draining": True, "admission_open": False, "token": token}}
                if path == "/v1/heartbeat":
                    return {"heartbeat": {"job_id": "123", "node_epoch": "epoch",
                                          "draining": True, "drain_token": token}}
                if kind == "reopen":
                    raise TimeoutError("response lost")
                return {}

            attempts = 0

            def ready(*args, **kwargs):
                nonlocal attempts
                attempts += 1
                if kind in {"health", "fence_lost"} and attempts == 1:
                    raise ValueError("bad candidate")
                return {"draining": True, "drain_token": token}

            def current_heartbeat(_):
                lost = kind == "fence_lost" and attempts > 0
                return {"draining": not lost, "admission_open": lost, "drain_token": token}

            with (
                patch.object(upgrade, "OVERRIDE", override),
                patch.object(upgrade, "sha", return_value="a" * 64),
                patch.object(upgrade, "receipt", return_value=before),
                patch.object(upgrade, "stage", return_value=target),
                patch.object(upgrade, "request", side_effect=request) as requests,
                patch.object(upgrade, "heartbeat", side_effect=current_heartbeat),
                patch.object(upgrade, "ready", side_effect=ready),
                patch.object(upgrade, "discover", return_value={"source": source}),
                patch.object(upgrade, "run", return_value="0") as commands,
            ):
                with self.assertRaises((ValueError, TimeoutError)):
                    upgrade.apply(state, root / "wheel", "a" * 64, root)
            result = json.loads((root / "upgrade-receipt.json").read_text())
            return result, commands.call_args_list, requests.call_args_list, override.exists()

    def test_admission_race_never_stops_service_and_removes_only_owned_fence(self):
        result, commands, requests, override_exists = self.exercise_failure("race")
        self.assertEqual(commands, [])
        self.assertTrue(result["rollback_verified"])
        self.assertFalse(override_exists)
        self.assertFalse(requests[-1].args[2]["draining"])

    def test_lost_reopen_response_never_stops_possible_new_work(self):
        result, commands, _, override_exists = self.exercise_failure("reopen")
        stops = [call for call in commands if call.args[0][:2] == ["systemctl", "stop"]]
        self.assertEqual(len(stops), 1)
        self.assertEqual(result["rollback_skipped"], "admission_reopen_attempted_preserve_possible_new_work")
        self.assertTrue(override_exists)

    def test_failed_candidate_health_restores_original_override_and_unfences(self):
        result, commands, requests, override_exists = self.exercise_failure("health")
        self.assertTrue(result["rollback_verified"])
        self.assertFalse(override_exists)
        self.assertEqual(sum(call.args[0][:2] == ["systemctl", "restart"] for call in commands), 2)
        self.assertFalse(requests[-1].args[2]["draining"])

    def test_candidate_fence_loss_never_stops_possible_new_work(self):
        result, commands, _, override_exists = self.exercise_failure("fence_lost")
        self.assertEqual(sum(call.args[0][:2] == ["systemctl", "stop"] for call in commands), 1)
        self.assertEqual(result["rollback_skipped"], "candidate_fence_or_idle_unverified")
        self.assertTrue(override_exists)


if __name__ == "__main__":
    unittest.main()
