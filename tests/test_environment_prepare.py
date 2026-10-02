import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from ucloud_sandboxes import environment_prepare as prepare
from ucloud_sandboxes.managed_registry import RegistryClient
from ucloud_sandboxes.oci_layer_materialize import UnsupportedLayer

TEST_TIER = "contract"


class PreparationProtocolTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "scratch"
        self.root.mkdir(mode=0o700)
        self.client = RegistryClient("http://registry.example", timeout_seconds=12)
        self.layers = [{"digest": "sha256:" + "a" * 64, "size": 123,
                        "mediaType": "application/vnd.oci.image.layer.v1.tar"}]
        self.diff_ids = ["sha256:" + "b" * 64]
        self.process = MagicMock()
        self.process.__enter__.return_value = self.process
        self.process.returncode = 0

    def invoke(self, **kwargs):
        return prepare.prepare_in_subprocess(self.client, "owned/image", self.layers,
            self.diff_ids, [1], kwargs.pop("root", self.root), **kwargs)

    def response(self, *, status="ok", groups=1, metrics=None):
        return {"status": status, "groups": groups,
                "metrics": {"selective_materialization_ms": 10.25, "squash_ms": 3.5}
                if metrics is None else metrics}

    def write_result(self, value):
        path = self.root / prepare.RESULT_NAME
        path.write_text(json.dumps(value))
        path.chmod(0o600)

    def test_fresh_exec_sends_only_bounded_metadata_and_derives_views_and_metrics(self):
        self.layers[0]["annotations"] = {"private": "must not cross protocol"}
        def communicate(data, *, timeout):
            request = json.loads(data)
            self.assertEqual(timeout, 600)
            self.assertEqual(request["group_counts"], [1])
            self.assertEqual(request["diff_ids"], self.diff_ids)
            self.assertEqual(set(request["layers"][0]), {"digest", "size", "mediaType"})
            self.assertEqual(request["root"], str(self.root.resolve()))
            self.assertLess(len(data), prepare.MAX_REQUEST_BYTES)
            (self.root / "view-0").mkdir()
            self.write_result(self.response())
        self.process.communicate.side_effect = communicate
        with patch.object(prepare.subprocess, "Popen", return_value=self.process) as launch:
            result = self.invoke()
        self.assertEqual(launch.call_args.args[0], (prepare.sys.executable, "-m", prepare.__name__))
        self.assertIs(launch.call_args.kwargs["stdout"], subprocess.DEVNULL)
        self.assertIs(launch.call_args.kwargs["stderr"], subprocess.DEVNULL)
        self.assertTrue(launch.call_args.kwargs["close_fds"])
        self.assertEqual(launch.call_args.kwargs["cwd"], Path(prepare.__file__).resolve().parent.parent)
        self.assertFalse(result.fallback)
        self.assertEqual(result.views, (self.root / "view-0",))
        self.assertEqual(result.metrics["squash_ms"], 3.5)
        self.assertGreaterEqual(result.metrics["selective_subprocess_ms"], 0)

    def test_parent_symlink_is_canonicalized_but_scratch_symlink_is_rejected(self):
        parent_alias = self.root.parent / "alias"
        parent_alias.symlink_to(self.root.parent, target_is_directory=True)
        def communicate(data, *, timeout):
            self.assertEqual(json.loads(data)["root"], str(self.root))
            self.write_result(self.response(status="fallback", groups=0))
        self.process.communicate.side_effect = communicate
        with patch.object(prepare.subprocess, "Popen", return_value=self.process):
            self.assertTrue(self.invoke(root=parent_alias / "scratch").fallback)
        scratch_alias = self.root.parent / "scratch-alias"
        scratch_alias.symlink_to(self.root, target_is_directory=True)
        with patch.object(prepare.subprocess, "Popen") as launch:
            with self.assertRaises(ValueError):
                self.invoke(root=scratch_alias)
            launch.assert_not_called()

    def test_timeout_and_interruption_kill_and_reap_before_returning(self):
        for failure in (subprocess.TimeoutExpired("private command", 0.01), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__):
                self.process.reset_mock()
                self.process.communicate.side_effect = failure
                self.process.poll.return_value = None
                expected = prepare.PreparationError if isinstance(failure, subprocess.TimeoutExpired) else KeyboardInterrupt
                with patch.object(prepare.subprocess, "Popen", return_value=self.process):
                    with self.assertRaises(expected) as caught:
                        self.invoke(timeout_seconds=0.01)
                self.process.kill.assert_called_once_with()
                self.process.wait.assert_called_once_with()
                self.assertLess(self.process.mock_calls.index(unittest.mock.call.kill()),
                                self.process.mock_calls.index(unittest.mock.call.wait()))
                self.assertNotIn("private command", str(caught.exception))

    def test_result_schema_metrics_and_view_identity_fail_closed(self):
        cases = [self.response(groups=2), self.response(status="error", groups=0),
                 self.response(metrics={"selective_materialization_ms": float("nan"), "squash_ms": 0}),
                 self.response(metrics={"selective_materialization_ms": 1, "squash_ms": True}),
                 self.response(metrics={"selective_materialization_ms": 1, "squash_ms": 0, "secret": 1})]
        for value in cases:
            with self.subTest(case=value):
                self.process.communicate.side_effect = lambda *a, **k: self.write_result(value)
                with patch.object(prepare.subprocess, "Popen", return_value=self.process):
                    with self.assertRaises(prepare.PreparationError):
                        self.invoke()
                (self.root / prepare.RESULT_NAME).unlink()
        def symlink_view(*args, **kwargs):
            self.write_result(self.response())
            (self.root / "view-0").symlink_to(self.root.parent, target_is_directory=True)
        self.process.communicate.side_effect = symlink_view
        with patch.object(prepare.subprocess, "Popen", return_value=self.process):
            with self.assertRaises(prepare.PreparationError):
                self.invoke()

    def test_result_symlink_fifo_and_oversize_cannot_be_read_as_response(self):
        for kind in ("symlink", "fifo", "oversize"):
            with self.subTest(kind=kind):
                def communicate(*args, **kwargs):
                    result = self.root / prepare.RESULT_NAME
                    if kind == "symlink":
                        result.symlink_to(self.root.parent / "not-a-result")
                    elif kind == "fifo":
                        os.mkfifo(result, 0o600)
                    else:
                        result.write_bytes(b" " * (prepare.MAX_RESULT_BYTES + 1))
                        result.chmod(0o600)
                self.process.communicate.side_effect = communicate
                with patch.object(prepare.subprocess, "Popen", return_value=self.process):
                    with self.assertRaises(prepare.PreparationError):
                        self.invoke()
                (self.root / prepare.RESULT_NAME).unlink()

    def test_request_binding_and_private_root_checked_before_spawn(self):
        with patch.object(prepare.subprocess, "Popen") as launch:
            self.root.chmod(0o755)
            with self.assertRaises(ValueError):
                self.invoke()
            self.root.chmod(0o700)
            self.layers[0]["size"] = prepare.MAX_COMPRESSED_BYTES + 1
            with self.assertRaises(UnsupportedLayer):
                self.invoke()
            self.layers[0]["size"] = 1
            self.diff_ids.clear()
            with self.assertRaises(ValueError):
                self.invoke()
            launch.assert_not_called()

    def test_spawn_failure_is_terminal_and_does_not_expose_os_error_detail(self):
        with patch.object(prepare.subprocess, "Popen", side_effect=OSError("sensitive detail")):
            with self.assertRaises(prepare.PreparationError) as caught:
                self.invoke()
        self.assertNotIn("sensitive detail", str(caught.exception))

    def test_fallback_reason_survives_child_protocol_without_exception_text(self):
        def communicate(*args, **kwargs):
            self.write_result({**self.response(status="fallback", groups=0),
                               "fallback_reason": "parent_context"})
        self.process.communicate.side_effect = communicate
        with patch.object(prepare.subprocess, "Popen", return_value=self.process):
            result = self.invoke()
        self.assertTrue(result.fallback)
        self.assertEqual(result.fallback_reason, "parent_context")

    def test_unknown_or_misplaced_fallback_reason_fails_closed(self):
        for status, reason in (("fallback", "private exception text"),
                               ("fallback", []), ("ok", "parent_context")):
            with self.subTest(status=status, reason=reason):
                self.process.communicate.side_effect = lambda *a, **k: self.write_result(
                    {**self.response(status=status, groups=0 if status == "fallback" else 1),
                     "fallback_reason": reason})
                with patch.object(prepare.subprocess, "Popen", return_value=self.process):
                    with self.assertRaises(prepare.PreparationError):
                        self.invoke()
                (self.root / prepare.RESULT_NAME).unlink()

    def test_compressed_budget_reason_is_available_before_child_spawn(self):
        self.layers[0]["size"] = prepare.MAX_COMPRESSED_BYTES + 1
        with patch.object(prepare.subprocess, "Popen") as launch:
            with self.assertRaises(UnsupportedLayer) as caught:
                self.invoke()
        self.assertEqual(caught.exception.reason, "compressed_budget")
        launch.assert_not_called()

    def test_streaming_metrics_allow_bytes_above_time_limit_but_remain_bounded(self):
        def communicate(*args, **kwargs):
            (self.root / "view-0").mkdir(exist_ok=True)
            self.write_result(self.response(metrics={
                "selective_materialization_ms": 40, "squash_ms": 2,
                "oci_transfer_ms": 10, "oci_decompress_ms": 20, "oci_extract_ms": 10,
                "oci_download_bytes_actual": prepare.MAX_COMPRESSED_BYTES,
            }))
        self.process.communicate.side_effect = communicate
        with patch.object(prepare.subprocess, "Popen", return_value=self.process):
            result = self.invoke()
        self.assertEqual(result.metrics["oci_download_bytes_actual"], prepare.MAX_COMPRESSED_BYTES)
        value = self.response(metrics={"selective_materialization_ms": 1, "squash_ms": 0,
                                       "oci_download_bytes_actual": prepare.MAX_COMPRESSED_BYTES + 2})
        self.write_result(value)
        with self.assertRaisesRegex(ValueError, "invalid preparation metrics"):
            prepare._read_result(self.root, 1, 1)
