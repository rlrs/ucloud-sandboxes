import os
import json
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from runtime.gvisor import compatibility_workload as workload


class GvisorCompatibilityWorkloadTests(unittest.TestCase):
    def test_detached_guest_error_is_reported_and_reraised(self):
        client = Mock()
        with self.assertRaisesRegex(AssertionError, "inherited group mismatch"):
            with workload.report_guest_errors(client):
                raise AssertionError("inherited group mismatch")
        client.sendall.assert_called_once()
        payload = json.loads(client.sendall.call_args.args[0])
        self.assertEqual(
            payload["guest_error"], "AssertionError('inherited group mismatch')"
        )
        self.assertIn("AssertionError: inherited group mismatch", payload["traceback"])

    def test_identity_helper_reports_child_failure_and_signal_exit(self):
        # Validate helper failure propagation without requiring host privileges.
        with patch.object(workload.os, "setgroups"), patch.object(
            workload.os, "setgid"
        ), patch.object(workload.os, "setuid"):
            workload.as_user(os.getuid(), lambda: None)

            def fail():
                raise RuntimeError("cross-user write denied")

            with self.assertRaisesRegex(AssertionError, "cross-user write denied"):
                workload.as_user(os.getuid(), fail)
            with self.assertRaisesRegex(AssertionError, "exited with status"):
                workload.as_user(os.getuid(), lambda: os.kill(os.getpid(), signal.SIGTERM))

    @unittest.skipUnless(sys.platform == "linux" and os.geteuid() == 0, "requires Linux root ACL semantics")
    def test_acl_matrix_matches_native_linux_and_detects_premasked_creation(self):
        with tempfile.TemporaryDirectory(prefix="gvisor-acl-matrix-") as directory:
            root = Path(directory)
            root.chmod(0o755)
            workload.prepare_acl_inheritance_root(root)
            workload.qualify_acl_umask(root, 1)
            original = workload.create_file

            def prematurely_masked(path, mode, content="created"):
                mask = os.umask(0)
                os.umask(mask)
                return original(path, mode & ~mask, content)

            with patch.object(workload, "create_file", side_effect=prematurely_masked):
                with self.assertRaisesRegex(AssertionError, "ACL identity"):
                    workload.qualify_acl_umask(root, 2)


if __name__ == "__main__":
    unittest.main()
