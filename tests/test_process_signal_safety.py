"""Exercise process fencing without sending signals to system processes."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

from ucloud_sandboxes.direct_service import DirectProcessRunner
from ucloud_sandboxes.direct_warden import (
    DirectWardenError,
    LinuxPidfdFencer,
    LinuxPidfdHandle,
)


class ProcessFenceSignalTests(unittest.TestCase):
    def test_stale_identity_never_opens_a_pidfd(self):
        with (
            patch(
                "ucloud_sandboxes.direct_warden.hibernation_process_identity_matches",
                return_value=False,
            ),
            patch.object(os, "pidfd_open", create=True) as open_pidfd,
        ):
            with self.assertRaisesRegex(DirectWardenError, "before fencing"):
                LinuxPidfdFencer().open(4242, 123)
            open_pidfd.assert_not_called()

    def test_identity_change_while_opening_closes_fence(self):
        with (
            patch(
                "ucloud_sandboxes.direct_warden.hibernation_process_identity_matches",
                side_effect=[True, False],
            ),
            patch.object(os, "pidfd_open", return_value=99, create=True),
            patch.object(os, "close") as close,
        ):
            with self.assertRaisesRegex(DirectWardenError, "while fencing"):
                LinuxPidfdFencer().open(4242, 123)
            close.assert_called_once_with(99)

    def test_live_fence_sends_only_sigkill_to_exact_descriptor(self):
        poller = Mock()
        poller.poll.side_effect = [[], [(99, 1)]]
        handle = LinuxPidfdHandle(4242, 123, 99, proc_root=Path("/proc"))
        with (
            patch("ucloud_sandboxes.direct_warden.select.poll", return_value=poller),
            patch(
                "ucloud_sandboxes.direct_warden.hibernation_process_identity_matches",
                return_value=True,
            ),
            patch.object(signal, "pidfd_send_signal", create=True) as send,
            patch.object(os, "kill") as kill,
            patch.object(os, "killpg") as killpg,
        ):
            handle.terminate(timeout=1)
            send.assert_called_once_with(99, signal.SIGKILL, None, 0)
            kill.assert_not_called()
            killpg.assert_not_called()

    def test_exited_fence_never_signals_a_recycled_numeric_pid(self):
        poller = Mock()
        poller.poll.return_value = [(99, 1)]
        handle = LinuxPidfdHandle(4242, 123, 99, proc_root=Path("/proc"))
        with (
            patch("ucloud_sandboxes.direct_warden.select.poll", return_value=poller),
            patch.object(signal, "pidfd_send_signal", create=True) as send,
        ):
            handle.terminate(timeout=1)
            send.assert_not_called()

    def test_unavailable_pidfd_signal_has_no_numeric_kill_fallback(self):
        handle = LinuxPidfdHandle(4242, 123, 99, proc_root=Path("/proc"))
        with (
            patch.object(signal, "pidfd_send_signal", None, create=True),
            patch.object(os, "kill") as kill,
            patch.object(os, "killpg") as killpg,
        ):
            with self.assertRaisesRegex(DirectWardenError, "required"):
                handle.terminate(timeout=1)
            kill.assert_not_called()
            killpg.assert_not_called()

    def test_closed_fence_never_sends_signal(self):
        handle = LinuxPidfdHandle(4242, 123, 99, proc_root=Path("/proc"))
        with patch.object(os, "close"):
            handle.close()
        with patch.object(signal, "pidfd_send_signal", create=True) as send:
            with self.assertRaisesRegex(DirectWardenError, "closed"):
                handle.terminate(timeout=1)
            send.assert_not_called()


@unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
class ExecCleanupSignalTests(unittest.TestCase):
    def test_timeout_kills_only_the_new_child_process_group(self):
        # The sibling shares the test runner's group. A group-zero or parent
        # group mistake is caught before any real signal is sent.
        with subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ) as sibling:
            killpg = os.killpg
            targets = []

            def checked_killpg(pgid, sig):
                self.assertGreater(pgid, 1)
                self.assertNotEqual(pgid, os.getpgrp())
                self.assertNotEqual(pgid, os.getpgid(sibling.pid))
                self.assertEqual(os.getsid(pgid), pgid)
                self.assertEqual(os.getpgid(pgid), pgid)
                self.assertEqual(sig, signal.SIGKILL)
                targets.append(pgid)
                killpg(pgid, sig)

            try:
                with patch.object(os, "killpg", side_effect=checked_killpg):
                    with self.assertRaisesRegex(DirectWardenError, "timed out"):
                        DirectProcessRunner().run(
                            [sys.executable, "-c", "import time; time.sleep(5)"],
                            input_bytes=None,
                            timeout_seconds=0.2,
                            max_stdout_bytes=1024,
                            max_stderr_bytes=1024,
                        )
                self.assertEqual(len(targets), 1)
                self.assertIsNone(sibling.poll())
            finally:
                sibling.terminate()
                sibling.wait(timeout=5)
