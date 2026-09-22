import os
import signal
import subprocess
import sys
import unittest
from unittest.mock import patch

from ucloud_sandboxes.direct_warden import SubprocessCommandRunner


@unittest.skipUnless(sys.platform == "linux" and hasattr(os, "pidfd_open"), "Linux pidfd integration")
class SubprocessCommandRunnerTests(unittest.TestCase):
    def test_exit_and_output_without_waitpid_polling_sleeps(self):
        with patch("subprocess.time.sleep", side_effect=AssertionError("busy wait")):
            result = SubprocessCommandRunner().run(
                (sys.executable, "-c", "import sys; print('out'); print('err',file=sys.stderr); sys.exit(7)"),
                timeout=5,
            )
        self.assertEqual((result.returncode, result.stdout, result.stderr), (7, "out\n", "err\n"))

    def test_timeout_reaps_the_command(self):
        children = []
        real_popen = subprocess.Popen

        def launch(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            children.append(process)
            return process

        with patch("ucloud_sandboxes.direct_warden.subprocess.Popen", side_effect=launch):
            with self.assertRaises(subprocess.TimeoutExpired):
                SubprocessCommandRunner().run(
                    (sys.executable, "-c", "import time; time.sleep(30)"), timeout=0.05,
                )
        self.assertEqual(children[0].returncode, -signal.SIGKILL)

    def test_daemon_inheriting_output_does_not_block_parent_completion(self):
        code = (
            "import subprocess,sys; "
            "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
            "print(p.pid,flush=True)"
        )
        result = SubprocessCommandRunner().run((sys.executable, "-c", code), timeout=5)
        child = int(result.stdout.strip())
        descriptor = os.pidfd_open(child)
        try:
            self.assertEqual(result.returncode, 0)
            signal.pidfd_send_signal(descriptor, signal.SIGKILL)
        finally:
            os.close(descriptor)

    def test_unavailable_pidfd_retains_timeout_fallback(self):
        with patch("os.pidfd_open", side_effect=OSError("unavailable")):
            result = SubprocessCommandRunner().run((sys.executable, "-c", "print(42)"), timeout=5)
        self.assertEqual(result.stdout, "42\n")
