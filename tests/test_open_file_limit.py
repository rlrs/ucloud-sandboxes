import resource
import unittest
from unittest.mock import patch

from ucloud_sandboxes.cli import raise_open_file_limit


class OpenFileLimitTests(unittest.TestCase):
    def test_raises_soft_limit_within_hard_limit(self):
        with patch.object(resource, "getrlimit", return_value=(1024, 524288)), \
                patch.object(resource, "setrlimit") as setrlimit:
            self.assertEqual(raise_open_file_limit(65536), 65536)
        setrlimit.assert_called_once_with(resource.RLIMIT_NOFILE, (65536, 524288))

    def test_caps_at_hard_limit_and_never_lowers(self):
        with patch.object(resource, "getrlimit", return_value=(1024, 4096)), \
                patch.object(resource, "setrlimit") as setrlimit:
            self.assertEqual(raise_open_file_limit(65536), 4096)
        setrlimit.assert_called_once_with(resource.RLIMIT_NOFILE, (4096, 4096))
        with patch.object(resource, "getrlimit", return_value=(100000, 524288)), \
                patch.object(resource, "setrlimit") as setrlimit:
            self.assertEqual(raise_open_file_limit(65536), 100000)
        setrlimit.assert_not_called()

    def test_refused_raise_keeps_current_limit(self):
        with patch.object(resource, "getrlimit", return_value=(1024, 524288)), \
                patch.object(resource, "setrlimit", side_effect=ValueError("denied")):
            self.assertEqual(raise_open_file_limit(65536), 1024)


if __name__ == "__main__":
    unittest.main()
