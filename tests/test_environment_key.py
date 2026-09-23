from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.environment_producer_key import provision


class EnvironmentProducerKeyTests(unittest.TestCase):
    def test_repeated_provision_recovers_same_key_without_rotating(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "producer"
            first = provision(root)
            private = Path(first["private_key_file"])
            original = private.read_bytes()
            self.assertEqual(private.stat().st_mode & 0o777, 0o600)
            self.assertEqual(provision(root), first)
            self.assertEqual(private.read_bytes(), original)
            private.unlink()
            with self.assertRaisesRegex(ValueError, "do not silently rotate"):
                provision(root)
            self.assertFalse(private.exists())

    def test_distrusts_public_mismatch_private_permissions_and_symlink(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "producer"
            result = provision(root)
            private, public = Path(result["private_key_file"]), Path(result["public_trust_file"])
            private.chmod(0o640)
            with self.assertRaisesRegex(ValueError, "ownership/mode"):
                provision(root)
            private.chmod(0o600)
            real = private.with_suffix(".saved")
            private.rename(real)
            private.symlink_to(real)
            with self.assertRaises(OSError):
                provision(root)
            private.unlink()
            real.rename(private)
            other = provision(Path(temporary) / "other")
            public.write_bytes(Path(other["public_trust_file"]).read_bytes())
            with self.assertRaisesRegex(ValueError, "explicit key rotation"):
                provision(root)


if __name__ == "__main__":
    unittest.main()
