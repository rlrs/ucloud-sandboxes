from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

TEST_TIER = "contract"


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/configure_hetzner_sdk_ingress.sh"


def capacity_renderer():
    return SCRIPT.read_text().split("<<'PY_NGINX_CAPACITY'\n", 1)[1].split(
        "\nPY_NGINX_CAPACITY", 1
    )[0]


class NginxCapacityTests(unittest.TestCase):
    def install(self, root, *, valid):
        body = SCRIPT.read_text().split("install_nginx_site() {\n", 1)[1].split(
            '\n}\n\ncat >"$temporary_site"', 1
        )[0]
        code = '''set -euo pipefail
nginx_main="$1/main.conf"
nginx_site="$1/site.conf"
nginx_enabled="$1/enabled.conf"
temporary_main="$1/candidate-main.conf"
temporary_site="$1/candidate-site.conf"
previous_site="$1/previous-site.conf"
nginx() { return "$2"; }
'''.replace('nginx() { return "$2"; }', f"nginx() {{ return {0 if valid else 1}; }}")
        code += "install_nginx_site() {\n" + body + "\n}\ninstall_nginx_site\n"
        result = subprocess.run(["bash", "-c", code, "fixture", str(root)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode == 0, valid, result.stderr)

    def test_installer_validates_backs_up_and_is_idempotent(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            original = "events { worker_connections 768; }\nhttp {}\n"
            (root / "main.conf").write_text(original)
            (root / "site.conf").write_text("previous site")
            (root / "candidate-site.conf").write_text("new site")
            self.install(root, valid=True)
            self.assertIn("worker_connections 4096", (root / "main.conf").read_text())
            self.assertIn("worker_rlimit_nofile 65536", (root / "main.conf").read_text())
            self.assertEqual((root / "site.conf").read_text(), "new site")
            self.assertEqual(next(root.glob("main.conf.ucloud-backup.*")).read_text(), original)
            self.assertEqual(next(root.glob("site.conf.ucloud-backup.*")).read_text(), "previous site")
            self.install(root, valid=True)
            self.assertEqual(len(list(root.glob("main.conf.ucloud-backup.*"))), 1)
            self.assertEqual(len(list(root.glob("site.conf.ucloud-backup.*"))), 1)

    def test_failed_validation_restores_site_and_never_changes_main(self):
        for existing_site in (False, True):
            with self.subTest(existing_site=existing_site), TemporaryDirectory() as raw:
                root = Path(raw)
                original = "events { worker_connections 768; }\nhttp {}\n"
                (root / "main.conf").write_text(original)
                (root / "candidate-site.conf").write_text("invalid new site")
                if existing_site:
                    (root / "site.conf").write_text("previous site")
                self.install(root, valid=False)
                self.assertEqual((root / "main.conf").read_text(), original)
                self.assertEqual(list(root.glob("main.conf.ucloud-backup.*")), [])
                if existing_site:
                    self.assertEqual((root / "site.conf").read_text(), "previous site")
                else:
                    self.assertFalse((root / "site.conf").exists())
                    self.assertFalse((root / "enabled.conf").is_symlink())

    def render(self, source, *, succeeds=True):
        with TemporaryDirectory() as raw:
            original, candidate = Path(raw) / "original.conf", Path(raw) / "candidate.conf"
            original.write_text(source)
            result = subprocess.run(
                [sys.executable, "-", str(original), str(candidate)],
                input=capacity_renderer(), capture_output=True, text=True,
            )
            self.assertEqual(original.read_text(), source)
            self.assertEqual(result.returncode == 0, succeeds, result.stderr)
            if succeeds:
                return candidate.read_text()
            self.assertFalse(candidate.exists())

    def test_capacity_edit_preserves_comments_quoted_text_and_other_directives(self):
        source = '''# worker_rlimit_nofile 123;
worker_processes auto;
worker_rlimit_nofile 1024; # custom comment
events { worker_connections 768; multi_accept on; }
http {
    log_format fixture 'events { worker_connections 5; } # text';
    keepalive_timeout 30;
    include /etc/nginx/sites-enabled/*;
}
'''
        expected = source.replace("worker_rlimit_nofile 1024", "worker_rlimit_nofile 65536")
        expected = expected.replace("worker_connections 768", "worker_connections 4096")
        self.assertEqual(self.render(source), expected)
        self.assertEqual(self.render(expected), expected)

    def test_missing_directives_are_added_and_larger_limits_are_retained(self):
        source = "worker_processes auto;\nevents { }\nhttp { }\n"
        candidate = self.render(source)
        self.assertIn("worker_connections 4096;", candidate)
        self.assertIn("worker_rlimit_nofile 65536;", candidate)
        self.assertEqual(self.render(candidate), candidate)
        larger = "worker_rlimit_nofile 131072;\nevents { worker_connections 8192; }\n"
        self.assertEqual(self.render(larger), larger)

    def test_ambiguous_or_incomplete_config_does_not_produce_a_candidate(self):
        for source in (
            "http {}",
            "events { worker_connections 1; worker_connections 2; }",
            "events {} events {}",
            "events { worker_connections 768;",
            "events {} worker_rlimit_nofile unexpected;",
        ):
            with self.subTest(source=source):
                self.render(source, succeeds=False)


if __name__ == "__main__":
    unittest.main()
