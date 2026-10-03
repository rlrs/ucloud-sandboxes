"""The M1 gate driver (scripts/chunk_store_gate.py): command form, resume,
teardown and the report's verdicts, against a fake runner."""
import importlib.util
import io
import json
from pathlib import Path
import shlex
import sys
import tempfile
import unittest
from unittest.mock import patch

from ucloud_sandboxes import chunk_convert

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = sys.modules[name] = importlib.util.module_from_spec(spec)  # Dataclasses look the module up.
    spec.loader.exec_module(module)
    return module


gate = load("chunk_store_gate")
remote = load("chunk_store_gate_remote")
BLOCK = {"endpoint": "https://hel1.your-objectstorage.com", "bucket": "b", "region": "hel1", "prefix": "x",
         "access_key_id_env": "HETZNER_S3_ACCESS_KEY", "secret_access_key_env": "HETZNER_S3_SECRET_KEY",
         "force_path_style": False, "index_url": "http://unused", "index_listen": "0.0.0.0:8095",
         "index_database": "/var/lib/m1-gate/index.sqlite", "read_token_file": "/var/lib/m1-gate/read.token",
         "write_token_file": "/var/lib/m1-gate/write.token", "url_ttl_seconds": 86400,
         "mount_granularity": "image", "nydus_image": "nydus-image", "concurrent_misses": 16}


class FakeRunner:
    """Answers like the helpers would; ``fail`` maps a substring to an exit
    code, ``answers`` to a whole (rc, stdout, stderr)."""

    def __init__(self, fail=None, answers=None):
        self.calls, self.fail, self.answers = [], dict(fail or {}), dict(answers or {})

    def __call__(self, argv, timeout):
        assert isinstance(argv, list) and all(isinstance(item, str) for item in argv)
        self.calls.append(argv)
        joined = " ".join(argv)  # The remote command as the gateway's shell receives it.
        for needle, answer in self.answers.items():
            if needle in joined:
                return answer
        for needle, code in list(self.fail.items()):
            if needle in joined:
                return code, "", f"forced failure {code}"
        if argv[:3] == ["python3", gate.HZ, "server"]:
            return 0, json.dumps({"id": 1000 + len(self.calls), "name": argv[3]}) + "\n", ""
        if argv[:3] == ["python3", gate.HZ, "delete-server"]:
            return 0, f"deleted server {argv[3]}\n", ""
        if "cat /var/lib/m1-gate/out" in joined:
            return 0, '{"status": 0}\n', ""
        return 0, "", ""


class GateTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "block.json").write_text(json.dumps(BLOCK))
        patcher = patch.object(gate, "LEDGER", self.root / "no-ledger.json")
        patcher.start()
        self.addCleanup(patcher.stop)

    def argv(self, phase, *extra):
        return ["--phase", phase, "--run-id", "20261002t1800", "--state-dir", str(self.root / "state"),
                "--docs-dir", str(self.root / "docs"), "--chunk-store-block", str(self.root / "block.json"),
                "--bundle", "/work/release/sandbox-node-package.tar.gz", "--nydus-sha256", "1ad7b793" + "0" * 51 + "19072",
                "--distribution-sha256", "ab" * 32, "--poll-seconds", "0", *extra]

    def run_gate(self, phase, runner, *extra):
        with patch.object(gate.time, "sleep", lambda _: None), patch.object(gate.sys, "stderr", io.StringIO()):
            return gate.main(self.argv(phase, *extra), runner=runner)

    def state(self):
        return json.loads((self.root / "state" / "20261002t1800" / "state.json").read_text())


class CommandFormTests(GateTest):
    def test_every_command_is_one_plain_helper_invocation(self):
        runner = FakeRunner()
        for phase in ("provision", "configure", "convert", "crash", "workers", "rollback", "teardown"):
            self.assertEqual(self.run_gate(phase, runner, "--accept-canary-placement"), 0, phase)
        for argv in runner.calls:
            if argv[0] == gate.GW:
                self.assertEqual(len(argv), 2, argv)  # gw '<one remote command>'
            elif argv[0] == gate.GSCP:
                self.assertTrue(argv[-1].startswith(gate.GATEWAY + ":") or argv[1].startswith(gate.GATEWAY + ":"))
            else:
                self.assertEqual(argv[:2], ["python3", gate.HZ], argv)
        joined = "\n".join(" ".join(argv) for argv in runner.calls)
        self.assertNotIn("10.42.0.4", joined.replace("10.42.0.48", "").replace("10.42.0.49", ""))
        # The production registry is read once, as the mirror's source; nothing else names it.
        self.assertEqual(joined.count("10.42.0.2:5000"), joined.count("--source-url http://10.42.0.2:5000"), 1)
        self.assertNotIn("/etc/ucloud-sandboxes/deployment.json root@", joined)  # The live config is only read.
        self.assertIn("--prefix spike/m1/20261002t1800", joined)

    def test_subprocess_runner_never_uses_a_shell(self):
        with patch.object(gate.subprocess, "run") as run:
            run.return_value.returncode, run.return_value.stdout, run.return_value.stderr = 0, "", ""
            gate.SubprocessRunner()(gate.gw("true"), 10)
        self.assertEqual(run.call_args.args[0], [gate.GW, "true"])
        self.assertNotIn("shell", run.call_args.kwargs)
        self.assertEqual(run.call_args.kwargs["cwd"], gate.REPO)

    def test_host_commands_survive_the_gateway_shell(self):
        command = "echo '{\"status\": 0}' && systemctl is-active x"
        words = shlex.split(gate.on_host("10.42.0.48", command, "/key"))
        self.assertEqual(words[0], "ssh")
        self.assertEqual(words[-2:], ["root@10.42.0.48", command])
        self.assertEqual(gate.hz("server", "n", "ccx43", 438728121, "10.42.0.48", "public"),
                         ["python3", "scripts/hetzner_prod/hz.py", "server", "n", "ccx43", "438728121",
                          "10.42.0.48", "public"])

    def test_addresses_avoid_spike_sources_and_used_ips(self):
        self.assertEqual(gate.allocate_ips("10.42.0.48-10.42.0.52", 3, ["10.42.0.49"]),
                         ["10.42.0.48", "10.42.0.50", "10.42.0.51"])
        for spec in ("10.42.0.40-10.42.0.50", "10.42.0.47", "10.42.0.1-10.42.0.5"):
            with self.assertRaises(gate.GateError):
                gate.allocate_ips(spec, 1)

    def test_write_path_steps_match_the_converter(self):
        self.assertEqual(gate.STEPS, chunk_convert.STEPS)

    def test_gateway_ssh_failure_stops_at_once(self):
        runner = FakeRunner({"scripts/hetzner_prod/gw true": 255})
        self.assertEqual(self.run_gate("provision", runner), 3)
        self.assertEqual(runner.calls, [gate.gw("true"), gate.gw("true")])  # The step, then one probe.
        self.assertFalse([argv for argv in runner.calls if argv[:2] == ["python3", gate.HZ]])

    def test_secrets_are_redacted(self):
        self.assertNotIn("abc", gate.redact("https://x?X-Amz-Signature=abc HETZNER_S3_SECRET_KEY=abc"))


class ResumeAndTeardownTests(GateTest):
    def test_provision_resumes_without_recreating_servers(self):
        runner = FakeRunner({"converter ccx63": 1})
        self.assertEqual(self.run_gate("provision", runner), 1)
        servers = self.state()["resources"]["servers"]
        self.assertTrue(servers["sandboxes-m1-20261002t1800-store"]["id"])
        self.assertIsNone(servers["sandboxes-m1-20261002t1800-converter"]["id"])  # Recorded before it exists.
        runner.fail.clear()
        self.assertEqual(self.run_gate("provision", runner), 0)
        creates = [argv[3] for argv in runner.calls if argv[:3] == ["python3", gate.HZ, "server"]]
        self.assertEqual(creates.count("sandboxes-m1-20261002t1800-store"), 1)
        self.assertEqual(creates.count("sandboxes-m1-20261002t1800-converter"), 2)
        self.assertEqual(self.state()["phases"]["provision"]["status"], "done")

    def test_teardown_after_a_crash_removes_everything_it_recorded(self):
        self.run_gate("provision", FakeRunner({"converter ccx63": 1}))  # Converter recorded, never created.
        name = "sandboxes-m1-20261002t1800-converter"
        runner = FakeRunner(answers={f"delete-server {name}": (1, "", f"unknown server {name}")})
        self.assertEqual(self.run_gate("teardown", runner), 0)
        resources = self.state()["resources"]
        self.assertEqual(resources, {"servers": {}, "staging": None, "known_hosts": [], "s3_prefix": None})
        joined = "\n".join(" ".join(argv) for argv in runner.calls)
        self.assertIn("rm -rf --one-file-system /var/tmp/m1-gate-20261002t1800", joined)
        self.assertIn("-R 10.42.0.48", joined)
        self.assertFalse((self.root / "state" / "current").exists())

    def test_teardown_drains_workers_first_and_keeps_one_it_cannot_drain(self):
        for phase in ("provision", "configure", "convert"):
            self.assertEqual(self.run_gate(phase, FakeRunner()), 0)
        self.run_gate("workers", FakeRunner({"bench --kind seq": 1}), "--accept-canary-placement")
        runner = FakeRunner({"root@10.42.0.51 '/usr/bin/python3 /opt/m1-gate/chunk_store_gate_remote.py drain": 1})
        self.assertEqual(self.run_gate("teardown", runner), 1)
        servers = self.state()["resources"]["servers"]
        self.assertEqual(list(servers), ["sandboxes-m1-20261002t1800-w2"])
        calls = [shlex.join(argv) for argv in runner.calls]
        drain = next(i for i, call in enumerate(calls) if "10.42.0.50" in call and " drain " in call)
        delete = calls.index(shlex.join(gate.hz("delete-server", "sandboxes-m1-20261002t1800-w1")))
        self.assertLess(drain, delete)
        self.assertIsNotNone(self.state()["resources"]["staging"])  # Kept while a server remains.

    def test_teardown_keeps_staging_when_the_s3_delete_fails(self):
        self.run_gate("provision", FakeRunner())
        self.run_gate("configure", FakeRunner({"nydus.tgz": 1}))  # After the S3 key reached the hosts.
        self.assertEqual(self.run_gate("teardown", FakeRunner({"s3-delete": 1})), 1)
        resources = self.state()["resources"]
        self.assertEqual(resources["s3_prefix"], "spike/m1/20261002t1800")
        self.assertEqual(resources["staging"], "/var/tmp/m1-gate-20261002t1800")
        self.assertEqual(resources["servers"], {})

    def test_a_baseline_worker_runs_the_burst_on_todays_path(self):
        for phase in ("provision", "configure", "convert"):
            self.run_gate(phase, FakeRunner())
        runner = FakeRunner()
        self.assertEqual(self.run_gate("workers", runner, "--accept-canary-placement"), 0)
        calls = [" ".join(argv) for argv in runner.calls]
        derive = next(call for call in calls if "deployment-baseline.json" in call and "derive-config" in call)
        self.assertNotIn("--block", derive)  # The live config, no chunk store.
        self.assertNotIn("registry_private_ip", derive)  # The live registry.
        self.assertTrue(any("deployment-baseline.json --role sandbox" in call for call in calls))
        self.assertTrue(any("10.42.0.52" in call and "bench --kind burst --images /opt/m1-gate/bench-burst-base.json"
                            in call for call in calls))
        images = json.loads((self.root / "state" / "20261002t1800" / "stage" / "bench-burst-base.json").read_text())
        self.assertTrue(all(ref.startswith("ucloud-sandbox-registry:5000/ucloud-managed/") and "@sha256:" in ref
                            for ref in images.values()))
        self.assertIn("sandboxes-m1-20261002t1800-b1", self.state()["resources"]["servers"])

    def test_workers_refuse_without_the_placement_acknowledgement(self):
        for phase in ("provision", "configure", "convert"):
            self.run_gate(phase, FakeRunner())
        runner = FakeRunner()
        self.assertEqual(self.run_gate("workers", runner), 1)
        self.assertEqual(runner.calls, [])


def passing_results():
    seq = {f"{index}:{command}": {"traced": {"wall": 1.2 * baseline if baseline else 2.4, "rc": 0}}
           for index, baselines in gate.S10_BASELINES.items() for command, baseline in baselines.items()}
    held = gate.SAMPLE_SIZE - len(gate.STEPS)
    return {"convert": [{"index": i, "ok": True, "verified": True} for i in range(held)],
            "crash": [{"index": held + n, "step": step, "killed": True, "ok": True, "verified": True}
                      for n, step in enumerate(gate.STEPS)],
            "tally": {"s3": {"stored_bytes": 17.6e9}}, "bench_seq": seq,
            "bench_burst": {"traced": {"wall": 5.1, "n": 20}},
            "rollback": [{"index": i, "ok": True} for i in gate.ROLLBACK_IMAGES]}


class ReportTests(unittest.TestCase):
    def status(self, results):
        return {item["name"]: item["status"] for item in gate.evaluate(results)["criteria"]}

    def test_a_command_missing_from_the_image_is_not_timed_and_bursts_are_compared(self):
        results = passing_results()
        results["bench_seq"]["72:pip_version"] = {"traced": {"wall": 0.17, "rc": 127}}
        rows = [{"create": 1.0 + n, "import_sys": {"wall": 0.2}, "pip_version": {"wall": 1.0}} for n in range(3)]
        results["bench_burst_base"] = {"traced": {"wall": 4.0, "n": 3, "rows": rows}}
        verdict = gate.evaluate(results)
        cold = next(item for item in verdict["criteria"] if item["name"] == "cold_commands")
        self.assertEqual(cold["status"], "pass")
        self.assertEqual([row["note"] for row in cold["measured"] if row.get("note")], ["not in image"])
        self.assertEqual(verdict["burst_comparison"]["today"]["traced"]["create_p50"], 2.0)
        self.assertIn("20-way burst against today's path", gate.render_readme(
            {**verdict, "run_id": "r", "prefix": "p", "resources": {"vm_hours": 0, "eur_estimate": 0, "servers": []}},
            "2026-10-03"))

    def test_all_criteria_pass(self):
        verdict = gate.evaluate(passing_results())
        self.assertTrue(verdict["pass"], verdict)
        self.assertIn("pass", gate.render_readme({**verdict, "run_id": "r", "prefix": "spike/m1/r",
                                                  "resources": gate.vm_hours({"resources": {"servers": {}}}, 0)},
                                                 "2026-10-02"))

    def test_each_criterion_fails_on_its_own(self):
        cases = {"stored_bytes": lambda r: r["tally"]["s3"].update(stored_bytes=18.5e9),
                 "full_tree": lambda r: r["convert"][0].update(verified=False),
                 "cold_commands": lambda r: r["bench_seq"]["63:import_sys"]["traced"].update(wall=0.41 * 1.31),
                 "burst_20": lambda r: r["bench_burst"]["traced"].update(wall=5.6),
                 "crash_injection": lambda r: r["crash"][3].update(ok=False),
                 "rollback_10": lambda r: r["rollback"].pop()}
        for name, spoil in cases.items():
            results = passing_results()
            spoil(results)
            statuses = self.status(results)
            self.assertEqual(statuses.pop(name), "fail", name)
            self.assertEqual(set(statuses.values()), {"pass"}, name)

    def test_missing_results_are_not_run_and_fail_the_gate(self):
        verdict = gate.evaluate({})
        self.assertFalse(verdict["pass"])
        self.assertEqual({item["status"] for item in verdict["criteria"]}, {"not run"})

    def test_pip_without_an_s12_baseline_uses_the_s11_limit(self):
        results = passing_results()
        results["bench_seq"]["72:pip_version"]["traced"]["wall"] = 2.6
        self.assertEqual(self.status(results)["cold_commands"], "fail")


class LastJsonTests(unittest.TestCase):
    def test_a_multi_line_done_record_is_parsed(self):  # The first gate run read it as "lost".
        self.assertEqual(gate.last_json('noise\n{\n "finished": 1.5,\n "status": 0\n}\n')["status"], 0)
        self.assertEqual(gate.last_json('{"status": "running"}\nstarted')["status"], "running")


class StoreNodeAdapterTests(unittest.TestCase):
    def test_phase_b_starts_both_services_and_blocks_name_this_run_s_node(self):
        from ucloud_sandboxes.environment_config import ChunkStoreConfig
        node = {"url": "http://10.42.0.200:5091", "listen": "10.42.0.200:5091", "cache_dir": "/var/lib/c",
                "cache_bytes": 1 << 30, "extent_bytes": 1 << 22, "s3_concurrency": 64, "serve_index": True}
        block = {**BLOCK, "index_listen": "10.42.0.200:8095", "store_node": node}
        store = gate.store_node_adapter(block, store_ip="10.42.0.49")
        self.assertEqual((store.url, store.chunk_url), ("http://10.42.0.49:8095", "http://10.42.0.49:5091"))
        for command in ("serve-chunk-index --chunk-store-config", "serve-chunk-store --chunk-store-config", "token_hex(32)", "/var/lib/m1-gate/read.token", "reset-failed m1-gate-store"):  # noqa: E501
            self.assertIn(command, store.start_command)
        for role, value in gate.role_blocks(block, store, "spike/m1/r", "/var/tmp/s").items():
            parsed = ChunkStoreConfig.from_dict(value)  # The release's own validation, serve_index rules included.
            self.assertEqual((parsed.store_node.url, parsed.index_url), (store.chunk_url, store.url), role)


class RemoteHelperTests(unittest.TestCase):
    def test_reset_unmounts_only_image_mounts_overlays_first(self):
        listing = ("/ x\n/s/environment-io/components/c1 erofs\n/v/ucloud-rootfs-cache/images/i1/rootfs overlay\n"
                   "/v/ucloud-rootfs-cache/images/i2/rootfs erofs\n/home/other erofs\n")
        calls = []

        def runner(argv, **_):
            calls.append(argv)
            return type("Done", (), {"stdout": listing})()
        remote.unmount_images(runner)
        self.assertEqual([argv[1] for argv in calls[1:]], ["/v/ucloud-rootfs-cache/images/i1/rootfs",
                                                           "/v/ucloud-rootfs-cache/images/i2/rootfs",
                                                           "/s/environment-io/components/c1"])
        listing += "/s/direct-runtime/bundles/m1-x.sandbox-1/rootfs overlay\n"
        with self.assertRaises(SystemExit):
            remote.unmount_images(runner)

    def test_bench_creates_carry_the_operation_a_node_agent_accepts(self):
        from ucloud_sandboxes.sandbox import SandboxOperation, SandboxSpec
        package = Path(sys.modules["ucloud_sandboxes"].__file__).parents[1]
        with tempfile.TemporaryDirectory() as directory:
            cli = Path(directory) / "ucloud-sandboxes"
            cli.write_text(f"#!/bin/sh\nexec env PYTHONPATH={package} /usr/bin/python3 -m ucloud_sandboxes.cli\n")
            spec = {"id": "m1-run-0-imp-d", "image": "registry:5000/a:b", "network": "bridge", "command": ["sleep", "1"]}
            operation = SandboxOperation.from_dict(remote.create_operation(spec, 1791013358000, cli=cli))
        operation.validate_spec(SandboxSpec.from_dict(spec))

    def test_derive_config_without_a_block_keeps_the_live_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "live.json").write_text(json.dumps({"immutable_environments": {"repository": "environments"}}))
            argv = ["derive-config", "--source", str(root / "live.json"), "--out", str(root / "base.json"),
                    "--set", 'sandbox.docker_quota_image_gb=64']
            self.assertEqual(remote.main(argv), 0)
            copy = json.loads((root / "base.json").read_text())
            self.assertNotIn("chunk_store", copy["immutable_environments"])
            self.assertEqual(copy["sandbox"]["docker_quota_image_gb"], 64)

    def test_derive_config_writes_only_a_copy_under_the_run_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "live.json"
            source.write_text(json.dumps({"immutable_environments": {"repository": "environments"}, "x": 1}))
            before = source.read_bytes()
            (root / "block.json").write_text(json.dumps({**BLOCK, "prefix": "spike/m1/r"}))
            argv = ["derive-config", "--source", str(source), "--block", str(root / "block.json"),
                    "--out", str(root / "copy.json"), "--set", 'registry_private_ip="10.42.0.48"']
            with patch.object(sys, "stdout"):
                self.assertEqual(remote.main(argv), 0)
            copy = json.loads((root / "copy.json").read_text())
            self.assertEqual(copy["immutable_environments"]["chunk_store"]["prefix"], "spike/m1/r")
            self.assertEqual(copy["registry_private_ip"], "10.42.0.48")
            self.assertEqual(source.read_bytes(), before)
            (root / "block.json").write_text(json.dumps({**BLOCK, "prefix": "production/chunks"}))
            with self.assertRaises(SystemExit):
                remote.main(argv)

    def test_stored_bytes_classes(self):
        self.assertEqual([remote.classify(key) for key in (
            "spike/m1/r/packs/ab/ab12.pack", "spike/m1/r/meta/cd.boot.zst", "spike/m1/r/meta/cd.map", "x")],
            ["packs", "bootstraps", "maps", "other"])


if __name__ == "__main__":
    unittest.main()
