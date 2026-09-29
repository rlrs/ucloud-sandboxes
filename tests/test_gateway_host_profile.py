from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from scripts.gateway_host_profile import analyze_records, discover_groups, network_snapshot, process_stat


class GatewayHostProfileTests(unittest.TestCase):
    def test_discovers_postgresql_instance_in_nested_host_slice_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nested = root / "system.slice/system-postgresql.slice/postgresql@18-main.service"
            nested.mkdir(parents=True)
            (root / "system.slice/ucloud-sandbox-gateway.service").mkdir()
            delegated = root / "system.slice/docker.service/system.slice/postgresql@other.service"
            delegated.mkdir(parents=True)
            groups = discover_groups(root, {"container:registry": "/system.slice/docker-registry.scope"})
            self.assertEqual(groups["postgresql@18-main.service"],
                             "/system.slice/system-postgresql.slice/postgresql@18-main.service")
            self.assertIn("ucloud-sandbox-gateway.service", groups)
            self.assertIn("docker.service", groups)
            self.assertIn("container:registry", groups)
            self.assertNotIn("postgresql@other.service", groups)
            self.assertNotIn("system-postgresql.slice", groups)

    def sample(self, when, ticks, usec, inode=1, start=20):
        return {"type": "sample", "unix_seconds": 1700000000 + when, "monotonic_seconds": when,
                "phase": "loaded", "host": {"cpu_ticks": dict(user=ticks, nice=0, system=0,
                    idle=0, iowait=0, irq=0, softirq=0, steal=0), "cpu_pressure": {},
                    "memory_pressure": {}, "io_pressure": {}, "disks": {}},
                "cgroups": {"gateway": {"inode": inode, "cpu": {"usage_usec": usec}}},
                "processes": {"10": {"name": "python", "start_ticks": start,
                    "user_ticks": ticks, "system_ticks": 0}}}

    def test_elapsed_time_weighted_cores_and_cpu_seconds(self):
        # One second at one core, three seconds at three cores: 10 CPU seconds,
        # mean 2.5 cores rather than the unweighted sample mean of two.
        result = analyze_records([{"type": "metadata", "clock_ticks": 100},
            self.sample(0, 0, 0), self.sample(1, 100, 1000000), self.sample(4, 1000, 10000000)])
        for key in ("host/busy_cores", "cgroup/gateway/cores"):
            row = result["summary"][key]
            self.assertEqual(row["cpu_seconds"], 10)
            self.assertEqual(row["mean"], 2.5)
            self.assertEqual(row["p95"], 3)
        self.assertEqual(result["hottest_processes"][0]["cpu_seconds"], 10)

    def test_cgroup_recreation_and_pid_reuse_do_not_create_false_spikes(self):
        result = analyze_records([{"type": "metadata", "clock_ticks": 100},
            self.sample(0, 500, 5000000), self.sample(1, 2000, 20000000, inode=2, start=30)])
        self.assertNotIn("cgroup/gateway/cores", result["summary"])
        self.assertEqual(result["hottest_processes"], [])

    def test_recovers_missing_postgres_group_from_original_process_samples(self):
        before, after = self.sample(0, 0, 0), self.sample(2, 100, 1000000)
        for row in (before, after):
            row["processes"]["10"]["name"] = "postgres"
            row["processes"]["10"]["cgroup"] = "/system.slice/system-postgresql.slice/postgresql@18-main.service"
        result = analyze_records([{"type": "metadata", "clock_ticks": 100}, before, after])
        key = "sampled_process_cgroup/system.slice/system-postgresql.slice/postgresql@18-main.service/cores"
        self.assertEqual(result["summary"][key]["cpu_seconds"], 1)
        self.assertEqual(result["summary"][key]["mean"], .5)

    def test_process_comm_parentheses_are_not_treated_as_fields(self):
        fields = ["S"] + ["0"] * 21
        fields[11], fields[12], fields[17], fields[19], fields[21] = "12", "13", "3", "40", "50"
        row = process_stat("10 (name with ) parens) " + " ".join(fields))
        self.assertEqual(row["name"], "name with ) parens")
        self.assertEqual(row["user_ticks"], 12)
        self.assertEqual(row["rss_pages"], 50)

    def test_phase_join_does_not_mistake_partial_completion_for_cleanup(self):
        records = [{"type": "metadata", "clock_ticks": 100},
                   self.sample(0, 0, 0), self.sample(1, 100, 1000000),
                   self.sample(2, 200, 2000000), self.sample(3, 300, 3000000)]
        events = [{"event": name, "at": datetime.fromtimestamp(1700000000 + when, timezone.utc).isoformat()}
                  for name, when in (("capacity_requested", .1), ("all_agents_ready", 1.1),
                                     ("scenario_completed", 2.1), ("benchmark_finished", 3.1))]
        events[-1]["correct"] = False
        result = analyze_records(records, events)
        self.assertNotIn("cleanup", result["harness_phases"])
        events[-1]["correct"] = True
        result = analyze_records(records, events)
        self.assertEqual(result["harness_phases"]["cleanup"]["host/busy_cores"]["cpu_seconds"], 1)

    def test_network_counters_and_conntrack_are_numeric_and_optional(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            (proc / "net").mkdir()
            (proc / "net/dev").write_text("Inter-| Receive | Transmit\n eth0: 1024 8 0 2 0 0 0 0 2048 9 0 3 0 0 0 0\n")
            (proc / "net/softnet_stat").write_text("00000010 00000002 00000003\n00000020 00000004 00000005\n")
            (proc / "net/snmp").write_text("Tcp: InSegs OutSegs RetransSegs\nTcp: 100 200 7\n")
            (proc / "net/netstat").write_text("TcpExt: ListenDrops ListenOverflows\nTcpExt: 9 4\n")
            conntrack = proc / "sys/net/netfilter"
            conntrack.mkdir(parents=True)
            (conntrack / "nf_conntrack_count").write_text("50\n")
            (conntrack / "nf_conntrack_max").write_text("100\n")
            network = network_snapshot(proc)
            self.assertEqual(network["interfaces"]["eth0"]["tx_drops"], 3)
            self.assertEqual(network["softnet"], {"processed": 48, "dropped": 6, "time_squeeze": 8})
            self.assertEqual(network["tcp"]["RetransSegs"], 7)
            self.assertEqual(network["tcp_ext"]["ListenDrops"], 9)
            before, after = self.sample(0, 0, 0), self.sample(2, 200, 2000000)
            before["host"]["network"] = {"tcp": {"RetransSegs": 3}}
            after["host"]["network"] = network
            result = analyze_records([{"type": "metadata", "clock_ticks": 100}, before, after])
            retrans = result["summary"]["network/tcp/RetransSegs_per_second"]
            self.assertEqual(retrans["mean"], 2)
            self.assertEqual(retrans["counter_delta"], 4)
            self.assertEqual(result["summary"]["network/conntrack/utilization_fraction"]["max"], .5)
            self.assertEqual(network_snapshot(proc / "missing")["conntrack"], {})


if __name__ == "__main__":
    unittest.main()
