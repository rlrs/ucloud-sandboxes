from contextlib import redirect_stderr
import errno
import io
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest

from runtime.gvisor import spike_rl_scale as spike

TEST_TIER = "contract"


SCRIPT = Path(spike.__file__)

# Excerpt of `runsc flags` from release-20260817.0 (Go flag.PrintDefaults).
RUNSC_FLAGS = """\
  -EXPERIMENTAL-xdp value
    \twhether and how to use XDP. Can be one of: "off" (default), "ns", "redirect:<device name>", or "tunnel:<device name>"
  -debug
    \tenable debug logging.
  -file-access value
    \tspecifies which filesystem validation to use for the root mount: exclusive (default), shared.
  -host-uds value
    \tcontrols permission to access host Unix-domain sockets. Values: none|open|create|all, default: none
  -overlay2 value
    \twrap mounts with overlayfs. Format is
    \t* 'none' to turn overlay mode off
    \t* {mount}:{medium}[,size={size}], where
    \t    'mount' can be 'root' or 'all'
    \t    'medium' can be 'memory', 'self' or 'dir=/abs/dir/path' in which filestore will be created
    \t    'size' optional parameter overrides default overlay upper layer size
    \t (default root:self)
  -panic-log-fd int
    \tfile descriptor to write Go's runtime messages. (default -1)
  -platform string
    \tspecifies which platform to use: systrap (default), ptrace, kvm. (default "systrap")
  -root string
    \troot directory for storage of container state, defaults are $XDG_RUNTIME_DIR/runsc, /var/run/runsc.
  -x\tshort single-letter usage
"""

# `runsc help tar` from the same release.
RUNSC_HELP_TAR = """\
Usage: tar <flags> <subcommand> <subcommand args>

Subcommands:
\tflags            describe all known top-level flags
\thelp             describe subcommands and their syntax
\trootfs-upper     extracts the upper layer of a container's rootfs into a tar archive
"""

RUNSC_HELP = """\
Usage: runsc <flags> <subcommand> <subcommand args>

Subcommands:
\tcheckpoint       checkpoint current state of container (experimental)
\tcreate           create a secure container
\ttar              creates tar archives from container filesystems

Subcommands for helpers:
\tgofer            launch a gofer process that proxies access to container files

Use "runsc flags" for a list of top-level flags
"""

SMAPS_ROLLUP = """\
5581f3a00000-7ffc7f7f2000 ---p 00000000 00:00 0                          [rollup]
Rss:               40960 kB
Pss:               20480 kB
Pss_Anon:           4096 kB
Pss_File:          16384 kB
Pss_Shmem:             0 kB
Shared_Clean:      32768 kB
Shared_Dirty:          0 kB
Private_Clean:      4096 kB
Private_Dirty:      4096 kB
Swap:                  0 kB
"""

MOUNTINFO = r"""22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
30 22 0:26 / /sys/fs/cgroup rw,nosuid shared:9 - cgroup2 cgroup2 rw
40 22 253:0 / /srv/spike\040root rw,relatime shared:20 - xfs /dev/mapper/vg-spike rw,attr2
41 40 0:50 / /srv/spike\040root/run-1/s1/guest/bundle/rootfs rw - overlay overlay rw,lowerdir=/x
42 22 0:51 / /srv/spike\040rootless rw - tmpfs tmpfs rw
"""


def elf64(interpreter=None):
    """A minimal little-endian ELF64 image with optional PT_INTERP."""
    headers = [struct.pack("<IIQQQQQQ", 1, 5, 0, 0, 0, 0, 0, 0x1000)]
    payload = b""
    if interpreter is not None:
        payload = interpreter.encode() + b"\0"
        offset = 64 + 56 * 2
        headers.append(struct.pack("<IIQQQQQQ", 3, 4, offset, 0, 0, len(payload),
                                   len(payload), 1))
    ident = b"\x7fELF" + bytes([2, 1, 1, 0]) + b"\0" * 8
    header = ident + struct.pack("<HHIQQQIHHHHHH", 2, 62, 1, 0, 64, 0, 0, 64, 56,
                                 len(headers), 0, 0, 0)
    return header + b"".join(headers) + payload


class HelpParsingTests(unittest.TestCase):
    def test_go_flag_defaults_types_and_usage(self):
        flags = spike.parse_go_flags(RUNSC_FLAGS)
        self.assertEqual(len(flags), 9)
        self.assertEqual(flags["overlay2"]["default"], "root:self")
        self.assertEqual(flags["overlay2"]["type"], "value")
        self.assertIn("'medium' can be 'memory', 'self'", flags["overlay2"]["usage"])
        # Defaults stated inline because Go omits "(default X)" for zero values.
        self.assertEqual(flags["file-access"]["default"], "exclusive")
        self.assertEqual(flags["host-uds"]["default"], "none")
        self.assertEqual(flags["EXPERIMENTAL-xdp"]["default"], "off")
        self.assertEqual(flags["platform"]["default"], "systrap")
        self.assertEqual(flags["panic-log-fd"]["default"], "-1")
        self.assertIsNone(flags["root"]["default"])
        self.assertIsNone(flags["debug"]["default"])
        self.assertIsNone(flags["debug"]["type"])
        self.assertEqual(flags["x"]["usage"], "short single-letter usage")
        self.assertEqual(spike.parse_go_flags("Usage: nothing here\n"), {})

    def test_flag_values_are_read_from_usage(self):
        usage = spike.parse_go_flags(RUNSC_FLAGS)["host-uds"]["usage"]
        self.assertEqual(spike.flag_values(usage), ["none", "open", "create", "all"])
        self.assertEqual(spike.flag_values(
            'Values: "none"|"open"|"create", default: "none" (default none)'),
            ["none", "open", "create"])
        self.assertEqual(spike.flag_values("no enumerated values here"), [])

    def test_subcommand_parsing_and_detection(self):
        self.assertEqual(spike.parse_subcommands(RUNSC_HELP),
                         ["checkpoint", "create", "tar", "gofer"])
        self.assertEqual(spike.parse_subcommands(RUNSC_HELP_TAR),
                         ["flags", "help", "rootfs-upper"])
        self.assertTrue(spike.mentions_subcommand(RUNSC_HELP_TAR, "rootfs-upper"))
        self.assertFalse(spike.mentions_subcommand("rootfs-uppers and xrootfs-upper", "rootfs-upper"))

    def test_version_and_effective_flag(self):
        self.assertEqual(spike.parse_runsc_version("runsc version release-20260817.0\nspec: 1.1\n"),
                         "release-20260817.0")
        self.assertIsNone(spike.parse_runsc_version("unknown"))
        self.assertEqual(spike.effective_flag_value("root:self", ["--debug"], "overlay2"),
                         "root:self")
        self.assertEqual(spike.effective_flag_value(
            "root:self", ["--overlay2=none", "--overlay2=root:memory"], "overlay2"), "root:memory")


class ProcParsingTests(unittest.TestCase):
    def test_smaps_rollup_bytes_and_uss(self):
        values = spike.parse_smaps_rollup(SMAPS_ROLLUP)
        self.assertEqual(values["Rss"], 40960 * 1024)
        self.assertEqual(values["Pss"], 20480 * 1024)
        self.assertEqual(values["Uss"], 8192 * 1024)
        self.assertNotIn("Uss", spike.parse_smaps_rollup("Rss: 1 kB\n"))

    def test_meminfo_kv_and_swaps(self):
        meminfo = spike.parse_meminfo("MemTotal:  16 kB\nMemAvailable: 8 kB\nHugePages_Total: 3\n")
        self.assertEqual(meminfo, {"MemTotal": 16384, "MemAvailable": 8192, "HugePages_Total": 3})
        self.assertEqual(spike.parse_kv_lines("anon 10\nfile 20\nbad line here\nfrozen 1\n"),
                         {"anon": 10, "file": 20, "frozen": 1})
        swaps = spike.parse_proc_swaps(
            "Filename Type Size Used Priority\n/dev/vdb partition 8388604 1024 -2\n")
        self.assertEqual(swaps, [{"filename": "/dev/vdb", "type": "partition",
                                  "size_bytes": 8388604 * 1024, "used_bytes": 1024 * 1024,
                                  "priority": "-2"}])
        self.assertEqual(spike.parse_proc_swaps("Filename Type Size Used Priority\n"), [])

    def test_kernel_config_parsing(self):
        config = spike.parse_kernel_config(
            "# comment\nCONFIG_EROFS_FS=m\nCONFIG_ZSWAP=y\n# CONFIG_EROFS_FS_ONDEMAND is not set\n"
            'CONFIG_ZSWAP_COMPRESSOR_DEFAULT="zstd"\nCONFIG_NR_CPUS=64\n')
        self.assertEqual(config, {"CONFIG_EROFS_FS": "m", "CONFIG_ZSWAP": "y",
                                  "CONFIG_EROFS_FS_ONDEMAND": "n",
                                  "CONFIG_ZSWAP_COMPRESSOR_DEFAULT": "zstd",
                                  "CONFIG_NR_CPUS": "64"})

    def test_module_presence(self):
        texts = {"loaded_text": "erofs 135168 0 - Live 0x0\nnbd 61440 0 - Live 0x0\n",
                 "builtin_text": "kernel/fs/overlayfs/overlay.ko\n",
                 "dep_text": "kernel/drivers/block/ublk_drv.ko.zst:\nkernel/fs/erofs/erofs.ko: \n"}
        self.assertEqual(spike.module_presence("erofs", **texts),
                         {"loaded": True, "builtin": False, "loadable": True})
        self.assertEqual(spike.module_presence("overlay", **texts),
                         {"loaded": False, "builtin": True, "loadable": False})
        self.assertEqual(spike.module_presence("ublk-drv", **texts),
                         {"loaded": False, "builtin": False, "loadable": True})
        self.assertFalse(any(spike.module_presence("xfs", **texts).values()))

    def test_mountinfo_lookup_and_mounts_under(self):
        mounts = spike.parse_mountinfo(MOUNTINFO)
        self.assertEqual(mounts[2]["mount_point"], "/srv/spike root")
        self.assertEqual(spike.mount_for_path("/srv/spike root/run-1", mounts)["fstype"], "xfs")
        self.assertEqual(spike.mount_for_path("/srv/spike rootless/a", mounts)["fstype"], "tmpfs")
        self.assertEqual(spike.mount_for_path("/home/x", mounts)["fstype"], "ext4")
        self.assertEqual(spike.mounts_under("/srv/spike root/run-1", mounts),
                         ["/srv/spike root/run-1/s1/guest/bundle/rootfs"])
        self.assertEqual(spike.mounts_under("/srv/spike root/run-2", mounts), [])

    def test_xfs_info_reflink(self):
        info = "meta-data=/dev/vdb isize=512\n         =  crc=1 finobt=1\n         =  reflink=1 bigtime=1\n"
        self.assertTrue(spike.parse_xfs_info_reflink(info))
        self.assertFalse(spike.parse_xfs_info_reflink(info.replace("reflink=1", "reflink=0")))
        self.assertIsNone(spike.parse_xfs_info_reflink("ext4"))

    def test_elf_interpreter(self):
        self.assertIsNone(spike.elf_interpreter(elf64()))
        self.assertEqual(spike.elf_interpreter(elf64("/lib64/ld-linux-x86-64.so.2")),
                         "/lib64/ld-linux-x86-64.so.2")
        with self.assertRaises(ValueError):
            spike.elf_interpreter(b"#!/bin/sh\n" + b"\0" * 64)
        with self.assertRaises(ValueError):
            spike.elf_interpreter(elf64("/lib/ld.so")[:100])

    def test_fd_link_classification(self):
        self.assertEqual(spike.classify_fd_link("/memfd:runsc-memory (deleted)"), "memory_file")
        self.assertEqual(spike.classify_fd_link("/var/lib/x/application_memory.img"),
                         "memory_file")
        self.assertEqual(spike.classify_fd_link("/r/rootfs/.gvisor.filestore.abc (deleted)"),
                         "filestore")
        self.assertIsNone(spike.classify_fd_link("/dev/null"))
        self.assertIsNone(spike.classify_fd_link("socket:[1234]"))


class ComputationTests(unittest.TestCase):
    def test_page_sharing_ratio_and_classification(self):
        self.assertEqual(spike.page_sharing_ratio(800, 8, 100), 1.0)
        self.assertEqual(spike.page_sharing_ratio(100, 8, 100), 0.125)
        self.assertIsNone(spike.page_sharing_ratio(100, 8, 0))
        self.assertIsNone(spike.page_sharing_ratio(None, 8, 100))
        self.assertEqual(spike.classify_sharing(1.0, 32), "duplicated")
        self.assertEqual(spike.classify_sharing(1.2 / 32, 32), "shared")
        self.assertEqual(spike.classify_sharing(0.5, 32), "partially_shared")
        self.assertEqual(spike.classify_sharing(None, 32), "unknown")
        self.assertEqual(spike.classify_sharing(1.0, 1), "unknown")

    def stats(self, current, file, pss):
        return {"memory_current": current, "memory_stat": {"file": file, "anon": 1, "shmem": 2},
                "sentry_smaps": {"Rss": pss * 2, "Pss": pss, "Uss": pss // 2},
                "memory_file_allocated": current // 2}

    def test_totals_and_ratios(self):
        host = {"before": {"MemAvailable": 1000}, "idle": {"MemAvailable": 900},
                "after_read": {"MemAvailable": 500}}
        idle = [self.stats(10, 1, 4), self.stats(10, 1, 4)]
        read = [self.stats(110, 51, 54), self.stats(110, 51, 54)]
        totals = spike.k_totals(host, idle, read)
        self.assertEqual(totals["idle"]["host_mem_available_drop"], 100)
        self.assertEqual(totals["after_read"]["host_mem_available_drop"], 500)
        self.assertEqual(totals["read_increment"]["host_mem_available_drop"], 400)
        self.assertEqual(totals["read_increment"]["cgroup_memory_current_sum"], 200)
        self.assertEqual(totals["read_increment"]["sentry_pss_sum"], 100)

        per_k = {1: {"after_read": {"cgroup_memory_current_sum": 110},
                     "read_increment": {"cgroup_memory_current_sum": 100}},
                 8: {"after_read": {"cgroup_memory_current_sum": 880},
                     "read_increment": {"cgroup_memory_current_sum": 800}},
                 32: {"after_read": {"cgroup_memory_current_sum": 400},
                      "read_increment": {"cgroup_memory_current_sum": 110}}}
        ratios = spike.sharing_ratios(per_k)
        increment = ratios["read_increment"]["cgroup_memory_current_sum"]
        self.assertEqual(increment["1"]["ratio"], 1.0)
        self.assertEqual(increment["8"]["classification"], "duplicated")
        self.assertEqual(increment["32"]["scaling_vs_k1"], 1.1)
        self.assertEqual(increment["32"]["classification"], "shared")
        self.assertIsNone(ratios["read_increment"]["sentry_pss_sum"]["8"]["ratio"])
        with self.assertRaises(ValueError):
            spike.sharing_ratios({8: per_k[8]})

    def test_missing_per_sandbox_metric_makes_sum_unknown(self):
        totals = spike.aggregate_sandbox_stats([self.stats(10, 1, 4), {"memory_current": 5}])
        self.assertEqual(totals["cgroup_memory_current_sum"], 15)
        self.assertIsNone(totals["sentry_pss_sum"])
        self.assertTrue(all(value is None for value in spike.aggregate_sandbox_stats([]).values()))

    def test_s1_evaluation(self):
        write = 64 * spike.MIB
        status, answer = spike.evaluate_s1(
            overlay2_default="root:self", overlay2_effective="root:self", write_bytes=write,
            filestore_allocated=write + 4096, guest_file_on_host=False,
            upper_other_allocated=8192)
        self.assertEqual(status, "pass")
        self.assertIn("'root:self'", answer)
        for changes in ({"overlay2_default": "none", "overlay2_effective": "none"},
                        {"filestore_allocated": None},
                        {"filestore_allocated": write // 2},
                        {"guest_file_on_host": True},
                        {"upper_other_allocated": write}):
            arguments = dict(overlay2_default="root:self", overlay2_effective="root:self",
                             write_bytes=write, filestore_allocated=write,
                             guest_file_on_host=False, upper_other_allocated=0)
            arguments.update(changes)
            self.assertEqual(spike.evaluate_s1(**arguments)[0], "fail", changes)

    def test_reclaim_probe_classification(self):
        self.assertTrue(spike.classify_reclaim_probe(None, None)["swappiness_argument"])
        rejected = spike.classify_reclaim_probe(None, errno.EINVAL)
        self.assertFalse(rejected["swappiness_argument"])
        self.assertIn("EINVAL", rejected["detail"])
        missing = spike.classify_reclaim_probe(errno.ENOENT, None)
        self.assertFalse(missing["memory_reclaim_writable"])
        self.assertIsNone(missing["swappiness_argument"])

    def test_oci_config(self):
        config = spike.oci_config(["/bin/sh", "-c", "true"], cgroups_path="/spike/c1",
                                  annotations={"b": "2", "a": "1"})
        self.assertEqual(config["process"]["args"], ["/bin/sh", "-c", "true"])
        self.assertEqual(config["linux"]["cgroupsPath"], "/spike/c1")
        self.assertEqual(list(config["annotations"]), ["a", "b"])
        self.assertEqual(config["root"], {"path": "rootfs", "readonly": False})
        self.assertNotIn("cgroupsPath", spike.oci_config(["true"])["linux"])
        self.assertIn({"type": "network"}, config["linux"]["namespaces"])

    def test_cleanup_runs_lifo_and_keeps_every_error(self):
        order = []
        cleanup = spike.Cleanup()
        cleanup.push("first", lambda: order.append("first"))

        def fail():
            order.append("second")
            raise RuntimeError("boom")
        cleanup.push("second", fail)
        cleanup.push("third", lambda: order.append("third"))
        errors = cleanup.run()
        self.assertEqual(order, ["third", "second", "first"])
        self.assertEqual(errors, ["second: RuntimeError: boom"])
        self.assertEqual(cleanup.run(), [])

    def test_overlay_path_problem(self):
        self.assertIsNone(spike.overlay_path_problem(Path("/srv/spike/rootfs")))
        self.assertIsNotNone(spike.overlay_path_problem(Path("relative/path")))
        self.assertIsNotNone(spike.overlay_path_problem(Path("/srv/a,b")))
        self.assertIsNotNone(spike.overlay_path_problem(Path("/srv/a:b")))
        self.assertIsNotNone(spike.overlay_path_problem(Path("/srv/a b")))


class WorkRootTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.euid = os.geteuid()

    def tearDown(self):
        self.temporary.cleanup()

    def test_empty_private_directory_is_accepted(self):
        self.assertIsNone(spike.work_root_problem(self.root, euid=self.euid))

    def test_foreign_contents_are_refused_until_marked(self):
        (self.root / "data").write_text("not ours")
        self.assertIn("not empty", spike.work_root_problem(self.root, euid=self.euid))
        (self.root / spike.OWNER_MARKER).write_text("{}")
        self.assertIsNone(spike.work_root_problem(self.root, euid=self.euid))

    def test_unsafe_targets_are_refused(self):
        self.assertIn("does not exist", spike.work_root_problem(self.root / "missing",
                                                                 euid=self.euid))
        regular = self.root / "file"
        regular.write_text("x")
        self.assertIn("not a directory", spike.work_root_problem(regular, euid=self.euid))
        link = self.root / "link"
        link.symlink_to(self.root)
        self.assertIn("symlink", spike.work_root_problem(link, euid=self.euid))
        self.assertIn("owned by", spike.work_root_problem(self.root, euid=self.euid + 1))
        shared = self.root / "shared"
        shared.mkdir()
        os.chmod(shared, 0o770)
        self.assertIn("writable", spike.work_root_problem(shared, euid=self.euid))


class ArgumentTests(unittest.TestCase):
    base = ["--runsc", sys.executable, "--work-root", "/srv/spike", "--output", "/tmp/out.json"]

    def parse_error(self, *argv):
        with redirect_stderr(io.StringIO()) as stderr, self.assertRaises(SystemExit) as raised:
            spike.parse_args(list(argv))
        self.assertEqual(raised.exception.code, 2)
        return stderr.getvalue()

    def test_probe_selection_and_defaults(self):
        args = spike.parse_args([*self.base, "--probe", "S7,s1"])
        self.assertEqual(args.probe, ["s1", "s7"])
        self.assertEqual(args.k, [1, 8, 32])
        self.assertEqual(args.variant, ["gofer"])
        self.assertTrue(args.warmup)
        args = spike.parse_args([*self.base, "--probe", "s8", "--runsc-flag=--overlay2=none"])
        self.assertEqual(args.runsc_flag, ["--overlay2=none"])

    def test_s2_inputs_are_validated(self):
        with tempfile.TemporaryDirectory() as root:
            image = Path(root) / "image.erofs"
            image.write_bytes(b"\0")
            args = spike.parse_args([*self.base, "--probe", "s2", "--rootfs", root,
                                     "--variant", "erofs,gofer", "--erofs-image", str(image),
                                     "--k", "8,1"])
            self.assertEqual(args.variant, ["gofer", "erofs"])
            self.assertEqual(args.k, [1, 8])
            self.assertTrue(args.rootfs.is_absolute())
            self.assertIn("--k must include 1", self.parse_error(
                *self.base, "--probe", "s2", "--rootfs", root, "--k", "8,32"))
            self.assertIn("--erofs-image", self.parse_error(
                *self.base, "--probe", "s2", "--variant", "erofs"))
        self.assertIn("--rootfs", self.parse_error(*self.base))
        self.parse_error(*self.base, "--probe", "s3")
        self.parse_error(*self.base, "--probe", "s8", "--k", "0,1")
        self.assertIn("is not a file", self.parse_error(
            "--runsc", "/nonexistent/runsc", "--work-root", "/srv", "--output", "/tmp/o",
            "--probe", "s8"))

    def test_help_works_without_root(self):
        completed = subprocess.run([sys.executable, str(SCRIPT), "--help"],
                                   capture_output=True, text=True, timeout=60)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--allow-swap-test", completed.stdout)

    @unittest.skipIf(os.geteuid() == 0, "root may run the probe")
    def test_refuses_to_run_without_root(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "out.json"
            message = self.parse_error_main(["--runsc", sys.executable, "--work-root", root,
                                             "--output", str(output), "--probe", "s7"])
            self.assertIn("requires root", message)
            self.assertFalse(output.exists())

    def parse_error_main(self, argv):
        with redirect_stderr(io.StringIO()) as stderr, self.assertRaises(SystemExit) as raised:
            spike.main(argv)
        self.assertEqual(raised.exception.code, 2)
        return stderr.getvalue()


if __name__ == "__main__":
    unittest.main()
