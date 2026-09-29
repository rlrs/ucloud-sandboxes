#!/usr/bin/env python3
"""Local, bounded filesystem-preparation A/B benchmark; never contacts a registry.

Each arm imports its selected source tree in a fresh process. Timed phases exclude
fixture generation and exact-tree validation. Optional profiles use extra runs.
"""
from __future__ import annotations

import argparse
import cProfile
from concurrent.futures import ThreadPoolExecutor
import errno
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import pstats
import shutil
import stat
import statistics
import subprocess
import sys
import tarfile
import tempfile
import time

STAMP = 1_700_000_000
MIB = 1024**2


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def make_layer(files, *, links=(), symlinks=()):
    """Deterministic self-contained diff: all parents have explicit metadata."""
    directories = {"."}
    for name in [*files, *(name for name, _ in links), *(name for name, _ in symlinks)]:
        directories.update(str(p) for p in Path(name).parents)
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as archive:
        def add(name, kind, data=b"", target="", mode=0o644):
            member = tarfile.TarInfo(name)
            member.type, member.mode = kind, mode
            member.uid, member.gid, member.mtime = os.geteuid(), os.getegid(), STAMP
            member.linkname = target
            member.size = len(data) if kind == tarfile.REGTYPE else 0
            archive.addfile(member, io.BytesIO(data) if member.size else None)
        for name in sorted(directories, key=lambda s: (len(Path(s).parts), s)):
            add(name, tarfile.DIRTYPE, mode=0o755)
        for name, data in sorted(files.items()):
            add(name, tarfile.REGTYPE, data)
        for name, target in links:
            add(name, tarfile.LNKTYPE, target=target)
        for name, target in symlinks:
            add(name, tarfile.SYMTYPE, target=target, mode=0o777)
    data = raw.getvalue()
    blob = gzip.compress(data, compresslevel=6, mtime=0)
    return {"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": digest(blob),
            "size": len(blob)}, digest(data), blob, len(data)


def fixture(root, modules):
    layers = []
    packages = {}
    for index in range(modules):
        base = f"app/node_modules/package-{index:05d}"
        packages[base + "/package.json"] = canonical({"name": f"fixture-{index}", "version": "1.0.0"})
        packages[base + "/index.js"] = (f"exports.value = {index};\n" * 24).encode()
        packages[base + "/index.d.ts"] = b"export declare const value: number;\n" * 4
        packages[base + "/LICENSE"] = b"Synthetic benchmark fixture.\n" * 8
    layers.append(make_layer(packages,
        links=[("app/node_modules/package-00000/index-alias.js", "app/node_modules/package-00000/index.js")],
        symlinks=[("app/node_modules/tool-current", "package-00000")]))
    layers.append(make_layer({f"app/node_modules/package-{i:05d}/index.js":
                              (f"exports.value = {i + 10000};\n" * 32).encode()
                              for i in range(0, modules, 4)}))
    sources = {f"app/src/module-{i:05d}.ts": (f"export const value{i} = {i};\n" * 32).encode()
               for i in range(modules)}
    sources["app/package.json"] = b'{"name":"synthetic-app","private":true}\n'
    sources["app/dist/run.js"] = b"console.log('synthetic');\n"
    layers.append(make_layer(sources,
        links=[("app/dist/run-alias.js", "app/dist/run.js")],
        symlinks=[("app/run", "dist/run.js")]))
    layers.append(make_layer({f"app/src/module-{i:05d}.ts":
                              (f"export const value{i} = {i + 1};\n" * 36).encode()
                              for i in range(0, modules, 5)},
        symlinks=[("app/current-source", "src/module-00000.ts")]))
    manifest = {"schema": 1, "modules": modules, "uid": os.geteuid(), "gid": os.getegid(),
                "stamp": STAMP, "layers": [], "diff_ids": [], "uncompressed_bytes": 0}
    for descriptor, diff_id, blob, size in layers:
        (root / descriptor["digest"].removeprefix("sha256:")).write_bytes(blob)
        manifest["layers"].append(descriptor)
        manifest["diff_ids"].append(diff_id)
        manifest["uncompressed_bytes"] += size
    manifest["fixture_sha256"] = digest(canonical(manifest))
    (root / "fixture.json").write_bytes(canonical(manifest) + b"\n")
    return manifest


def tree_snapshot(root, *, generated_whiteouts=False):
    """Compare contents, links, owners, modes, mtime, xattrs and device identity.

    atime/ctime and numeric inode IDs are intentionally excluded: reads and
    independent extraction change them. Hardlink equivalence classes are kept.
    """
    rows, inodes = {}, {}
    paths = [root]
    for current, directories, files in os.walk(root, followlinks=False):
        paths.extend(Path(current) / name for name in directories + files)
    for path in sorted(paths):
        info = path.lstat()
        name = str(path.relative_to(root))
        row = {"mode": info.st_mode, "uid": info.st_uid, "gid": info.st_gid,
               "mtime_ns": info.st_mtime_ns,
               "xattrs": {key: os.getxattr(path, key, follow_symlinks=False).hex()
                          for key in sorted(os.listxattr(path, follow_symlinks=False))}}
        if stat.S_ISREG(info.st_mode):
            row.update(size=info.st_size, digest=digest(path.read_bytes()))
            inodes.setdefault((info.st_dev, info.st_ino), []).append(name)
        elif stat.S_ISLNK(info.st_mode):
            row["target"] = os.readlink(path)
        elif stat.S_ISCHR(info.st_mode):
            row["device"] = [os.major(info.st_rdev), os.minor(info.st_rdev)]
            if generated_whiteouts and info.st_rdev == os.makedev(0, 0):
                # The existing squasher creates retained whiteouts with the
                # current time, rather than copying the input device mtime.
                row["mtime_ns"] = "generated-whiteout-time"
        rows[name] = row
    hardlinks = sorted(sorted(names) for names in inodes.values() if len(names) > 1)
    return {"entries": len(rows), "tree_digest": digest(canonical(rows)),
            "hardlink_groups": hardlinks, "rows": rows}


def compact(snapshot):
    return {key: value for key, value in snapshot.items() if key != "rows"}


def semantic_layers(root):
    """Overlay-only cases selective OCI must reject, with real capability probes."""
    lower, first, upper = (root / name for name in ("lower", "first", "upper"))
    for directory in (lower, first, upper):
        directory.mkdir()
        (directory / "data").mkdir()
    (lower / "data/lower-delete").write_bytes(b"lower")
    (first / "data/delete").write_bytes(b"delete")
    (first / "data/value").write_bytes(b"original")
    os.link(first / "data/value", first / "data/alias")
    (first / "data/replace-link").symlink_to("value")
    (upper / "data/value").write_bytes(b"replacement")
    (upper / "data/replace-link").write_bytes(b"now regular")
    (upper / "data/new-link").symlink_to("alias")
    exercised, skipped = [], {}
    probes = {
        "user_xattr": lambda: os.setxattr(first / "data/alias", "user.fixture", b"retained"),
        "whiteout": lambda: os.mknod(upper / "data/delete", stat.S_IFCHR | 0o600, os.makedev(0, 0)),
        "lower_whiteout": lambda: os.mknod(upper / "data/lower-delete", stat.S_IFCHR | 0o600, os.makedev(0, 0)),
    }
    (lower / "opaque").mkdir()
    (lower / "opaque/hidden").write_bytes(b"hidden")
    (upper / "opaque").mkdir()
    (upper / "opaque/visible").write_bytes(b"visible")
    probes["trusted_opaque"] = lambda: os.setxattr(upper / "opaque", "trusted.overlay.opaque", b"y")
    for name, action in probes.items():
        try:
            action()
            exercised.append(name)
        except OSError as exc:
            if exc.errno not in (errno.EPERM, errno.EACCES, errno.ENOTSUP, errno.EOPNOTSUPP):
                raise
            skipped[name] = errno.errorcode.get(exc.errno, str(exc.errno))
    for directory in (lower, first, upper):
        for path in sorted([directory, *directory.rglob("*")], reverse=True):
            os.utime(path, (STAMP, STAMP), follow_symlinks=False)
    return lower, [first, upper], exercised, skipped


def semantic_checks(output, exercised):
    assert (output / "data/value").read_bytes() == b"replacement"
    assert (output / "data/alias").read_bytes() == b"original"
    assert (output / "data/replace-link").read_bytes() == b"now regular"
    assert os.readlink(output / "data/new-link") == "alias"
    if "user_xattr" in exercised:
        assert os.getxattr(output / "data/alias", "user.fixture") == b"retained"
        assert "user.fixture" not in os.listxattr(output / "data/value")
    if "whiteout" in exercised:
        assert not (output / "data/delete").exists()
    if "lower_whiteout" in exercised:
        info = (output / "data/lower-delete").lstat()
        assert stat.S_ISCHR(info.st_mode) and info.st_rdev == os.makedev(0, 0)
    if "trusted_opaque" in exercised:
        assert os.getxattr(output / "opaque", "trusted.overlay.opaque") == b"y"
    return True


def worker(args):
    sys.path.insert(0, str(args.source_root.resolve()))
    from ucloud_sandboxes import environment_builder as builder, oci_layer_materialize as materializer
    sources = {Path(module.__file__).name: digest(Path(module.__file__).read_bytes())
               for module in (builder, materializer)}
    for module in (builder, materializer):
        if not Path(module.__file__).resolve().is_relative_to(args.source_root.resolve()):
            raise RuntimeError("runtime import escaped selected source root")
    fixture_root = args.fixture_root
    manifest = json.loads((fixture_root / "fixture.json").read_text())
    if (manifest["uid"], manifest["gid"]) != (os.geteuid(), os.getegid()):
        raise ValueError("generate and run fixture under the same uid/gid")

    class Client:
        def open_blob(self, repository, key):
            return (fixture_root / key.removeprefix("sha256:")).open("rb")

    profiler = cProfile.Profile() if args.profile_worker else None
    batch_phases, publication_phases = {}, [{} for _ in range(args.concurrency)]
    def phase(index, name, fn):
        started, cpu = time.perf_counter(), time.process_time()
        if profiler:
            profiler.enable()
        try:
            return fn()
        finally:
            if profiler:
                profiler.disable()
            publication_phases[index][name] = {
                "wall_seconds": time.perf_counter() - started,
                "process_cpu_seconds": time.process_time() - cpu if args.concurrency == 1 else None}
    def stage(pool, name, functions):
        started, cpu = time.perf_counter(), time.process_time()
        pending = [pool.submit(phase, index, name, fn) for index, fn in enumerate(functions)]
        values = [future.result() for future in pending]
        batch_phases[name] = {"wall_seconds": time.perf_counter() - started,
                              "process_cpu_seconds": time.process_time() - cpu}
        return values
    with tempfile.TemporaryDirectory(prefix="preparation-", dir=args.work_root) as temporary:
        root = Path(temporary)
        roots = [root / f"publication-{index}" for index in range(args.concurrency)]
        for publication_root in roots:
            publication_root.mkdir(mode=0o700)
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            directories = stage(pool, "materialize", [
                lambda destination=destination: materializer.materialize_layers(
                    Client(), "fixture", manifest["layers"], manifest["diff_ids"], destination / "materialized")
                for destination in roots])
            # Validation never runs alongside a timed phase in another thread.
            before = [[compact(tree_snapshot(path)) for path in layers] for layers in directories]
            keywords = {"consume_private_diffs": True} if args.consume_private_diffs else {}
            stage(pool, "squash", [
                lambda layers=layers, destination=destination: builder.squash_layer_diffs(
                    layers, destination / "squashed", **keywords)
                for layers, destination in zip(directories, roots)])
        combined = []
        for index, (layers, destination) in enumerate(zip(directories, roots)):
            combined.append(compact(tree_snapshot(destination / "squashed")))
            if not args.consume_private_diffs:
                assert before[index] == [compact(tree_snapshot(path)) for path in layers], "squash mutated borrowed source"
            assert (destination / "squashed/app/node_modules/package-00000/index-alias.js").read_bytes() == b"exports.value = 0;\n" * 24
            assert (destination / "squashed/app/node_modules/package-00000/index.js").read_bytes() == b"exports.value = 10000;\n" * 32
            assert os.stat(destination / "squashed/app/dist/run.js").st_ino == os.stat(destination / "squashed/app/dist/run-alias.js").st_ino
        lower, layers, exercised, skipped = semantic_layers(root)
        # The tiny general-overlay fixture always exercises the borrowed-source
        # contract separately; the timed OCI fixture exercises private consume.
        builder.squash_layer_diffs(layers, root / "overlay", lower_dirs=[lower])
        semantic_checks(root / "overlay", exercised)
        overlay = compact(tree_snapshot(root / "overlay", generated_whiteouts=True))
    phases = {name: {key: statistics.mean(values[name][key] for values in publication_phases)
                     if key == "wall_seconds" or args.concurrency == 1 else None
                     for key in ("wall_seconds", "process_cpu_seconds")}
              for name in ("materialize", "squash")}
    result = {"schema": 2, "sources": sources, "fixture_sha256": manifest["fixture_sha256"],
              "phases": phases, "batch_phases": batch_phases, "publication_phases": publication_phases,
              "concurrency": args.concurrency, "consume_private_diffs": args.consume_private_diffs,
              "materialized_trees": before, "squashed_tree": combined,
              "overlay_tree": overlay, "semantic_checks_passed": True,
              "capabilities_exercised": exercised, "capability_skips": skipped,
              "profiled": bool(profiler), "python": platform.python_version(),
              "platform": platform.platform(), "uid": os.geteuid(), "gid": os.getegid()}
    if profiler:
        stats = pstats.Stats(profiler)
        result["profile"] = [{"file": Path(key[0]).name, "line": key[1], "function": key[2],
                              "primitive_calls": value[0], "calls": value[1],
                              "self_seconds": value[2], "cumulative_seconds": value[3]}
                             for key, value in sorted(stats.stats.items(), key=lambda item: item[1][3], reverse=True)[:60]]
    args.output.write_bytes(canonical(result) + b"\n")


def run(args):
    if args.output.exists():
        raise ValueError("output directory already exists; choose a fresh path")
    args.output.mkdir(parents=True)
    fixture_root = args.output / "fixture"
    fixture_root.mkdir(mode=0o700)
    manifest = fixture(fixture_root, args.modules)
    roots = {"baseline": args.baseline_root.resolve(), "candidate": args.candidate_root.resolve()}
    runs = []
    # ABBA alternation reduces simple order bias; every run starts a new process.
    for repeat in range(args.repeats):
        for arm in (["baseline", "candidate"] if repeat % 2 == 0 else ["candidate", "baseline"]):
            runs.append((arm, repeat, False))
    if args.profile:
        runs.extend((arm, 0, True) for arm in roots)
    results = []
    for arm, repeat, profiling in runs:
        output = args.output / f'{arm}-{repeat}{"-profile" if profiling else ""}.json'
        command = [sys.executable, str(Path(__file__).resolve()), "--worker", "--source-root", str(roots[arm]),
                   "--fixture-root", str(fixture_root.resolve()), "--work-root", str(args.work_root.resolve()),
                   "--output", str(output.resolve()), "--concurrency", str(args.concurrency)]
        if arm == "candidate" and args.candidate_consume_private_diffs:
            command.append("--consume-private-diffs")
        if profiling:
            command.append("--profile-worker")
        subprocess.run(command, check=True, timeout=args.timeout)
        result = json.loads(output.read_text())
        result.update(arm=arm, repeat=repeat)
        results.append(result)
        print(arm, repeat, "profile" if profiling else "timed", result["batch_phases"], flush=True)
    reference = results[0]
    equality_fields = ("fixture_sha256", "materialized_trees", "squashed_tree", "overlay_tree",
                       "semantic_checks_passed", "capabilities_exercised", "capability_skips")
    mismatches = [{"arm": result["arm"], "repeat": result["repeat"], "profiled": result["profiled"],
                   "fields": [key for key in equality_fields if result[key] != reference[key]]}
                  for result in results if any(result[key] != reference[key] for key in equality_fields)]
    stable_sources = all(len({canonical(result["sources"]) for result in results if result["arm"] == arm}) == 1
                         for arm in roots)
    report = {"schema": 2, "fixture": manifest, "concurrency": args.concurrency, "exact_semantics_equal": not mismatches,
              "sources_stable_within_arms": stable_sources, "mismatches": mismatches, "runs": results,
              "limitations": ["Synthetic filesystem preparation only; no network, Docker, registry, mkfs, or signatures.",
                "Each arm uses a fresh interpreter and fresh destination, with shared warmed fixture bytes. No cache dropping.",
                "Profiling runs are separate and excluded from timing comparisons. Four-thread mode cannot be profiled by this helper.",
                "Concurrent phases synchronize materialization then squash, with validation outside timed stages; this isolates shared-interpreter contention but does not reproduce asynchronous production phase overlap.",
                "batch_phases measures total wall/process CPU across threads; phases is mean per-publication wall time. Per-publication process CPU is unavailable when concurrent.",
                "OCI whiteouts and extended metadata remain unsupported by selective extraction; real overlay cases are tested separately.",
                "Unavailable privileged whiteout/opaque operations are explicitly skipped. Nonroot results do not qualify root-only semantics.",
                "Content, mode/type, uid/gid, nanosecond mtime, xattrs and hardlink equivalence are compared; read-dependent atime, ctime and numeric inode IDs are excluded. Generated whiteout mtime is normalized because the existing squasher creates it with the current time."]}
    (args.output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    if mismatches or not stable_sources:
        raise RuntimeError("semantic comparison or stable-source check failed; see summary.json")
    print("exact_semantics_equal=true", args.output / "summary.json")
    if not args.keep_fixture:
        shutil.rmtree(fixture_root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", type=Path)
    parser.add_argument("--candidate-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--modules", type=int, default=2000)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--work-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--concurrency", type=int, choices=(1, 4), default=1)
    parser.add_argument("--candidate-consume-private-diffs", action="store_true")
    parser.add_argument("--consume-private-diffs", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--keep-fixture", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--source-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--fixture-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--profile-worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.worker and (args.baseline_root is None or not 1 <= args.modules <= 5000
                            or not 1 <= args.repeats <= 10 or not 0 < args.timeout <= 600):
        parser.error("baseline-root is required; modules 1..5000, repeats 1..10, timeout 0..600")
    if args.concurrency > 1 and (args.profile or args.profile_worker):
        parser.error("run cProfile separately with concurrency 1")
    worker(args) if args.worker else run(args)


if __name__ == "__main__":
    main()
