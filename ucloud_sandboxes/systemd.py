from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import logging
import os
from pathlib import Path
import shutil
import stat
import subprocess
import time
from typing import Any, Callable, Mapping, Sequence
from urllib import request

from .config import DeploymentConfig
from .managed_registry import registry_maintenance_lock
from .models import parse_iso_datetime
from .registry_disk import (
    RegistryDiskUsage,
    read_registry_maintenance_state,
    record_registry_gc,
    registry_disk_usage,
)
from .registry_sweep import RegistrySweepResult, sweep_registry_blobs


_LOG = logging.getLogger(__name__)
CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
HealthWaiter = Callable[[str, str], None]
REGISTRY_IMAGE = "registry:3.1.1"
REGISTRY_CONFIG_PATH = "/etc/distribution/config.yml"
REGISTRY_S3_CHUNK_BYTES = 32 * 1024 * 1024
REGISTRY_SERVICE = "ucloud-sandbox-registry.service"
CHUNK_INDEX_SERVICE = "ucloud-sandbox-chunk-index.service"
# flock(1) in the prune unit locks the same file: "<path>.lock".
REGISTRY_MAINTENANCE_LOCK = Path("/run/lock/ucloud-sandbox-registry-maintenance")
REGISTRY_WRITER_LOCK = Path("/run/lock/ucloud-sandbox-registry-writer")
# Bounded wait for a running prune or GC instead of failing the unit.
REGISTRY_MAINTENANCE_WAIT_SECONDS = 1800.0
# Keep the filesystem authoritative for blob existence. Collection also
# replaces the registry process, so no cached descriptors survive deletion.
REGISTRY_BLOB_CACHE_ENV = "REGISTRY_STORAGE_CACHE_BLOBDESCRIPTOR"
REGISTRY_BLOB_CACHE_DISABLED = "none"


def registry_restart_marker(writer_lock: Path) -> Path:
    return Path(str(writer_lock) + ".stopped")


@contextmanager
def stopped_registry(*, runner=subprocess.run, writer_lock: Path = REGISTRY_WRITER_LOCK):
    """Exclude startup and prove the old writer exited before touching blobs."""
    marker = registry_restart_marker(writer_lock)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    try:
        runner(["systemctl", "stop", REGISTRY_SERVICE], check=True, text=True)
        with registry_maintenance_lock(writer_lock, timeout_seconds=REGISTRY_MAINTENANCE_WAIT_SECONDS):
            running = runner(["docker", "ps", "--all", "--filter", "name=^/ucloud-sandbox-registry$",
                              "--format", "{{.ID}}"], check=True, text=True, capture_output=True)
            if running.stdout.strip():
                raise RuntimeError("registry container still exists; refusing blob collection")
            yield
    finally:
        # Release the exclusive fence before starting its shared-lock holder.
        runner(["systemctl", "start", REGISTRY_SERVICE], check=True, text=True)
        marker.unlink(missing_ok=True)


def run_registry_process(config, *, writer_lock: Path = REGISTRY_WRITER_LOCK,
                         runner=subprocess.run, environ=None) -> int:
    """Hold the shared writer fence for the entire Distribution process lifetime."""
    lock = Path(str(writer_lock) + ".lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_SH)
        # Stale-container cleanup must also happen inside the startup fence.
        runner(["docker", "rm", "-f", "ucloud-sandbox-registry"], check=False, text=True,
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        registry_restart_marker(writer_lock).unlink(missing_ok=True)
        return runner(registry_run_command(config), check=False, text=True,
                      env=registry_process_environment(config, environ=environ)).returncode


def require_registry_mount(config: DeploymentConfig) -> None:
    if config.registry_store.kind != "filesystem":
        return
    mount_point = Path(config.registry_mount_point)
    if not mount_point.is_mount():
        raise RuntimeError(f"registry storage is not mounted at {mount_point}")


def run_registry_gc(
    *,
    config: DeploymentConfig,
    lock_file: Path,
    runner: CommandRunner = subprocess.run,
    environ: Mapping[str, str] | None = None,
    maintenance_state_file: Path | None = None,
    lock_timeout_seconds: float = REGISTRY_MAINTENANCE_WAIT_SECONDS,
    writer_lock: Path = REGISTRY_WRITER_LOCK,
) -> bool:
    """Offline Distribution GC: stops the registry for the whole run.

    Only S3 registry stores and explicit operator requests use this; the
    filesystem store uses the grace-aware quiescent collector.
    """

    with registry_maintenance_lock(lock_file, timeout_seconds=lock_timeout_seconds):
        # Distribution exits non-zero when its repository tree has never been
        # created. That is the normal state of a fresh deployment, not a GC
        # failure. Check under the maintenance fence so the empty-registry
        # decision cannot race another maintenance run.
        if config.registry_store.kind == "filesystem":
            repositories_dir = (
                config.registry_data_dir()
                / "docker"
                / "registry"
                / "v2"
                / "repositories"
            )
            if not repositories_dir.exists():
                return False
        environment = registry_process_environment(config, environ=environ)
        with stopped_registry(runner=runner, writer_lock=writer_lock):
            runner(
                registry_gc_command(config),
                check=True,
                text=True,
                env=environment,
            )
        if maintenance_state_file is not None:
            record_registry_gc(maintenance_state_file, kind="offline")
        return True


def run_registry_sweep_locked(
    *,
    config: DeploymentConfig,
    maintenance_state_file: Path | None = None,
    sweep: Callable[..., RegistrySweepResult] = sweep_registry_blobs,
    runner: CommandRunner = subprocess.run,
    writer_lock: Path = REGISTRY_WRITER_LOCK,
) -> RegistrySweepResult | None:
    """Quiescent filesystem collection; caller already holds the maintenance lock."""

    if config.registry_store.kind != "filesystem":
        return None
    with stopped_registry(runner=runner, writer_lock=writer_lock):
        result = sweep(config.registry_data_dir(), grace_seconds=config.registry_blob_grace_seconds,
                       writers_stopped=True)
    if maintenance_state_file is not None:
        record_registry_gc(
            maintenance_state_file,
            kind="quiescent",
            deleted_bytes=result.deleted_bytes,
        )
    return result


def run_registry_sweep(
    *,
    config: DeploymentConfig,
    lock_file: Path,
    maintenance_state_file: Path | None = None,
    lock_timeout_seconds: float = REGISTRY_MAINTENANCE_WAIT_SECONDS,
    sweep: Callable[..., RegistrySweepResult] = sweep_registry_blobs,
    runner: CommandRunner = subprocess.run,
    writer_lock: Path = REGISTRY_WRITER_LOCK,
) -> RegistrySweepResult | None:
    with registry_maintenance_lock(lock_file, timeout_seconds=lock_timeout_seconds):
        return run_registry_sweep_locked(
            config=config,
            maintenance_state_file=maintenance_state_file,
            sweep=sweep,
            runner=runner,
            writer_lock=writer_lock,
        )



def run_registry_pressure_cleanup(
    *,
    config: DeploymentConfig,
    lock_file: Path,
    prune: Callable[[bool], dict[str, Any]],
    gc: Callable[[], bool],
    usage: Callable[[], RegistryDiskUsage | None],
    maintenance_state_file: Path,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    lock_timeout_seconds: float = REGISTRY_MAINTENANCE_WAIT_SECONDS,
) -> dict[str, Any]:
    """Free registry space above the cleanup threshold.

    1. Prune by age and reference (``prune(False)``). Deleting manifests frees
       no bytes until the blob sweep (``gc``) removes their blobs, so a sweep
       follows when anything is awaiting it or the volume stays above the
       threshold, at most once per ``registry_disk_gc_interval_seconds``: a
       sweep reads every manifest and blob directory.
    2. Still above the threshold: evict least-recently-used managed images
       down to the target (``prune(True)``) and sweep at once. Eviction
       projects from measured usage, so garbage from step 1 is swept first;
       otherwise the projection would count it as live and evict too many.
    """

    before = usage()
    result: dict[str, Any] = {
        "action": "none",
        "registry_disk_before": before.to_dict() if before is not None else None,
        "gc_runs": 0,
    }
    if before is None:
        result["reason"] = "registry store has no local filesystem"
        return result
    if not before.cleanup_needed:
        return result
    _LOG.warning(
        "registry disk %s is %.1f%% full (cleanup threshold %.0f%%); pruning",
        before.path, before.used_percent, before.cleanup_percent,
    )

    def pending_gc() -> tuple[int, float | None]:
        state = read_registry_maintenance_state(maintenance_state_file)
        pending = state.get("deleted_since_gc")
        last_gc = parse_iso_datetime(str(state.get("last_gc_at") or ""))
        elapsed = (now() - last_gc).total_seconds() if last_gc is not None else None
        return (pending if isinstance(pending, int) else 0), elapsed

    def run_gc() -> None:
        if gc():
            result["gc_runs"] += 1

    with registry_maintenance_lock(lock_file, timeout_seconds=lock_timeout_seconds):
        # Another maintenance run may have freed space while this one waited.
        current = usage()
        if current is None or not current.cleanup_needed:
            result["reason"] = "space was freed while waiting for maintenance"
            return result
        plan = prune(False)
        result["action"] = "prune"
        result["deleted_manifests"] = int(plan.get("deleted_manifest_count") or 0)
        result["reference_retention"] = _reference_summary(plan)
        pending, elapsed = pending_gc()
        after_prune = usage()
        wanted = bool(pending or (after_prune is not None and after_prune.cleanup_needed))
        due = elapsed is None or elapsed >= config.registry_disk_gc_interval_seconds
        result["gc"] = {"wanted": wanted, "due": due, "seconds_since_last": elapsed}
        if wanted and due:
            run_gc()
        current = usage()
        if current is not None and current.cleanup_needed:
            if pending_gc()[0]:
                run_gc()
                current = usage()
        if current is not None and current.cleanup_needed:
            _LOG.warning(
                "registry disk %s is %.1f%% full after pruning; evicting "
                "least-recently-used managed images down to %.0f%%",
                current.path, current.used_percent, current.target_percent,
            )
            eviction = prune(True)
            evicted = int((eviction.get("lru_eviction") or {}).get("evicted_images") or 0)
            result["action"] = "evict"
            result["lru_eviction"] = {
                key: value
                for key, value in (eviction.get("lru_eviction") or {}).items()
                if key != "evict_sample"
            }
            result["deleted_manifests"] += int(eviction.get("deleted_manifest_count") or 0)
            if evicted or pending_gc()[0]:
                run_gc()
    after = usage()
    result["registry_disk_after"] = after.to_dict() if after is not None else None
    if after is not None and after.refusing_writes:
        _LOG.warning(
            "registry disk %s is still %.1f%% full after cleanup; the gateway "
            "keeps refusing image builds above %.0f%%",
            after.path, after.used_percent, after.refuse_percent,
        )
    return result


def _reference_summary(plan: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {key: item.get(key) for key in (
            "reason", "skipped", "deleted_manifests", "kept_manifests",
        )}
        for item in plan.get("reference_retention", {}).get("decisions", [])
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="UCloud systemd service helpers")
    subparsers = parser.add_subparsers(dest="command", required=True)
    registry_gc = subparsers.add_parser(
        "registry-gc",
        help=(
            "reclaim unreferenced registry blobs with registry writers stopped: filesystem "
            "store, offline Distribution GC for S3 or with --offline"
        ),
    )
    registry_gc.add_argument("--config", type=Path, required=True)
    registry_gc.add_argument(
        "--offline",
        action="store_true",
        help="stop the registry and run Distribution garbage-collect",
    )
    registry_pressure = subparsers.add_parser(
        "registry-pressure",
        help="prune, evict, and garbage collect above the registry disk cleanup threshold",
    )
    registry_pressure.add_argument("--config", type=Path, required=True)
    for maintenance in (registry_gc, registry_pressure):
        maintenance.add_argument(
            "--allow-service-interruption", action="store_true",
            help="explicit maintenance window: allow collection to stop the live registry",
        )
    registry = subparsers.add_parser(
        "registry",
        help="run the deployment Docker Distribution service",
    )
    registry.add_argument("--config", type=Path, required=True)
    subparsers.add_parser("registry-recover", help="restart a registry stopped by interrupted collection")
    reconcile = subparsers.add_parser(
        "gateway-reconcile",
        help="converge and health-check the common gateway services",
    )
    reconcile.add_argument("--config", type=Path, required=True)
    mirror = subparsers.add_parser("upstream-mirror", help="run one pull-through mirror (C2.15)")
    mirror.add_argument("--config", type=Path, required=True)
    mirror.add_argument("--upstream", required=True, help="configured upstream registry, e.g. docker.io")
    trim = subparsers.add_parser("upstream-mirror-trim", help="empty mirror caches above upstream_mirror.max_bytes")
    trim.add_argument("--config", type=Path, required=True)
    return parser


def wait_for_http(
    name: str,
    url: str,
    *,
    attempts: int = 60,
    delay_seconds: float = 1.0,
) -> None:
    last_error: BaseException | None = None
    for _attempt in range(max(1, attempts)):
        try:
            with request.urlopen(url, timeout=2.0) as response:
                if 200 <= int(response.status) < 300:
                    return
                last_error = RuntimeError(f"HTTP {response.status}")
        except Exception as exc:
            last_error = exc
        time.sleep(max(0.0, delay_seconds))
    raise RuntimeError(f"timed out waiting for {name} at {url}: {last_error}")


def reconcile_gateway_services(
    *,
    config: DeploymentConfig,
    runner: CommandRunner = subprocess.run,
    wait_for: HealthWaiter = wait_for_http,
) -> None:
    """Converge the provider-independent gateway service graph once."""

    def systemctl(*arguments: str, check: bool = True) -> None:
        runner(["systemctl", *arguments], check=check, text=True)

    systemctl("daemon-reload")
    systemctl("enable", "ucloud-sandbox-registry.service")
    systemctl("enable", "--now", "ucloud-sandbox-registry-prune.timer")
    systemctl("enable", "--now", "ucloud-sandbox-registry-gc.timer")
    systemctl("enable", "--now", "ucloud-sandbox-registry-pressure.timer")
    if config.snapshot_store.kind == "s3":
        systemctl("enable", "--now", "ucloud-sandbox-snapshot-gc.timer")
    else:
        systemctl(
            "disable",
            "--now",
            "ucloud-sandbox-snapshot-gc.timer",
            check=False,
        )
        systemctl(
            "reset-failed",
            "ucloud-sandbox-snapshot-gc.service",
            check=False,
        )
    for service in (
        "ucloud-sandbox-placement.service",
        "ucloud-sandbox-gateway.service",
        "ucloud-sandbox-relay.service",
        "ucloud-sandbox-autoscaler.service",
        "ucloud-sandbox-builds.service",  # Exits 78 while sandbox_builds is off.
    ):
        systemctl("enable", service)
    # ucloud-chunk-index (decision 5: on the gateway until C2.6), only once
    # immutable_environments.chunk_store is configured.
    environments = config.immutable_environments
    chunk_store = environments.chunk_store if environments is not None else None
    # C2.6: with the index on the store node the gateway's unit still runs
    # once, to make the tokens, and exits 78.
    index_here = chunk_store is not None and not (chunk_store.store_node and chunk_store.store_node.serve_index)
    if chunk_store is None:
        systemctl("disable", "--now", CHUNK_INDEX_SERVICE, check=False)
    else:
        systemctl("enable", CHUNK_INDEX_SERVICE)

    # A removed or disabled mirror instance stays stopped: at boot its helper
    # finds no configured upstream and exits with RestartPreventExitStatus.
    systemctl("stop", UPSTREAM_MIRROR_UNIT.format("*"), check=False)
    mirror = config.upstream_mirror
    systemctl(*(("enable", "--now") if mirror else ("disable", "--now")),
              "ucloud-sandbox-upstream-mirror-trim.timer", check=mirror is not None)
    for upstream in mirror.upstreams if mirror else ():
        systemctl("enable", "--now", UPSTREAM_MIRROR_UNIT.format(upstream.registry))
        wait_for(f"upstream mirror {upstream.registry}", mirror.local_url(upstream) + "/v2/")
    systemctl("restart", "ucloud-sandbox-registry.service")
    wait_for("registry", f"http://127.0.0.1:{config.registry_port}/v2/")
    for service in (
        "ucloud-sandbox-placement.service",
        "ucloud-sandbox-gateway.service",
        "ucloud-sandbox-relay.service",
        "ucloud-sandbox-autoscaler.service",
        "ucloud-sandbox-builds.service",
    ):
        systemctl("restart", service)
    wait_for("gateway", f"http://127.0.0.1:{config.gateway_port}/healthz")
    wait_for("relay", f"http://127.0.0.1:{config.relay_port}/healthz")
    if chunk_store is not None:
        systemctl("restart", CHUNK_INDEX_SERVICE)
        if index_here:
            wait_for("chunk index", chunk_store.index_url.rstrip("/") + "/healthz")


def registry_run_command(config: DeploymentConfig) -> list[str]:
    """Build the registry command without Docker's kernel port-publish path."""

    return [
        "docker",
        "run",
        "--rm",
        "--name",
        "ucloud-sandbox-registry",
        "--network",
        "host",
        *_registry_storage_docker_args(config),
        *_registry_forwarded_environment_args(config),
        REGISTRY_IMAGE,
    ]


def registry_gc_command(config: DeploymentConfig) -> list[str]:
    return [
        "docker",
        "run",
        "--rm",
        *_registry_storage_docker_args(config),
        *_registry_forwarded_environment_args(config),
        REGISTRY_IMAGE,
        "garbage-collect",
        "--delete-untagged",
        REGISTRY_CONFIG_PATH,
    ]


def registry_process_environment(
    config: DeploymentConfig,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve registry settings without exposing S3 secrets in process args."""

    result = dict(os.environ if environ is None else environ)
    result["REGISTRY_STORAGE_DELETE_ENABLED"] = "true"
    result["REGISTRY_HTTP_ADDR"] = f"0.0.0.0:{config.registry_port}"
    result["REGISTRY_HTTP_DEBUG_ADDR"] = "127.0.0.1:5001"
    result["REGISTRY_LOG_LEVEL"] = "info"
    result["OTEL_TRACES_EXPORTER"] = "none"
    store = config.registry_store
    result["REGISTRY_STORAGE"] = store.kind
    if store.kind == "filesystem":
        result["REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY"] = "/var/lib/registry"
        result[REGISTRY_BLOB_CACHE_ENV] = REGISTRY_BLOB_CACHE_DISABLED
        return result
    access_key = result.get(store.access_key_id_env, "").strip()
    secret_key = result.get(store.secret_access_key_env, "").strip()
    if not access_key or not secret_key:
        raise RuntimeError(
            "S3 registry credentials are missing from "
            f"{store.access_key_id_env} and {store.secret_access_key_env}"
        )
    result.update(
        {
            "REGISTRY_STORAGE_S3_ACCESSKEY": access_key,
            "REGISTRY_STORAGE_S3_SECRETKEY": secret_key,
            "REGISTRY_STORAGE_S3_REGION": store.region,
            "REGISTRY_STORAGE_S3_REGIONENDPOINT": store.endpoint,
            "REGISTRY_STORAGE_S3_FORCEPATHSTYLE": str(
                store.force_path_style
            ).lower(),
            "REGISTRY_STORAGE_S3_BUCKET": store.bucket,
            "REGISTRY_STORAGE_S3_ROOTDIRECTORY": store.prefix,
            "REGISTRY_STORAGE_S3_SECURE": str(
                store.endpoint.startswith("https://")
            ).lower(),
            "REGISTRY_STORAGE_S3_V4AUTH": "true",
            "REGISTRY_STORAGE_S3_CHUNKSIZE": str(REGISTRY_S3_CHUNK_BYTES),
        }
    )
    return result


UPSTREAM_MIRROR_UNIT = "ucloud-sandbox-upstream-mirror@{}.service"
UPSTREAM_MIRROR_UNCONFIGURED = 78  # EX_CONFIG: the unit neither fails nor restarts
_MIRROR_CREDENTIALS = ("REGISTRY_PROXY_USERNAME", "REGISTRY_PROXY_PASSWORD")


def read_mirror_credentials(path: Path) -> dict[str, str]:
    """Parse a root-only KEY=VALUE file; no error message ever includes a value."""

    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise RuntimeError(f"upstream mirror credentials {path} must be a regular owner-only (0600) file")
        text = stream.read(65536)
    values: dict[str, str] = {}
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or key.strip() not in _MIRROR_CREDENTIALS or key.strip() in values:
            raise RuntimeError(f"upstream mirror credentials {path}:{number} must set {' or '.join(_MIRROR_CREDENTIALS)} once")
        values[key.strip()] = value.strip()
    if set(values) != set(_MIRROR_CREDENTIALS) or not all(values.values()):
        raise RuntimeError(f"upstream mirror credentials {path} need non-empty {' and '.join(_MIRROR_CREDENTIALS)}")
    return values


def upstream_mirror_command(config: DeploymentConfig, upstream) -> list[str]:
    names = ("REGISTRY_HTTP_ADDR", "REGISTRY_LOG_LEVEL", "OTEL_TRACES_EXPORTER", "REGISTRY_STORAGE",
             "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", REGISTRY_BLOB_CACHE_ENV,
             "REGISTRY_PROXY_REMOTEURL", "REGISTRY_PROXY_TTL",
             *(_MIRROR_CREDENTIALS if upstream.credentials_file else ()))
    return ["docker", "run", "--rm", "--name", f"ucloud-sandbox-upstream-mirror-{upstream.registry}",
            "--network", "host", "-v", f"{config.upstream_mirror.storage_dir(upstream)}:/var/lib/registry",
            *[item for name in names for item in ("-e", name)], REGISTRY_IMAGE]


def upstream_mirror_environment(config: DeploymentConfig, upstream, *,
                                environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Distribution in proxy mode; credentials reach Docker only by environment."""

    mirror = config.upstream_mirror
    result = {k: v for k, v in (os.environ if environ is None else environ).items() if k not in _MIRROR_CREDENTIALS}
    result.update({
        "REGISTRY_HTTP_ADDR": f"{mirror.listen_address}:{upstream.port}", "REGISTRY_LOG_LEVEL": "info",
        "OTEL_TRACES_EXPORTER": "none", "REGISTRY_STORAGE": "filesystem",
        "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY": "/var/lib/registry",
        # TTL expiry deletes blobs underneath; never answer from a stale descriptor.
        REGISTRY_BLOB_CACHE_ENV: REGISTRY_BLOB_CACHE_DISABLED,
        "REGISTRY_PROXY_REMOTEURL": upstream.remote_url, "REGISTRY_PROXY_TTL": f"{mirror.ttl_hours}h",
    })
    if upstream.credentials_file:
        result.update(read_mirror_credentials(Path(upstream.credentials_file)))
    return result


def run_upstream_mirror(config: DeploymentConfig, registry: str, *, runner=subprocess.run, environ=None) -> int:
    mirror = config.upstream_mirror
    upstream = next((u for u in mirror.upstreams if u.registry == registry), None) if mirror else None
    if upstream is None:
        print(f"no upstream mirror is configured for {registry}; staying stopped")
        return UPSTREAM_MIRROR_UNCONFIGURED
    require_registry_mount(config)
    mirror.storage_dir(upstream).mkdir(parents=True, exist_ok=True)
    name = f"ucloud-sandbox-upstream-mirror-{registry}"
    runner(["docker", "rm", "-f", name], check=False, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return runner(upstream_mirror_command(config, upstream), check=False, text=True,
                  env=upstream_mirror_environment(config, upstream, environ=environ)).returncode


def _tree_bytes(path: Path) -> int:
    total = 0
    for directory, _subdirectories, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(directory, name)).st_blocks * 512
            except FileNotFoundError:
                pass
    return total


def trim_upstream_mirrors(config: DeploymentConfig, *, runner=subprocess.run, measure=_tree_bytes) -> dict[str, Any]:
    """Enforce max_bytes by emptying the largest caches: content is refetchable.

    Distribution has no size bound, only the TTL. A stopped mirror makes
    BuildKit and Docker fall back to the upstream until it restarts.
    """

    mirror = config.upstream_mirror
    if mirror is None:
        return {"action": "none", "reason": "upstream mirror disabled"}
    sizes = {u.registry: measure(mirror.storage_dir(u)) for u in mirror.upstreams}
    total, purged = sum(sizes.values()), []
    for upstream in sorted(mirror.upstreams, key=lambda u: -sizes[u.registry]):
        if total <= mirror.max_bytes:
            break
        unit = UPSTREAM_MIRROR_UNIT.format(upstream.registry)
        runner(["systemctl", "stop", unit], check=True, text=True)
        try:
            if mirror.storage_dir(upstream).exists():
                shutil.rmtree(mirror.storage_dir(upstream))
        finally:
            runner(["systemctl", "start", unit], check=True, text=True)
        total -= sizes[upstream.registry]
        purged.append(upstream.registry)
    return {"bytes": sizes, "max_bytes": mirror.max_bytes, "purged": purged}


def _registry_storage_docker_args(config: DeploymentConfig) -> list[str]:
    if config.registry_store.kind == "filesystem":
        return ["-v", f"{config.registry_data_dir()}:/var/lib/registry"]
    return []


def _registry_forwarded_environment_args(config: DeploymentConfig) -> list[str]:
    names = [
        "REGISTRY_STORAGE_DELETE_ENABLED",
        "REGISTRY_HTTP_ADDR",
        "REGISTRY_HTTP_DEBUG_ADDR",
        "REGISTRY_LOG_LEVEL",
        "OTEL_TRACES_EXPORTER",
        "REGISTRY_STORAGE",
    ]
    if config.registry_store.kind == "filesystem":
        names.extend(("REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", REGISTRY_BLOB_CACHE_ENV))
    else:
        names.extend(
            (
                "REGISTRY_STORAGE_S3_ACCESSKEY",
                "REGISTRY_STORAGE_S3_SECRETKEY",
                "REGISTRY_STORAGE_S3_REGION",
                "REGISTRY_STORAGE_S3_REGIONENDPOINT",
                "REGISTRY_STORAGE_S3_FORCEPATHSTYLE",
                "REGISTRY_STORAGE_S3_BUCKET",
                "REGISTRY_STORAGE_S3_ROOTDIRECTORY",
                "REGISTRY_STORAGE_S3_SECURE",
                "REGISTRY_STORAGE_S3_V4AUTH",
                "REGISTRY_STORAGE_S3_CHUNKSIZE",
            )
        )
    return [item for name in names for item in ("-e", name)]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "registry-recover":
        if registry_restart_marker(REGISTRY_WRITER_LOCK).exists():
            subprocess.run(["systemctl", "start", "--no-block", REGISTRY_SERVICE], check=True, text=True)
        return 0
    config = DeploymentConfig.from_file(args.config)
    if args.command in {"registry-gc", "registry-pressure"}:
        require_registry_mount(config)
        # A timer is not a maintenance window. Even a read-only mark phase
        # fences Distribution and interrupts reads/uploads for the full scan.
        # Keep online age/reference pruning in its separate prune unit; don't
        # evict live artifacts when physical reclamation cannot follow safely.
        allowed = args.allow_service_interruption or getattr(args, "offline", False)
        if not allowed:
            disk = registry_disk_usage(config)
            print(json.dumps({
                "action": "deferred" if args.command == "registry-gc" or (disk and disk.cleanup_needed) else "none",
                "reason": "physical collection requires an explicit maintenance window",
                "registry_disk": disk.to_dict() if disk else None,
            }, sort_keys=True))
            return 0
    if args.command == "registry-gc":
        require_registry_mount(config)
        if args.offline or config.registry_store.kind != "filesystem":
            run_registry_gc(
                config=config,
                lock_file=REGISTRY_MAINTENANCE_LOCK,
                maintenance_state_file=config.registry_maintenance_state_file(),
            )
            return 0
        sweep = run_registry_sweep(
            config=config,
            lock_file=REGISTRY_MAINTENANCE_LOCK,
            maintenance_state_file=config.registry_maintenance_state_file(),
        )
        print(json.dumps(sweep.to_dict() if sweep else None, sort_keys=True))
        return 0
    if args.command == "registry-pressure":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        require_registry_mount(config)

        def prune(evict_lru: bool) -> dict[str, Any]:
            from .cli import run_registry_prune

            return run_registry_prune(config, execute=True, evict_lru=evict_lru)

        result = run_registry_pressure_cleanup(
            config=config,
            lock_file=REGISTRY_MAINTENANCE_LOCK,
            prune=prune,
            gc=lambda: run_registry_sweep_locked(
                config=config,
                maintenance_state_file=config.registry_maintenance_state_file(),
            ) is not None,
            usage=lambda: registry_disk_usage(config),
            maintenance_state_file=config.registry_maintenance_state_file(),
        )
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    if args.command == "registry":
        require_registry_mount(config)
        if config.registry_store.kind == "filesystem":
            config.registry_data_dir().mkdir(parents=True, exist_ok=True)
        return run_registry_process(config)
    if args.command == "gateway-reconcile":
        reconcile_gateway_services(config=config)
        return 0
    if args.command == "upstream-mirror":
        return run_upstream_mirror(config, args.upstream)
    if args.command == "upstream-mirror-trim":
        print(json.dumps(trim_upstream_mirrors(config), sort_keys=True))
        return 0
    raise ValueError(f"unsupported systemd helper: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
