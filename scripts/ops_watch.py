#!/usr/bin/env python3
"""Check the control plane once and alert on state changes (run by a 1-minute timer).

Each check is ok or failing with a detail. A check alerts when it has failed
twice in a row, again every --remind-hours while it keeps failing, and once
when it recovers. Alerts go to the journal and, when /etc/ucloud-sandboxes/
alerts.env sets ALERT_WEBHOOK_URL, as a Slack-compatible {"text": ...} POST.
The last result is in --state for dashboards and operators. Run as root on the
gateway; it sends and prints no credentials.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.request

ETC = Path("/etc/ucloud-sandboxes")
SERVICES = ("ucloud-sandbox-gateway", "ucloud-sandbox-relay", "ucloud-sandbox-placement",
            "ucloud-sandbox-autoscaler", "ucloud-sandbox-registry", "postgresql")


def fetch(url: str, timeout: float = 10) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read(1 << 20)
    except urllib.error.HTTPError as error:
        return error.code, b""


def pressure(resource: str) -> dict[str, float]:
    values = {}
    for line in Path(f"/proc/pressure/{resource}").read_text().splitlines():
        kind, *fields = line.split()
        values[kind] = float(dict(field.split("=") for field in fields)["avg60"])
    return values


def newest_age_hours(directory: Path, pattern: str) -> float | None:
    found = sorted(directory.glob(pattern))
    return (time.time() - found[-1].stat().st_mtime) / 3600 if found else None


def checks(args, config) -> dict[str, tuple[bool, str]]:
    results: dict[str, tuple[bool, str]] = {}

    def guard(name, function):
        try:
            results[name] = function()
        except Exception as error:  # noqa: BLE001 - a broken probe is a failing check
            results[name] = (False, f"{type(error).__name__}: {error}"[:300])

    def http_ok(url):
        status, body = fetch(url)
        return status == 200, f"HTTP {status} {body[:120].decode(errors='replace')}".strip()

    guard("services", lambda: (lambda down: (not down, "down: " + ", ".join(down) if down else "all active"))(
        [unit for unit in SERVICES if subprocess.run(["systemctl", "is-active", "--quiet", unit]).returncode]))
    guard("gateway", lambda: http_ok(f"http://127.0.0.1:{config.gateway_port}/healthz"))
    guard("relay", lambda: http_ok(f"http://127.0.0.1:{config.relay_port}/healthz"))
    guard("registry", lambda: http_ok(f"{config.registry_url}/v2/"))
    guard("postgres", lambda: (lambda r: (r.returncode == 0, r.stdout.strip()[:120]))(
        subprocess.run(["pg_isready", "-q", "-h", "/var/run/postgresql"], capture_output=True, text=True)))

    def gateway_pressure():
        cpu, memory, io = pressure("cpu")["some"], pressure("memory")["full"], pressure("io")["full"]
        ok = cpu < args.cpu_some and memory < args.memory_full and io < args.io_full
        return ok, f"avg60 cpu some {cpu:.1f}%, memory full {memory:.1f}%, io full {io:.1f}%"
    guard("gateway_pressure", gateway_pressure)

    def disks():
        root = shutil.disk_usage("/")
        free = root.free / root.total * 100
        probe = Path(config.registry_store.data_root).parent / ".ops-watch-probe"
        probe.write_text(str(time.time()))
        probe.unlink()
        return free >= args.root_free_percent, f"root {free:.0f}% free, project drive writable"
    guard("disks", disks)

    store = config.immutable_environments.chunk_store if config.immutable_environments else None
    if store is not None and store.store_node is not None:
        def store_node():
            status, body = fetch(store.store_node.url.rstrip("/") + "/healthz")
            health = json.loads(body or b"{}")
            ok = status == 200 and health.get("ok") is True and not health.get("verify_failures")
            filled = health.get("bytes", 0) / max(1, health.get("budget", 1)) * 100
            return ok, (f"HTTP {status}, {filled:.0f}% of budget, verify_failures {health.get('verify_failures')}, "
                        f"full_refusals {health.get('full_refusals')}")
        guard("store_node", store_node)
        guard("chunk_index", lambda: http_ok(store.index_url.rstrip("/") + "/healthz"))

    def backups():
        stale = []
        for part, pattern, hours in (("gateway", "gateway-*.tar.gz", 2.5), ("chunk-index", "chunk-index-*.sqlite.gz", 13)):
            age = newest_age_hours(args.backups / part, pattern)
            if age is None or age > hours:
                stale.append(f"{part} {'none' if age is None else f'{age:.1f} h old'}")
        return not stale, "stale: " + ", ".join(stale) if stale else "fresh"
    guard("backups", backups)

    def node_init():
        log = subprocess.run(["journalctl", "-u", "ucloud-sandbox-autoscaler", "--since", "-15min", "--no-pager",
                              "-o", "cat"], capture_output=True, text=True, timeout=60).stdout
        failed = sum(line.startswith("Init failed for") for line in log.splitlines())
        succeeded = sum(line.startswith("Init succeeded for") for line in log.splitlines())
        return failed < args.init_failures, f"last 15 min: {failed} init failures, {succeeded} successes"
    guard("node_init", node_init)

    for name, url in (("public_gateway", args.public_gateway), ("public_relay", args.public_relay)):
        if url:
            # /healthz is unauthenticated: no credential leaves the gateway.
            guard(name, lambda url=url: http_ok(url.rstrip("/") + "/healthz"))
    return results


def notify(webhook: str | None, text: str) -> None:
    print(text, flush=True)
    if webhook:
        body = json.dumps({"text": text}).encode()
        request = urllib.request.Request(webhook, data=body, headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(request, timeout=15).close()
        except Exception as error:  # noqa: BLE001 - the journal still has the alert
            print(f"alert webhook failed: {type(error).__name__}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", type=Path, default=ETC / "deployment.json")
    parser.add_argument("--state", type=Path, default=Path("/var/lib/ucloud-sandboxes/ops-watch.json"))
    parser.add_argument("--backups", type=Path, required=True)
    parser.add_argument("--public-gateway", default="")
    parser.add_argument("--public-relay", default="")
    parser.add_argument("--cpu-some", type=float, default=60.0)
    parser.add_argument("--memory-full", type=float, default=5.0)
    parser.add_argument("--io-full", type=float, default=10.0)
    parser.add_argument("--root-free-percent", type=float, default=15.0)
    parser.add_argument("--init-failures", type=int, default=3)
    parser.add_argument("--remind-hours", type=float, default=6.0)
    args = parser.parse_args()
    from ucloud_sandboxes.config import DeploymentConfig
    config = DeploymentConfig.from_file(args.config)
    webhook = None
    alerts_env = ETC / "alerts.env"
    if alerts_env.exists():
        for line in alerts_env.read_text().splitlines():
            key, _, value = line.partition("=")
            if key.strip() == "ALERT_WEBHOOK_URL" and value.strip():
                webhook = value.strip()
    previous = json.loads(args.state.read_text()) if args.state.exists() else {"checks": {}}
    now = time.time()
    current = {}
    for name, (ok, detail) in checks(args, config).items():
        before = previous["checks"].get(name, {})
        failures = 0 if ok else before.get("failures", 0) + 1
        alerted = before.get("alerted_at")
        entry = {"ok": ok, "detail": detail, "failures": failures, "alerted_at": alerted}
        prefix = f"[{config.deployment_id}] {name}"
        if not ok and failures >= 2 and (alerted is None or now - alerted >= args.remind_hours * 3600):
            notify(webhook, f"{prefix} FAILING ({failures} checks): {detail}")
            entry["alerted_at"] = now
        elif ok and alerted is not None:
            notify(webhook, f"{prefix} recovered: {detail}")
            entry["alerted_at"] = None
        current[name] = entry
    state = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "checks": current,
             "failing": sorted(name for name, entry in current.items() if not entry["ok"])}
    temporary = args.state.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    os.chmod(temporary, 0o644)
    temporary.replace(args.state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
