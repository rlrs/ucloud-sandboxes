#!/usr/bin/env python3
"""Run a real Verifiers harness through relay-driven UCloud park/wake."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
from pathlib import Path
from pathlib import PurePosixPath
import shlex
import time
from typing import Any
from urllib import error, request
from uuid import uuid4

from ucloud_sandboxes_sdk import (
    AsyncRelayWorkerClient,
    AsyncSandboxClient,
    Image,
    SandboxApiError,
    SandboxSecuritySpec,
    http_tunnel_url,
)
from verifiers.v1.clients import Client, ModelContext
from verifiers.v1.configs.agent import AgentConfig
from verifiers.v1.harnesses.null import harness as null_harness_module
from verifiers.v1.harnesses.null.harness import NullHarness, NullHarnessConfig
from verifiers.v1.interception.server import InterceptionServer
from verifiers.v1.rollout import RolloutRun
from verifiers.v1.runtimes import (
    ProgramResult,
    SubprocessConfig,
    SubprocessRuntime,
)
from verifiers.v1.task import Task, TaskData
from verifiers.v1.types import SamplingConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8090")
    parser.add_argument(
        "--relay-url",
        default="https://app-sandboxes-relay.cloud.sdu.dk",
    )
    parser.add_argument(
        "--sandbox-relay-url",
        help=(
            "Relay URL reachable from inside the sandbox. Defaults to "
            "--relay-url; isolated canaries may use a private-network address."
        ),
    )
    parser.add_argument(
        "--gateway-token-file",
        type=Path,
        default=Path("/work/data/ucloud-sandboxes/state/gateway-token"),
    )
    parser.add_argument(
        "--relay-worker-token-file",
        type=Path,
        default=Path("/work/data/ucloud-sandboxes/state/relay-worker-token"),
    )
    parser.add_argument(
        "--image",
        default="ghcr.io/astral-sh/uv:python3.13-bookworm-slim",
    )
    parser.add_argument("--park-timeout-seconds", type=float, default=60)
    parser.add_argument("--migration-timeout-seconds", type=float, default=300)
    parser.add_argument("--sandbox-timeout-seconds", type=float, default=600)
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Migrate the parked sandbox before committing the relay response.",
    )
    return parser.parse_args()


def token(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"empty token file: {path}")
    return value


def sandbox_state(record: dict[str, Any] | None) -> str:
    if not record:
        return "missing"
    for container in (record, record.get("status"), record.get("sandbox")):
        if isinstance(container, dict):
            value = container.get("state") or container.get("status")
            if isinstance(value, str) and value:
                return value.lower()
    return "unknown"


def sandbox_generation(record: dict[str, Any]) -> int:
    for container in (record, record.get("status"), record.get("sandbox")):
        if not isinstance(container, dict):
            continue
        value = container.get("generation")
        if isinstance(value, int) and value > 0:
            return value
    raise RuntimeError(
        "gateway sandbox record did not contain a positive generation: "
        + json.dumps(record, sort_keys=True)
    )


def configure_retryable_null_harness_transport() -> None:
    """Give this upstream harness the transport contract required by parking.

    The pinned Null harness uses an infinite HTTP read timeout. Cross-node
    migration deliberately closes its checkpointed socket, so the model client
    needs a bounded read timeout and ordinary transport retries. The relay's
    transport-epoch handshake makes the implicit logical request reattachable;
    this qualification intentionally supplies no UCloud request-ID header.
    """

    source = null_harness_module.PROGRAM_SOURCE
    timeout_old = "timeout=httpx.Timeout(None, connect=5.0),"
    timeout_new = (
        "timeout=httpx.Timeout(10.0, connect=5.0),\n"
        "        max_retries=3,"
    )
    for marker in (timeout_old,):
        if source.count(marker) != 1:
            raise RuntimeError(
                f"pinned Null harness transport marker changed: {marker!r}"
            )
    null_harness_module.PROGRAM_SOURCE = source.replace(timeout_old, timeout_new)


class CountingClient(Client):
    def __init__(self) -> None:
        self.calls = 0

    async def get_response(
        self,
        dialect,
        body,
        model,
        sampling_args,
        **_kwargs,
    ):
        self.calls += 1
        raw = {
            "id": "chatcmpl-ucloud-live-park",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "relay-live-park-ok",
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 4,
                "completion_tokens": 4,
                "total_tokens": 8,
            },
        }
        response = dialect.parse_response(dialect.validate_response(raw))
        response.raw = raw
        return response


class UCloudVerifiersRuntime(SubprocessRuntime):
    """Verifiers Runtime backed by one checkpoint-owned SDK primary job."""

    def __init__(
        self,
        config: SubprocessConfig,
        *,
        gateway_url: str,
        gateway_token: str,
        image: str,
        sandbox_id: str,
        timeout_seconds: float,
    ) -> None:
        super().__init__(config, name=sandbox_id)
        self.client = AsyncSandboxClient(
            gateway_url,
            api_token=gateway_token,
            timeout_seconds=30,
        )
        self.image = image
        self.sandbox_id = sandbox_id
        self.timeout_seconds = timeout_seconds
        self.handle = None
        self.generation = 0
        self.relay_tunnel_url = ""
        self.last_program_result: ProgramResult | None = None
        self.job = None
        self.lifecycle_states: list[str] = []

    async def start(self) -> None:
        await self.client.__aenter__()
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                self.handle = await self.client.create_sandbox(
                    id=self.sandbox_id,
                    image=Image.from_registry(self.image),
                    memory_mb=2048,
                    cpus=0.5,
                    disk_mb=4096,
                    network="bridge",
                    ttl_seconds=900,
                    parkable=True,
                    managed_process=True,
                    security=SandboxSecuritySpec(user="0:0"),
                    request_timeout_seconds=self.timeout_seconds,
                )
                break
            except SandboxApiError as exc:
                if exc.status_code != 503 or exc.retryable is not True:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "timed out waiting for autoscaled sandbox capacity"
                    ) from exc
                await asyncio.sleep(min(2.0, remaining))
        record = await self.client.get_sandbox(self.sandbox_id)
        if record is None:
            raise RuntimeError("created sandbox is absent from gateway inventory")
        generation_sources = (
            self.handle.record,
            self.handle.create_response,
            record,
        )
        for source in generation_sources:
            try:
                self.generation = sandbox_generation(source)
                break
            except RuntimeError:
                continue
        if self.generation < 1:
            raise RuntimeError(
                "SDK create response and gateway inventory omitted sandbox generation"
            )
        self.info.id = self.sandbox_id

    def host_url(self, _url: str) -> str:
        if not self.relay_tunnel_url:
            raise RuntimeError("relay tunnel URL was not configured")
        return self.relay_tunnel_url

    async def run(self, argv: list[str], env: dict[str, str]) -> ProgramResult:
        if self.handle is None:
            raise RuntimeError("sandbox runtime is not started")
        result = await self.handle.exec(
            argv,
            env=env,
            working_dir="/workspace",
            timeout_seconds=self.timeout_seconds,
        )
        return ProgramResult(
            exit_code=result.exit_code if result.exit_code is not None else 1,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    async def prepare_uv_script(
        self,
        script: str | bytes,
        env: dict[str, str] | None = None,
    ) -> list[str]:
        """Prepare a PEP 723 script using the uv guaranteed by this image.

        Verifiers' generic bootstrap unconditionally attempts to upgrade uv,
        even after selecting an official uv image. That path can invoke apt and
        is both unnecessary and hostile to tightly capability-bounded
        sandboxes. The UCloud qualification image makes uv part of the runtime
        contract, while retaining Verifiers' content-addressed environment.
        """
        data = script.encode() if isinstance(script, str) else script
        digest = hashlib.sha256(data).hexdigest()
        path = f"/tmp/vf-scripts/{digest}.py"
        if digest not in self._uv_interpreters:
            async with self._uv_script_locks.setdefault(digest, asyncio.Lock()):
                if digest not in self._uv_interpreters:
                    temporary = f"{path}.{uuid4().hex}.tmp"
                    await self.write(temporary, data)
                    command = (
                        f"mv -f {shlex.quote(temporary)} {shlex.quote(path)} "
                        "&& command -v uv >/dev/null "
                        f"&& uv sync --script {shlex.quote(path)} -q --no-config "
                        f"&& uv python find --script {shlex.quote(path)} --no-config"
                    )
                    result = await self.run(["sh", "-c", command], env or {})
                    if result.exit_code != 0:
                        raise RuntimeError(
                            "failed to prepare uv script: "
                            f"{result.stderr.strip()[-2000:]}"
                        )
                    self._uv_interpreters[digest] = result.stdout.strip().splitlines()[
                        -1
                    ]
        interpreter = self._uv_interpreters[digest]
        venv = str(PurePosixPath(interpreter).parent.parent)
        command = (
            'export VIRTUAL_ENV="$1" PATH="${1}/bin:$HOME/.local/bin:$PATH" '
            'UV_INSTALL_DIR="$HOME/.local/bin" UV_RUN_RECURSION_DEPTH=1; '
            'shift; exec "$@"'
        )
        return [
            "sh",
            "-c",
            command,
            "uv-script",
            venv,
            interpreter,
            path,
        ]

    async def run_program(
        self,
        argv: list[str],
        env: dict[str, str],
    ) -> ProgramResult:
        if self.handle is None:
            raise RuntimeError("sandbox runtime is not started")
        self.job = await self.handle.start_job(
            argv,
            job_id=f"verifiers-{uuid4().hex}",
            env=env,
            working_dir="/workspace",
        )

        # Both inventory and job status are control-plane reads while parked.
        # Logs are fetched only after completion, when the relay has restored
        # the sandbox and the supervisor has durably published terminal state.
        deadline = time.monotonic() + self.timeout_seconds
        saw_parked = False
        last_state = "unknown"
        while time.monotonic() < deadline:
            record = await self.client.get_sandbox(self.sandbox_id)
            last_state = sandbox_state(record)
            if last_state == "parked":
                saw_parked = True
            job = await self.job.refresh()
            if job.terminal:
                stdout, stderr = await asyncio.gather(
                    self.job.logs("stdout"),
                    self.job.logs("stderr"),
                )
                result = ProgramResult(
                    exit_code=job.exit_code if job.exit_code is not None else 1,
                    stdout=stdout.data.decode("utf-8", errors="replace"),
                    stderr=stderr.data.decode("utf-8", errors="replace"),
                )
                self.last_program_result = result
                return result
            if not self.lifecycle_states or self.lifecycle_states[-1] != last_state:
                self.lifecycle_states.append(last_state)
            await asyncio.sleep(0.1)
        raise TimeoutError(
            "durable harness job did not finish after a real park/wake cycle; "
            f"saw_parked={saw_parked}, last_state={last_state}"
        )

    async def read(self, path: str) -> bytes:
        if self.handle is None:
            raise RuntimeError("sandbox runtime is not started")
        return await self.handle.download_file(path)

    async def write(self, path: str, data: bytes) -> None:
        if self.handle is None:
            raise RuntimeError("sandbox runtime is not started")
        await self.handle.upload_file(path, data)

    async def teardown(self) -> None:
        try:
            if self.handle is not None:
                with contextlib.suppress(Exception):
                    await self.handle.delete()
                self.handle = None
        finally:
            await self.client.close()


async def wait_for_parked(
    runtime: UCloudVerifiersRuntime,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last: dict[str, Any] | None = None
    while asyncio.get_running_loop().time() < deadline:
        last = await runtime.client.get_sandbox(runtime.sandbox_id)
        if sandbox_state(last) == "parked":
            assert last is not None
            return last
        await asyncio.sleep(0.1)
    raise TimeoutError(
        f"sandbox did not become parked; last state={sandbox_state(last)}"
    )


def sandbox_owner(record: dict[str, Any] | None) -> tuple[str, str, str]:
    if record is None:
        return ("", "", "")
    for container in (
        record,
        record.get("node"),
        record.get("status"),
        record.get("sandbox"),
    ):
        if not isinstance(container, dict):
            continue
        node_id = str(container.get("node_id") or container.get("nodeId") or "")
        job_id = str(container.get("job_id") or container.get("jobId") or "")
        node_url = str(container.get("node_url") or container.get("nodeUrl") or "")
        if node_id or job_id or node_url:
            return (node_id, job_id, node_url)
    return ("", "", "")


def migrate_parked_sandbox(
    gateway_url: str,
    gateway_token: str,
    sandbox_id: str,
    migration_id: str,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    url = (
        f"{gateway_url.rstrip('/')}/v1/sandboxes/"
        f"{sandbox_id}/migration"
    )
    payload = json.dumps(
        {"migration_id": migration_id},
        separators=(",", ":"),
    ).encode("utf-8")
    while True:
        migration_request = request.Request(
            url,
            data=payload,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-UCloud-Sandbox-Token": gateway_token,
            },
        )
        try:
            with request.urlopen(migration_request, timeout=120) as response:
                result = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            if exc.code != 503 or body.get("retryable") is not True:
                raise RuntimeError(
                    f"sandbox migration failed with HTTP {exc.code}: {body}"
                ) from exc
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"timed out waiting for migration capacity: {body}"
                ) from exc
            time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))
            continue
        migration = result.get("migration")
        if not isinstance(migration, dict) or migration.get("phase") != "complete":
            raise RuntimeError(f"migration did not complete: {result}")
        timings_ms = result.get("timings_ms")
        if isinstance(timings_ms, dict):
            migration["timings_ms"] = timings_ms
        return migration


async def run(args: argparse.Namespace) -> dict[str, Any]:
    configure_retryable_null_harness_transport()
    gateway_token = token(args.gateway_token_file)
    sandbox_relay_url = args.sandbox_relay_url or args.relay_url
    suffix = uuid4().hex[:10]
    rollout_id = f"vf-live-{suffix}"
    sandbox_id = f"vf-live-{suffix}"
    runtime = UCloudVerifiersRuntime(
        SubprocessConfig(),
        gateway_url=args.gateway_url,
        gateway_token=gateway_token,
        image=args.image,
        sandbox_id=sandbox_id,
        timeout_seconds=args.sandbox_timeout_seconds,
    )
    relay = AsyncRelayWorkerClient(
        args.relay_url,
        worker_token=token(args.relay_worker_token_file),
        timeout_seconds=30,
    )
    model = CountingClient()
    worker_task: asyncio.Task[dict[str, Any]] | None = None
    rollout: RolloutRun | None = None
    rollout_registered = False
    started_at = time.monotonic()
    try:
        await runtime.start()
        relay_probe = await runtime.run(
            [
                "python",
                "-c",
                (
                    "import sys,urllib.request; "
                    "r=urllib.request.urlopen(sys.argv[1],timeout=15); "
                    "print(r.status); "
                    "raise SystemExit(0 if r.status == 200 else 1)"
                ),
                f"{sandbox_relay_url.rstrip('/')}/healthz",
            ],
            {},
        )
        if relay_probe.exit_code != 0:
            raise RuntimeError(
                "sandbox could not reach relay health endpoint: "
                f"{relay_probe.stderr.strip()[-2000:]}"
            )
        async with relay, InterceptionServer() as interception:
            registration = await relay.register_rollout(
                rollout_id,
                metadata={
                    "integration": "live-verifiers-parking",
                    "sandbox_id": sandbox_id,
                    "sandbox_generation": runtime.generation,
                },
            )
            rollout_registered = True
            registration_token = str(
                registration["rollout"]["registration_token"]
            )
            runtime.relay_tunnel_url = http_tunnel_url(
                sandbox_relay_url,
                rollout_id,
                registration_token=registration_token,
            ).rstrip("/")

            async def relay_worker() -> dict[str, Any]:
                while True:
                    polled = await relay.poll(
                        rollout_id,
                        worker_id="live-verifiers-worker",
                        timeout_seconds=1,
                        lease_seconds=600,
                    )
                    request = polled.request
                    if request is None:
                        continue
                    print(
                        json.dumps(
                            {
                                "event": "relay_claimed",
                                "request_id": request.request_id,
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
                    parked_at = time.monotonic()
                    parked_record = await wait_for_parked(
                        runtime,
                        timeout_seconds=args.park_timeout_seconds,
                    )
                    if (
                        not runtime.lifecycle_states
                        or runtime.lifecycle_states[-1] != "parked"
                    ):
                        runtime.lifecycle_states.append("parked")
                    source_owner = sandbox_owner(parked_record)
                    if not any(source_owner):
                        raise RuntimeError(
                            "parked sandbox inventory omitted its route owner: "
                            + json.dumps(parked_record, sort_keys=True)
                        )
                    migration: dict[str, Any] | None = None
                    destination_owner = source_owner
                    if args.migrate:
                        print(
                            json.dumps(
                                {
                                    "event": "migration_started",
                                    "request_id": request.request_id,
                                    "source_owner": source_owner,
                                },
                                sort_keys=True,
                            ),
                            flush=True,
                        )
                        migration = await asyncio.to_thread(
                            migrate_parked_sandbox,
                            args.gateway_url,
                            gateway_token,
                            sandbox_id,
                            f"relay-live:{request.request_id}",
                            timeout_seconds=args.migration_timeout_seconds,
                        )
                        migrated_record = await runtime.client.get_sandbox(
                            runtime.sandbox_id
                        )
                        destination_owner = sandbox_owner(migrated_record)
                        if not any(destination_owner):
                            raise RuntimeError(
                                "migrated sandbox inventory omitted its route owner: "
                                + json.dumps(migrated_record, sort_keys=True)
                            )
                        if source_owner == destination_owner:
                            raise RuntimeError(
                                "migration completed without changing sandbox owner: "
                                f"{source_owner}"
                            )
                        print(
                            json.dumps(
                                {
                                    "destination_owner": destination_owner,
                                    "event": "migration_completed",
                                    "request_id": request.request_id,
                                    "seconds": time.monotonic() - parked_at,
                                },
                                sort_keys=True,
                            ),
                            flush=True,
                        )
                    forward_started = time.monotonic()
                    await relay.forward_to(
                        request,
                        interception.base_url,
                        timeout_seconds=60,
                    )
                    print(
                        json.dumps(
                            {
                                "event": "relay_response_committed",
                                "request_id": request.request_id,
                                "seconds": time.monotonic() - forward_started,
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
                    return {
                        "request_id": request.request_id,
                        "parked_state": sandbox_state(parked_record),
                        "park_wait_seconds": time.monotonic() - parked_at,
                        "migration": migration,
                        "source_owner": source_owner,
                        "destination_owner": destination_owner,
                    }

            worker_task = asyncio.create_task(relay_worker())
            harness_config = NullHarnessConfig(
                id="null",
                env={
                    "HOME": "/workspace/.vf-home",
                    "UV_CACHE_DIR": "/workspace/.vf-home/.cache/uv",
                },
            )
            rollout = RolloutRun(
                task=Task(
                    TaskData(
                        idx=0,
                        prompt="Reply with relay-live-park-ok.",
                    )
                ),
                agent_config=AgentConfig(
                    harness=harness_config,
                    runtime=SubprocessConfig(),
                ),
                harness=NullHarness(harness_config),
                ctx=ModelContext(
                    model="ucloud-live-park-test",
                    client=model,
                    sampling=SamplingConfig(max_tokens=16, temperature=0),
                ),
                runtime_config=SubprocessConfig(),
                runtime=runtime,
                interception=interception,
                setup_timeout=args.sandbox_timeout_seconds,
                harness_timeout=args.sandbox_timeout_seconds,
                finalize_timeout=30,
                scoring_timeout=30,
            )
            if not await rollout.open():
                raise RuntimeError(f"rollout open failed: {rollout.failure}")
            step_task = asyncio.create_task(rollout.step())
            done, _pending = await asyncio.wait(
                {step_task, worker_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if worker_task in done:
                await worker_task
            if not await step_task:
                result = runtime.last_program_result
                raise RuntimeError(
                    "rollout step failed: "
                    f"failure={rollout.failure!r}, "
                    f"turns={rollout.trace.num_turns}, "
                    f"lifecycle_states={runtime.lifecycle_states!r}, "
                    f"program_exit={None if result is None else result.exit_code}, "
                    f"program_stdout={'' if result is None else result.stdout[-2000:]!r}, "
                    f"program_stderr={'' if result is None else result.stderr[-2000:]!r}"
                )
            trace = await rollout.close()
            worker = await asyncio.wait_for(worker_task, timeout=30)
            stats = await relay.stats()
            final_record = await runtime.client.get_sandbox(sandbox_id)
            await relay.unregister_rollout(rollout_id)
            rollout_registered = False
            reattached = stats["counters"].get("reattached") or 0
            return {
                "ok": trace.ok,
                "sandbox_id": sandbox_id,
                "sandbox_generation": runtime.generation,
                "rollout_id": rollout_id,
                "model_calls": model.calls,
                "trace_turns": trace.num_turns,
                "assistant": trace.branches[-1].messages[-1].content,
                "worker": worker,
                "lifecycle_states": runtime.lifecycle_states,
                "relay_connection_mode": (
                    "reattached" if reattached else "checkpoint-preserved"
                ),
                "final_sandbox_state": sandbox_state(final_record),
                "relay_counters": {
                    key: stats["counters"].get(key)
                    for key in (
                        "enqueued",
                        "reattached",
                        "completed",
                        "accepted_notifications",
                        "wake_notifications",
                        "transport_resets",
                    )
                },
                "relay_health_status": relay_probe.stdout.strip(),
                "elapsed_seconds": time.monotonic() - started_at,
            }
    finally:
        if rollout is not None and not rollout.closed:
            await rollout.abort()
        if worker_task is not None and not worker_task.done():
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
        if rollout_registered:
            with contextlib.suppress(Exception):
                await relay.unregister_rollout(rollout_id)
        await relay.close()
        await runtime.stop()


def main() -> None:
    args = parse_args()
    result = asyncio.run(run(args))
    print(json.dumps(result, indent=2, sort_keys=True))
    if not (
        result["ok"]
        and result["worker"]["parked_state"] == "parked"
        and result["model_calls"] == 1
        and result["trace_turns"] == 1
        and "parked" in result["lifecycle_states"]
        and result["final_sandbox_state"] == "running"
        and result["assistant"] == "relay-live-park-ok"
        and (
            not args.migrate
            or (
                result["worker"]["migration"]["phase"] == "complete"
                and result["relay_connection_mode"] == "reattached"
            )
        )
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
