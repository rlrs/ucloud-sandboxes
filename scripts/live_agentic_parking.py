#!/usr/bin/env python3
"""Run a small coding agent through relay-driven sandbox park/wake cycles.

This is intentionally a live qualification harness.  It creates a real
managed-process sandbox, registers a model-relay rollout, forwards intercepted
requests to an OpenAI-compatible endpoint, and grades the resulting workspace.
Every model request is therefore a real local park/wake cycle. An optional
cycle migrates the parked sandbox to another node and exercises durable
storage-native publication and restore.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import os
from pathlib import Path
import time
from typing import Any, Mapping
from uuid import uuid4

import aiohttp
from ucloud_sandboxes_sdk import (
    AsyncRelayWorkerClient,
    AsyncSandboxClient,
    Image,
    SandboxApiError,
    SandboxSecuritySpec,
    SandboxSpec,
    http_tunnel_url,
)


AGENT_PROGRAM = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path("/workspace").resolve()
RELAY_URL = os.environ["AGENT_RELAY_URL"]
MODEL = os.environ["AGENT_MODEL"]

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_files",
            "description": "List files in the workspace.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read a UTF-8 workspace file.",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Replace a UTF-8 workspace file with corrected content.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_tests",
            "description": "Run the complete unittest suite in the workspace.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]


def workspace_path(raw):
    candidate = (ROOT / str(raw)).resolve()
    if candidate != ROOT and ROOT not in candidate.parents:
        raise ValueError("path escapes workspace")
    return candidate


def run_tool(name, arguments):
    if name == "list_files":
        return {"files": sorted(str(p.relative_to(ROOT)) for p in ROOT.rglob("*") if p.is_file())}
    if name == "read_file":
        path = workspace_path(arguments["path"])
        return {"path": str(path.relative_to(ROOT)), "content": path.read_text()}
    if name == "write_file":
        path = workspace_path(arguments["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(arguments["content"])
        return {"path": str(path.relative_to(ROOT)), "bytes": path.stat().st_size}
    if name == "run_tests":
        result = subprocess.run(
            [sys.executable, "-m", "unittest", "-v"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
        )
        return {"exit_code": result.returncode, "output": result.stdout[-12000:]}
    raise ValueError("unknown tool: " + name)


def call_model(payload, stable_id):
    body = json.dumps(payload, separators=(",", ":")).encode()
    last = None
    for attempt in range(12):
        req = urllib.request.Request(
            RELAY_URL,
            data=body,
            method="POST",
            headers={
                "Authorization": "Bearer intercepted",
                "Content-Type": "application/json",
                "X-UCloud-Relay-Request-Id": stable_id,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            if exc.code < 500:
                raise RuntimeError("relay HTTP %s: %s" % (exc.code, detail)) from exc
            last = RuntimeError("relay HTTP %s: %s" % (exc.code, detail))
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            last = exc
        time.sleep(min(2.0, 0.2 * (attempt + 1)))
    raise RuntimeError("model request retries exhausted: %r" % (last,))


messages = [
    {
        "role": "system",
        "content": (
            "You are a coding agent in a constrained workspace. Fix the bug and prove "
            "the fix. You MUST inspect calculator.py and test_calculator.py with tools, "
            "then use write_file for the minimal correction, then run_tests. Do not give "
            "a final answer until run_tests reports success. Use one tool at a time."
        ),
    },
    {
        "role": "user",
        "content": (
            "weighted_average is wrong. Diagnose it, implement the correct behavior, "
            "run the tests, and briefly summarize the verified fix."
        ),
    },
]
events = []
final_answer = ""
read_files = set()
wrote_file = False
tests_succeeded = False
for step in range(16):
    stable_id = "coding-agent-step-%02d" % step
    response = call_model(
        {
            "model": MODEL,
            "messages": messages,
            "tools": TOOLS,
            "tool_choice": "auto",
            "parallel_tool_calls": False,
        },
        stable_id,
    )
    choice = response["choices"][0]
    message = choice["message"]
    assistant = {"role": "assistant", "content": message.get("content")}
    if message.get("tool_calls"):
        assistant["tool_calls"] = message["tool_calls"]
    messages.append(assistant)
    tool_calls = message.get("tool_calls") or []
    events.append({"step": step, "finish_reason": choice.get("finish_reason"), "tool_calls": [x.get("function", {}).get("name") for x in tool_calls]})
    print(json.dumps({"event": "model_step", **events[-1]}, sort_keys=True), flush=True)
    if not tool_calls:
        missing = []
        if "calculator.py" not in read_files:
            missing.append("read calculator.py")
        if "test_calculator.py" not in read_files:
            missing.append("read test_calculator.py")
        if not wrote_file:
            missing.append("write the correction with write_file")
        if not tests_succeeded:
            missing.append("run_tests successfully")
        if missing:
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "The task is not complete. Continue using tools; remaining required "
                        "work: " + ", ".join(missing) + "."
                    ),
                }
            )
            continue
        final_answer = str(message.get("content") or "")
        break
    for tool_call in tool_calls:
        function = tool_call["function"]
        arguments = json.loads(function.get("arguments") or "{}")
        try:
            output = run_tool(function["name"], arguments)
            if function["name"] == "read_file":
                read_files.add(str(arguments.get("path") or ""))
            elif function["name"] == "write_file":
                wrote_file = True
            elif function["name"] == "run_tests":
                tests_succeeded = output.get("exit_code") == 0
        except Exception as exc:
            output = {"error": str(exc)}
        messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "content": json.dumps(output, sort_keys=True),
            }
        )
        print(json.dumps({"event": "tool", "name": function["name"], "result": output}, sort_keys=True), flush=True)
else:
    raise RuntimeError("agent exceeded step limit")

grade = subprocess.run(
    [sys.executable, "-m", "unittest", "-v"],
    cwd=ROOT,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    timeout=60,
)
result = {
    "ok": grade.returncode == 0,
    "final_answer": final_answer,
    "events": events,
    "test_exit_code": grade.returncode,
    "test_output": grade.stdout[-12000:],
    "calculator": (ROOT / "calculator.py").read_text(),
}
(ROOT / "agent_result.json").write_text(json.dumps(result, indent=2, sort_keys=True))
print(json.dumps({"event": "agent_complete", "ok": result["ok"]}, sort_keys=True), flush=True)
raise SystemExit(0 if result["ok"] else 1)
'''


CALCULATOR = '''def weighted_average(values, weights):
    """Return the weighted average for equally-sized non-empty sequences."""
    if not values or len(values) != len(weights):
        raise ValueError("values and weights must be non-empty and equally sized")
    denominator = sum(weights)
    if denominator == 0:
        raise ValueError("weights must not sum to zero")
    return sum(values) / denominator
'''


TEST_CALCULATOR = '''import unittest

from calculator import weighted_average


class WeightedAverageTests(unittest.TestCase):
    def test_nonuniform_weights(self):
        self.assertAlmostEqual(weighted_average([10, 20, 40], [1, 2, 7]), 33.0)

    def test_fractional_weights(self):
        self.assertAlmostEqual(weighted_average([2, 8], [0.25, 0.75]), 6.5)

    def test_invalid_shapes(self):
        with self.assertRaises(ValueError):
            weighted_average([], [])
        with self.assertRaises(ValueError):
            weighted_average([1], [1, 2])

    def test_zero_denominator(self):
        with self.assertRaises(ValueError):
            weighted_average([1, 2], [1, -1])


if __name__ == "__main__":
    unittest.main()
'''


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8090")
    parser.add_argument("--relay-url", default="http://127.0.0.1:8092")
    parser.add_argument("--sandbox-relay-url", default="http://10.42.0.2:8092")
    parser.add_argument("--gateway-token-file", type=Path, required=True)
    parser.add_argument("--relay-worker-token-file", type=Path, required=True)
    parser.add_argument("--upstream-base-url")
    parser.add_argument("--upstream-api-key")
    parser.add_argument("--model")
    parser.add_argument(
        "--upstream-secrets-stdin",
        action="store_true",
        help="Read base_url, api_key, and model as one JSON object from stdin.",
    )
    parser.add_argument("--image", default="python:3.13-slim-bookworm")
    parser.add_argument("--migrate-cycle", type=int, default=0)
    parser.add_argument("--timeout-seconds", type=float, default=900)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def read_token(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"empty token file: {path}")
    return value


def sandbox_state(record: Mapping[str, Any] | None) -> str:
    if not record:
        return "missing"
    for container in (record, record.get("status"), record.get("sandbox")):
        if isinstance(container, Mapping):
            value = container.get("state") or container.get("status")
            if isinstance(value, str) and value:
                return value.lower()
    return "unknown"


def sandbox_generation(record: Mapping[str, Any]) -> int:
    for container in (record, record.get("status"), record.get("sandbox")):
        if isinstance(container, Mapping):
            value = container.get("generation")
            if isinstance(value, int) and value > 0:
                return value
    raise RuntimeError("sandbox record omitted generation")


def sandbox_owner(record: Mapping[str, Any] | None) -> dict[str, str]:
    if not record:
        return {"node_id": "", "job_id": "", "node_url": ""}
    for container in (record, record.get("node"), record.get("status"), record.get("sandbox")):
        if not isinstance(container, Mapping):
            continue
        result = {
            "node_id": str(container.get("node_id") or container.get("nodeId") or ""),
            "job_id": str(container.get("job_id") or container.get("jobId") or ""),
            "node_url": str(container.get("node_url") or container.get("nodeUrl") or ""),
        }
        if any(result.values()):
            return result
    return {"node_id": "", "job_id": "", "node_url": ""}


def portable_snapshot_summary(record: Mapping[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}

    def visit(value: object) -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if key in {
                    "snapshot_sha256",
                    "storage_schema",
                    "storage_snapshot",
                    "snapshot_repository",
                    "snapshot_tag",
                }:
                    summary[key] = child
                elif isinstance(child, (Mapping, list)):
                    visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(record)
    return summary


def resource_summary(nodes_payload: Mapping[str, Any], owner: Mapping[str, str]) -> dict[str, Any]:
    nodes = nodes_payload.get("nodes")
    if not isinstance(nodes, list):
        return {}
    for node in nodes:
        if not isinstance(node, Mapping):
            continue
        if str(node.get("node_id") or "") != owner.get("node_id"):
            continue
        raw_metrics = node.get("runtime_metrics")
        metrics = {
            str(key): value
            for key, value in (raw_metrics.items() if isinstance(raw_metrics, Mapping) else ())
            if str(key).startswith("storage_")
            or key in {"cpu_vcpu", "memory_used_mb", "memory_available_mb"}
        }
        return {
            "active_sandboxes": node.get("active_sandboxes"),
            "used_resources": node.get("used_resources"),
            "runtime_metrics": metrics,
            "inventory": node.get("inventory"),
        }
    return {}


async def wait_for_node_resources(
    session: aiohttp.ClientSession,
    gateway_url: str,
    gateway_token: str,
    owner: Mapping[str, str],
    *,
    active_sandboxes: int,
    timeout_seconds: float = 10,
) -> tuple[dict[str, Any], float]:
    started = time.monotonic()
    deadline = started + timeout_seconds
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        payload = await gateway_json(
            session,
            gateway_url,
            gateway_token,
            "GET",
            "/v1/nodes",
        )
        last = resource_summary(payload, owner)
        if last.get("active_sandboxes") == active_sandboxes:
            return last, time.monotonic() - started
        await asyncio.sleep(0.2)
    return last, time.monotonic() - started


async def wait_for_state(
    client: AsyncSandboxClient,
    sandbox_id: str,
    expected: set[str],
    *,
    timeout_seconds: float,
) -> tuple[dict[str, Any], float]:
    started = time.monotonic()
    deadline = started + timeout_seconds
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last = await client.get_sandbox(sandbox_id)
        if sandbox_state(last) in expected:
            assert last is not None
            return last, time.monotonic() - started
        await asyncio.sleep(0.1)
    raise TimeoutError(
        f"sandbox did not reach {sorted(expected)}; last={sandbox_state(last)}"
    )


async def gateway_json(
    session: aiohttp.ClientSession,
    gateway_url: str,
    gateway_token: str,
    method: str,
    path: str,
    *,
    payload: object | None = None,
    timeout_seconds: float = 120,
) -> dict[str, Any]:
    headers = {"X-UCloud-Sandbox-Token": gateway_token, "Accept": "application/json"}
    async with session.request(
        method,
        gateway_url.rstrip("/") + path,
        json=payload,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout_seconds),
    ) as response:
        body = await response.read()
        decoded = json.loads(body.decode()) if body else {}
        if response.status >= 400:
            raise RuntimeError(f"gateway {method} {path} failed ({response.status}): {decoded}")
        if not isinstance(decoded, dict):
            raise RuntimeError("gateway returned non-object JSON")
        return decoded


async def migrate_parked(
    session: aiohttp.ClientSession,
    gateway_url: str,
    gateway_token: str,
    sandbox_id: str,
    migration_id: str,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            return await gateway_json(
                session,
                gateway_url,
                gateway_token,
                "POST",
                f"/v1/sandboxes/{sandbox_id}/migration",
                payload={"migration_id": migration_id},
                timeout_seconds=180,
            )
        except RuntimeError as exc:
            if "(503)" not in str(exc) or time.monotonic() >= deadline:
                raise
            await asyncio.sleep(2)


def safe_upstream_headers(headers: Mapping[str, str], api_key: str) -> dict[str, str]:
    blocked = {"authorization", "content-length", "host", "connection", "transfer-encoding"}
    result = {str(k): str(v) for k, v in headers.items() if str(k).lower() not in blocked}
    result["Authorization"] = f"Bearer {api_key}"
    result.setdefault("Content-Type", "application/json")
    return result


async def forward_upstream(
    session: aiohttp.ClientSession,
    relay_request: Any,
    base_url: str,
    api_key: str,
) -> tuple[int, dict[str, str], bytes, float]:
    started = time.monotonic()
    url = base_url.rstrip("/") + "/" + relay_request.endpoint.lstrip("/")
    async with session.request(
        relay_request.method,
        url,
        data=relay_request.body_bytes or None,
        headers=safe_upstream_headers(relay_request.headers, api_key),
        timeout=aiohttp.ClientTimeout(total=300),
    ) as response:
        body = await response.read()
        headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() not in {"content-length", "transfer-encoding", "connection"}
        }
        return response.status, headers, body, time.monotonic() - started


async def run(args: argparse.Namespace) -> dict[str, Any]:
    secrets = {}
    if args.upstream_secrets_stdin:
        secrets = json.loads(input())
        if not isinstance(secrets, dict):
            raise ValueError("upstream secrets stdin must be a JSON object")
    upstream_base_url = str(args.upstream_base_url or secrets.get("base_url") or os.environ.get("OPENAI_BASE_URL") or "").strip()
    upstream_api_key = str(args.upstream_api_key or secrets.get("api_key") or os.environ.get("OPENAI_API_KEY") or "").strip()
    model = str(args.model or secrets.get("model") or os.environ.get("OPENAI_MODEL") or "").strip()
    if not upstream_base_url or not upstream_api_key or not model:
        raise ValueError("upstream base URL, API key, and model are required")

    gateway_token = read_token(args.gateway_token_file)
    relay_token = read_token(args.relay_worker_token_file)
    suffix = uuid4().hex[:10]
    sandbox_id = f"agent-e2e-{suffix}"
    rollout_id = f"agent-e2e-{suffix}"
    managed_job_id = f"coding-agent-{suffix}"
    started_at = time.monotonic()
    sandbox_create_started = time.monotonic()
    cycles: list[dict[str, Any]] = []
    lifecycle_states: list[str] = []
    handle = None
    rollout_registered = False
    relay_worker_task: asyncio.Task[None] | None = None
    stop_worker = asyncio.Event()

    async with AsyncSandboxClient(
        args.gateway_url,
        api_token=gateway_token,
        timeout_seconds=30,
    ) as client, AsyncRelayWorkerClient(
        args.relay_url,
        worker_token=relay_token,
        timeout_seconds=30,
    ) as relay, aiohttp.ClientSession() as session:
        try:
            deadline = time.monotonic() + args.timeout_seconds
            while True:
                try:
                    handle = await client.create_sandbox(
                        SandboxSpec(
                            id=sandbox_id,
                            image=Image.from_registry(args.image),
                            memory_mb=2048,
                            cpus=1.0,
                            disk_mb=4096,
                            network="bridge",
                            ttl_seconds=int(args.timeout_seconds + 300),
                            parkable=True,
                            managed_process=True,
                            security=SandboxSecuritySpec(user="0:0"),
                        ),
                        request_timeout_seconds=min(args.timeout_seconds, 600),
                    )
                    break
                except SandboxApiError as exc:
                    if exc.status_code != 503 or getattr(exc, "retryable", None) is not True or time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(2)
            sandbox_create_seconds = time.monotonic() - sandbox_create_started
            record = await client.get_sandbox(sandbox_id)
            if record is None:
                raise RuntimeError("created sandbox disappeared")
            generation = 0
            for source in (handle.record, handle.create_response, record):
                try:
                    generation = sandbox_generation(source)
                    break
                except RuntimeError:
                    continue
            if generation < 1:
                raise RuntimeError("create response and inventory omitted generation")

            for path, content in (
                ("/workspace/agent.py", AGENT_PROGRAM),
                ("/workspace/calculator.py", CALCULATOR),
                ("/workspace/test_calculator.py", TEST_CALCULATOR),
            ):
                await handle.upload_file(path, content)

            registration = await relay.register_rollout(
                rollout_id,
                metadata={
                    "integration": "live-agentic-parking",
                    "sandbox_id": sandbox_id,
                    "sandbox_generation": generation,
                },
            )
            rollout_registered = True
            stats_before = await relay.stats()
            registration_token = str(registration["rollout"]["registration_token"])
            tunnel = http_tunnel_url(
                args.sandbox_relay_url,
                rollout_id,
                registration_token=registration_token,
            ).rstrip("/") + "/chat/completions"

            async def relay_worker() -> None:
                while not stop_worker.is_set():
                    polled = await relay.poll(
                        rollout_id,
                        worker_id="live-agentic-worker",
                        timeout_seconds=1,
                        lease_seconds=600,
                    )
                    for item in polled.requests:
                        cycle_number = len(cycles) + 1
                        request_claimed = time.monotonic()
                        parked_record, park_wait = await wait_for_state(
                            client,
                            sandbox_id,
                            {"parked"},
                            timeout_seconds=180,
                        )
                        owner_before = sandbox_owner(parked_record)
                        parked_job = await gateway_json(
                            session,
                            args.gateway_url,
                            gateway_token,
                            "GET",
                            f"/v1/sandboxes/{sandbox_id}/jobs/{managed_job_id}",
                        )
                        parked_resources_task = asyncio.create_task(
                            wait_for_node_resources(
                                session,
                                args.gateway_url,
                                gateway_token,
                                owner_before,
                                active_sandboxes=0,
                            )
                        )
                        migration = None
                        if args.migrate_cycle == cycle_number:
                            migration_started = time.monotonic()
                            migration = await migrate_parked(
                                session,
                                args.gateway_url,
                                gateway_token,
                                sandbox_id,
                                f"agent-e2e-{suffix}-cycle-{cycle_number}",
                                timeout_seconds=300,
                            )
                            migration["wall_seconds"] = time.monotonic() - migration_started
                        pre_response_record = await client.get_sandbox(sandbox_id)
                        owner_after_migration = sandbox_owner(pre_response_record)
                        status, response_headers, response_body, model_seconds = await forward_upstream(
                            session,
                            item,
                            upstream_base_url,
                            upstream_api_key,
                        )
                        parked_resources, parked_resource_observation = await parked_resources_task
                        commit_started = time.monotonic()
                        await relay.commit_response_bytes_to(
                            item,
                            response_body,
                            status=status,
                            headers=response_headers,
                            attempts=180,
                            retry_delay_seconds=1,
                        )
                        commit_seconds = time.monotonic() - commit_started
                        running_record, wake_wait = await wait_for_state(
                            client,
                            sandbox_id,
                            # A fast agent can consume the response, issue its
                            # next model request, and be parked again before a
                            # 100 ms observer sees the intermediate RUNNING
                            # state. Reaching the next PARKED state is itself
                            # proof that the managed process resumed.
                            {"running", "parked"},
                            timeout_seconds=180,
                        )
                        running_job = await gateway_json(
                            session,
                            args.gateway_url,
                            gateway_token,
                            "GET",
                            f"/v1/sandboxes/{sandbox_id}/jobs/{managed_job_id}",
                        )
                        running_resources_started = time.monotonic()
                        running_nodes = await gateway_json(
                            session,
                            args.gateway_url,
                            gateway_token,
                            "GET",
                            "/v1/nodes",
                        )
                        running_resources = resource_summary(
                            running_nodes,
                            sandbox_owner(running_record),
                        )
                        running_resource_observation = (
                            time.monotonic() - running_resources_started
                        )
                        cycle = {
                            "cycle": cycle_number,
                            "request_id": item.request_id,
                            "delivery_count": item.delivery_count,
                            "park_wait_seconds": park_wait,
                            "parked_resource_observation_seconds": parked_resource_observation,
                            "model_seconds": model_seconds,
                            "response_commit_seconds": commit_seconds,
                            "wake_observation_seconds": wake_wait,
                            "running_resource_observation_seconds": running_resource_observation,
                            "total_seconds": time.monotonic() - request_claimed,
                            "upstream_status": status,
                            "parked_snapshot": portable_snapshot_summary(parked_record),
                            "owner_before": owner_before,
                            "owner_after_migration": owner_after_migration,
                            "owner_after_wake": sandbox_owner(running_record),
                            "managed_job_parked": parked_job.get("job"),
                            "managed_job_running": running_job.get("job"),
                            "resources_parked": parked_resources,
                            "resources_running": running_resources,
                            "migration": migration,
                        }
                        cycles.append(cycle)
                        print(json.dumps({"event": "relay_cycle", **cycle}, sort_keys=True), flush=True)

            relay_worker_task = asyncio.create_task(relay_worker())
            job_start = await gateway_json(
                session,
                args.gateway_url,
                gateway_token,
                "POST",
                f"/v1/sandboxes/{sandbox_id}/jobs",
                payload={
                    "job_id": managed_job_id,
                    "argv": ["python", "/workspace/agent.py"],
                    "cwd": "/workspace",
                    "env": {"AGENT_RELAY_URL": tunnel, "AGENT_MODEL": model},
                },
            )
            job = job_start["job"]
            while not bool(job.get("state") in {"exited", "signaled", "failed"}):
                current = await client.get_sandbox(sandbox_id)
                state = sandbox_state(current)
                if not lifecycle_states or lifecycle_states[-1] != state:
                    lifecycle_states.append(state)
                status_payload = await gateway_json(
                    session,
                    args.gateway_url,
                    gateway_token,
                    "GET",
                    f"/v1/sandboxes/{sandbox_id}/jobs/{managed_job_id}",
                )
                job = status_payload["job"]
                if time.monotonic() >= deadline:
                    raise TimeoutError("managed coding agent timed out")
                await asyncio.sleep(0.2)
            await asyncio.sleep(1.2)
            stop_worker.set()
            await asyncio.wait_for(relay_worker_task, timeout=10)

            async def read_log(stream: str) -> str:
                payload = await gateway_json(
                    session,
                    args.gateway_url,
                    gateway_token,
                    "GET",
                    f"/v1/sandboxes/{sandbox_id}/jobs/{managed_job_id}/logs/{stream}?offset=0&limit=1048576",
                )
                return base64.b64decode(str(payload.get("data") or "")).decode(errors="replace")

            stdout, stderr = await asyncio.gather(read_log("stdout"), read_log("stderr"))
            result_bytes = await handle.download_file("/workspace/agent_result.json")
            calculator_bytes = await handle.download_file("/workspace/calculator.py")
            agent_result = json.loads(result_bytes.decode())
            calculator = calculator_bytes.decode()
            hidden_probe = await handle.exec(
                [
                    "python",
                    "-c",
                    (
                        "import json; from calculator import weighted_average as f; "
                        "print(json.dumps([f([3,11,19],[2,3,5]), "
                        "f([-5,5],[9,1]), f([7],[42])]))"
                    ),
                ],
                working_dir="/workspace",
                timeout_seconds=60,
            )
            try:
                hidden_values = json.loads(hidden_probe.stdout.strip())
            except json.JSONDecodeError:
                hidden_values = []
            stats = await relay.stats()
            counters_before = stats_before.get("counters", {})
            relay_counters = {
                key: int(value) - int(counters_before.get(key, 0))
                for key, value in stats.get("counters", {}).items()
            }
            final_record = await client.get_sandbox(sandbox_id)
            nodes_final = await gateway_json(
                session,
                args.gateway_url,
                gateway_token,
                "GET",
                "/v1/nodes",
            )
            managed_identities = [
                (
                    cycle.get("managed_job_parked", {}).get("pid"),
                    cycle.get("managed_job_parked", {}).get("spec_sha256"),
                    cycle.get("managed_job_running", {}).get("pid"),
                    cycle.get("managed_job_running", {}).get("spec_sha256"),
                )
                for cycle in cycles
            ]
            grade = {
                "agent_exit_zero": job.get("exit_code") == 0,
                "tests_passed": agent_result.get("test_exit_code") == 0,
                "hidden_cases_passed": hidden_probe.exit_code == 0 and hidden_values == [13.4, -4.0, 7.0],
                "used_read_file": any("read_file" in event.get("tool_calls", []) for event in agent_result.get("events", [])),
                "used_write_file": any("write_file" in event.get("tool_calls", []) for event in agent_result.get("events", [])),
                "used_run_tests": any("run_tests" in event.get("tool_calls", []) for event in agent_result.get("events", [])),
                "multiple_park_cycles": len(cycles) >= 3,
                "all_upstream_ok": all(cycle["upstream_status"] == 200 for cycle in cycles),
                "resources_released_while_parked": all(
                    cycle.get("resources_parked", {}).get("active_sandboxes") == 0
                    and cycle.get("resources_parked", {}).get("used_resources", {}).get("vcpu") == 0.0
                    and cycle.get("resources_parked", {}).get("used_resources", {}).get("memory_mb") == 0
                    and cycle.get("resources_parked", {}).get("runtime_metrics", {}).get("storage_ublk_active_devices") == 0
                    for cycle in cycles
                ),
                "managed_process_identity_preserved": all(
                    parked_pid > 0
                    and parked_hash
                    and parked_hash == running_hash
                    and (
                        parked_pid == running_pid
                        or running_pid == 0
                    )
                    for parked_pid, parked_hash, running_pid, running_hash in managed_identities
                ),
                "final_lifecycle_healthy": sandbox_state(final_record)
                in {"running", "parked"},
                "all_park_notifications": relay_counters.get(
                    "accepted_notifications", 0
                )
                >= len(cycles),
                "all_wake_notifications": relay_counters.get("wake_notifications", 0)
                >= len(cycles),
            }
            if args.migrate_cycle:
                migrated = next((item for item in cycles if item["cycle"] == args.migrate_cycle), None)
                grade["migration_snapshot_published"] = bool(
                    migrated
                    and portable_snapshot_summary(migrated.get("migration", {})).get("storage_snapshot")
                )
                grade["migration_completed"] = bool(
                    migrated
                    and migrated.get("migration", {}).get("migration", {}).get("phase") == "complete"
                    and migrated["owner_before"] != migrated["owner_after_migration"]
                )
            ok = all(grade.values())
            result = {
                "ok": ok,
                "sandbox_id": sandbox_id,
                "generation": generation,
                "rollout_id": rollout_id,
                "managed_job_id": managed_job_id,
                "sandbox_create_seconds": sandbox_create_seconds,
                "elapsed_seconds": time.monotonic() - started_at,
                "model_calls": len(cycles),
                "lifecycle_states": lifecycle_states,
                "cycles": cycles,
                "job": job,
                "agent_result": agent_result,
                "hidden_probe": {
                    "exit_code": hidden_probe.exit_code,
                    "stdout": hidden_probe.stdout,
                    "stderr": hidden_probe.stderr,
                    "values": hidden_values,
                },
                "final_calculator": calculator,
                "grade": grade,
                "stdout": stdout,
                "stderr": stderr,
                "final_sandbox_state": sandbox_state(final_record),
                "final_resources": resource_summary(nodes_final, sandbox_owner(final_record)),
                "relay_counters": relay_counters,
            }
            return result
        finally:
            stop_worker.set()
            if relay_worker_task is not None and not relay_worker_task.done():
                relay_worker_task.cancel()
                await asyncio.gather(relay_worker_task, return_exceptions=True)
            if rollout_registered:
                with contextlib.suppress(Exception):
                    await relay.unregister_rollout(rollout_id)
            if handle is not None:
                with contextlib.suppress(Exception):
                    await handle.delete()


def main() -> None:
    args = parse_args()
    result = asyncio.run(run(args))
    encoded = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
