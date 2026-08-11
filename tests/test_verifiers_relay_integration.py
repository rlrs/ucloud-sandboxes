from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import os
from pathlib import Path
import signal
import tempfile
import time
import unittest

from aiohttp import web

from ucloud_sandboxes.model_relay import create_model_relay_app


HAS_VERIFIERS = importlib.util.find_spec("verifiers") is not None
HAS_SANDBOX_SDK = importlib.util.find_spec("ucloud_sandboxes_sdk") is not None


@unittest.skipUnless(
    HAS_VERIFIERS and HAS_SANDBOX_SDK,
    "requires the pinned Verifiers checkout and ucloud-sandboxes-sdk",
)
class VerifiersRelayIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_null_harness_survives_park_and_wake_retry_through_sdk(
        self,
    ) -> None:
        from ucloud_sandboxes_sdk import AsyncRelayWorkerClient, http_tunnel_url
        from verifiers.v1.clients import Client, ModelContext
        from verifiers.v1.configs.agent import AgentConfig
        from verifiers.v1.harnesses.null.harness import (
            NullHarness,
            NullHarnessConfig,
        )
        from verifiers.v1.interception.server import InterceptionServer
        from verifiers.v1.rollout import RolloutRun
        from verifiers.v1.runtimes import (
            ProgramResult,
            SubprocessConfig,
            SubprocessRuntime,
        )
        from verifiers.v1.task import Task, TaskData
        from verifiers.v1.types import SamplingConfig

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
                    "id": "chatcmpl-ucloud-relay",
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "relay-ok",
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 3,
                        "completion_tokens": 2,
                        "total_tokens": 5,
                    },
                }
                response = dialect.parse_response(dialect.validate_response(raw))
                response.raw = raw
                return response

        class PausableRelayRuntime(SubprocessRuntime):
            def __init__(
                self,
                config: SubprocessConfig,
                relay_tunnel_url: str,
            ) -> None:
                super().__init__(
                    config,
                    name=f"vf-ucloud-relay-e2e-{time.time_ns()}",
                )
                self.relay_tunnel_url = relay_tunnel_url.rstrip("/")
                self.program_pid: int | None = None

            def host_url(self, _url: str) -> str:
                return self.relay_tunnel_url

            async def run_program(
                self,
                argv: list[str],
                env: dict[str, str],
            ) -> ProgramResult:
                full_env = {
                    key: value
                    for key, value in os.environ.items()
                    if "API_KEY" not in key.upper()
                }
                full_env.update(env)
                process = await asyncio.create_subprocess_exec(
                    *argv,
                    env=full_env,
                    cwd=self.workdir,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    start_new_session=True,
                )
                self.program_pid = process.pid
                try:
                    stdout, stderr = await process.communicate()
                finally:
                    if process.returncode is None:
                        with contextlib.suppress(ProcessLookupError, PermissionError):
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                return ProgramResult(
                    exit_code=process.returncode or 0,
                    stdout=stdout.decode(errors="replace"),
                    stderr=stderr.decode(errors="replace"),
                )

            def pause_program(self) -> None:
                assert self.program_pid is not None
                os.killpg(os.getpgid(self.program_pid), signal.SIGSTOP)

            def resume_program(self) -> None:
                assert self.program_pid is not None
                os.killpg(os.getpgid(self.program_pid), signal.SIGCONT)

        sandbox_token = "sandbox-token"
        worker_token = "worker-token"
        rollout_id = "verifiers-e2e"
        sandbox_id = "sandbox-e2e"
        sandbox_generation = 7
        parked = asyncio.Event()
        wake_attempts = 0
        lifecycle: list[tuple[str, str, int]] = []
        runtime: PausableRelayRuntime | None = None

        async def park(relay_request) -> None:
            assert relay_request.sandbox_id == sandbox_id
            assert relay_request.sandbox_generation == sandbox_generation
            assert runtime is not None
            runtime.pause_program()
            lifecycle.append(("park", sandbox_id, sandbox_generation))
            parked.set()

        async def wake(relay_request) -> None:
            nonlocal wake_attempts
            wake_attempts += 1
            assert relay_request.sandbox_id == sandbox_id
            assert relay_request.sandbox_generation == sandbox_generation
            if wake_attempts == 1:
                raise RuntimeError("simulated transient gateway wake failure")
            assert runtime is not None
            runtime.resume_program()
            lifecycle.append(("wake", sandbox_id, sandbox_generation))

        with tempfile.TemporaryDirectory() as directory:
            relay_app = create_model_relay_app(
                sandbox_bearer_token=sandbox_token,
                worker_bearer_token=worker_token,
                worker_poll_timeout_seconds=0.1,
                worker_lease_seconds=30,
                state_path=Path(directory) / "relay.sqlite3",
                accepted_notifier=park,
                result_notifier=wake,
            )
            relay_runner = web.AppRunner(relay_app)
            await relay_runner.setup()
            relay_site = web.TCPSite(relay_runner, "127.0.0.1", 0)
            await relay_site.start()
            socket = relay_site._server.sockets[0]
            relay_url = f"http://127.0.0.1:{socket.getsockname()[1]}"

            runtime = PausableRelayRuntime(
                SubprocessConfig(),
                http_tunnel_url(relay_url, rollout_id).rstrip("/"),
            )
            await runtime.start()
            assert runtime.workdir is not None
            sandbox_home = runtime.workdir / "home"
            sandbox_home.mkdir()
            model_client = CountingClient()

            async with InterceptionServer() as interception:
                registered = asyncio.Event()

                async def relay_worker() -> None:
                    async with AsyncRelayWorkerClient(
                        relay_url,
                        worker_token=worker_token,
                    ) as sdk:
                        registration = await sdk.register_rollout(
                            rollout_id,
                            metadata={
                                "integration": "verifiers-null-harness",
                                "sandbox_id": sandbox_id,
                                "sandbox_generation": sandbox_generation,
                            },
                        )
                        runtime.relay_tunnel_url = str(
                            registration["tunnel_url"]
                        ).rstrip("/")
                        registered.set()
                        while True:
                            polled = await sdk.poll(
                                rollout_id,
                                worker_id="verifiers-worker",
                                timeout_seconds=0.1,
                                lease_seconds=30,
                            )
                            if not polled.requests:
                                continue
                            request = polled.requests[0]
                            self.assertEqual(
                                request.sandbox_id,
                                sandbox_id,
                            )
                            self.assertEqual(
                                request.sandbox_generation,
                                sandbox_generation,
                            )
                            await sdk.forward_to(
                                request,
                                interception.base_url,
                                timeout_seconds=30,
                            )
                            return

                worker_task = asyncio.create_task(relay_worker())
                await asyncio.wait_for(registered.wait(), timeout=5)

                harness_config = NullHarnessConfig(
                    id="null",
                    env={
                        "HOME": str(sandbox_home),
                        "UV_CACHE_DIR": str(sandbox_home / ".cache" / "uv"),
                    },
                )
                rollout = RolloutRun(
                    task=Task(
                        TaskData(
                            idx=0,
                            prompt="Reply with relay-ok.",
                        )
                    ),
                    agent_config=AgentConfig(
                        harness=harness_config,
                        runtime=SubprocessConfig(),
                    ),
                    harness=NullHarness(harness_config),
                    ctx=ModelContext(
                        model="ucloud-relay-test",
                        client=model_client,
                        sampling=SamplingConfig(
                            max_tokens=16,
                            temperature=0,
                        ),
                    ),
                    runtime_config=SubprocessConfig(),
                    runtime=runtime,
                    interception=interception,
                    setup_timeout=120,
                    harness_timeout=60,
                    finalize_timeout=10,
                    scoring_timeout=10,
                )
                self.assertTrue(await rollout.open(), rollout.failure)
                self.assertTrue(await rollout.step(), rollout.failure)
                trace = await rollout.close()
                await asyncio.wait_for(worker_task, timeout=5)

            async with AsyncRelayWorkerClient(
                relay_url,
                worker_token=worker_token,
            ) as stats_client:
                stats = await stats_client.stats()
            await runtime.stop()
            await relay_runner.cleanup()

        self.assertTrue(parked.is_set())
        self.assertEqual(
            lifecycle,
            [
                ("park", sandbox_id, sandbox_generation),
                ("wake", sandbox_id, sandbox_generation),
            ],
        )
        self.assertEqual(wake_attempts, 2)
        self.assertEqual(model_client.calls, 1)
        self.assertTrue(trace.ok)
        self.assertEqual(trace.num_turns, 1)
        self.assertEqual(trace.branches[-1].messages[-1].content, "relay-ok")
        self.assertEqual(stats["counters"]["enqueued"], 1)
        self.assertEqual(stats["counters"]["completed"], 1)


if __name__ == "__main__":
    unittest.main()
