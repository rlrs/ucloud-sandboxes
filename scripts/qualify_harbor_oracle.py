#!/usr/bin/env python3
"""Run a Harbor task's setup, public solution and grader on UCloud.

This supplements tasksets that do not implement validate(). It requires the
pinned tasksets, compatible Verifiers and verifiers-ucloud, and supports shared
verifiers only. It does not replace or relax task setup or grading.
"""

import argparse
import asyncio
import json
from pathlib import Path
import uuid


async def qualify(args) -> dict:
    import verifiers.v1 as vf
    from verifiers.v1.state import state_cls
    from verifiers.v1.tasksets.harbor.taskset import make_tar
    from verifiers.v1.trace import Trace, TraceTask
    from verifiers.v1.utils.compile import resolve_runtime_config
    from verifiers.v1.utils.loaders import taskset_class, taskset_config_type
    from verifiers_ucloud.runtime import UCloudRuntime, UCloudRuntimeConfig

    cls = taskset_class(args.taskset)
    task = next(
        task
        for task in cls(taskset_config_type(args.taskset)()).load()
        if Path(task.data.task_dir).resolve() == args.task_dir.resolve()
    )
    result = {"task": task.data.name, "image": args.image, "status": "failed"}
    if task.data.verifier is not None:
        raise ValueError(
            "separate verifier requires its full Harbor environment lifecycle"
        )
    config = resolve_runtime_config(UCloudRuntimeConfig(), task)
    config.image = args.image
    runtime = UCloudRuntime(config, name="oracle-compat-" + uuid.uuid4().hex[:16])
    runtime.env = task.runtime_env()
    trace = Trace(
        task=TraceTask(
            type=type(task).__name__, data=task.data, key=task.key, hash=task.hash
        ),
        state=state_cls(type(task))(),
        agent=vf.AgentInfo(
            config=vf.AgentConfig(runtime=config), name="oracle", trainable=False
        ),
    )
    try:
        await runtime.start()
        await asyncio.wait_for(task.setup(runtime), 900)
        result["setup"] = "passed"
        await runtime.write("/tmp/oracle.tgz", make_tar(args.task_dir / "solution"))
        stage = await runtime.run(
            ["sh", "-c", "mkdir -p /oracle && tar -xzf /tmp/oracle.tgz -C /oracle"], {}
        )
        if stage.exit_code:
            raise RuntimeError("oracle staging failed")
        run = await asyncio.wait_for(runtime.run(["bash", "/oracle/solve.sh"], {}), 900)
        result["oracle"] = {
            "exit_code": run.exit_code,
            "stdout": run.stdout,
            "stderr": run.stderr,
        }
        if run.exit_code:
            raise RuntimeError("oracle solution failed")
        value = await asyncio.wait_for(task.solved(runtime, trace), 900)
        result["reward"] = value
        scores = list(value.values()) if isinstance(value, dict) else [value]
        if not scores or any(
            type(score) not in (int, float) or score != 1 for score in scores
        ):
            raise RuntimeError("verifier did not report all rewards 1")
        result["status"] = "oracle_passed"
    except Exception as exc:
        result["error_type"] = type(exc).__name__
    finally:
        try:
            await runtime.stop()
            result["cleanup"] = "deleted"
        except Exception as exc:
            result.update(cleanup="failed", cleanup_error_type=type(exc).__name__)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--taskset", required=True)
    parser.add_argument("--task-dir", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; choose a new evidence file")
    if not (args.task_dir / "solution/solve.sh").is_file():
        parser.error("task does not ship solution/solve.sh")
    result = asyncio.run(qualify(args))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(result["task"], result["status"], result["cleanup"])
    return int(result["status"] != "oracle_passed" or result["cleanup"] != "deleted")


if __name__ == "__main__":
    raise SystemExit(main())
