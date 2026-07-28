#!/usr/bin/env python3
"""Exercise every durable hibernate/restore crash region by reopening state."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import tempfile
import time

from ucloud_sandboxes.hibernation import (
    HibernationArtifactFile,
    HibernationArtifactStore,
    HibernationAuthority,
    HibernationFileRole,
    HibernationJournal,
    HibernationManifest,
    HibernationReconciler,
    HibernationRecoveryAction,
    HibernationRuntimeFingerprint,
    HibernationState,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
CONTAINER_ID = "d" * 64
RUNSC_COMMIT = "e" * 40


@dataclass(frozen=True)
class CaseResult:
    name: str
    cycles: int
    elapsed_ms: float

    def to_dict(self) -> dict[str, object]:
        return {
            "cycles": self.cycles,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "name": self.name,
            "operations_per_second": round(
                self.cycles / (self.elapsed_ms / 1000),
                3,
            ),
        }


def runtime_fingerprint() -> HibernationRuntimeFingerprint:
    return HibernationRuntimeFingerprint(
        runsc_sha256=SHA_A,
        runsc_commit=RUNSC_COMMIT,
        platform="systrap",
        architecture="x86_64",
        page_size=4096,
        cpu_features_sha256=SHA_B,
        boot_config_sha256=SHA_C,
        rootfs_sha256=SHA_A,
    )


def write_process(proc_root: Path, pid: int, ticks: int) -> None:
    process = proc_root / str(pid)
    process.mkdir(parents=True, exist_ok=True)
    suffix = ["S", *(["0"] * 18), str(ticks), "0"]
    (process / "stat").write_text(
        f"{pid} (runsc worker) " + " ".join(suffix) + "\n",
        encoding="ascii",
    )


def make_manifest(
    generation: Path,
    *,
    sandbox_id: str,
) -> HibernationManifest:
    paths = {
        HibernationFileRole.MAIN_MEMORY: generation / "application_memory.img",
        HibernationFileRole.KERNEL_STATE: generation / "checkpoint.img",
        HibernationFileRole.ALLOCATOR_METADATA: generation / "pages_meta.img",
    }
    for role, path in paths.items():
        path.write_bytes(role.value.encode("ascii"))
    return HibernationManifest(
        sandbox_id=sandbox_id,
        sandbox_generation=7,
        hibernation_generation=1,
        operation_id="park:1",
        spec_sha256=SHA_B,
        container_id=CONTAINER_ID,
        created_ns=1,
        runtime=runtime_fingerprint(),
        files=tuple(
            HibernationArtifactFile.from_path(path, role=role)
            for role, path in paths.items()
        ),
    )


def initialize(journal: HibernationJournal, sandbox_id: str):
    return journal.initialize_running(
        sandbox_id=sandbox_id,
        sandbox_generation=7,
        spec_sha256=SHA_B,
        operation_id="create:7",
        sentry_pid=101,
        sentry_start_time_ticks=1001,
    )


def park(
    journal: HibernationJournal,
    artifacts: HibernationArtifactStore,
    sandbox_id: str,
):
    running = initialize(journal, sandbox_id)
    hibernating = journal.begin_hibernate(
        operation_id="park:1",
        expected_revision=running.revision,
    )
    generation = artifacts.prepare_generation(
        sandbox_id=sandbox_id,
        sandbox_generation=7,
        hibernation_generation=1,
    )
    manifest = make_manifest(generation, sandbox_id=sandbox_id)
    artifacts.publish_complete(manifest)
    pending = journal.mark_sentry_reaped(
        operation_id="park:1",
        expected_revision=hibernating.revision,
    )
    parked = journal.commit_parked(
        manifest,
        operation_id="park:1",
        expected_revision=pending.revision,
    )
    return parked


def require_result(
    result,
    *,
    action: HibernationRecoveryAction,
    state: HibernationState,
    authority: HibernationAuthority,
) -> None:
    if (
        result.action != action
        or result.record.state != state
        or result.record.authority != authority
    ):
        raise AssertionError(
            "unexpected recovery result: "
            f"{result.action.value}/{result.record.state.value}/"
            f"{result.record.authority.value}"
        )
    live_owners = sum(
        value is not None
        for value in (result.record.sentry_pid, result.record.candidate_pid)
    )
    if live_owners > 1:
        raise AssertionError("recovery retained more than one process owner")


def run_case(
    root: Path,
    *,
    name: str,
    cycles: int,
) -> CaseResult:
    started = time.monotonic()
    for cycle in range(cycles):
        sandbox_id = f"{name}-{cycle}"
        case_root = root / sandbox_id
        case_root.mkdir(mode=0o700)
        case_root.chmod(0o700)
        journal_path = (case_root / "journal.json").resolve()
        journal = HibernationJournal(journal_path)
        artifacts = HibernationArtifactStore((case_root / "artifacts").resolve())
        alive_proc = case_root / "proc-alive"
        dead_proc = case_root / "proc-dead"
        alive_proc.mkdir(parents=True)
        dead_proc.mkdir()
        write_process(alive_proc, 101, 1001)

        expected_action: HibernationRecoveryAction
        expected_state: HibernationState
        expected_authority: HibernationAuthority
        proc_root = dead_proc
        resolver = None

        if name == "hibernate-begin-live":
            running = initialize(journal, sandbox_id)
            journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            proc_root = alive_proc
            expected_action = HibernationRecoveryAction.RESUME_OR_RETRY_HIBERNATE
            expected_state = HibernationState.HIBERNATING
            expected_authority = HibernationAuthority.LIVE
        elif name == "hibernate-sentry-dead-before-reap":
            running = initialize(journal, sandbox_id)
            journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            expected_action = HibernationRecoveryAction.FINISH_PENDING_GENERATION
            expected_state = HibernationState.HIBERNATING
            expected_authority = HibernationAuthority.PENDING
        elif name == "hibernate-complete-live-before-stop":
            running = initialize(journal, sandbox_id)
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            generation = artifacts.prepare_generation(
                sandbox_id=sandbox_id,
                sandbox_generation=7,
                hibernation_generation=hibernating.hibernation_generation,
            )
            artifacts.publish_complete(
                make_manifest(generation, sandbox_id=sandbox_id)
            )
            proc_root = alive_proc
            expected_action = (
                HibernationRecoveryAction.FINISH_PUBLISHED_GENERATION
            )
            expected_state = HibernationState.HIBERNATING
            expected_authority = HibernationAuthority.LIVE
        elif name == "hibernate-pending-no-complete":
            running = initialize(journal, sandbox_id)
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            journal.mark_sentry_reaped(
                operation_id="park:1",
                expected_revision=hibernating.revision,
            )
            expected_action = HibernationRecoveryAction.FINISH_PENDING_GENERATION
            expected_state = HibernationState.HIBERNATING
            expected_authority = HibernationAuthority.PENDING
        elif name == "hibernate-complete-before-commit":
            running = initialize(journal, sandbox_id)
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            journal.mark_sentry_reaped(
                operation_id="park:1",
                expected_revision=hibernating.revision,
            )
            generation = artifacts.prepare_generation(
                sandbox_id=sandbox_id,
                sandbox_generation=7,
                hibernation_generation=1,
            )
            artifacts.publish_complete(make_manifest(generation, sandbox_id=sandbox_id))
            expected_action = HibernationRecoveryAction.KEEP_PARKED
            expected_state = HibernationState.PARKED
            expected_authority = HibernationAuthority.PARKED
        elif name == "hibernate-committed":
            park(journal, artifacts, sandbox_id)
            expected_action = HibernationRecoveryAction.KEEP_PARKED
            expected_state = HibernationState.PARKED
            expected_authority = HibernationAuthority.PARKED
        elif name == "restore-before-launch":
            parked = park(journal, artifacts, sandbox_id)
            journal.begin_restore(
                operation_id="wake:1",
                expected_revision=parked.revision,
            )
            expected_action = HibernationRecoveryAction.RETRY_RESTORE
            expected_state = HibernationState.RESTORING
            expected_authority = HibernationAuthority.PARKED
        elif name == "restore-started-before-identity":
            parked = park(journal, artifacts, sandbox_id)
            journal.begin_restore(
                operation_id="wake:1",
                expected_revision=parked.revision,
            )
            write_process(alive_proc, 202, 2002)
            proc_root = alive_proc

            def resolve_candidate(_record):
                return (202, 2002)

            resolver = resolve_candidate
            expected_action = HibernationRecoveryAction.VERIFY_CANDIDATE
            expected_state = HibernationState.RESTORING
            expected_authority = HibernationAuthority.CANDIDATE
        elif name in {"restore-candidate-alive", "restore-candidate-dead"}:
            parked = park(journal, artifacts, sandbox_id)
            restoring = journal.begin_restore(
                operation_id="wake:1",
                expected_revision=parked.revision,
            )
            journal.mark_candidate_started(
                operation_id="wake:1",
                expected_revision=restoring.revision,
                candidate_pid=202,
                candidate_start_time_ticks=2002,
            )
            if name == "restore-candidate-alive":
                write_process(alive_proc, 202, 2002)
                proc_root = alive_proc
                expected_action = HibernationRecoveryAction.VERIFY_CANDIDATE
                expected_state = HibernationState.RESTORING
                expected_authority = HibernationAuthority.CANDIDATE
            else:
                expected_action = HibernationRecoveryAction.KEEP_PARKED
                expected_state = HibernationState.PARKED
                expected_authority = HibernationAuthority.PARKED
        elif name == "restore-running-committed":
            parked = park(journal, artifacts, sandbox_id)
            restoring = journal.begin_restore(
                operation_id="wake:1",
                expected_revision=parked.revision,
            )
            candidate = journal.mark_candidate_started(
                operation_id="wake:1",
                expected_revision=restoring.revision,
                candidate_pid=202,
                candidate_start_time_ticks=2002,
            )
            journal.commit_running(
                operation_id="wake:1",
                expected_revision=candidate.revision,
                sentry_pid=202,
                sentry_start_time_ticks=2002,
            )
            write_process(alive_proc, 202, 2002)
            proc_root = alive_proc
            expected_action = HibernationRecoveryAction.ADOPT_RUNNING
            expected_state = HibernationState.RUNNING
            expected_authority = HibernationAuthority.LIVE
        else:
            raise ValueError(f"unknown case: {name}")

        reopened = HibernationReconciler(
            HibernationJournal(journal_path),
            HibernationArtifactStore(artifacts.root),
            runtime_sha256=runtime_fingerprint().digest,
            proc_root=proc_root,
            candidate_identity_resolver=resolver,
        )
        require_result(
            reopened.reconcile(),
            action=expected_action,
            state=expected_state,
            authority=expected_authority,
        )
    return CaseResult(
        name=name,
        cycles=cycles,
        elapsed_ms=(time.monotonic() - started) * 1000,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycles", type=int, default=1000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.cycles < 1:
        parser.error("--cycles must be positive")
    cases = (
        "hibernate-begin-live",
        "hibernate-sentry-dead-before-reap",
        "hibernate-complete-live-before-stop",
        "hibernate-pending-no-complete",
        "hibernate-complete-before-commit",
        "hibernate-committed",
        "restore-before-launch",
        "restore-started-before-identity",
        "restore-candidate-alive",
        "restore-candidate-dead",
        "restore-running-committed",
    )
    with tempfile.TemporaryDirectory(prefix="gvisor-crash-recovery-") as raw_root:
        root = Path(raw_root)
        results = [run_case(root, name=name, cycles=args.cycles) for name in cases]
    payload = {
        "cases": [result.to_dict() for result in results],
        "cycles_per_case": args.cycles,
        "schema": 1,
        "total_restarts": args.cycles * len(cases),
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
