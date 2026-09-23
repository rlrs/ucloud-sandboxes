#!/usr/bin/env python3
"""Two-phase database crash test; run only against an isolated test PostgreSQL.

Run prepare, kill/restart the test database, then run verify with the same schema.
The DSN comes from UCLOUD_TEST_POSTGRES_DSN, never printed or passed as an argument.
"""

import argparse
import asyncio
import os
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from psycopg import sql  # noqa: E402
from ucloud_sandboxes.shared_control import fixtures  # noqa: E402
from ucloud_sandboxes.shared_control.model import WakeProof  # noqa: E402
from ucloud_sandboxes.shared_control.qualification import QualificationControlStore  # noqa: E402
from ucloud_sandboxes.shared_control.database import PostgresDatabase  # noqa: E402
from ucloud_sandboxes.shared_control.relay import PostgresRelayState  # noqa: E402
from ucloud_sandboxes import model_relay as api  # noqa: E402


async def run(args):
    if not args.schema.startswith("ucloud_shared_crash_"):
        raise ValueError("a dedicated ucloud_shared_crash_ schema is required")
    store = QualificationControlStore(
        os.environ["UCLOUD_TEST_POSTGRES_DSN"], "crash-test", schema=args.schema
    )
    await store.open()
    try:
        if args.phase == "prepare":
            await store.migrate()
            relay_database = PostgresDatabase(
                os.environ["UCLOUD_TEST_POSTGRES_DSN"], "relay-crash-test", schema=args.schema,
            )
            await relay_database.open()
            try:
                await relay_database.migrate()
            finally:
                await relay_database.close()
            await fixtures.node(store, "node")
            await fixtures.sandbox(store, "sandbox", "node")
            await fixtures.request(store, "request", "sandbox")
            await store.accept_result(
                "request",
                registration_id="registration-1",
                lease_id="lease-1",
                body=b"durable model response",
            )
            (op,) = await store.claim_due()
            assert await store.prepare_dispatch(op)

            async def wake(request):
                return "crash-test-epoch"

            relay = PostgresRelayState(
                PostgresDatabase(
                    os.environ["UCLOUD_TEST_POSTGRES_DSN"],
                    "relay-crash-test",
                    schema=args.schema,
                ),
                result_notifier=wake,
            )
            # Keep the durable claim pending without canceling arbitrary
            # database work; shutdown/restart recovery has separate coverage.
            with patch.object(relay, "_dispatch_loop", asyncio.Event().wait):
                await relay.open()
            try:
                reg = await relay.register_rollout(
                    "crash-test",
                    {
                        "sandbox_id": "sandbox",
                        "sandbox_generation": 1,
                        api.AGENT_LIFECYCLE_METADATA_KEY: api.MANAGED_AGENT_LIFECYCLE,
                    },
                )
                request = await relay.enqueue(
                    rollout_id="crash-test",
                    endpoint="/v1/responses",
                    body={"q": "crash"},
                    headers={},
                    idempotency_key="crash",
                )
                (leased,) = await relay.poll(
                    rollout_id="crash-test",
                    registration_token=reg["registration_token"],
                    timeout_seconds=1,
                )
                await relay.respond(
                    request_id=request.request_id,
                    registration_token=reg["registration_token"],
                    lease_id=leased.lease_id,
                    response=api.RelayWorkerResponse(
                        200, b"live relay durable response"
                    ),
                    defer_delivery=True,
                )
                (work,) = await relay._claim_lifecycle(1)
                assert work["request_id"] == request.request_id
            finally:
                await relay.aclose()
            print(
                "Prepared durable responses, wake operations, relay claim and reservation."
            )
        else:
            snapshot = await store.snapshot()
            assert snapshot["responses"] == 1 and snapshot["operations"] == {
                "dispatching": 1
            }, snapshot
            assert snapshot["nodes"][0]["reserved_restore_mb"] == 128, snapshot
            # Move only the dispatcher claim clock forward; never release capacity.
            async with store.transaction("test_expire_claim") as conn:
                await conn.execute(
                    "UPDATE wake_operations SET claim_until=clock_timestamp()-interval '1 second'"
                )
            (op,) = await store.claim_due()
            assert await store.prepare_dispatch(op)
            assert await store.complete(
                op,
                WakeProof(
                    op.operation_id,
                    op.generation,
                    op.node_epoch,
                    op.lifecycle_sequence,
                    1,
                ),
            )
            assert (
                await store.read_result("request", registration_id="registration-1")
                == b"durable model response"
            )
            assert (await store.snapshot())["nodes"][0]["reserved_restore_mb"] == 0
            async with store.transaction("expire_relay_test_claim") as conn:
                await conn.execute(
                    "UPDATE relay_lifecycle SET claim_until=clock_timestamp()-interval '1 second'"
                )
                row = await (
                    await conn.execute(
                        "SELECT request_id FROM relay_requests WHERE deployment_id='relay-crash-test'"
                    )
                ).fetchone()
                assert row is not None
            seen = []

            async def wake(request):
                seen.append(request.request_id)
                return "crash-test-epoch"

            relay = PostgresRelayState(
                PostgresDatabase(
                    os.environ["UCLOUD_TEST_POSTGRES_DSN"],
                    "relay-crash-test",
                    schema=args.schema,
                ),
                result_notifier=wake,
            )
            await relay.open()
            try:
                async with relay.store.transaction("read_recovered_request") as conn:
                    request = await relay._load(conn, row["request_id"])
                response = await relay.wait_for_response(request, timeout_seconds=5)
                assert response.body == b"live relay durable response"
                assert seen == [request.request_id]
            finally:
                await relay.aclose()
            async with store.pool.connection() as conn:
                await conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(
                        sql.Identifier(args.schema)
                    )
                )
            print("Database crash recovery passed; test schema removed.")
    finally:
        await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "verify"))
    parser.add_argument("--schema", required=True)
    asyncio.run(run(parser.parse_args()))
