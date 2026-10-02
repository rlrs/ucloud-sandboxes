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
from ucloud_sandboxes.shared_control.database import PostgresDatabase  # noqa: E402
from ucloud_sandboxes.shared_control.relay import PostgresRelayState  # noqa: E402
from ucloud_sandboxes import model_relay as api  # noqa: E402

DEPLOYMENT = "relay-crash-test"


def database(schema):
    return PostgresDatabase(os.environ["UCLOUD_TEST_POSTGRES_DSN"], DEPLOYMENT, schema=schema)


async def prepare(schema):
    admin = database(schema)
    await admin.open()
    try:
        await admin.migrate()
    finally:
        await admin.close()

    async def wake(request):
        return "crash-test-epoch"

    relay = PostgresRelayState(database(schema), result_notifier=wake)
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
            rollout_id="crash-test", endpoint="/v1/responses", body={"q": "crash"},
            headers={}, idempotency_key="crash",
        )
        (leased,) = await relay.poll(
            rollout_id="crash-test", registration_token=reg["registration_token"], timeout_seconds=1,
        )
        await relay.respond(
            request_id=request.request_id,
            registration_token=reg["registration_token"],
            lease_id=leased.lease_id,
            response=api.RelayWorkerResponse(200, b"live relay durable response"),
            defer_delivery=True,
        )
        (work,) = await relay._claim_lifecycle(1)
        assert work["request_id"] == request.request_id
    finally:
        await relay.aclose()
    print("Prepared a durable relay response, wake claim and reservation.")


async def verify(schema):
    admin = database(schema)
    await admin.open()
    try:
        async with admin.transaction("expire_relay_test_claim") as conn:
            await conn.execute(
                "UPDATE relay_lifecycle SET claim_until=clock_timestamp()-interval '1 second'"
            )
            row = await (await conn.execute(
                "SELECT request_id FROM relay_requests WHERE deployment_id=%s", (DEPLOYMENT,),
            )).fetchone()
            assert row is not None
        seen = []

        async def wake(request):
            seen.append(request.request_id)
            return "crash-test-epoch"

        relay = PostgresRelayState(database(schema), result_notifier=wake)
        await relay.open()
        try:
            async with relay.store.transaction("read_recovered_request") as conn:
                request = await relay._load(conn, row["request_id"])
            response = await relay.wait_for_response(request, timeout_seconds=5)
            assert response.body == b"live relay durable response"
            assert seen == [request.request_id]
        finally:
            await relay.aclose()
        async with admin.pool.connection() as conn:
            await conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    finally:
        await admin.close()
    print("Database crash recovery passed; test schema removed.")


async def run(args):
    if not args.schema.startswith("ucloud_shared_crash_"):
        raise ValueError("a dedicated ucloud_shared_crash_ schema is required")
    await (prepare if args.phase == "prepare" else verify)(args.schema)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "verify"))
    parser.add_argument("--schema", required=True)
    asyncio.run(run(parser.parse_args()))
