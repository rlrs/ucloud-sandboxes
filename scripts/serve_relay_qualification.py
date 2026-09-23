#!/usr/bin/env python3
"""Mount a separate PostgreSQL qualification relay beside the live PostgreSQL relay.

This is a temporary Linux experiment entrypoint, not a production cutover. The
original relay retains its journal and URLs. Only /qualification-NAME uses the
separately initialized test schema, and registrations must use relay-load-* IDs.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import re

from aiohttp import web

from ucloud_sandboxes import model_relay as api
from ucloud_sandboxes.cli import telemetry_from_config
from ucloud_sandboxes.relay_lifecycle import RelayLifecycleDispatcher
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.routing import RoutingStore
from ucloud_sandboxes.shared_control.credentials import read_private_dsn
from ucloud_sandboxes.shared_control.database import PostgresDatabase


@web.middleware
async def qualification_scope(request, handler):
    rollout = request.match_info.get("rollout_id") or request.query.get("rollout_id")
    if request.method == "POST" and request.path.endswith("/v1/relay/rollouts"):
        payload = await request.json()
        rollout = payload.get("rollout_id")
        sandbox = (payload.get("metadata") or {}).get("sandbox_id")
        if sandbox != rollout:
            raise web.HTTPBadRequest(
                text="qualification requires matching sandbox and rollout IDs"
            )
    if rollout is not None and not str(rollout).startswith("relay-load-"):
        raise web.HTTPForbidden(text="qualification accepts only relay-load-* IDs")
    return await handler(request)


def combined_app(config, store, prefix, *, live_store=None):
    lifecycle = RelayLifecycleDispatcher(
        f"http://127.0.0.1:{config.gateway_port}",
        config.gateway_token_file().read_text().strip(),
    )
    routes = RoutingStore(config.routing_file())
    telemetry = telemetry_from_config(config, "ucloud-sandboxes-model-relay")
    duration = telemetry.meter.create_histogram(
        "ucloud.platform.postgres.duration", unit="s"
    )

    def observe(sample):
        for phase in ("pool_wait", "transaction", "commit", "lock_query"):
            duration.record(
                getattr(sample, phase + "_seconds"),
                {
                    "operation": sample.operation,
                    "phase": phase,
                    "status": "ok" if sample.succeeded else "error",
                },
            )

    store.observe = observe

    async def unavailable(candidates):
        return await asyncio.to_thread(routes.terminal_sandbox_incarnations, candidates)

    async def park(request):
        return await lifecycle.notify(request, action="park")

    async def wake(request):
        return await lifecycle.notify(request, action="wake")

    common = dict(
        sandbox_bearer_token=config.relay_sandbox_token_file().read_text().strip(),
        worker_bearer_token=config.relay_worker_token_file().read_text().strip(),
        request_timeout_seconds=config.relay_request_timeout_seconds,
        worker_lease_seconds=config.relay_worker_lease_seconds,
        completed_request_retention_seconds=config.relay_completed_request_retention_seconds,
        accepted_notifier=park,
        result_notifier=wake,
        unavailable_callers=unavailable,
        telemetry=telemetry,
    )
    if live_store is None:
        if config.relay_postgres is None:
            raise ValueError(api.RELAY_POSTGRES_REQUIRED)
        pg = config.relay_postgres
        live_store = PostgresDatabase(
            read_private_dsn(Path(pg.dsn_file)), config.deployment_id, schema=pg.schema
        )
    from ucloud_sandboxes.shared_control.migration import assert_relay_cutover

    cutover = assert_relay_cutover(
        config.relay_state_file(), live_store.deployment_id, live_store.schema
    )
    live_store.expected_relay_import_digest = (
        cutover["source_digest"] if cutover else None
    )
    live = api.create_model_relay_app(postgres_store=live_store, **common)
    candidate = api.create_model_relay_app(postgres_store=store, **common)
    candidate.middlewares.append(qualification_scope)
    live.add_subapp(prefix, candidate)

    async def close(_app):
        await lifecycle.close()
        await asyncio.to_thread(telemetry.shutdown)

    live.on_cleanup.append(close)
    return live


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9]{1,24}", args.name):
        parser.error("name must be 1–24 lowercase letters/digits")
    config = DeploymentConfig.from_file(args.config)
    if config.relay_postgres is None:
        parser.error(api.RELAY_POSTGRES_REQUIRED)
    store = PostgresDatabase(
        read_private_dsn(args.dsn_file),
        config.deployment_id + ":qualification:" + args.name,
        schema="ucloud_shared_live_" + args.name,
    )
    web.run_app(
        combined_app(config, store, "/qualification-" + args.name),
        host="0.0.0.0",
        port=config.relay_port,
        print=None,
    )


if __name__ == "__main__":
    main()
