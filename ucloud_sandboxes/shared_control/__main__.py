"""Explicit live relay migration/status and isolated scheduling qualification."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys


async def run(args):
    from .database import PostgresDatabase
    from .credentials import read_private_dsn

    qualification = args.command.startswith("qualification-")
    if qualification:
        from .qualification import QualificationControlStore
        database = QualificationControlStore
    else:
        database = PostgresDatabase
    store = database(read_private_dsn(args.dsn_file), args.deployment_id, schema=args.schema)
    try:
        await store.open()
        if args.command in {"migrate", "qualification-migrate"}:
            await store.migrate()
            return {
                "schema": args.schema,
                "authority": "qualification-only" if qualification else "relay",
                "version": store.schema_version,
                "migrated": True,
            }
        if args.command == "import-idle-relay":
            from .migration import import_idle_relay
            if args.sqlite_file is None:
                raise ValueError("--sqlite-file is required for idle relay cutover")
            return await import_idle_relay(store, args.sqlite_file)
        if qualification:
            return {"authority": "qualification-only", **await store.snapshot()}
        from .relay import PostgresRelayState
        # Status is read-only: do not start dispatchers, record runtime mode,
        # initialize quota, or otherwise pretend this CLI is a relay process.
        return {"authority": "relay", "schema": args.schema, **await PostgresRelayState(store).stats()}
    finally:
        await store.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("migrate", "status", "import-idle-relay", "qualification-migrate", "qualification-status"))
    parser.add_argument("--sqlite-file", type=Path)
    parser.add_argument("--dsn-file", type=Path, required=True)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--schema", default="ucloud_shared")
    args = parser.parse_args(argv)
    try:
        print(json.dumps(asyncio.run(run(args))))
    except ImportError:
        print("Install the server's postgres extra to use shared-control commands.", file=sys.stderr)
        return 1
    except Exception as exc:
        # Database connection errors may carry connection parameters. Never
        # print the DSN or an unfiltered exception to a shell/CI log.
        print(f"Shared-control {args.command} failed ({type(exc).__name__}).", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
