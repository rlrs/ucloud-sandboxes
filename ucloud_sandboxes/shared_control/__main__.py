"""Explicit schema management for the qualification backend."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys


async def run(args):
    from .postgres import PostgresControlStore
    from .credentials import read_private_dsn
    store = PostgresControlStore(read_private_dsn(args.dsn_file), args.deployment_id, schema=args.schema)
    try:
        await store.open()
        if args.command == "migrate":
            await store.migrate()
            return {"schema": args.schema, "version": 1, "relay_version": 1, "migrated": True}
        if args.command == "import-idle-relay":
            from .migration import import_idle_relay
            if args.sqlite_file is None:
                raise ValueError("--sqlite-file is required for idle relay cutover")
            return await import_idle_relay(store, args.sqlite_file)
        return await store.snapshot()
    finally:
        await store.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("migrate", "status", "import-idle-relay"))
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
