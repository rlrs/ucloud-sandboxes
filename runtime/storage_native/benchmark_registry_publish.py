#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from ucloud_sandboxes.managed_registry import RegistryClient  # noqa: E402
from ucloud_sandboxes.storage_native import AgentEnvUblkClient  # noqa: E402
from ucloud_sandboxes.storage_native_registry import (  # noqa: E402
    RegistrySnapshotPublisher,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend-socket", required=True, type=Path)
    parser.add_argument("--source-layer", required=True, type=Path)
    parser.add_argument("--virtual-size", required=True, type=int)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--socket-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    publisher = RegistrySnapshotPublisher(
        RegistryClient(args.registry_url, timeout_seconds=120),
        repository=args.repository,
        stream_socket_root=args.socket_root.resolve(),
    )
    started = time.monotonic()
    publication = publisher.publish(
        exporter=AgentEnvUblkClient(args.backend_socket.resolve()),
        source_layer_paths=(args.source_layer.resolve(),),
        virtual_size=args.virtual_size,
    )
    elapsed = time.monotonic() - started
    payload = {**publication.to_dict(), "elapsed_seconds": elapsed}
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
