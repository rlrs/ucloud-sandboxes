#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from ucloud_sandboxes.storage_native import (  # noqa: E402
    AgentEnvUblkClient,
    StorageNativeLayer,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend-socket", required=True, type=Path)
    parser.add_argument("--source-layer", required=True, type=Path)
    parser.add_argument("--socket-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--capture", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = AgentEnvUblkClient(args.backend_socket.resolve())
    args.socket_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="dense-stream-",
        dir=args.socket_root,
    ) as raw_dir:
        stream_socket = Path(raw_dir) / "stream.sock"
        result: list[StorageNativeLayer] = []
        failure: list[BaseException] = []
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(stream_socket))
            stream_socket.chmod(0o600)
            listener.listen(1)

            def export() -> None:
                try:
                    result.append(
                        client.export_dense_layer(
                            source_layer_path=args.source_layer.resolve(),
                            stream_socket_path=stream_socket,
                        )
                    )
                except BaseException as exc:
                    failure.append(exc)

            thread = threading.Thread(target=export, daemon=True)
            started = time.monotonic()
            thread.start()
            hasher = hashlib.sha256()
            byte_count = 0
            connection, _ = listener.accept()
            capture = args.capture.open("wb") if args.capture else None
            with connection:
                while chunk := connection.recv(8 * 1024 * 1024):
                    hasher.update(chunk)
                    byte_count += len(chunk)
                    if capture is not None:
                        capture.write(chunk)
            if capture is not None:
                capture.close()
            thread.join(timeout=120)
            elapsed = time.monotonic() - started
        if thread.is_alive():
            raise TimeoutError("dense exporter did not finish")
        if failure:
            raise failure[0]
        if len(result) != 1:
            raise RuntimeError("dense exporter returned no descriptor")
        observed = StorageNativeLayer(
            digest=f"sha256:{hasher.hexdigest()}",
            size=byte_count,
        )
        if observed != result[0]:
            raise RuntimeError("stream digest does not match backend descriptor")

    payload = {
        "backend_descriptor": {
            "digest": result[0].digest,
            "size": result[0].size,
        },
        "elapsed_seconds": elapsed,
        "source_layer": str(args.source_layer.resolve()),
        "streamed_bytes": byte_count,
        "temporary_dense_bytes": 0,
    }
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
