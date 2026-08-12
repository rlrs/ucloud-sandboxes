from __future__ import annotations

import json
from pathlib import Path
import socket
import struct
from tempfile import TemporaryDirectory
import threading
import unittest

from ucloud_sandboxes.storage_native import (
    AgentEnvUblkClient,
    StorageNativeError,
    StorageNativeTerminalError,
)


class FakeUblkDaemon:
    def __init__(self, socket_path: Path, responses: list[dict]) -> None:
        self.socket_path = socket_path
        self.responses = responses
        self.requests: list[dict] = []
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self) -> "FakeUblkDaemon":
        self._thread.start()
        if not self._ready.wait(timeout=2):
            raise RuntimeError("fake ublk daemon did not start")
        return self

    def __exit__(self, *_args) -> None:
        self._thread.join(timeout=2)

    def _serve(self) -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(self.socket_path))
            server.listen()
            self._ready.set()
            for response in self.responses:
                connection, _ = server.accept()
                with connection:
                    length = struct.unpack(">I", self._recv_exact(connection, 4))[0]
                    request = json.loads(
                        self._recv_exact(connection, length).decode("ascii")
                    )
                    self.requests.append(request)
                    payload = json.dumps(response).encode("utf-8")
                    connection.sendall(struct.pack(">I", len(payload)) + payload)

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int) -> bytes:
        result = bytearray()
        while len(result) < size:
            chunk = connection.recv(size - len(result))
            if not chunk:
                raise EOFError
            result.extend(chunk)
        return bytes(result)


class StorageNativeTests(unittest.TestCase):
    def test_owner_inventory_protocol(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            socket_path = root / "ublk.sock"
            with FakeUblkDaemon(
                socket_path,
                [
                    {
                        "status": "exclusive_owners",
                        "owners": [
                            {
                                "owner_id": "device:test-runtime",
                                "dev_id": 7,
                                "device_path": "/dev/ublkb7",
                                "image_config_path": str(root / "runtime/image.json"),
                            }
                        ],
                    }
                ],
            ) as daemon:
                owners = AgentEnvUblkClient(
                    socket_path
                ).list_runtime_device_owners()

            self.assertEqual(len(owners), 1)
            self.assertEqual(owners[0].owner_id, "device:test-runtime")
            self.assertEqual(owners[0].device_id, 7)
            self.assertEqual(
                daemon.requests,
                [{"kind": "list_exclusive_owners"}],
            )

    def test_runtime_device_and_snapshot_protocol(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            socket_path = root / "ublk.sock"
            responses = [
                {
                    "status": "overlaybd_runtime_device_created",
                    "dev_id": 7,
                    "device_path": "/dev/ublkb7",
                    "actual_virtual_size": 1 << 30,
                    "runtime_image_config_path": str(root / "runtime/image.json"),
                },
                {
                    "status": "restack_snapshot_created",
                    "descriptor": {
                        "digest": "sha256:" + "a" * 64,
                        "size": 1234,
                        "uuid": "layer-1",
                    },
                },
                {
                    "status": "dense_layer_exported",
                    "digest": "sha256:" + "b" * 64,
                    "size": 1222,
                },
                {
                    "status": "dense_layer_exported",
                    "digest": "sha256:" + "c" * 64,
                    "size": 2444,
                },
                {"status": "released"},
                {"status": "deleted"},
            ]
            with FakeUblkDaemon(socket_path, responses) as daemon:
                client = AgentEnvUblkClient(socket_path)
                device = client.create_runtime_device(
                    source_image_config=(root / "source.json").resolve(),
                    global_config=(root / "global.json").resolve(),
                    runtime_dir=(root / "runtime").resolve(),
                    virtual_size=1 << 30,
                    owner_id="device:test-runtime",
                )
                layer = client.restack_snapshot(
                    device.device_id,
                    (root / "snapshot.commit").resolve(),
                )
                dense = client.export_dense_layer(
                    source_layer_path=(root / "snapshot.commit").resolve(),
                    stream_socket_path=(root / "export.sock").resolve(),
                )
                compacted = client.export_compacted_image(
                    source_image_config=(root / "compact-source.json").resolve(),
                    global_config=(root / "global.json").resolve(),
                    stream_socket_path=(root / "compact.sock").resolve(),
                )
                client.release(device.device_id)
                client.delete(device.device_id)

            self.assertEqual(device.device_id, 7)
            self.assertEqual(device.device_path, Path("/dev/ublkb7"))
            self.assertIsNotNone(layer)
            assert layer is not None
            self.assertEqual(layer.size, 1234)
            self.assertEqual(dense.digest, "sha256:" + "b" * 64)
            self.assertEqual(dense.size, 1222)
            self.assertEqual(compacted.digest, "sha256:" + "c" * 64)
            self.assertEqual(compacted.size, 2444)
            self.assertEqual(
                daemon.requests[0],
                {
                    "allow_shrink": False,
                    "global_config": str((root / "global.json").resolve()),
                    "kind": "create_overlaybd_runtime_device",
                    "known_source_virtual_size": 1 << 30,
                    "owner_id": "device:test-runtime",
                    "read_only": False,
                    "requested_virtual_size": 1 << 30,
                    "runtime_dir": str((root / "runtime").resolve()),
                    "runtime_upper_mode": "logStructured",
                    "source_image_config": str((root / "source.json").resolve()),
                },
            )
            self.assertEqual(daemon.requests[1]["kind"], "restack_snapshot")
            self.assertEqual(
                daemon.requests[2],
                {
                    "kind": "export_dense_layer",
                    "source_layer_path": str(
                        (root / "snapshot.commit").resolve()
                    ),
                    "stream_socket_path": str((root / "export.sock").resolve()),
                },
            )
            self.assertEqual(
                daemon.requests[3],
                {
                    "global_config": str((root / "global.json").resolve()),
                    "kind": "export_compacted_image",
                    "source_image_config": str(
                        (root / "compact-source.json").resolve()
                    ),
                    "stream_socket_path": str((root / "compact.sock").resolve()),
                },
            )
            self.assertEqual(
                daemon.requests[4],
                {"kind": "release_overlaybd", "dev_id": 7},
            )

    def test_errors_are_typed(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            socket_path = root / "ublk.sock"
            with FakeUblkDaemon(
                socket_path,
                [
                    {"status": "error", "message": "ordinary"},
                    {"status": "terminal_error", "message": "mutated"},
                ],
            ):
                client = AgentEnvUblkClient(socket_path)
                with self.assertRaisesRegex(StorageNativeError, "ordinary"):
                    client.get_features()
                with self.assertRaisesRegex(StorageNativeTerminalError, "mutated"):
                    client.get_features()

    def test_rejects_relative_paths_and_invalid_sizes(self) -> None:
        with self.assertRaisesRegex(ValueError, "absolute"):
            AgentEnvUblkClient(Path("relative.sock"))
        client = AgentEnvUblkClient(Path("/tmp/does-not-exist.sock"))
        with self.assertRaisesRegex(ValueError, "positive"):
            client.create_runtime_device(
                source_image_config=Path("/tmp/source.json"),
                global_config=Path("/tmp/global.json"),
                runtime_dir=Path("/tmp/runtime"),
                virtual_size=0,
                owner_id="device:test-invalid-size",
            )


if __name__ == "__main__":
    unittest.main()
