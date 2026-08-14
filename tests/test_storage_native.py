from __future__ import annotations

import json
import socket
import struct
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from typing_extensions import Self

from ucloud_sandboxes.storage_native import (
    AGENTENV_UBLK_PROTOCOL_MAX_BYTES,
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

    def __enter__(self) -> Self:
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


class RawUblkDaemon:
    def __init__(self, socket_path: Path, response_chunks: list[bytes]) -> None:
        self.socket_path = socket_path
        self.response_chunks = response_chunks
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self) -> Self:
        self._thread.start()
        if not self._ready.wait(timeout=2):
            raise RuntimeError("raw ublk daemon did not start")
        return self

    def __exit__(self, *_args) -> None:
        self._thread.join(timeout=2)
        if self._thread.is_alive():
            raise RuntimeError("raw ublk daemon did not stop")

    def _serve(self) -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(self.socket_path))
            server.listen()
            self._ready.set()
            connection, _ = server.accept()
            with connection:
                request_length = struct.unpack(
                    ">I", FakeUblkDaemon._recv_exact(connection, 4)
                )[0]
                FakeUblkDaemon._recv_exact(connection, request_length)
                for chunk in self.response_chunks:
                    connection.sendall(chunk)


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
                [request["kind"] for request in daemon.requests],
                [
                    "create_overlaybd_runtime_device",
                    "restack_snapshot",
                    "export_dense_layer",
                    "export_compacted_image",
                    "release_overlaybd",
                    "delete",
                ],
            )
            self.assertEqual(daemon.requests[0]["owner_id"], "device:test-runtime")
            self.assertEqual(daemon.requests[0]["requested_virtual_size"], 1 << 30)
            self.assertEqual(daemon.requests[-2]["dev_id"], 7)

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

    def test_protocol_accepts_a_response_split_across_short_reads(self) -> None:
        with TemporaryDirectory() as raw_dir:
            socket_path = Path(raw_dir) / "ublk.sock"
            body = b'{"flags":7,"status":"features"}'
            framed = struct.pack(">I", len(body)) + body
            with RawUblkDaemon(
                socket_path,
                [framed[index : index + 1] for index in range(len(framed))],
            ):
                self.assertEqual(AgentEnvUblkClient(socket_path).get_features(), 7)

    def test_protocol_rejects_oversized_truncated_and_malformed_frames(self) -> None:
        cases = {
            "too large": [
                struct.pack(">I", AGENTENV_UBLK_PROTOCOL_MAX_BYTES + 1)
            ],
            "truncated": [struct.pack(">I", 20), b"{}"],
            "malformed JSON": [struct.pack(">I", 1), b"{"],
            "object": [struct.pack(">I", 2), b"[]"],
        }
        for expected, chunks in cases.items():
            with self.subTest(expected=expected), TemporaryDirectory() as raw_dir:
                socket_path = Path(raw_dir) / "ublk.sock"
                with RawUblkDaemon(socket_path, chunks), self.assertRaisesRegex(
                    StorageNativeError, expected
                ):
                    AgentEnvUblkClient(socket_path).get_features()

    def test_owner_inventory_rejects_duplicate_and_malformed_entries(self) -> None:
        valid_owner = {
            "owner_id": "device:test-runtime",
            "dev_id": 7,
            "device_path": "/dev/ublkb7",
            "image_config_path": "/run/ucloud/image.json",
        }
        responses = (
            {"status": "exclusive_owners", "owners": [valid_owner, valid_owner]},
            {"status": "exclusive_owners", "owners": [{"owner_id": "missing"}]},
            {"status": "exclusive_owners", "owners": "not-a-list"},
        )
        for response in responses:
            with self.subTest(response=response), TemporaryDirectory() as raw_dir:
                socket_path = Path(raw_dir) / "ublk.sock"
                with FakeUblkDaemon(socket_path, [response]), self.assertRaisesRegex(
                    StorageNativeError, "owner inventory|duplicate"
                ):
                    AgentEnvUblkClient(socket_path).list_runtime_device_owners()


if __name__ == "__main__":
    unittest.main()
