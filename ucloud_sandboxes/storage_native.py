from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import socket
import struct
import time
from typing import Any


AGENTENV_UBLK_PROTOCOL_MAX_BYTES = 16 * 1024 * 1024


class StorageNativeError(RuntimeError):
    pass


class StorageNativeTerminalError(StorageNativeError):
    pass


@dataclass(frozen=True)
class StorageNativeDevice:
    device_id: int
    device_path: Path
    virtual_size: int
    image_config_path: Path

    def __post_init__(self) -> None:
        if self.device_id < 0:
            raise ValueError("storage-native device id must be non-negative")
        if not self.device_path.is_absolute():
            raise ValueError("storage-native device path must be absolute")
        if self.virtual_size <= 0:
            raise ValueError("storage-native virtual size must be positive")
        if not self.image_config_path.is_absolute():
            raise ValueError("storage-native image config path must be absolute")


@dataclass(frozen=True)
class StorageNativeDeviceOwner:
    owner_id: str
    device_id: int
    device_path: Path
    image_config_path: Path

    def __post_init__(self) -> None:
        if not self.owner_id or len(self.owner_id) > 256:
            raise ValueError("storage-native device owner id is invalid")
        if "\n" in self.owner_id or "\r" in self.owner_id:
            raise ValueError("storage-native device owner id is invalid")
        if self.device_id < 0:
            raise ValueError("storage-native device id must be non-negative")
        if not self.device_path.is_absolute():
            raise ValueError("storage-native device path must be absolute")
        if not self.image_config_path.is_absolute():
            raise ValueError("storage-native image config path must be absolute")


@dataclass(frozen=True)
class StorageNativeLayer:
    digest: str
    size: int
    uuid: str = ""

    def __post_init__(self) -> None:
        if (
            not self.digest.startswith("sha256:")
            or len(self.digest) != len("sha256:") + 64
            or any(character not in "0123456789abcdef" for character in self.digest[7:])
        ):
            raise ValueError("storage-native layer digest must be sha256:<hex>")
        if self.size <= 0:
            raise ValueError("storage-native layer size must be positive")


class AgentEnvUblkClient:
    """Narrow synchronous client for AgentEnv's root-only ublk daemon.

    The storage-native experiment deliberately consumes only the stable device
    and restack operations. Sandbox lifecycle ownership remains in the direct
    Warden.
    """

    def __init__(
        self,
        socket_path: Path,
        *,
        timeout_seconds: float = 120.0,
    ) -> None:
        if not socket_path.is_absolute():
            raise ValueError("ublk daemon socket path must be absolute")
        if timeout_seconds <= 0:
            raise ValueError("ublk daemon timeout must be positive")
        self.socket_path = socket_path
        self.timeout_seconds = timeout_seconds

    def wait_ready(self, *, timeout_seconds: float = 30.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("ublk daemon readiness timeout must be positive")
        deadline = time.monotonic() + timeout_seconds
        last_error: OSError | None = None
        while time.monotonic() < deadline:
            try:
                response = self.get_features()
                if response >= 0:
                    return
            except (OSError, StorageNativeError) as exc:
                if isinstance(exc, OSError):
                    last_error = exc
                time.sleep(0.05)
        reason = f": {last_error}" if last_error is not None else ""
        raise StorageNativeError(f"ublk daemon did not become ready{reason}")

    def get_features(self) -> int:
        response = self._call({"kind": "get_features"})
        if response.get("status") != "features":
            raise StorageNativeError("ublk daemon returned an invalid feature response")
        flags = int(response.get("flags", -1))
        if flags < 0:
            raise StorageNativeError("ublk daemon returned invalid feature flags")
        return flags

    def create_runtime_device(
        self,
        *,
        source_image_config: Path,
        global_config: Path,
        runtime_dir: Path,
        virtual_size: int,
        owner_id: str,
        upper_mode: str = "logStructured",
    ) -> StorageNativeDevice:
        for label, path in {
            "source image config": source_image_config,
            "global config": global_config,
            "runtime directory": runtime_dir,
        }.items():
            if not path.is_absolute():
                raise ValueError(f"{label} must be absolute")
        if virtual_size <= 0:
            raise ValueError("storage-native virtual size must be positive")
        if not owner_id or len(owner_id) > 256 or "\n" in owner_id or "\r" in owner_id:
            raise ValueError("storage-native device owner id is invalid")
        if upper_mode not in {"sparse", "logStructured", "hybridLogStructured"}:
            raise ValueError("unsupported overlaybd upper mode")
        response = self._call(
            {
                "kind": "create_overlaybd_runtime_device",
                "source_image_config": str(source_image_config),
                "global_config": str(global_config),
                "runtime_dir": str(runtime_dir),
                "read_only": False,
                "runtime_upper_mode": upper_mode,
                "requested_virtual_size": virtual_size,
                "known_source_virtual_size": virtual_size,
                "allow_shrink": False,
                "owner_id": owner_id,
            }
        )
        if response.get("status") != "overlaybd_runtime_device_created":
            raise StorageNativeError("ublk daemon returned an invalid device response")
        return StorageNativeDevice(
            device_id=int(response["dev_id"]),
            device_path=Path(str(response["device_path"])),
            virtual_size=int(response["actual_virtual_size"]),
            image_config_path=Path(str(response["runtime_image_config_path"])),
        )

    def list_runtime_device_owners(self) -> tuple[StorageNativeDeviceOwner, ...]:
        response = self._call({"kind": "list_exclusive_owners"})
        if response.get("status") != "exclusive_owners":
            raise StorageNativeError("ublk daemon returned invalid owner inventory")
        raw_owners = response.get("owners")
        if not isinstance(raw_owners, list):
            raise StorageNativeError("ublk daemon returned invalid owner inventory")
        try:
            owners = tuple(
                StorageNativeDeviceOwner(
                    owner_id=str(raw["owner_id"]),
                    device_id=int(raw["dev_id"]),
                    device_path=Path(str(raw["device_path"])),
                    image_config_path=Path(str(raw["image_config_path"])),
                )
                for raw in raw_owners
                if isinstance(raw, dict)
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageNativeError(
                "ublk daemon returned invalid owner inventory"
            ) from exc
        if len(owners) != len(raw_owners):
            raise StorageNativeError("ublk daemon returned invalid owner inventory")
        if len({owner.owner_id for owner in owners}) != len(owners):
            raise StorageNativeError("ublk daemon returned duplicate owner identity")
        if len({owner.device_id for owner in owners}) != len(owners):
            raise StorageNativeError("ublk daemon returned duplicate owned device")
        return owners

    def restack_snapshot(
        self,
        device_id: int,
        output_layer_path: Path,
    ) -> StorageNativeLayer | None:
        if device_id < 0:
            raise ValueError("storage-native device id must be non-negative")
        if not output_layer_path.is_absolute():
            raise ValueError("snapshot layer path must be absolute")
        response = self._call(
            {
                "kind": "restack_snapshot",
                "dev_id": device_id,
                "output_layer_path": str(output_layer_path),
            }
        )
        if response.get("status") != "restack_snapshot_created":
            raise StorageNativeError(
                "ublk daemon returned an invalid snapshot response"
            )
        raw = response.get("descriptor")
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise StorageNativeError("ublk daemon returned an invalid layer descriptor")
        try:
            return StorageNativeLayer(
                digest=str(raw["digest"]),
                size=int(raw["size"]),
                uuid=str(raw.get("uuid") or ""),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageNativeError(
                "ublk daemon returned an invalid layer descriptor"
            ) from exc

    def export_dense_layer(
        self,
        *,
        source_layer_path: Path,
        stream_socket_path: Path,
    ) -> StorageNativeLayer:
        """Stream one sealed layer to a caller-owned Unix socket.

        The caller must bind and accept ``stream_socket_path`` concurrently
        before invoking this RPC. The returned descriptor authenticates the
        exact sequential byte stream sent by the backend.
        """

        if not source_layer_path.is_absolute():
            raise ValueError("source layer path must be absolute")
        if not stream_socket_path.is_absolute():
            raise ValueError("dense export stream socket path must be absolute")
        response = self._call(
            {
                "kind": "export_dense_layer",
                "source_layer_path": str(source_layer_path),
                "stream_socket_path": str(stream_socket_path),
            }
        )
        if response.get("status") != "dense_layer_exported":
            raise StorageNativeError(
                "ublk daemon returned an invalid dense export response"
            )
        try:
            return StorageNativeLayer(
                digest=str(response["digest"]),
                size=int(response["size"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageNativeError(
                "ublk daemon returned an invalid dense layer descriptor"
            ) from exc

    def delete(self, device_id: int) -> None:
        if device_id < 0:
            raise ValueError("storage-native device id must be non-negative")
        response = self._call({"kind": "delete", "dev_id": device_id})
        if response.get("status") != "deleted":
            raise StorageNativeError("ublk daemon did not delete the device")

    def release(self, device_id: int) -> None:
        """Return one exclusive runtime device to AgentEnv's warm pool."""

        if device_id < 0:
            raise ValueError("storage-native device id must be non-negative")
        response = self._call({"kind": "release_overlaybd", "dev_id": device_id})
        if response.get("status") != "released":
            raise StorageNativeError("ublk daemon did not release the device")

    def shutdown(self) -> None:
        try:
            response = self._call({"kind": "shutdown"})
        except (BrokenPipeError, ConnectionResetError, EOFError):
            return
        if response.get("status") != "ok":
            raise StorageNativeError("ublk daemon did not acknowledge shutdown")

    def _call(self, request: dict[str, Any]) -> dict[str, Any]:
        payload = json.dumps(
            request,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        if len(payload) > AGENTENV_UBLK_PROTOCOL_MAX_BYTES:
            raise ValueError("ublk daemon request is too large")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(self.timeout_seconds)
            connection.connect(str(self.socket_path))
            connection.sendall(struct.pack(">I", len(payload)))
            connection.sendall(payload)
            length = struct.unpack(">I", self._recv_exact(connection, 4))[0]
            if length > AGENTENV_UBLK_PROTOCOL_MAX_BYTES:
                raise StorageNativeError("ublk daemon response is too large")
            response = json.loads(self._recv_exact(connection, length).decode("utf-8"))
        if not isinstance(response, dict):
            raise StorageNativeError("ublk daemon response must be an object")
        status = str(response.get("status") or "")
        if status == "terminal_error":
            raise StorageNativeTerminalError(
                str(response.get("message") or "ublk daemon terminal failure")
            )
        if status in {"error", "invalid_request"}:
            raise StorageNativeError(
                str(response.get("message") or "ublk daemon request failed")
            )
        return response

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int) -> bytes:
        result = bytearray()
        while len(result) < size:
            chunk = connection.recv(size - len(result))
            if not chunk:
                raise EOFError("ublk daemon closed the connection early")
            result.extend(chunk)
        return bytes(result)
