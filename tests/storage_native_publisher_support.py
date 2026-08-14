from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket

from ucloud_sandboxes.storage_native import StorageNativeLayer


def digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


class FakeExporter:
    """Streams test layers through the publisher's real Unix-socket boundary."""

    def __init__(
        self,
        payloads: dict[Path, bytes],
        *,
        compact_payload: bytes = b"compact",
        wrong_digest: bool = False,
    ) -> None:
        self.payloads = payloads
        self.compact_payload = compact_payload
        self.wrong_digest = wrong_digest
        self.compact_calls: list[tuple[dict, Path]] = []

    def export_dense_layer(
        self,
        *,
        source_layer_path: Path,
        stream_socket_path: Path,
    ) -> StorageNativeLayer:
        return self._send(self.payloads[source_layer_path], stream_socket_path)

    def export_compacted_image(
        self,
        *,
        source_image_config: Path,
        global_config: Path,
        stream_socket_path: Path,
    ) -> StorageNativeLayer:
        self.compact_calls.append(
            (
                json.loads(source_image_config.read_text(encoding="ascii")),
                global_config,
            )
        )
        return self._send(self.compact_payload, stream_socket_path)

    def _send(self, payload: bytes, stream_socket_path: Path) -> StorageNativeLayer:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(str(stream_socket_path))
            for offset in range(0, len(payload), 7):
                connection.sendall(payload[offset : offset + 7])
        observed = digest(payload)
        if self.wrong_digest:
            observed = "sha256:" + "0" * 64
        return StorageNativeLayer(digest=observed, size=len(payload))
