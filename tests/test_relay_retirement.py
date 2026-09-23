from contextlib import closing
import asyncio
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from tests.legacy_relay_fixture import write_legacy_journal
from ucloud_sandboxes import model_relay
from ucloud_sandboxes.shared_control import legacy_relay


class RelayRetirementTests(TestCase):
    def test_old_live_configuration_fails_before_any_sqlite_write(self):
        self.assertFalse(hasattr(model_relay, "ModelRelayState"))
        self.assertFalse(hasattr(model_relay, "RelaySqliteStore"))
        with TemporaryDirectory() as raw:
            path = Path(raw) / "old.sqlite"
            for kwargs in (
                {},
                {"state_path": path},
                {"state_path": path, "postgres_store": object()},
            ):
                with (
                    self.subTest(kwargs=kwargs),
                    patch("sqlite3.connect", side_effect=AssertionError("live SQLite")),
                ):
                    with self.assertRaisesRegex(
                        ValueError, "0.5.114 requires PostgreSQL.*import-idle-relay"
                    ):
                        model_relay.create_model_relay_app(**kwargs)
            self.assertFalse(path.exists())

    def test_cli_rejects_old_configuration_before_reading_credentials_or_opening_routes(
        self,
    ):
        from ucloud_sandboxes.cli import cmd_serve_model_relay

        with patch(
            "ucloud_sandboxes.cli.load_config",
            return_value=SimpleNamespace(relay_postgres=None),
        ):
            with self.assertRaisesRegex(ValueError, "configure relay_postgres"):
                cmd_serve_model_relay(SimpleNamespace())

    def test_offline_import_limits_are_checked_before_json_decoding(self):

        with TemporaryDirectory() as raw:
            path = Path(raw) / "old.sqlite"
            write_legacy_journal(
                path, rollouts=[{"rollout_id": "r", "payload": "x" * 100}]
            )
            with closing(sqlite3.connect(path)) as source:
                for setting in ("MAX_IMPORT_ROWS", "MAX_IMPORT_PAYLOAD_BYTES"):
                    with (
                        patch.object(legacy_relay, setting, 0),
                        patch.object(
                            legacy_relay.json,
                            "loads",
                            side_effect=AssertionError("unbounded decode"),
                        ),
                    ):
                        with self.assertRaisesRegex(
                            ValueError, "bounded offline import"
                        ):
                            legacy_relay.read_rows(source)

    def test_offline_codec_preserves_exact_completed_binary_and_binding(self):
        loop = asyncio.new_event_loop()
        try:
            request = model_relay.RelayRequest(
                "request",
                "rollout",
                "a" * 32,
                "/v1/responses",
                "POST",
                None,
                {},
                1,
                loop.create_future(),
                state="completed",
                sandbox_id="sandbox",
                sandbox_generation=7,
                completed_response=model_relay.RelayWorkerResponse(
                    207, b"\0\xffreply", {"x-value": "preserved"}
                ),
                completed_at=2,
                completed_bytes=10,
                idempotency_key="retry",
                request_digest="b" * 64,
            )
            encoded = legacy_relay.encode_request(request)
            decoded = legacy_relay.decode_request(
                json.loads(json.dumps(encoded)), loop=loop
            )
            self.assertEqual(decoded.completed_response, request.completed_response)
            self.assertEqual(
                (decoded.sandbox_id, decoded.sandbox_generation), ("sandbox", 7)
            )
            self.assertEqual(decoded.idempotency_key, "retry")
            for field in ("delivery_pending", "reattachable"):
                with self.assertRaises(ValueError):
                    legacy_relay.decode_request({**encoded, field: 1}, loop=loop)
        finally:
            loop.close()
