from contextlib import contextmanager
from dataclasses import replace
import errno
import hashlib
from http.client import HTTPConnection
from io import BytesIO
import json
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import tests.test_direct_provisioner as fixtures
from tests.test_control_plane import _gateway_server, _running_server, _seed_gateway_node
from ucloud_sandboxes.direct_service import DirectExecResult, DirectSandboxService
from ucloud_sandboxes.http_server import RequestBodyStream, TRANSFER_CHUNK_BYTES
from ucloud_sandboxes.sandbox import SandboxStartupBusyError
from ucloud_sandboxes.upload_spool import UploadSpool


class UploadSpoolTests(unittest.TestCase):
    def test_body_reader_bounds_reads_and_preserves_next_message(self):
        body = b'x' * (TRANSFER_CHUNK_BYTES * 3 + 7)
        source = BytesIO(body + b'next request')
        reader = RequestBodyStream(source, len(body))
        chunks = list(iter(reader.read, b''))
        self.assertEqual(b''.join(chunks), body)
        self.assertTrue(all(len(c) <= TRANSFER_CHUNK_BYTES for c in chunks))
        self.assertEqual(source.read(), b'next request')
        with self.assertRaisesRegex(ValueError, 'Content-Length'):
            RequestBodyStream(BytesIO(), 1).read()

    def test_staging_cleans_up_on_short_body_and_after_dispatch_failure(self):
        with TemporaryDirectory() as raw:
            spool = UploadSpool(Path(raw), min_free_bytes=0)
            with self.assertRaisesRegex(ValueError, 'Content-Length'):
                with spool.receive(BytesIO(b'partial'), 32):
                    self.fail('truncated upload was dispatched')
            self.assertEqual(spool._unwritten_bytes, 0)
            self.assertEqual(list(Path(raw).iterdir()), [])
            # ENOSPC after dispatch is not a safe admission retry.
            with self.assertRaises(OSError) as caught:
                with spool.receive(BytesIO(b'complete'), 8) as staged:
                    self.assertEqual(staged.read(), b'complete')
                    raise OSError(errno.ENOSPC, 'after dispatch')
            self.assertNotIsInstance(caught.exception, SandboxStartupBusyError)
            self.assertEqual(spool._unwritten_bytes, 0)
            self.assertTrue(staged.closed)
            self.assertEqual(list(Path(raw).iterdir()), [])

    def test_disk_admission_accounts_for_incoming_bytes_without_fifo_blocking(self):
        with TemporaryDirectory() as raw:
            spool = UploadSpool(Path(raw), min_free_bytes=10)
            space = SimpleNamespace(f_bavail=100, f_frsize=1)

            class Source(BytesIO):
                def read(inner, size):
                    self.assertLessEqual(size, TRANSFER_CHUNK_BYTES)
                    with self.assertRaises(SandboxStartupBusyError):
                        with spool.receive(BytesIO(b'x' * 40), 40):
                            self.fail('overcommitted incoming space')
                    with spool.receive(BytesIO(b'small'), 5) as small:
                        self.assertEqual(small.read(), b'small')
                    return super().read(size)

            with patch('ucloud_sandboxes.upload_spool.os.statvfs', return_value=space):
                with spool.receive(Source(b'x' * 60), 60) as staged:
                    self.assertEqual(staged.read(), b'x' * 60)
            self.assertEqual(spool._unwritten_bytes, 0)


class _DigestRunner:
    def __init__(self):
        self.calls = []

    def run(self, argv, *, input_bytes, timeout_seconds, max_stdout_bytes,
            max_stderr_bytes, input_file=None):
        digest = hashlib.sha256()
        size = 0
        if input_file is not None:
            assert input_bytes is None
            for chunk in iter(lambda: input_file.read(TRANSFER_CHUNK_BYTES), b''):
                size += len(chunk)
                digest.update(chunk)
        else:
            size = len(input_bytes or b'')
            digest.update(input_bytes or b'')
        self.calls.append((size, digest.hexdigest(), input_file is not None))
        return DirectExecResult(tuple(argv), 0, b'', b'')


class StreamingUploadTests(unittest.TestCase):
    @contextmanager
    def servers(self):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            node_root = root / 'node'
            node_root.mkdir()
            fixture = fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(node_root)
            runner = _DigestRunner()
            service = DirectSandboxService(provisioner, process_runner=runner)
            for sid in ('bulk', 'small'):
                fixture.create(service, replace(fixture.spec(), id=sid), generation=1)
            node = fixtures.build_direct_node_agent_server(
                '127.0.0.1', 0, service=service, image_file=node_root / 'images.json',
                job_id='job-1', node_id='node-1',
            )
            with _running_server(node) as node_url:
                for sid in ('bulk', 'small'):
                    heartbeat_file, route_file = _seed_gateway_node(
                        root, node_url=node_url, sandbox_id=sid,
                    )
                gateway = _gateway_server(root, heartbeat_file=heartbeat_file, routing_file=route_file)
                with _running_server(gateway):
                    yield gateway, service, runner

    def test_small_write_passes_a_stalled_large_upload_and_full_cold_start_queue(self):
        payload = b'abc\0' * (TRANSFER_CHUNK_BYTES // 4 * 3)
        with self.servers() as (gateway, service, runner):
            received = Event()
            original = service.upload_spool.receive

            @contextmanager
            def observed(source, length):
                # The worker sees bytes before the gateway receives the full
                # upload. This cannot pass with whole-request buffering.
                read = source.read
                def observe(size):
                    result = read(size)
                    received.set()
                    return result
                source.read = observe
                with original(source, length) as staged:
                    yield staged

            memory = gateway.RequestHandlerClass.upload_memory_limiter
            self.assertTrue(memory.acquire(weight=memory.capacity, blocking=False))
            cold = service._startup_slots
            self.assertTrue(cold.acquire(weight=cold.capacity, blocking=False))
            bulk = HTTPConnection(*gateway.server_address, timeout=5)
            small = HTTPConnection(*gateway.server_address, timeout=5)
            try:
                with patch.object(service.upload_spool, 'receive', observed):
                    bulk.putrequest('PUT', '/v1/sandboxes/bulk/files?path=/bulk')
                    bulk.putheader('Content-Length', str(len(payload)))
                    bulk.endheaders()
                    bulk.send(payload[:TRANSFER_CHUNK_BYTES])
                    self.assertTrue(received.wait(3), 'worker did not receive streaming bytes')
                    small.request('PUT', '/v1/sandboxes/small/files?path=/tiny', body=b'tiny\0')
                    response = small.getresponse()
                    result = json.load(response)
                    self.assertEqual(response.status, 200, result)
                    self.assertEqual(result['size'], 5)
                    self.assertEqual(runner.calls, [(5, hashlib.sha256(b'tiny\0').hexdigest(), False)])
                    bulk.send(payload[TRANSFER_CHUNK_BYTES:])
                    response = bulk.getresponse()
                    result = json.load(response)
                    self.assertEqual(response.status, 200, result)
                    self.assertEqual(result['size'], len(payload))
                self.assertEqual(runner.calls[-1], (len(payload), hashlib.sha256(payload).hexdigest(), True))
                self.assertEqual(service.upload_spool._unwritten_bytes, 0)
                self.assertEqual(list(service.upload_spool.directory.iterdir()), [])
            finally:
                bulk.close()
                small.close()
                memory.release(weight=memory.capacity)
                cold.release(weight=cold.capacity)

    def test_truncated_upload_never_dispatches_a_file_write(self):
        with self.servers() as (gateway, service, runner):
            connection = HTTPConnection(*gateway.server_address, timeout=5)
            try:
                connection.putrequest('PUT', '/v1/sandboxes/bulk/files?path=/bulk')
                connection.putheader('Content-Length', str(TRANSFER_CHUNK_BYTES * 2))
                connection.endheaders()
                connection.send(b'x' * TRANSFER_CHUNK_BYTES)
                connection.sock.shutdown(socket.SHUT_WR)
                response = connection.getresponse()
                self.assertEqual(response.status, 400, response.read())
                self.assertEqual(runner.calls, [])
            finally:
                connection.close()

    def test_disk_pressure_returns_safe_retry_after_consuming_large_body(self):
        with self.servers() as (gateway, service, runner):
            service.upload_spool.min_free_bytes = 2**63
            connection = HTTPConnection(*gateway.server_address, timeout=5)
            try:
                connection.request('PUT', '/v1/sandboxes/bulk/files?path=/bulk', body=b'x' * (8 * 1024**2))
                response = connection.getresponse()
                result = json.load(response)
                self.assertEqual(response.status, 503, result)
                self.assertEqual(result['error_code'], 'node_startup_busy')
                self.assertEqual(runner.calls, [])
                self.assertEqual(service.upload_spool._unwritten_bytes, 0)
            finally:
                connection.close()

    def test_upload_fences_replacement_during_receive(self):
        with self.servers() as (gateway, service, runner):
            original = service._require_registration
            calls = 0
            def replaced(sid):
                nonlocal calls
                calls += 1
                registration = original(sid)
                return registration if calls == 1 else replace(registration, sandbox_generation=2)
            connection = HTTPConnection(*gateway.server_address, timeout=5)
            try:
                with patch.object(service, '_require_registration', replaced):
                    connection.request('PUT', '/v1/sandboxes/bulk/files?path=/bulk', body=b'x' * (TRANSFER_CHUNK_BYTES + 1))
                    response = connection.getresponse()
                    self.assertEqual(response.status, 409, response.read())
                    self.assertEqual(runner.calls, [])
            finally:
                connection.close()
