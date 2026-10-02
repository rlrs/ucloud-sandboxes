import unittest
from unittest.mock import Mock, patch

from ucloud_sandboxes.managed_registry import RegistryClient, RegistryRequestError
from tests.test_registry_client_contract import _RegistryHTTPServer

TEST_TIER = "contract"

DIGEST = 'sha256:' + 'a' * 64


class RegistryBlobMountTests(unittest.TestCase):
    def test_mount_links_without_transferring_content_over_http(self):
        def respond(method, path, headers, body):
            self.assertEqual(method, 'POST')
            self.assertEqual(path, '/v2/ucloud-managed/target/blobs/uploads/?mount=sha256%3A' + 'a' * 64 + '&from=ucloud-build-cache')
            self.assertEqual(body, b'')
            return 201, {'Docker-Content-Digest': DIGEST}, b''
        with _RegistryHTTPServer(respond) as server:
            self.assertTrue(RegistryClient(server.base_url).mount_blob('ucloud-managed/target', 'ucloud-build-cache', DIGEST))
        self.assertEqual(len(server.requests), 1)

    def test_declined_mount_cancels_only_its_session(self):
        response = Mock(status=202, headers={'Location': 'http://registry/v2/target/blobs/uploads/session?state=opaque'})
        cancelled = Mock()
        client = RegistryClient('http://registry')
        with patch.object(client, '_request', side_effect=[response, cancelled]) as request:
            self.assertFalse(client.mount_blob('target', 'source', DIGEST, timeout_seconds=2))
        self.assertEqual(request.call_args_list[1].args, ('/v2/target/blobs/uploads/session?state=opaque',))
        self.assertEqual(request.call_args_list[1].kwargs['method'], 'DELETE')
        self.assertLessEqual(request.call_args_list[1].kwargs['timeout_seconds'], 1)
        response.close.assert_called_once()
        cancelled.close.assert_called_once()

    def test_declined_mount_cleanup_over_http(self):
        location = '/v2/target/blobs/uploads/new-session?state=a%2Bb'

        def respond(method, path, headers, body):
            self.assertEqual(body, b'')
            if method == 'POST':
                return 202, {'Location': location}, b''
            self.assertEqual((method, path), ('DELETE', location))
            return 204, {}, b''

        server = _RegistryHTTPServer(respond)
        server.server.RequestHandlerClass.do_DELETE = server.server.RequestHandlerClass.do_POST
        with server:
            self.assertFalse(RegistryClient(server.base_url).mount_blob('target', 'source', DIGEST))
        self.assertEqual([row[0] for row in server.requests], ['POST', 'DELETE'])

    def test_declined_mount_rejects_foreign_or_wrong_session_locations(self):
        for location in ('https://foreign/v2/target/blobs/uploads/id', '/v2/other/blobs/uploads/id',
                         '/v2/target/manifests/tag', '/v2/target/blobs/uploads/',
                         '/v2/target/blobs/uploads/id/extra', '/v2/target/blobs/uploads/%2e%2e/admin'):
            with self.subTest(location=location):
                response = Mock(status=202, headers={'Location': location})
                client = RegistryClient('http://registry')
                with patch.object(client, '_request', return_value=response) as request:
                    with self.assertRaises(ValueError):
                        client.mount_blob('target', 'source', DIGEST)
                self.assertEqual(request.call_count, 1)
                response.close.assert_called_once()

    def test_mount_response_identity_status_and_close(self):
        for status, headers in ((201, {'Docker-Content-Digest': 'sha256:' + 'b'*64}), (200, {}), (204, {})):
            response = Mock(status=status, headers=headers)
            client = RegistryClient('http://registry')
            with patch.object(client, '_request', return_value=response):
                with self.assertRaises(ValueError):
                    client.mount_blob('target', 'source', DIGEST)
            response.close.assert_called_once()

    def test_mount_digest_header_is_optional_under_registry_protocol(self):
        response = Mock(status=201, headers={})
        with patch.object(RegistryClient, '_request', return_value=response):
            self.assertTrue(RegistryClient('http://registry').mount_blob('target', 'source', DIGEST))
        response.close.assert_called_once()

    def test_missing_cancelled_session_is_already_clean(self):
        response = Mock(status=202, headers={'Location': '/v2/target/blobs/uploads/id'})
        with patch.object(RegistryClient, '_request', side_effect=[response, RegistryRequestError(404, 'DELETE', '/v2/target/blobs/uploads/id', '')]):
            self.assertFalse(RegistryClient('http://registry').mount_blob('target', 'source', DIGEST))

    def test_expired_optional_budget_still_allows_bounded_cleanup(self):
        response = Mock(status=202, headers={'Location': '/v2/target/blobs/uploads/id'})
        with patch('ucloud_sandboxes.managed_registry.time.monotonic', side_effect=[0, 3]), \
                patch.object(RegistryClient, '_request', side_effect=[response, Mock()]) as request:
            self.assertFalse(RegistryClient('http://registry').mount_blob('target', 'source', DIGEST, timeout_seconds=2))
        self.assertEqual(request.call_args.kwargs['timeout_seconds'], 1)

    def test_timeout_and_repository_validation_precede_network(self):
        with patch.object(RegistryClient, '_request') as request:
            client = RegistryClient('http://registry')
            for timeout in (0, -1, float('inf'), float('nan')):
                with self.assertRaises(ValueError):
                    client.mount_blob('target', 'source', DIGEST, timeout_seconds=timeout)
            for repository in ('', '../source', 'source?from=foreign', 'https://foreign/source', 'a' * 256):
                with self.assertRaises(ValueError):
                    client.mount_blob('target', repository, DIGEST)
            with self.assertRaises(ValueError):
                client.mount_blob('target', 'source', 'sha256:invalid')
            request.assert_not_called()


if __name__ == '__main__':
    unittest.main()
