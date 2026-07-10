import socket
import unittest

from ucloud_sandboxes.http_server import HighBacklogThreadingHTTPServer


class _NoopHandler:
    def __init__(self, request, client_address, server) -> None:
        del request, client_address, server


class HttpServerTests(unittest.TestCase):
    def test_accepted_clients_get_read_timeout_and_limits_are_validated(self) -> None:
        with self.assertRaisesRegex(ValueError, "timeout"):
            HighBacklogThreadingHTTPServer(
                ("127.0.0.1", 0),
                _NoopHandler,
                client_socket_timeout_seconds=0,
            )
        with self.assertRaisesRegex(ValueError, "threads"):
            HighBacklogThreadingHTTPServer(
                ("127.0.0.1", 0),
                _NoopHandler,
                max_request_threads=0,
            )

        server = HighBacklogThreadingHTTPServer(
            ("127.0.0.1", 0),
            _NoopHandler,
            client_socket_timeout_seconds=1.25,
            max_request_threads=1,
        )
        client = socket.create_connection(server.server_address)
        accepted = None
        try:
            accepted, _address = server.get_request()
            self.assertEqual(accepted.gettimeout(), 1.25)
            self.assertEqual(server.max_request_threads, 1)
        finally:
            client.close()
            if accepted is not None:
                accepted.close()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
