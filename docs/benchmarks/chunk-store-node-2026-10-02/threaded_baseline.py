"""The first ucloud-chunk-store HTTP layer, kept as the benchmark's baseline:
ThreadingHTTPServer, one thread per connection, sendfile or a copy through
Python. It serves object GETs only; the shipped server is asyncio."""
import _thread
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import os
import socket

from ucloud_sandboxes.chunk_store_node import MIB, parse_range


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    timeout = 120

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def log_message(self, *args):
        pass

    def do_GET(self):  # noqa: N802
        supplied = self.headers.get("Authorization", "").removeprefix("Bearer ").encode()
        if not hmac.compare_digest(supplied, self.server.read_token):
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        first, last, suffix = parse_range(self.headers.get("Range"))
        total, start, length, pieces = self.server.node.read(self.path.removeprefix("/v1/objects/"), first, last,
                                                             suffix)
        try:
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{start + length - 1}/{total}")
            self.send_header("Content-Length", str(length))
            self.end_headers()
            for stream, offset, count in pieces:
                if self.server.sendfile:
                    self.connection.sendfile(stream, offset, count)
                else:
                    while count:
                        block = os.pread(stream.fileno(), min(count, MIB), offset)
                        self.wfile.write(block)
                        offset, count = offset + len(block), count - len(block)
        finally:
            for stream, _, _ in pieces:
                stream.close()


class Server(ThreadingHTTPServer):
    daemon_threads, request_queue_size, allow_reuse_address = True, 4096, True

    def __init__(self, address, node, read_token, sendfile):
        self.node, self.read_token, self.sendfile = node, read_token.encode(), sendfile
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        # Thread.start() waits for the new thread: serial accepts under load.
        _thread.start_new_thread(self.process_request_thread, (request, client_address))

    def handle_error(self, request, client_address):
        pass
