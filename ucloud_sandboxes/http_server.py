from __future__ import annotations

from http.server import ThreadingHTTPServer


DEFAULT_HTTP_REQUEST_QUEUE_SIZE = 1024


class HighBacklogThreadingHTTPServer(ThreadingHTTPServer):
    request_queue_size = DEFAULT_HTTP_REQUEST_QUEUE_SIZE
    daemon_threads = True
    allow_reuse_address = True
