"""Lifecycle retries must not accumulate native TLS certificate stores."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, local
import ssl
import unittest
from unittest.mock import patch

from ucloud_sandboxes import cli


class ControlTransportTests(unittest.TestCase):
    def test_reuses_per_thread_openers_and_one_verified_tls_context(self):
        barrier = Barrier(4)
        factory = ssl.create_default_context
        with (
            patch.object(cli, "_CONTROL_HTTP", local()),
            patch.object(cli, "_CONTROL_TLS_CONTEXT", None),
            patch.object(cli.ssl, "create_default_context", wraps=factory) as create,
        ):

            def retries():
                opener = cli._control_opener()
                barrier.wait(3)
                for _ in range(2000):
                    self.assertIs(cli._control_opener(), opener)
                self.assertTrue(
                    any(
                        isinstance(h, cli._RejectControlRedirects)
                        for h in opener.handlers
                    )
                )
                tls = next(
                    h._context
                    for h in opener.handlers
                    if isinstance(h, cli.HTTPSHandler)
                )
                self.assertTrue(tls.check_hostname)
                self.assertEqual(tls.verify_mode, ssl.CERT_REQUIRED)
                return opener, tls

            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda _: retries(), range(4)))
            self.assertEqual(len({id(opener) for opener, _ in results}), 4)
            self.assertEqual(len({id(tls) for _, tls in results}), 1)
            create.assert_called_once_with()
