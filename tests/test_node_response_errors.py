import unittest
from unittest.mock import Mock, patch
from urllib3.exceptions import ProtocolError, ReadTimeoutError

from ucloud_sandboxes.control_plane import ControlPlaneHandler
from ucloud_sandboxes.telemetry import Telemetry


class NodeResponseErrorTests(unittest.TestCase):
    def test_body_transport_failure_returns_structured_error_without_replay(self):
        for failure, status, code in (
            (ReadTimeoutError(None, '/status', 'Read timed out.'), 504, 'node_request_timeout'),
            (ProtocolError('incomplete response'), 502, 'node_transport_error'),
        ):
            with self.subTest(failure=type(failure).__name__):
                handler = object.__new__(ControlPlaneHandler)
                handler.telemetry = Telemetry.disabled('test')
                handler._build_proxy_request = Mock()
                response = Mock(status=200, headers={})
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=False)
                response.read.side_effect = failure
                with patch('ucloud_sandboxes.control_plane._open_node_request', return_value=response) as opened:
                    result = handler._proxy_request('http://node', '/status', method='POST')
                self.assertEqual(result.status, status)
                self.assertEqual(result.json()['code'], code)
                opened.assert_called_once()
                response.__exit__.assert_called_once()
