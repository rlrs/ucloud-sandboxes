import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.request import Request

from ucloud_sandboxes.managed_registry import RegistryRequestError

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
with patch.object(sys, 'path', [str(SCRIPTS), *sys.path]):
    import prepare_image_pool as pool
    spec = importlib.util.spec_from_file_location('staging', SCRIPTS / 'stage_source_image.py')
    staging = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(staging)


def digest(data):
    return 'sha256:' + hashlib.sha256(data).hexdigest()


class StageSourceTests(unittest.TestCase):
    def source(self):
        config = b'{"os":"linux","architecture":"amd64","rootfs":{"diff_ids":[]}}'
        layer = b'compressed layer bytes'
        manifest = json.dumps({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                               'config': {'digest': digest(config), 'size': len(config)},
                               'layers': [{'digest': digest(layer), 'size': len(layer)}]}).encode()
        return {'reference': 'docker.io/example/source@' + digest(manifest),
                'manifest_json': manifest.decode(), 'config_json': config.decode()}, config, layer, manifest

    def test_one_download_then_local_reuse_preserves_manifest_and_layers(self):
        resolved, config, layer, manifest = self.source()
        present = set()
        client = Mock(base_url='http://127.0.0.1:5000')
        client.blob_exists.side_effect = lambda repo, d: d in present
        client.start_blob_upload.return_value = '/v2/ucloud-upstream/blobs/uploads/session'
        client._validate_upload_location.side_effect = lambda location: location
        uploaded = []

        def upload(path, **kwargs):
            data = b''.join(kwargs['data'])
            self.assertEqual(int(kwargs['headers']['Content-Length']), len(data))
            present.add(digest(data))
            uploaded.append(data)
            return Mock(headers={'Docker-Content-Digest': digest(data)})

        client._request.side_effect = upload
        client.manifest_digest.side_effect = [RegistryRequestError(404, 'HEAD', '/manifest', ''), digest(manifest)]
        client.put_manifest.return_value = digest(manifest)
        opener = Mock(side_effect=lambda *args, **kwargs: io.BytesIO(layer))
        protect = Mock(return_value=True)
        hints = Mock()
        hints.repository.side_effect = lambda d: 'removed-image' if d == digest(layer) else None
        client.mount_blob.return_value = False
        auth = Mock(return_value={})
        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, {'prepare_image_pool': pool}):
            for _ in range(2):
                ref = staging.stage_source(resolved, client, Path(directory), publication_url="http://registry.internal:5000", protect=protect,
                                           opener=opener, headers_factory=auth, blob_sources=hints)
                self.assertTrue(ref.endswith('@' + digest(manifest)))
                self.assertTrue(ref.startswith('registry.internal:5000/'))
        self.assertEqual(uploaded, [config, layer])
        self.assertEqual(opener.call_count, 1)
        self.assertEqual(auth.call_count, 1)
        client.put_manifest.assert_called_once_with(staging.REPOSITORY, 'sha256-' + digest(manifest)[7:], manifest,
                                                  media_type='application/vnd.oci.image.manifest.v1+json')
        self.assertEqual(protect.call_count, 2)

    def test_corruption_aborts_upload_and_does_not_publish_manifest(self):
        resolved, _, layer, _ = self.source()
        client = Mock(base_url='http://registry:5000')
        client.blob_exists.return_value = False
        client.start_blob_upload.return_value = '/upload'
        client._validate_upload_location.side_effect = lambda x: x
        client._request.side_effect = lambda path, **kwargs: (
            b''.join(kwargs['data']) and Mock(headers={}))
        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, {'prepare_image_pool': pool}):
            with self.assertRaisesRegex(ValueError, 'digest or size'):
                staging.stage_source(resolved, client, Path(directory), publication_url="http://registry.internal:5000", protect=lambda *args: True,
                    opener=lambda *args, **kwargs: io.BytesIO(b'x' * len(layer)), headers_factory=lambda *args: {})
        client.abort_blob_upload.assert_called_once()
        client.put_manifest.assert_not_called()
        damaged = {**resolved, 'manifest_json': resolved['manifest_json'] + ' '}
        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, {'prepare_image_pool': pool}):
            with self.assertRaisesRegex(ValueError, 'manifest identity'):
                staging.stage_source(damaged, client, Path(directory), publication_url="http://registry.internal:5000", protect=lambda *args: True)

    def test_redirect_drops_registry_bearer_on_cdn_and_rejects_plaintext(self):
        handler = staging.PublicBlobRedirect()
        req = Request('https://registry-1.docker.io/blob', headers={'Authorization': 'Bearer scoped-token'})
        redirected = handler.redirect_request(req, None, 307, '', {}, 'https://cdn.example/blob')
        self.assertIsNone(redirected.get_header('Authorization'))
        with self.assertRaises(ValueError):
            handler.redirect_request(req, None, 307, '', {}, 'http://cdn.example/blob')

    def test_existing_image_layers_mount_without_an_upstream_download(self):
        resolved, config, layer, manifest = self.source()
        client = Mock(base_url='http://127.0.0.1:5000')
        client.blob_exists.side_effect = lambda repo, d: d == digest(config)
        client.mount_blob.return_value = True
        client.manifest_digest.return_value = digest(manifest)
        opener = Mock(side_effect=AssertionError('already retained layer must not download'))
        metrics = {}
        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, {'prepare_image_pool': pool}):
            root = Path(directory)
            index = staging.BlobSourceIndex(root / 'blobs.sqlite')
            index.remember({'layers': [{'digest': digest(layer)}]}, 'existing-image')
            staging.stage_source(resolved, client, root, publication_url='http://registry:5000',
                protect=lambda *args: True, opener=opener, blob_sources=index, metrics=metrics)
        client.mount_blob.assert_called_once_with(staging.REPOSITORY, 'existing-image', digest(layer))
        self.assertEqual(metrics['downloaded_bytes'], 0)
        self.assertEqual(metrics['mounted_blob_bytes'], len(layer))
        opener.assert_not_called()

    def test_stream_rejects_short_long_and_expired_input(self):
        for content in (b'a', b'abc'):
            with self.assertRaises(ValueError):
                list(staging.verified_chunks(io.BytesIO(content), digest(b'ab'), 2, deadline=float('inf')))
        with self.assertRaises(TimeoutError):
            list(staging.verified_chunks(io.BytesIO(b'ab'), digest(b'ab'), 2, deadline=0))
