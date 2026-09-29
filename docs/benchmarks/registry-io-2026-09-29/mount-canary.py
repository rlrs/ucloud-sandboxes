#!/usr/bin/env python3
"""Exercise candidate mounts against Distribution using two tiny owned blobs."""
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from ucloud_sandboxes.managed_registry import RegistryClient


class ObservedClient(RegistryClient):
    def __init__(self):
        super().__init__('http://127.0.0.1:5000', timeout_seconds=5)
        self.events = []

    def _request(self, path, **kwargs):
        response = super()._request(path, **kwargs)
        self.events.append({'method': kwargs.get('method', 'GET'),
                            'mount': '?mount=' in path, 'status': response.status})
        return response


def main():
    client = ObservedClient()
    run = uuid4().hex[:16]
    source, target = 'ucloud-managed/registryio-source-' + run, 'ucloud-managed/registryio-target-' + run
    payload = (b'Owned registry mount qualification\n' * 64)
    digest = 'sha256:' + hashlib.sha256(payload).hexdigest()
    root = Path('/work/ucloud-sandboxes/registry-io-20260929-r1')
    path = root / ('mount-canary-' + run + '.blob')
    path.write_bytes(payload)
    try:
        client.upload_blob_file(source, path, digest, len(payload))
        assert not client.blob_exists(target, digest)
        before = len(client.events)
        assert client.mount_blob(target, source, digest, timeout_seconds=3)
        assert client.events[before:] == [{'method': 'POST', 'mount': True, 'status': 201}]
        assert client.blob_bytes(target, digest, max_bytes=4096) == payload
        assert client.mount_blob(target, source, digest, timeout_seconds=3)
        missing_payload = b'Owned fallback upload qualification\n'
        missing_digest = 'sha256:' + hashlib.sha256(missing_payload).hexdigest()
        before = len(client.events)
        assert not client.mount_blob(target, source, missing_digest, timeout_seconds=3)
        assert client.events[before:] == [
            {'method': 'POST', 'mount': True, 'status': 202},
            {'method': 'DELETE', 'mount': False, 'status': 204},
        ]
        path.write_bytes(missing_payload)
        client.upload_blob_file(target, path, missing_digest, len(missing_payload))
        assert client.blob_bytes(target, missing_digest, max_bytes=4096) == missing_payload
        result = {'passed': True, 'run_id': run, 'source_repository': source, 'target_repository': target,
                  'mount_reused_without_upload': True, 'mount_repeat_succeeded': True,
                  'fallback_upload_session_cancelled': True, 'ordinary_upload_fallback_verified': True,
                  'new_blob_bytes': len(payload) + len(missing_payload), 'http_events': client.events,
                  'retention': 'No manifests or image aliases created; tiny unreferenced blobs follow ordinary GC.'}
        (root / 'mount-canary.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result))
    finally:
        path.unlink()


if __name__ == '__main__':
    main()
