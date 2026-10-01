from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import io
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.build_context_store import BuildContextBlobStore
from ucloud_sandboxes.image_foundations import openswe_foundation, tmax_foundation, tmax_inline_foundation, terminal_foundation
from ucloud_sandboxes.images import ImageBuildSpec
from ucloud_sandboxes.prepared_images import PreparedImageCatalog, resolve_build, read_context, SMITH_TAIL

PIN = 'docker.io/library/ubuntu@sha256:'+'a'*64
PREPARED = 'private:5000/prepared:base@sha256:'+'b'*64
OTHER = 'private:5000/prepared:other@sha256:'+'c'*64


def archive(files):
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode='wb', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as tar:
            for name, data in files.items():
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mode = 0o755 if name.endswith('.sh') else 0o644
                tar.addfile(info, io.BytesIO(data))
    return out.getvalue()


class PreparedImagesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.catalog = PreparedImageCatalog(root/'prepared.sqlite3')
        self.store = BuildContextBlobStore(root/'contexts', max_blob_bytes=32*1024**2)
        self.protected = []

    def request(self, dockerfile, files=None, **extra):
        payload = archive({'Dockerfile': dockerfile.encode(), **(files or {})})
        digest = 'sha256:'+hashlib.sha256(payload).hexdigest()
        self.store.put_with_status(digest, io.BytesIO(payload), content_length=len(payload))
        raw = {'id': 'test', 'tag': 'private:5000/requested', 'context_path': '.', 'push': True,
               'context_archive_format': 'tar.gz', 'context_archive_digest': digest,
               'context_archive_size': len(payload), **extra}
        return raw, ImageBuildSpec.from_dict(raw)

    def resolve(self, raw, spec):
        return resolve_build(self.catalog, self.store, raw, spec, protect=self.protected.append)

    def source(self, source='ubuntu:22.04', preparation='source', reference=PREPARED):
        self.catalog.register_source({'status': 'ready', 'source': source, 'source_reference': PIN,
            'preparation': preparation, 'reference': reference})

    def foundation(self, value, **extra):
        self.catalog.register_foundation({'validated': True, 'key': value.key,
            'family': value.family, 'base': PIN, 'source_prefix': value.source_prefix,
            'reference': PREPARED, **extra})

    def test_source_reuses_private_digest_and_preserves_remainder_and_context_metadata(self):
        self.source()
        raw, spec = self.request('FROM ubuntu:22.04\nCOPY task.sh /task.sh\nRUN /task.sh\n', {'task.sh': b'echo hello'})
        result, match = self.resolve(raw, spec)
        members, files = read_context(self.store, result['context_archive_digest'])
        self.assertEqual(files['Dockerfile'], ('FROM '+PREPARED+'\nCOPY task.sh /task.sh\nRUN /task.sh\n').encode())
        self.assertEqual(files['task.sh'], b'echo hello')
        self.assertEqual(next(m.mode for m, _ in members if m.name == 'task.sh'), 0o755)
        self.assertEqual(self.protected, [PREPARED])
        self.assertEqual(result['id'], raw['id'])
        self.assertEqual(match['kind'], 'source')
        self.assertEqual(read_context(self.store, raw['context_archive_digest'])[1]['Dockerfile'],
                         b'FROM ubuntu:22.04\nCOPY task.sh /task.sh\nRUN /task.sh\n')

    def test_enriched_source_only_satisfies_exact_recipe(self):
        self.source(preparation='swesmith-v1')
        raw, spec = self.request('FROM ubuntu:22.04\nRUN echo unrelated\n')
        self.assertEqual(self.resolve(raw, spec), (raw, None))
        raw, spec = self.request('FROM ubuntu:22.04\n'+SMITH_TAIL, id='smith')
        result, match = self.resolve(raw, spec)
        self.assertEqual(match['kind'], 'complete_recipe')
        self.assertEqual(read_context(self.store, result['context_archive_digest'])[1]['Dockerfile'], ('FROM '+PREPARED+'\n').encode())

    def test_tmax_matches_installer_contents_not_filename(self):
        text = 'FROM ubuntu:22.04\nCOPY base_install.sh /tmp/base_install.sh\nRUN bash /tmp/base_install.sh && rm /tmp/base_install.sh\n'
        foundation = tmax_foundation(text, b'apt-get install -y git\n', ubuntu_base=PIN)
        self.foundation(foundation)
        raw, spec = self.request(text, {'base_install.sh': b'apt-get install -y curl\n'})
        self.assertEqual(self.resolve(raw, spec), (raw, None))
        raw, spec = self.request(text+'RUN echo task\n', {'base_install.sh': b'apt-get install -y git\n'})
        result, match = self.resolve(raw, spec)
        self.assertEqual(match['key'], foundation.key)
        self.assertEqual(read_context(self.store, result['context_archive_digest'])[1]['Dockerfile'], ('FROM '+PREPARED+'\nRUN echo task\n').encode())

    def test_inline_tmax_removes_only_recognized_dependency_commands(self):
        text = 'FROM ubuntu:22.04\nCOPY post_install.sh /tmp/post_install.sh\nRUN bash /tmp/post_install.sh && rm /tmp/post_install.sh\n'
        script = b'set -e\napt-get install -y git\necho task-specific\n'
        foundation, remainder = tmax_inline_foundation(text, script, ubuntu_base=PIN)
        self.foundation(foundation)
        raw, spec = self.request(text, {'post_install.sh': script})
        result, match = self.resolve(raw, spec)
        self.assertEqual(match['kind'], 'foundation')
        self.assertEqual(read_context(self.store, result['context_archive_digest'])[1]['post_install.sh'], remainder)

    def test_openswe_and_terminal_match_existing_foundation_keys(self):
        foundation = openswe_foundation('3.11', miniconda_base=PIN)
        self.foundation(foundation, python_version='3.11')
        raw, spec = self.request(foundation.source_prefix+'RUN echo task\n')
        self.assertEqual(self.resolve(raw, spec)[1]['key'], foundation.key)
        text = 'FROM ubuntu:22.04\nRUN apt-get update\nCOPY task_file /task_file\n'
        foundation = terminal_foundation(text, source_base='ubuntu:22.04', resolved_base={'reference': PIN, 'onbuild': []})
        self.foundation(foundation, resolved_base={'reference': PIN, 'onbuild': []})
        raw, spec = self.request(text, {'task_file': b'hello'}, id='terminal')
        self.assertEqual(self.resolve(raw, spec)[1]['key'], foundation.key)

    def test_ambiguous_or_context_observing_recipes_fall_back(self):
        self.source()
        for suffix in ['COPY . /app\n', 'COPY Dockerfile /app\n', 'COPY *.sh /app\n',
                       'RUN --mount=type=bind cat Dockerfile\n', 'ARG X=1\n', 'FROM other AS next\n']:
            raw, spec = self.request('FROM ubuntu:22.04\n'+suffix)
            self.assertEqual(self.resolve(raw, spec), (raw, None))
        raw, spec = self.request('# syntax=docker/dockerfile:1\nFROM ubuntu:22.04\n')
        self.assertEqual(self.resolve(raw, spec), (raw, None))

    def test_terminal_verifier_wrapper_preserves_tail_and_uses_longest_cached_prefix(self):
        prefix = 'FROM ubuntu:22.04\nRUN apt-get update\n'
        extended = prefix+'RUN apt-get install -y git\n'
        suffix = ('\nUSER root\n'
                  'RUN if ! command -v git >/dev/null || ! command -v patch >/dev/null || ! command -v bash >/dev/null; then apt-get update && apt-get install -y --no-install-recommends git patch bash ca-certificates; fi\n'
                  '\nCOPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /uvx /usr/local/bin/\n'
                  'ENV UV_PYTHON_INSTALL_DIR=/opt/terminal-lego-python UV_CACHE_DIR=/opt/terminal-lego-cache\n'
                  'COPY verifier-bootstrap.sh /opt/verifier-bootstrap.sh\n'
                  'RUN bash -e /opt/verifier-bootstrap.sh\n'
                  'RUN uvx --with pytest pytest --version\n'
                  'RUN echo fingerprint > /opt/terminal-lego-verifier.sha256\n')
        for text in (prefix, extended):
            foundation = terminal_foundation(text, source_base='ubuntu:22.04', resolved_base={'reference': PIN, 'onbuild': []})
            self.foundation(foundation, resolved_base={'reference': PIN, 'onbuild': []})
        for task_tail in ('', 'COPY task_file /app\n'):
            raw, spec = self.request(extended+task_tail+suffix, {'task_file': b'challenge', 'verifier-bootstrap.sh': b'echo setup'})
            result, match = self.resolve(raw, spec)
            files = read_context(self.store, result['context_archive_digest'])[1]
            self.assertEqual(match['key'], foundation.key)
            self.assertEqual(files['Dockerfile'], ('FROM '+PREPARED+'\n'+task_tail+suffix).encode())
            self.assertEqual(files['task_file'], b'challenge')
            self.assertEqual(files['verifier-bootstrap.sh'], b'echo setup')
            self.assertEqual(self.resolve(raw, spec), (result, match))

    def test_external_copy_does_not_bypass_context_or_stage_guards(self):
        self.source()
        valid = 'COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/bin/uv\n'
        raw, spec = self.request('FROM ubuntu:22.04\n'+valid)
        self.assertEqual(self.resolve(raw, spec)[1]['kind'], 'source')
        for suffix in (valid+'COPY . /app\n', valid+'COPY Dockerfile /app\n',
                       'COPY --from=0 /uv /usr/bin/uv\n',
                       'COPY --from=$IMAGE /uv /usr/bin/uv\n',
                       'COPY --from=uv /uv /usr/bin/uv\n',
                       'COPY --from=ghcr.io/uv:1 --chown=0 /uv /usr/bin/uv\n'):
            raw, spec = self.request('FROM ubuntu:22.04\n'+suffix)
            self.assertEqual(self.resolve(raw, spec), (raw, None))
        raw, spec = self.request('FROM ubuntu:22.04\n', {'.dockerignore': b'base_install.sh\n'})
        self.assertEqual(self.resolve(raw, spec), (raw, None))

    def test_retry_keeps_frozen_decision_after_catalog_changes(self):
        self.source()
        raw, spec = self.request('FROM ubuntu:22.04\n')
        first, _ = self.resolve(raw, spec)
        with self.catalog.connection() as db:
            db.execute('UPDATE prepared_sources SET reference=?', (OTHER,))
        self.assertEqual(self.resolve(raw, spec)[0], first)
        self.assertEqual(self.protected[-1], PREPARED)

    def test_miss_is_stable_and_explicit_opt_out_and_args_are_respected(self):
        raw, spec = self.request('FROM ubuntu:22.04\n')
        self.assertEqual(self.resolve(raw, spec), (raw, None))
        self.source()
        self.assertEqual(self.resolve(raw, spec), (raw, None))
        for options in [{'prepared_cache': 'off'}, {'build_args': {'VERSION': '1'}}]:
            raw, spec = self.request('FROM ubuntu:22.04\n', id='other', **options)
            self.assertEqual(self.resolve(raw, spec), (raw, None))
        raw, spec = self.request('FROM ubuntu:22.04\n', prepared_cache='invalid')
        with self.assertRaises(ValueError):
            self.resolve(raw, spec)

    def test_concurrent_resolvers_agree_and_protection_failure_does_not_freeze_hit(self):
        self.source()
        raw, spec = self.request('FROM ubuntu:22.04\n')
        with self.assertRaisesRegex(RuntimeError, 'missing'):
            resolve_build(self.catalog, self.store, raw, spec, protect=lambda _: (_ for _ in ()).throw(RuntimeError('missing')))
        with ThreadPoolExecutor(max_workers=4) as workers:
            results = list(workers.map(lambda _: self.resolve(raw, spec)[0], range(8)))
        self.assertTrue(all(r == results[0] for r in results))

    def test_unqualified_receipts_are_not_registered(self):
        self.assertFalse(self.catalog.register_source({'status': 'failed'}))
        self.assertFalse(self.catalog.register_foundation({'validated': False}))
        self.assertFalse(self.catalog.register_source({'status': 'ready', 'method': 'unknown'}))

    def test_legacy_foundation_receipt_is_not_reusable(self):
        self.assertFalse(self.catalog.register_foundation({'validated': True, 'key': 'a'*64}))

    def test_inline_script_copied_again_keeps_original_recipe(self):
        text = 'FROM ubuntu:22.04\nCOPY post_install.sh /tmp/post_install.sh\nRUN bash /tmp/post_install.sh && rm /tmp/post_install.sh\n'
        script = b'apt-get install -y git\necho task\n'
        foundation, _ = tmax_inline_foundation(text, script, ubuntu_base=PIN)
        self.foundation(foundation)
        raw, spec = self.request(text+'COPY post_install.sh /saved.sh\n', {'post_install.sh': script})
        self.assertEqual(self.resolve(raw, spec), (raw, None))

    def test_legacy_context_and_invalid_policy(self):
        raw, spec = self.request('FROM scratch\n')
        legacy = {k: v for k, v in raw.items() if not k.startswith('context_archive')}
        self.assertEqual(self.resolve(legacy, spec), (legacy, None))
        for value in ([], {}, 1, 'unknown'):
            with self.assertRaises(ValueError):
                self.resolve({**raw, 'prepared_cache': value}, spec)


class PreparedGatewayTests(unittest.TestCase):
    def test_ordinary_request_uses_preparation_and_preserves_output_identity(self):
        from tests.test_control_plane import (
            _gateway_server, _running_server, _store_build_context,
            ContextRecordingRuntime, ControlPlaneTests, build_builder_node_agent_server,
            post_heartbeat_with_headers, build_heartbeat, ResourceQuantity,
        )
        def fetch(*a, **kw):
            return ControlPlaneTests._json_request(self, *a, **kw)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = ContextRecordingRuntime()
            builder = build_builder_node_agent_server('127.0.0.1', 0,
                state_file=root/'builder-state.json', image_file=root/'builder-images.json',
                job_id='job-builder', node_id='builder-1', image_runtime=runtime,
                node_control_bearer_token='node-secret', build_context_store_dir=root/'builder-contexts')
            gateway = _gateway_server(root, routing_file=root/'routes.json',
                gateway_bearer_token='gateway-secret', node_control_bearer_token='node-secret',
                build_context_store_dir=root/'gateway-contexts')
            gateway.RequestHandlerClass.prepared_image_catalog.register_source({
                'status': 'ready', 'source': 'ubuntu:22.04', 'source_reference': PIN, 'reference': PREPARED})
            with _running_server(builder) as node, _running_server(gateway) as base:
                context = _store_build_context(gateway, archive({'Dockerfile': b'FROM ubuntu:22.04\nRUN echo task\n'}))
                post_heartbeat_with_headers(base+'/v1/nodes/heartbeat', build_heartbeat(
                    job_id='job-builder', node_id='builder-1', node_epoch=builder.RequestHandlerClass.node_epoch,
                    node_url=node, capabilities=('image-cache', 'image-build', 'snapshot'),
                    total_resources=ResourceQuantity(vcpu=16, memory_mb=49152, disk_mb=200000)),
                    {'Authorization': 'Bearer test-heartbeat-secret'})
                raw = {**context, 'id': 'ordinary-client', 'tag': 'private:5000/requested:latest',
                       'push': True, 'labels': {'test': 'preserved'}}
                result = fetch(base+'/v1/images/build', method='POST', payload=raw,
                    headers={'Authorization': 'Bearer gateway-secret'})
                self.assertEqual(result['prepared']['reference'], PREPARED)
                self.assertEqual(result['image']['id'], raw['id'])
                self.assertEqual(result['image']['tag'], raw['tag'])
                self.assertEqual(result['image']['labels'], raw['labels'])
                self.assertEqual(runtime.dockerfiles, [('FROM '+PREPARED+'\nRUN echo task\n').encode()])
                repeat = fetch(base+'/v1/images/build', method='POST', payload=raw,
                    headers={'Authorization': 'Bearer gateway-secret'})
                self.assertEqual(repeat['prepared'], result['prepared'])
