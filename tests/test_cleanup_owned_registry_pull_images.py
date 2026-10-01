from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import runpy
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from uuid import UUID

from ucloud_sandboxes.control_plane import _managed_registry_build_tag
from ucloud_sandboxes.images import ImageRecord, ImageStore
from ucloud_sandboxes.managed_registry import RegistryRequestError, digest_protection_tag, registry_repository_tag_from_image_ref

HELPER = runpy.run_path(str(Path(__file__).parents[1] / 'docs/benchmarks/registry-pulls-2026-09-30/cleanup-owned-images.py'))
DIGEST = 'sha256:' + '1' * 64
WORKER = 'http://10.42.0.2:5000'


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class CleanupRegistryPullTests(unittest.TestCase):
    def fixture(self, root):
        counter = 0
        for phase in sorted(HELPER['SOURCES']):
            records = []
            for index in ([45] if phase == 'python-source' else range(48)):
                counter += 1
                image_id = HELPER['CANARY_ID'] if phase == 'python-source' else f'{phase}-1234abcd-{index:03d}'
                record = dict(index=index, recipe='python-agent', variant='app-change-20',
                    image_id=image_id, deadline_missed=False, build=dict(status='succeeded',
                    build_id=str(UUID(int=counter)), image=dict(id=image_id, pushed=True,
                    tag=_managed_registry_build_tag(image_id, WORKER), manifest_digest=DIGEST)))
                records.append(record)
            if phase == 'python-source':
                put(root/phase/'summary.json', dict(complete=False, record=records[0]))
                put(root/phase/'launch.json', dict(image_id=image_id))
                repository, _ = registry_repository_tag_from_image_ref(record['build']['image']['tag'])
                put(root/phase/'source-verification.json', dict(complete=True,
                    helper_sha256=HELPER['VERIFIER_SHA'], source_receipt_sha256=HELPER['sha'](root/phase/'summary.json'),
                    source_launch_sha256=HELPER['sha'](root/phase/'launch.json'), source_rootfs_equal=True,
                    compressed_descriptors_equal=True, runtime_config_equal_except_owned_name=True,
                    registry_mutations=0, image_id=image_id, repository=repository, manifest_digest=DIGEST))
            else:
                put(root/phase/'summary.json', dict(phase=phase, passed=True, records=records))
                put(root/phase/'launch.json', dict(owned_image_ids=[r['image_id'] for r in records]))
        return HELPER['build_ledger'](root, WORKER)

    def test_145_bound_rows_include_failed_diagnostic_only_with_exact_verification(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            ledger = self.fixture(root)
            self.assertEqual(len(HELPER['load_owned'](ledger, root, WORKER)), 145)
            verification = root/'python-source'/'source-verification.json'
            data = json.loads(verification.read_text())
            data['runtime_config_equal_except_owned_name'] = False
            put(verification, data)
            ledger['sources']['python-source']['verification_sha256'] = HELPER['sha'](verification)
            with self.assertRaisesRegex(ValueError, 'independently verified'):
                HELPER['load_owned'](ledger, root, WORKER)

    def test_changed_source_launch_hash_or_duplicate_identity_is_rejected(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            ledger = self.fixture(root)
            ledger['rows'][1]['build_id'] = ledger['rows'][0]['build_id']
            with self.assertRaisesRegex(ValueError, 'Duplicate'):
                HELPER['load_owned'](ledger, root, WORKER)
            ledger = self.fixture(root)
            phase = sorted(HELPER['PHASES'])[0]
            put(root/phase/'launch.json', dict(owned_image_ids=[]))
            with self.assertRaisesRegex(ValueError, 'hash differs'):
                HELPER['load_owned'](ledger, root, WORKER)

    def test_verification_must_bind_original_launch_even_if_new_hash_is_pinned(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            ledger = self.fixture(root)
            path = root/'python-source'/'launch.json'
            data = json.loads(path.read_text())
            data['extra'] = 'new receipt'
            put(path, data)
            ledger['sources']['python-source']['launch_sha256'] = HELPER['sha'](path)
            with self.assertRaisesRegex(ValueError, 'independently verified'):
                HELPER['load_owned'](ledger, root, WORKER)

    def test_record_guard_accepts_same_digest_pin_but_protects_other_names(self):
        with TemporaryDirectory() as temp:
            store = ImageStore(Path(temp)/'images.sqlite')
            image_id = HELPER['CANARY_ID']
            ref = _managed_registry_build_tag(image_id, WORKER)
            repo, _ = registry_repository_tag_from_image_ref(ref)
            row = dict(image_id=image_id, repository=repo, image_ref=ref, manifest_digest=DIGEST)
            now = datetime.now(timezone.utc)
            record = ImageRecord(id=image_id, tag=ref+'@'+DIGEST, source='registry', state='available',
                                 created_at=now, updated_at=now, pushed=True, manifest_digest=DIGEST)
            store.upsert(record)
            before = store.path.read_bytes()
            with HELPER['database'](store.path) as db:
                HELPER['image_record_guard'](db, row)
            self.assertEqual(store.path.read_bytes(), before)
            store.upsert(replace(record, id='unrelated'))
            with HELPER['database'](store.path, write=True) as db:
                with self.assertRaisesRegex(ValueError, 'Unrelated image record'):
                    HELPER['image_record_guard'](db, row)
            self.assertEqual(len(store.load()), 2)
            store.upsert(replace(record, manifest_digest='sha256:'+'2'*64))
            with store._transaction(write=True) as db:
                db.execute('DELETE FROM image_state_v1_images WHERE record_id=?', ('unrelated',))
            with HELPER['database'](store.path) as db:
                with self.assertRaisesRegex(ValueError, 'was replaced'):
                    HELPER['image_record_guard'](db, row)

    def test_fence_blocks_writers_without_pruning_unrelated_expired_leases(self):
        with TemporaryDirectory() as temp:
            path = Path(temp)/'usage.sqlite'
            with sqlite3.connect(path) as db:
                db.execute('CREATE TABLE registry_leases(repository,tag,owner,acquired_at,renewed_at,expires_at,digest)')
                db.executemany('INSERT INTO registry_leases VALUES(?,?,?,?,?,?,?)',
                    [('owned','latest','owner','2000-01-01','2000-01-01','2999-01-01',DIGEST),
                     ('unrelated','latest','expired','2000-01-01','2000-01-01','2000-01-02',DIGEST)])
            before = path.read_bytes()
            with HELPER['database'](path, write=True) as conn:
                self.assertEqual(len(HELPER['active_leases'](conn, dict(repository='owned'))), 1)
                with sqlite3.connect(path, timeout=.001) as other:
                    with self.assertRaises(sqlite3.OperationalError):
                        other.execute('BEGIN IMMEDIATE')
            self.assertEqual(path.read_bytes(), before)
            missing = path.with_name('missing.sqlite')
            for write in (False, True):
                with self.assertRaises(sqlite3.OperationalError):
                    with HELPER['database'](missing, write=write):
                        pass
                self.assertFalse(missing.exists())

    def test_unknown_alias_and_changed_latest_protect_manifest(self):
        class Client:
            def __init__(self, values):
                self.values = values
            def tags(self, repository):
                return list(self.values)
            def manifest_digest(self, repository, reference):
                if reference in self.values:
                    return self.values[reference]
                if reference in self.values.values():
                    return reference
                raise RegistryRequestError(404, 'HEAD', '/fixture', '')
        row = dict(repository='ucloud-managed/owned', manifest_digest=DIGEST)
        aliases = {'latest':DIGEST, digest_protection_tag(DIGEST):DIGEST}
        self.assertEqual(HELPER['inventory'](Client(aliases), row)['state'], 'present')
        for values in (dict(aliases, other=DIGEST), dict(aliases, latest='sha256:'+'2'*64)):
            with self.assertRaises(ValueError):
                HELPER['inventory'](Client(values), row)

    def test_smokes_only_accept_three_distinct_successful_candidate_images(self):
        rows = [dict(phase='slotq-pulls-b-cold', image_id=str(i), recipe=recipe)
                for i, recipe in enumerate(sorted(HELPER['RECIPES']))]
        values = dict(passed=True, results=[dict(image_id=r['image_id'], recipe=r['recipe'],
            sandbox_id=f'registry-pull-smoke-abcdef123456-{i}', verified=True, deleted=True, exit_code=0)
            for i, r in enumerate(rows)])
        HELPER['verify_smokes'](values, rows)
        values['results'][0]['image_id'] = HELPER['CANARY_ID']
        with self.assertRaises(ValueError):
            HELPER['verify_smokes'](values, rows)
