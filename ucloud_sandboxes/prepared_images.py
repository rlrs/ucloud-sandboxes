"""Backend-owned prepared sources and conservative dependency-prefix resolution.

Only trusted preparation receipts are registered. Request identities freeze the
chosen transformation across retries; the ordinary builder still owns build
admission, output labels/names, publication, and completion status.
"""
from __future__ import annotations

from contextlib import contextmanager
import copy
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import posixpath
import re
import shlex
import sqlite3
import tarfile

from .image_foundations import (openswe_foundation, terminal_foundation_candidates,
                               tmax_foundation, tmax_inline_foundation, require_pinned_reference)
from .images import _ByteLimitedReader, _validate_context_member, image_build_fingerprint

MAX_CONTEXT_BYTES = 8 * 1024**2
MAX_TEXT_BYTES = 256 * 1024
SMITH_TAIL = ("USER root\nWORKDIR /testbed\n"
              "RUN git fetch origin '+refs/heads/*:refs/remotes/origin/*'\n"
              "RUN command -v rg || (apt-get update && apt-get install -y --no-install-recommends ripgrep)\n")


def catalog_path(image_file):
    return Path(image_file).with_name('prepared-images.sqlite3')


def first_source(text):
    if re.search(r'^\s*#\s*(syntax|escape|check)\s*=', text, re.I | re.M):
        return None
    lines = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith('#')]
    if not lines or len(re.findall(r'^\s*FROM\b', text, re.I | re.M)) != 1:
        return None
    match = re.fullmatch(r'FROM[ \t]+([^\s$]+)', lines[0])
    return match[1] if match else None


def safe_rewrite(text, dockerfile='Dockerfile'):
    # Rewriting a context file must not change bytes later observed by COPY,
    # ADD, or bind mounts. Unsupported syntax takes the ordinary build path.
    if re.search(r'^\s*(ARG|ONBUILD)\b|^\s*RUN\s+--|<<', text, re.I | re.M):
        return False
    for match in re.finditer(r'^\s*(COPY|ADD)\s+(.+)$', text, re.I | re.M):
        try:
            fields = shlex.split(match[2])
        except ValueError:
            return False
        if match[1].upper() == 'ADD' or len(fields) < 2:
            return False
        # In our single unnamed stage, a literal external image cannot observe
        # the context Dockerfile. Keep the entire COPY unchanged. Numeric stage
        # indexes, substitutions, JSON syntax and additional flags stay excluded.
        if fields[0].startswith('--from='):
            reference = fields[0][len('--from='):]
            if (len(fields) < 3 or not re.fullmatch(r'[a-z0-9][a-z0-9._:/@-]*', reference)
                    or not any(c in reference for c in '/:@')
                    or any(x.startswith('--') or any(c in x for c in '[]$\\') for x in fields[1:])):
                return False
            continue
        if any(x.startswith('--') for x in fields):
            return False
        for source in fields[:-1]:
            if (source in {'.', './', '/'} or any(c in source for c in '*?[]$\\')
                    or posixpath.normpath(source).split('/')[0] in {'.', '..', dockerfile, ''}):
                return False
    return True


class PreparedImageCatalog:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        with self.connection() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS prepared_sources (
                    source TEXT NOT NULL, preparation TEXT NOT NULL, reference TEXT NOT NULL,
                    PRIMARY KEY(source, preparation));
                CREATE TABLE IF NOT EXISTS prepared_foundations (
                    key TEXT PRIMARY KEY, source TEXT NOT NULL, family TEXT NOT NULL,
                    base TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS prepared_foundation_source ON prepared_foundations(source);
                CREATE TABLE IF NOT EXISTS prepared_decisions (
                    identity TEXT PRIMARY KEY, payload TEXT NOT NULL);
            ''')

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def register_source(self, row):
        if row.get('status') != 'ready':
            return False
        preparation = row.get('preparation', 'source')
        if preparation not in {'source', 'swesmith-v1'}:
            return False
        if row.get('method'):
            proof = row.get('qualification', {})
            if (row['method'] not in {'verified-flat-delta-v1', 'verified-oci-delta-v1'}
                    or proof.get('equivalent') is not True
                    or proof.get('mode') not in {'full_source_scan', 'identical_filesystem_certificate'}):
                return False
        reference = require_pinned_reference(row['reference'])
        pin = require_pinned_reference(row['source_reference'])
        aliases = {row['source'], pin}
        # Never overwrite preserved imports with an alternative artifact.
        aliases.update(a for a, status in row.get('import_aliases', {}).items()
                       if status in {'registered', 'already-ready'})
        with self.connection() as db:
            db.executemany('INSERT OR IGNORE INTO prepared_sources VALUES (?,?,?)',
                           [(s, preparation, reference) for s in aliases])
        return True

    def register_foundation(self, row):
        if row.get('validated') is not True or not all(row.get(k) for k in ('reference', 'base', 'key')):
            return False
        require_pinned_reference(row['reference'])
        require_pinned_reference(row['base'])
        if not re.fullmatch('[a-f0-9]{64}', row['key']):
            raise ValueError('invalid foundation identity')
        source = first_source(row.get('source_prefix', '')) or row.get('source_base')
        family = row.get('family', 'tmax')
        if not source or family not in {'tmax', 'tmax-inline', 'openswe', 'terminal-prefix'}:
            return False
        with self.connection() as db:
            db.execute('INSERT OR IGNORE INTO prepared_foundations VALUES (?,?,?,?,?)',
                       (row['key'], source, family, row['base'], json.dumps(row, sort_keys=True)))
        return True

    def choose(self, text, files):
        source = first_source(text)
        if not source or not safe_rewrite(text):
            return None
        with self.connection() as db:
            sources = dict(db.execute('SELECT preparation,reference FROM prepared_sources WHERE source=?', (source,)))
            bindings = db.execute('SELECT DISTINCT family,base FROM prepared_foundations WHERE source=?', (source,)).fetchall()
            # Exact enriched recipes may be substituted; an enriched image must
            # never occupy the faithful source slot for an unrelated recipe.
            meaningful = ''.join(line.strip()+'\n' for line in text.splitlines()
                                 if line.strip() and not line.lstrip().startswith('#'))
            if sources.get('swesmith-v1') and meaningful == 'FROM '+source+'\n'+SMITH_TAIL:
                return {'kind': 'complete_recipe', 'reference': sources['swesmith-v1'],
                        'dockerfile': 'FROM '+sources['swesmith-v1']+'\n', 'files': {}}
            matches = []
            # A small binding set yields exact keys; never scan every foundation.
            for family, base in bindings[:32]:
                try:
                    remainder = None
                    if family == 'tmax':
                        foundation = tmax_foundation(text, files.get('base_install.sh', b''), ubuntu_base=base)
                    elif family == 'tmax-inline':
                        copies = re.findall(r'^\s*COPY\s+(.+)$', text, re.I | re.M)
                        if sum(posixpath.normpath(s) == 'post_install.sh' for c in copies for s in shlex.split(c)[:-1]) != 1:
                            continue
                        foundation, remainder = tmax_inline_foundation(text, files.get('post_install.sh', b''), ubuntu_base=base)
                    elif family == 'terminal-prefix':
                        candidates = terminal_foundation_candidates(text, source_base=source, resolved_base={'reference': base, 'onbuild': []})
                        # Appended verifier setup can extend the maximal prefix.
                        # Probe existing keys longest-first at instruction boundaries.
                        for foundation in reversed(candidates):
                            found = db.execute('SELECT payload FROM prepared_foundations WHERE key=?', (foundation.key,)).fetchone()
                            if found and json.loads(found[0]).get('resolved_base', {}).get('onbuild') == []:
                                break
                        else:
                            continue
                    else:
                        version = re.search(r'python=((?:2|3)\.[0-9]{1,2})(?:\s|$)', text)
                        if version is None:
                            continue
                        foundation = openswe_foundation(version[1], miniconda_base=base)
                    found = db.execute('SELECT payload FROM prepared_foundations WHERE key=?', (foundation.key,)).fetchone()
                    if found is None:
                        continue
                    row = json.loads(found[0])
                    if family == 'terminal-prefix' and row.get('resolved_base', {}).get('onbuild') != []:
                        continue
                    rewritten = foundation.task_dockerfile(text, row['reference'])
                    changes = {'post_install.sh': remainder.decode()} if remainder is not None else {}
                    matches.append((len(foundation.source_prefix), {'kind': 'foundation', 'key': foundation.key,
                        'reference': row['reference'], 'dockerfile': rewritten, 'files': changes}))
                except (ValueError, UnicodeError):
                    continue
            if matches:
                return max(matches, key=lambda m: (m[0], m[1]['key']))[1]
            if sources.get('source'):
                rewritten = re.sub(r'^(FROM[ \t]+)'+re.escape(source)+r'(?=[ \t]*$)',
                                   lambda m: m[1]+sources['source'], text, count=1, flags=re.M)
                if rewritten != text:
                    return {'kind': 'source', 'reference': sources['source'], 'dockerfile': rewritten, 'files': {}}
        return None

    def decision(self, identity, candidate=None, *, write=False):
        with self.connection() as db:
            if write:
                db.execute('INSERT OR IGNORE INTO prepared_decisions VALUES (?,?)', (identity, json.dumps(candidate)))
            row = db.execute('SELECT payload FROM prepared_decisions WHERE identity=?', (identity,)).fetchone()
            return (bool(row), json.loads(row[0]) if row else None)



def read_context(store, digest):
    members, files = [], {}
    total = 0
    with store.open(digest) as stream, gzip.GzipFile(fileobj=stream) as zipped:
        limited = _ByteLimitedReader(zipped, 2*MAX_CONTEXT_BYTES)
        with tarfile.open(fileobj=limited, mode='r|') as archive:
            for member in archive:
                _validate_context_member(member)
                name = str(Path(member.name))
                if len(members) >= 1024 or name in files:
                    raise ValueError('context outside preparation matching bounds')
                total += member.size
                if total > MAX_CONTEXT_BYTES:
                    raise ValueError('context outside preparation matching bounds')
                data = archive.extractfile(member).read() if member.isfile() else b''
                members.append((member, data))
                files[name] = data
        while limited.read(64*1024):
            pass
    return members, files


def rewritten_archive(members, changes):
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=output, mode='wb', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as archive:
            for member, original in members:
                data = changes.get(str(Path(member.name)), original)
                item = copy.copy(member)
                item.pax_headers = dict(item.pax_headers)
                if member.isfile():
                    item.size = len(data)
                    item.pax_headers.pop('size', None)
                archive.addfile(item, io.BytesIO(data) if item.isfile() else None)
    return output.getvalue()


def resolve_build(catalog, store, raw, spec, *, protect):
    policy = raw.get('prepared_cache', 'auto')
    if not isinstance(policy, str) or policy not in {'auto', 'off'}:
        raise ValueError('prepared_cache must be auto or off')
    if policy == 'off' or spec.build_args or spec.dockerfile != 'Dockerfile' or not raw.get('context_archive_digest'):
        return raw, None
    digest = raw['context_archive_digest']
    identity = image_build_fingerprint(spec, context_identity='archive:'+digest, push=bool(raw.get('push')))
    exists, decision = catalog.decision(identity)
    if exists and decision is None:
        return raw, None
    try:
        members, files = read_context(store, digest)
        if files.get('.dockerignore') or files.get('Dockerfile.dockerignore'):
            return raw, None
        if any(len(files.get(name, b'')) > MAX_TEXT_BYTES
               for name in ['Dockerfile', 'base_install.sh', 'post_install.sh']):
            return raw, None
        text = files.get('Dockerfile', b'').decode()
        candidate = decision if exists else catalog.choose(text, files)
    except (ValueError, UnicodeError, tarfile.TarError, EOFError, OSError) as exc:
        # This optional bounded optimizer never broadens accepted archive syntax.
        # The ordinary builder validates/extracts an unmodified fallback context.
        if exists:
            raise ValueError('cannot recover the frozen prepared build context') from exc
        return raw, None
    if candidate:
        protect(candidate['reference'])
    _, decision = catalog.decision(identity, candidate, write=True)
    if decision is None:
        return raw, None
    if decision != candidate:
        protect(decision['reference'])
    changes = {name: value.encode() for name, value in decision['files'].items()}
    changes['Dockerfile'] = decision['dockerfile'].encode()
    archive = rewritten_archive(members, changes)
    new_digest = 'sha256:'+hashlib.sha256(archive).hexdigest()
    store.put_with_status(new_digest, io.BytesIO(archive), content_length=len(archive))
    result = {**raw, 'context_archive_digest': new_digest, 'context_archive_size': len(archive)}
    return result, {k: decision[k] for k in ('kind', 'reference', 'key') if k in decision}
