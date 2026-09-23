"""The lexical fast path must preserve the existing POSIX guest policy."""

from itertools import product
from pathlib import PurePosixPath
import unittest

from ucloud_sandboxes.guest_paths import (
    validate_guest_path,
    validate_setup_path,
    validate_workspace_path,
)


def original_guest(name, value):
    if not isinstance(value, str) or not value.startswith('/'):
        raise ValueError(f'{name} must be an absolute container path.')
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f'{name} contains unsupported control characters.')
    if '..' in PurePosixPath(value).parts:
        raise ValueError(f"{name} cannot contain '..'.")


def original_setup(name, value):
    original_guest(name, value)
    if value != '/' and any(part in {'', '.'} for part in value[1:].split('/')):
        raise ValueError(f'{name} must be a canonical absolute path.')
    if ':' in value or ',' in value:
        raise ValueError(f'{name} contains unsupported delimiters.')


def original_workspace(value):
    original_setup('workspace_path', value)
    path = PurePosixPath(value)
    roots = {'/', '/etc', '/bin', '/sbin', '/lib', '/lib64', '/usr', '/var',
             '/home', '/root', '/tmp', '/opt', '/boot', '/.ucloud-init', '/.ucloud-job-init'}
    trees = ('/proc', '/sys', '/dev', '/run', '/.ucloud-managed')
    if value in roots or any(path == PurePosixPath(root) or PurePosixPath(root) in path.parents
                             for root in trees):
        raise ValueError('workspace_path overlaps a reserved system or runtime path.')


def outcome(call, value):
    try:
        call(value)
    except ValueError as exc:
        return str(exc)
    return None


class GuestPathTests(unittest.TestCase):
    def test_posix_lexical_equivalence_including_error_precedence(self):
        parts = ('', '.', '..', '...', 'proc', 'proc-tools', 'run', '.ucloud-managed',
                 'a\\b', 'c:d', 'a,b', 'é', '分', '\u0085', '\udcff', 'space name')
        values = [None, 1, [], {}, b'/workspace', '', 'relative', '\\windows']
        values += ['/' + chr(character) + '/leaf' for character in range(128)]
        values += [prefix + '/'.join(pair) + suffix
                   for prefix, pair, suffix in product(('/', '//', '///'), product(parts, repeat=2), ('', '/'))]
        pairs = (
            (lambda value: original_guest('path', value), lambda value: validate_guest_path('path', value)),
            (lambda value: original_setup('path', value), lambda value: validate_setup_path('path', value)),
            (original_workspace, validate_workspace_path),
        )
        for before, after in pairs:
            for value in values:
                with self.subTest(value=value, validator=before):
                    self.assertEqual(outcome(before, value), outcome(after, value))

    def test_workspace_tree_boundaries_and_reserved_roots(self):
        for tree in ('/proc', '/sys', '/dev', '/run', '/.ucloud-managed'):
            for value in (tree, tree + '/child', tree + '/child/nested'):
                with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'reserved'):
                    validate_workspace_path(value)
            validate_workspace_path(tree + '-workspace')
        for root in ('/etc', '/usr', '/root', '/tmp'):
            with self.assertRaisesRegex(ValueError, 'reserved'):
                validate_workspace_path(root)
            validate_workspace_path(root + '/workspace')

    def test_guest_path_remains_lexical_and_does_not_resolve_host_symlinks(self):
        validate_guest_path('path', '/does-not-exist/on-the-controller')
        validate_guest_path('path', '//guest//directory/./file')
        with self.assertRaisesRegex(ValueError, 'canonical'):
            validate_setup_path('path', '//guest//directory/./file')
