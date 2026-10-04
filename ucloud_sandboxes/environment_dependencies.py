"""Trusted image artifact closure, retained by existing registry owner leases."""
from collections import OrderedDict
from threading import RLock

from .environment_artifact import load_environment, load_image_environment
from .managed_registry import (RegistryRequestError, digest_protection_tag, manifest_digest_from_image_ref,
                               registry_repository_tag_from_image_ref)


class EnvironmentDependencyResolver:
    """An image's environment root and its closure. With ``image_roots`` (chunk
    store M2), a dispatched root wins over the manifest's annotation."""

    def __init__(self, registry, *, cache_entries=256, image_roots=None):
        self.registry, self.cache_entries, self.image_roots = registry, cache_entries, image_roots
        self._cache, self._guard = OrderedDict(), RLock()

    def __call__(self, image_ref):
        return self._resolve(image_ref)[1]

    def root(self, image_ref):
        """The root a create of ``image_ref`` dispatches, or None (no environment)."""
        return self._resolve(image_ref)[0]

    def _resolve(self, image_ref):
        coordinates = registry_repository_tag_from_image_ref(image_ref)
        if coordinates is None:
            return None, ()
        repository, tag = coordinates
        digest = manifest_digest_from_image_ref(image_ref)
        dispatched = self.image_roots.dispatch_root(repository, digest) if self.image_roots and digest else None
        key = (repository, digest, dispatched)  # A switch is a new key, never a stale closure.
        with self._guard:
            if digest and key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        if not dispatched:
            try:
                attachment = load_image_environment(self.registry, repository, digest or tag, required=False)
            except RegistryRequestError as exc:
                # A released tag whose manifest went with OCI release (plan §5.4).
                released = self.image_roots.released_digest(repository, tag) if self.image_roots and not digest \
                    and exc.status_code == 404 else ""
                dispatched = released and self.image_roots.dispatch_root(repository, released)
                if not dispatched:
                    raise
        if dispatched:
            attachment = dispatched, load_environment(self.registry, dispatched)
        if attachment is None:
            resolved = None, ()
        else:
            root, environment = attachment
            resolved = root, tuple((self.registry.repository, digest_protection_tag(identity), identity)
                                   for identity in dict.fromkeys((root, *environment.components)))
        if digest:
            with self._guard:
                self._cache[key] = resolved
                self._cache.move_to_end(key)
                while len(self._cache) > self.cache_entries:
                    self._cache.popitem(last=False)
        return resolved

    def ensure_reference(self, repository, tag, digest):
        # Closure caching never implies continued physical retention. Called
        # for a new/expired owner under the gateway's registry-GC fence.
        if repository != self.registry.repository or tag != digest_protection_tag(digest):
            raise ValueError("invalid environment protection reference")
        self.registry.client.ensure_digest_protection_tag(repository, digest)
