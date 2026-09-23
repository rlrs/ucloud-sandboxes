"""Trusted image artifact closure, retained by existing registry owner leases."""
from collections import OrderedDict
from threading import RLock

from .environment_artifact import load_image_environment
from .managed_registry import (digest_protection_tag, manifest_digest_from_image_ref,
                               registry_repository_tag_from_image_ref)


class EnvironmentDependencyResolver:
    def __init__(self, registry, *, cache_entries=256):
        self.registry, self.cache_entries = registry, cache_entries
        self._cache, self._guard = OrderedDict(), RLock()

    def __call__(self, image_ref):
        coordinates = registry_repository_tag_from_image_ref(image_ref)
        if coordinates is None:
            return ()
        repository, tag = coordinates
        digest = manifest_digest_from_image_ref(image_ref)
        key = (repository, digest)
        with self._guard:
            if digest and key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        attachment = load_image_environment(self.registry, repository, digest or tag, required=False)
        if attachment is None:
            references = ()
        else:
            root, environment = attachment
            references = tuple((self.registry.repository, digest_protection_tag(identity), identity)
                               for identity in dict.fromkeys((root, *environment.components)))
        if digest:
            with self._guard:
                self._cache[key] = references
                self._cache.move_to_end(key)
                while len(self._cache) > self.cache_entries:
                    self._cache.popitem(last=False)
        return references

    def ensure_reference(self, repository, tag, digest):
        # Closure caching never implies continued physical retention. Called
        # for a new/expired owner under the gateway's registry-GC fence.
        if repository != self.registry.repository or tag != digest_protection_tag(digest):
            raise ValueError("invalid environment protection reference")
        self.registry.client.ensure_digest_protection_tag(repository, digest)
