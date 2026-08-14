"""Shared deterministic settings for the repository's generated tests."""

from hypothesis import settings


settings.register_profile(
    "ucloud",
    deadline=None,
    derandomize=True,
    print_blob=True,
)
settings.load_profile("ucloud")
