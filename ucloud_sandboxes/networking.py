from __future__ import annotations

import re


HOSTNAME_MAX_LENGTH = 63
HOSTNAME_RE = re.compile(r"[^a-z0-9-]+")


def stable_hostname(seed: str, *, prefix: str = "sandbox-node") -> str:
    base = seed.strip().lower()
    if prefix:
        base = f"{prefix}-{base}"
    hostname = HOSTNAME_RE.sub("-", base)
    hostname = re.sub(r"-+", "-", hostname).strip("-")
    if not hostname:
        hostname = prefix or "sandbox-node"
    hostname = hostname[:HOSTNAME_MAX_LENGTH].strip("-")
    if not hostname:
        hostname = "sandbox-node"
    return hostname


def validate_hostname(value: str) -> None:
    if not value:
        raise ValueError("hostname is required for private network attachment.")
    if len(value) > HOSTNAME_MAX_LENGTH:
        raise ValueError(f"hostname must be at most {HOSTNAME_MAX_LENGTH} characters.")
    if value.startswith("-") or value.endswith("-"):
        raise ValueError("hostname cannot start or end with '-'.")
    if HOSTNAME_RE.search(value) or value.lower() != value:
        raise ValueError(
            "hostname must contain only lowercase letters, digits, and '-'."
        )
