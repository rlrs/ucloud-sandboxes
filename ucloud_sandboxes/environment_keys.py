#!/usr/bin/env python3
"""Provision/recover one owned producer key; never print private key material."""
import argparse
import base64
import json
import os
from pathlib import Path
import stat

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, PublicFormat, load_pem_private_key
from .environment_artifact import content_digest
from .environment_config import load_trusted_keys


def write_new(path, data, mode):
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, mode)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def provision(directory):
    directory = directory.absolute()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError("producer key directory must be private and owned")
    private, public = directory / "producer.pem", directory / "producers.json"
    if private.exists():
        descriptor = os.open(private, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise ValueError("producer private key ownership/mode changed")
            key = load_pem_private_key(stream.read(16384), password=None)
        if not isinstance(key, Ed25519PrivateKey):
            raise ValueError("existing producer key is not Ed25519")
    else:
        if public.exists():
            raise ValueError("public trust exists but private key is missing; do not silently rotate")
        key = Ed25519PrivateKey.generate()
        write_new(private, key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()), 0o600)
    raw = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    identity = content_digest(raw)
    if public.exists():
        if load_trusted_keys(public) != {identity: raw}:
            raise ValueError("existing producer trust differs; explicit key rotation required")
    else:
        write_new(public, json.dumps({identity: base64.b64encode(raw).decode("ascii")}, sort_keys=True).encode() + b"\n", 0o644)
    return {"producer_key": identity, "private_key_file": str(private), "public_trust_file": str(public)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(provision(args.directory), sort_keys=True))


if __name__ == "__main__":
    main()
