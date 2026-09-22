"""Keep database credentials in a private file, outside deployment JSON."""

from pathlib import Path
import stat


def read_private_dsn(path: Path) -> str:
    with path.open() as stream:
        import os

        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            raise ValueError("DSN file must be a private regular file (mode 0600)")
        dsn = stream.read(65537).strip()
    if not dsn or len(dsn) > 65536:
        raise ValueError("invalid DSN file")
    return dsn
