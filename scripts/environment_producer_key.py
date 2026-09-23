#!/usr/bin/env python3
"""Compatibility entry point for the canonical producer-key provisioner."""
from ucloud_sandboxes.environment_keys import main, provision

__all__ = ["provision"]

if __name__ == "__main__":
    main()
