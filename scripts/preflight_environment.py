#!/usr/bin/env python3
"""Check a sandbox JSON specification without provisioning or reading secrets."""

import argparse
import json
from pathlib import Path

from ucloud_sandboxes.environment_contract import describe_environment
from ucloud_sandboxes.sandbox import SandboxSpec


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spec", type=Path)
    args = parser.parse_args()
    spec = SandboxSpec.from_dict(json.loads(args.spec.read_text()))
    report = describe_environment(spec)
    report["valid_spec"] = True
    try:
        spec.validate()
    except ValueError as exc:
        report["valid_spec"] = False
        # Validation errors can include user-provided values: do not echo them.
        report["validation_error"] = type(exc).__name__
    print(json.dumps(report, indent=2))
    return 0 if report["valid_spec"] and report["requirements_satisfied"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
