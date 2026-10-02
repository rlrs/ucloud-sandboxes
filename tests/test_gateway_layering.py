"""The gateway use cases sit below the HTTP handler (plan C6.1)."""
from __future__ import annotations

import ast
import inspect
from pathlib import Path
import subprocess
import sys
import unittest

GATEWAY = Path(__file__).resolve().parents[1] / "ucloud_sandboxes" / "gateway"


def gateway_modules() -> list[str]:
    return sorted(
        "ucloud_sandboxes.gateway" + ("" if path.stem == "__init__" else "." + path.stem)
        for path in GATEWAY.glob("*.py")
    )


def imported_modules(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = "." * node.level + (node.module or "")
            names.add(base)
            names.update(f"{base}.{alias.name}" for alias in node.names)
    return names


class GatewayLayeringTests(unittest.TestCase):
    def test_no_gateway_module_imports_the_handler_module(self):
        for path in sorted(GATEWAY.glob("*.py")):
            with self.subTest(module=path.name):
                offending = {
                    name for name in imported_modules(path)
                    if name.rsplit(".", 1)[-1] == "control_plane"
                    or ".control_plane." in name + "."
                }
                self.assertEqual(offending, set())

    def test_each_gateway_module_imports_standalone_without_the_handler(self):
        for module in gateway_modules():
            with self.subTest(module=module):
                # A fresh interpreter proves no transitive control_plane import
                # and no reliance on another module having been imported first.
                result = subprocess.run(
                    [sys.executable, "-c",
                     f"import sys, {module}; "
                     "sys.exit('ucloud_sandboxes.control_plane' in sys.modules)"],
                    cwd=GATEWAY.parents[1], capture_output=True, text=True, timeout=60,
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_handler_implements_every_exchange_method(self):
        from ucloud_sandboxes.control_plane import ControlPlaneHandler
        from ucloud_sandboxes.gateway.exchange import Exchange

        def shape(function):
            # Names, kinds and optionality; Protocol defaults are ``...``.
            return [(p.name, p.kind, p.default is p.empty)
                    for p in inspect.signature(function).parameters.values()]

        for name, member in vars(Exchange).items():
            if name.startswith("__") or not callable(member):
                continue
            with self.subTest(method=name):
                self.assertEqual(shape(getattr(ControlPlaneHandler, name)), shape(member))


if __name__ == "__main__":
    unittest.main()
