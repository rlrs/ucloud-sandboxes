from html.parser import HTMLParser
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

from ucloud_sandboxes.dashboard import (
    DASHBOARD_HTML,
    DASHBOARD_JS,
)


class _DashboardParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: list[str] = []
        self.tablists: list[dict[str, str | None]] = []
        self.tabs: list[dict[str, str | None]] = []
        self.panels: dict[str, dict[str, str | None]] = {}

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        values = dict(attrs)
        element_id = values.get("id")
        if element_id:
            self.ids.append(element_id)
        if values.get("role") == "tablist":
            self.tablists.append(values)
        if values.get("role") == "tab":
            self.tabs.append(values)
        if values.get("role") == "tabpanel" and element_id:
            self.panels[element_id] = values


class DashboardTests(unittest.TestCase):
    def test_dashboard_ids_and_tab_relationships_are_valid(self) -> None:
        parser = _DashboardParser()
        parser.feed(DASHBOARD_HTML)

        self.assertEqual(len(parser.ids), len(set(parser.ids)))
        self.assertEqual(len(parser.tablists), 1)
        self.assertGreater(len(parser.tabs), 0)
        self.assertEqual(
            sum(tab.get("aria-selected") == "true" for tab in parser.tabs),
            1,
        )
        for tab in parser.tabs:
            target = tab.get("aria-controls")
            self.assertIn(target, parser.panels)
            self.assertEqual(
                parser.panels[str(target)].get("aria-labelledby"),
                tab.get("id"),
            )

    def test_every_bootstrapped_element_id_exists(self) -> None:
        parser = _DashboardParser()
        parser.feed(DASHBOARD_HTML)
        match = re.search(
            r"for \(const id of \[(.*?)\]\) \{\s*els\[id\]",
            DASHBOARD_JS,
            re.DOTALL,
        )

        self.assertIsNotNone(match)
        boot_ids = re.findall(r'"([^"]+)"', match.group(1) if match else "")
        self.assertTrue(boot_ids)
        self.assertEqual(set(boot_ids) - set(parser.ids), set())

    def test_dashboard_javascript_parses(self) -> None:
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is not installed")
        with tempfile.TemporaryDirectory() as raw_dir:
            script = Path(raw_dir) / "dashboard.js"
            script.write_text(DASHBOARD_JS, encoding="utf-8")
            completed = subprocess.run(
                [node, "--check", str(script)],
                capture_output=True,
                check=False,
                text=True,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
