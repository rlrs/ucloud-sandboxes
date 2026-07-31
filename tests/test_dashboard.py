from html.parser import HTMLParser
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

from ucloud_sandboxes.dashboard import (
    DASHBOARD_CSS,
    DASHBOARD_HTML,
    DASHBOARD_JS,
)


class _DashboardParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: list[str] = []
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
        if values.get("role") == "tab":
            self.tabs.append(values)
        if values.get("role") == "tabpanel" and element_id:
            self.panels[element_id] = values


class DashboardTests(unittest.TestCase):
    def test_dashboard_ids_and_tab_relationships_are_valid(self) -> None:
        parser = _DashboardParser()
        parser.feed(DASHBOARD_HTML)

        self.assertEqual(len(parser.ids), len(set(parser.ids)))
        self.assertEqual(len(parser.tabs), 5)
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

    def test_dashboard_has_bounded_single_flight_live_rendering(self) -> None:
        self.assertIn("state.metricsRequest.abort()", DASHBOARD_JS)
        self.assertIn("document.hidden", DASHBOARD_JS)
        self.assertIn("requestSequence", DASHBOARD_JS)
        self.assertIn("MAX_PROGRAM_ROWS = 200", DASHBOARD_JS)
        self.assertIn("MAX_NODE_ROWS = 250", DASHBOARD_JS)
        self.assertIn("MAX_REGISTRY_REPOSITORY_ROWS = 250", DASHBOARD_JS)
        self.assertIn("requestAnimationFrame", DASHBOARD_JS)

    def test_dashboard_exposes_scheduler_controls_accessibly(self) -> None:
        self.assertIn('role="tablist"', DASHBOARD_HTML)
        self.assertIn('aria-live="polite"', DASHBOARD_HTML)
        self.assertNotIn('id="refreshSelect"', DASHBOARD_HTML)
        self.assertNotIn('id="pauseButton"', DASHBOARD_HTML)
        self.assertIn("DEFAULT_REFRESH_INTERVAL_MS = 2000", DASHBOARD_JS)
        self.assertIn('id="programResultFilter"', DASHBOARD_HTML)
        self.assertIn('id="nodeStateFilter"', DASHBOARD_HTML)
        self.assertIn("canvas.setAttribute(\"aria-label\"", DASHBOARD_JS)
        self.assertIn(".dark", DASHBOARD_CSS)

    def test_dashboard_uses_operator_decision_information_architecture(self) -> None:
        self.assertIn('class="brand-mark"', DASHBOARD_HTML)
        self.assertIn('class="nav-label"', DASHBOARD_HTML)
        self.assertIn('id="overviewNavBadge"', DASHBOARD_HTML)
        self.assertIn('aria-orientation="horizontal"', DASHBOARD_HTML)
        self.assertIn('class="command-grid overview-section"', DASHBOARD_HTML)
        self.assertIn('id="capacityFitBadge"', DASHBOARD_HTML)
        self.assertIn('class="overview-pipeline"', DASHBOARD_HTML)
        self.assertIn('class="capacity-equation-table"', DASHBOARD_HTML)
        self.assertIn('id="sandboxStateFilter"', DASHBOARD_HTML)
        self.assertIn('class="activity-grid overview-section"', DASHBOARD_HTML)
        self.assertNotIn('id="terminateAllSandboxesButton"', DASHBOARD_HTML)
        self.assertIn("--rail-width: 228px", DASHBOARD_CSS)
        self.assertIn(".overview-workbench", DASHBOARD_CSS)
        self.assertIn("prefers-reduced-motion", DASHBOARD_CSS)
        self.assertIn('setNavBadge("nodesNavBadge"', DASHBOARD_JS)
        self.assertIn('setText("readyWakeValue"', DASHBOARD_JS)
        self.assertNotIn('setText("cpuUtilizationValue"', DASHBOARD_JS)

    def test_dashboard_keeps_transport_and_fleet_health_separate(self) -> None:
        health = re.search(
            r"function renderHealth\(snapshot\) \{(.*?)\n\}",
            DASHBOARD_JS,
            re.DOTALL,
        )
        self.assertIsNotNone(health)
        self.assertNotIn("setStatus(", health.group(1) if health else "")
        self.assertIn('if (!state.lastSnapshot) setStatus("Connecting"', DASHBOARD_JS)
        self.assertNotIn('setStatus("Refreshing"', DASHBOARD_JS)

    def test_dashboard_formats_structured_autoscaler_actions(self) -> None:
        self.assertIn("function actionKind(action)", DASHBOARD_JS)
        self.assertIn("actionSummary(actions.concat(builderActions))", DASHBOARD_JS)
        self.assertNotIn('actions.concat(builderActions).join(", ")', DASHBOARD_JS)
