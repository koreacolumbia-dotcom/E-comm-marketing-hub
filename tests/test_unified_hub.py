#!/usr/bin/env python3
"""Regression tests for the single-route CSK desktop/mobile dashboard shell."""
from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "index.html"
APP = ROOT / "assets" / "csk-unified-hub.js"
BUNDLER = ROOT / "scripts" / "prepare_cloudflare_bundle.py"
DESIGN = ROOT / "assets" / "dashboard-redesign.js"

GROUP_IDS = ("overview", "commerce", "customer", "competitors", "operations")
OLD_KEYS = {
    "summary", "daily", "decision_health", "daily_intelligence", "ops3",
    "performance", "pdp_priority", "utm_channel", "funnel",
    "product_keyword", "purchase_pattern", "member", "owned", "ai_agent",
    "competitor_content", "edm_intelligence", "crm_intelligence",
    "price", "hero", "sale_calendar", "competitor", "voc", "crema",
}
ROUTE = re.compile(r'^\s*\["([a-z0-9_]+)","[^"]+","([^"]+)","fa-[^"]+"\]', re.M)


class UnifiedHubTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.shell = INDEX.read_text(encoding="utf-8")
        cls.js = APP.read_text(encoding="utf-8")
        cls.routes = ROUTE.findall(cls.js)

    def test_five_main_menu_groups(self) -> None:
        for group in GROUP_IDS:
            self.assertIn('["' + group + '",', self.js)
        self.assertEqual(self.js.count('["overview",'), 1)

    def test_every_previous_report_preserved_exactly_once(self) -> None:
        keys = [key for key, _ in self.routes]
        self.assertEqual(set(keys), OLD_KEYS)
        self.assertEqual(len(keys), len(OLD_KEYS))

    def test_local_first_party_report_targets_exist(self) -> None:
        for _, target in self.routes:
            with self.subTest(target=target):
                self.assertFalse(target.startswith(("/", "http://", "https://")))
                clean = target.split("?", 1)[0].split("#", 1)[0]
                self.assertNotIn("..", Path(clean).parts)
                self.assertTrue((ROOT / clean).is_file())

    def test_no_more_duplicated_pc_and_mobile_menu_source(self) -> None:
        self.assertIn('id="hub-menu-root"', self.shell)
        self.assertIn('id="hub-mobile-root"', self.shell)
        self.assertIn('src="assets/csk-unified-hub.js', self.shell)
        self.assertIn('src="reports/decision_health/index.html"', self.shell)
        self.assertNotIn('class="nav-item"', self.shell)
        self.assertNotIn('class="mo-tab"', self.shell)
        self.assertNotIn("function initialNavigate", self.shell)

    def test_one_controller_owns_menu_routes(self) -> None:
        self.assertIn("window.CSKUnifiedMenus", self.js)
        self.assertIn("function initialNavigate", self.js)
        self.assertNotIn("const navigate=el", DESIGN.read_text(encoding="utf-8"))
        self.assertIn("normalizeReportPage()", DESIGN.read_text(encoding="utf-8"))
        self.assertIn('"csk-unified-hub.js"', BUNDLER.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
