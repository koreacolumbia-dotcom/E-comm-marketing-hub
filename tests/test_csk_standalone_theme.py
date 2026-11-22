"""Verify direct CSK report SaaS styling and strict daily-report exclusions."""
from __future__ import annotations

import importlib.util
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("csk_theme_patch", ROOT / "scripts" / "apply_csk_saas_theme.py")
assert SPEC and SPEC.loader
THEME = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(THEME)
HUB = (ROOT / "assets" / "csk-unified-hub.js").read_text(encoding="utf-8")
ROUTE = re.compile(r'^\s*\["([a-z0-9_]+)","[^"]+","([^"]+)","fa-[^"]+"\]', re.M)


class StandaloneThemeTests(unittest.TestCase):
    def test_target_list_is_menu_less_daily(self) -> None:
        routes = dict(ROUTE.findall(HUB))
        self.assertEqual(len(routes), 23)
        self.assertEqual(set(THEME.TARGETS), set(routes) - {"daily"})
        for key, route in THEME.TARGETS.items():
            self.assertEqual(route, routes[key].split("?", 1)[0], key)
        self.assertNotIn("reports/daily_digest/index.html", THEME.TARGETS.values())
        self.assertIn("reports/daily_intelligence/index.html", THEME.TARGETS.values())

    def test_generated_pages_have_standalone_theme(self) -> None:
        for key, relative in THEME.TARGETS.items():
            path = ROOT / relative
            with self.subTest(key=key):
                if not path.is_file() or path.stat().st_size == 0:
                    # Empty generated placeholders must never become fake reports.
                    continue
                contents = path.read_text(encoding="utf-8")
                if not re.search(r"</head\s*>", contents, re.I):
                    continue
                self.assertIn("csk-saas-unified.css", contents)
                self.assertIn("csk-saas-standalone.js", contents)
                self.assertIn(f'data-csk-route="{key}"', contents)
                self.assertEqual(THEME.patch_report(path, key, check=True), "already_present")

    def test_fixture_idempotence_relative_paths_and_data_preservation(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            nested = root / "reports" / "competitor_content" / "index.html"
            nested.parent.mkdir(parents=True)
            original = '<!doctype html><html lang="ko"><head><title>REAL TITLE</title></head><body><main data-sales="1234"><h1>REAL DATA</h1></main></body></html>'
            nested.write_text(original, encoding="utf-8")
            (root / "assets").mkdir()
            with mock.patch.object(THEME, "ROOT", root):
                self.assertEqual(THEME.patch_report(nested, "competitor_content"), "updated")
                text = nested.read_text(encoding="utf-8")
                self.assertIn('href="../../assets/csk-saas-unified.css?', text)
                self.assertIn('src="../../assets/csk-saas-standalone.js?', text)
                self.assertIn('data-sales="1234"', text)
                self.assertIn("<h1>REAL DATA</h1>", text)
                self.assertEqual(THEME.patch_report(nested, "competitor_content"), "already_present")
                self.assertEqual(text, nested.read_text(encoding="utf-8"))
                self.assertEqual(THEME.patch_report(nested, "competitor_content", check=True), "already_present")

    def test_excluded_daily_paths_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for rel in ("reports/daily_digest/index.html",
                        "reports/daily_digest/daily/20261122.html"):
                with self.subTest(rel=rel), mock.patch.object(THEME, "ROOT", root):
                    path = root / rel
                    with self.assertRaisesRegex(ValueError, "Refusing to modify"):
                        THEME.patch_report(path, "daily")

    def test_absent_or_empty_generated_pages_not_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as d, mock.patch.object(THEME, "ROOT", Path(d)):
            p = Path(d) / "reports" / "utm_channel" / "index.html"
            p.parent.mkdir(parents=True)
            self.assertEqual(THEME.patch_report(p, "utm_channel"), "not_generated")
            p.write_text("")
            self.assertEqual(THEME.patch_report(p, "utm_channel"), "not_generated")
            self.assertEqual(p.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
