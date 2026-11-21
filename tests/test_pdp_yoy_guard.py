"""Offline regression tests: existing GA YoY must not be replaced by BQ."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from pdp_yoy_guard import (find_section_bounds, is_current_report, need_bq_fallback,
                           replace_section, sync_latest_index, verified_series)

HEAD = '<div class="mt-6 rounded-2xl border border-slate-200 bg-white/70 p-4">'
TAIL = '<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">\n' \
       '<div class="report-card rounded-2xl border border-slate-200 bg-white/70 p-4">\n' \
       '<div class="text-xs font-extrabold tracking-widest text-slate-500 uppercase">Search · New</div>'
DATA = {"pdp_series": {"has_yoy": True, "rows": [
    {"itemCategory": "OUTER", "has_yoy": True, "ys": [20]*7, "ys_yoy": [10]*7}
]}}
NEW = HEAD + '\n<div><div class="text-xs">PDP View Trend (Category) · 7D YoY</div><p>작년</p></div>' +       '\n<div class="legacy">old chart</div>\n</div>\n' + TAIL


class PdpYoyGuardTests(unittest.TestCase):
    def test_valid_existing_yoy_skips_bigquery(self):
        self.assertTrue(is_current_report(NEW, DATA))
        self.assertFalse(need_bq_fallback(NEW, DATA))

    def test_invalid_series_not_treated_as_valid(self):
        bad = {"pdp_series": {"has_yoy": True, "rows": [
            {"has_yoy": True, "ys": [1, 2], "ys_yoy": [3, 4]}
        ]}}
        self.assertFalse(verified_series(bad))
        self.assertTrue(need_bq_fallback(NEW, bad))

    def test_heading_change_does_not_break_section_match(self):
        a, b = find_section_bounds(NEW)
        self.assertEqual(a, 0)
        self.assertIn("Search · New", NEW[b:])
        fixed = replace_section(NEW, HEAD + "NEW VIEW</div>")
        self.assertIn("NEW VIEW", fixed)
        self.assertNotIn("old chart", fixed)
        self.assertIn("Search · New", fixed)

    def test_legacy_heading_still_supported(self):
        old = NEW.replace(" · 7D YoY", "")
        self.assertEqual(find_section_bounds(old)[0], 0)

    def test_sync_only_newest_date_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old, new = root / "2026-11-19.html", root / "2026-11-20.html"
            old.write_text("old", encoding="utf-8")
            new.write_text("new", encoding="utf-8")
            index = root.parent / (root.name + "-index.html")
            try:
                self.assertFalse(sync_latest_index(old, index, "old"))
                self.assertTrue(sync_latest_index(new, index, "new"))
                self.assertFalse(sync_latest_index(new, index, "new"))
                self.assertEqual(index.read_text(encoding="utf-8"), "new")
            finally:
                index.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
