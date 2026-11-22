"""Mobile dashboard regressions without requiring a headless browser."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / "index.html").read_text(encoding="utf-8")
CSS = (ROOT / "assets/mobile-edge-to-edge.css").read_text(encoding="utf-8")
JS = (ROOT / "assets/mobile-edge-to-edge.js").read_text(encoding="utf-8")
BUNDLE = (ROOT / "scripts/prepare_cloudflare_bundle.py").read_text(encoding="utf-8")


class MobileDashboardTests(unittest.TestCase):
    def test_hub_loads_mobile_assets(self):
        self.assertIn('href="assets/mobile-edge-to-edge.css', INDEX)
        self.assertIn('src="assets/mobile-edge-to-edge.js', INDEX)
        self.assertIn("mobile-edge-to-edge.css", BUNDLE)
        self.assertIn("mobile-edge-to-edge.js", BUNDLE)

    def test_remove_outer_frame_only_on_mobile(self):
        self.assertIn("@media (max-width: 1023px)", CSS)
        self.assertIn("body.csk-unified-hub > main > section", CSS)
        self.assertIn("body.csk-unified-hub #frame", CSS)
        self.assertIn("border: none !important", CSS)
        self.assertIn("body.csk-embedded-mobile", CSS)
        self.assertIn("#hub-mobile-root", CSS)

    def test_embedded_report_resizes_and_tables_remain_scrollable(self):
        self.assertIn("frame.addEventListener('load'", JS)
        self.assertIn("new ResizeObserver", JS)
        self.assertIn("window.matchMedia('(max-width: 1023px)')", JS)
        self.assertIn("csk-mobile-scroll-table", JS)
        self.assertIn("overflow-x: auto !important", CSS)
        self.assertIn("if (mobile.matches) install()", JS)


if __name__ == "__main__":
    unittest.main()
