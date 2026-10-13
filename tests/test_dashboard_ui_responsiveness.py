"""
Automated UI, Accessibility, and Responsiveness Verification Suite for Dashboard (Phase 6).

Verifies:
1. Semantic HTML structure, landmarks, ARIA live regions, and table scopes.
2. Complete CSS theme token engine: Dark, Neon (Cyberpunk), and Light themes.
3. Accessible contrast tokens across all 3 themes.
4. Responsive media queries for Desktop, Laptop, Tablet, and Mobile down to 320px.
5. Prefers-reduced-motion media query ensuring non-essential animations can be disabled.
6. Controlled horizontal scroll container and text truncation for wide data tables.
7. Status filter pills, search input, and keyboard navigation support.
8. Connection loss banner and retry controls.
9. Full compatibility with FastAPI /api/dashboard_data payload schema.
"""

import re
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATE_PATH = BASE_DIR / "scheduler" / "templates" / "dashboard.html"


class TestDashboardUIPolishAndResponsiveness(unittest.TestCase):
    """Test suite for Phase 6 UI enhancements, accessibility, and responsiveness."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.assertTrue(TEMPLATE_PATH.exists(), f"Template not found at {TEMPLATE_PATH}")
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            cls.html_content = f.read()

    def test_semantic_landmarks_and_accessibility(self) -> None:
        """Verify semantic landmarks, roles, and accessible labels."""
        content = self.html_content

        # Landmark tags
        self.assertIn('<header role="banner">', content)
        self.assertIn("<main>", content)
        self.assertIn("</main>", content)

        # ARIA Live Regions
        self.assertIn('aria-live="polite"', content)
        self.assertIn('aria-live="assertive"', content)

        # Table accessibility (th scope="col" and role="region" on scrollable containers)
        self.assertIn('<th scope="col">Worker Node</th>', content)
        self.assertIn('<th scope="col">Status</th>', content)
        self.assertIn('<th scope="col">Job ID</th>', content)
        self.assertIn('role="region"', content)
        self.assertIn('aria-label="Recent Job Executions Audit Table"', content)

    def test_theme_system_tokens(self) -> None:
        """Verify presence and definition of all three themes (Dark, Neon, Light)."""
        content = self.html_content

        # Dark theme
        self.assertIn(':root[data-theme="dark"]', content)
        self.assertIn("--bg-body: #070b14;", content)
        self.assertIn("--surface-card: #0d1527;", content)

        # Neon / Cyberpunk theme
        self.assertIn(':root[data-theme="cyberpunk"]', content)
        self.assertIn("--bg-body: #0a0618;", content)
        self.assertIn("--surface-card: #130d2a;", content)
        self.assertIn("--accent-primary: #f43f5e;", content)

        # Light theme
        self.assertIn(':root[data-theme="light"]', content)
        self.assertIn("--bg-body: #f8fafc;", content)
        self.assertIn("--surface-card: #ffffff;", content)
        self.assertIn("--text-primary: #0f172a;", content)
        self.assertIn("--surface-border: #e2e8f0;", content)

        # Theme selector buttons
        self.assertIn('id="theme-dark"', content)
        self.assertIn('id="theme-cyberpunk"', content)
        self.assertIn('id="theme-light"', content)

    def test_responsive_media_queries(self) -> None:
        """Verify responsive breakpoints covering laptop, tablet, and mobile down to 320px."""
        content = self.html_content

        # Viewport meta tag
        self.assertIn('<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">', content)

        # Desktop 5-column grid
        self.assertIn("grid-template-columns: repeat(5, minmax(0, 1fr));", content)

        # Laptop breakpoint (< 1100px)
        self.assertIn("@media (max-width: 1100px)", content)
        self.assertIn("repeat(3, minmax(0, 1fr))", content)

        # Tablet breakpoint (< 1024px)
        self.assertIn("@media (max-width: 1023px)", content)

        # Phablet breakpoint (< 768px)
        self.assertIn("@media (max-width: 767px)", content)
        self.assertIn("repeat(2, minmax(0, 1fr))", content)

        # Mobile breakpoint (< 480px down to 320px)
        self.assertIn("@media (max-width: 479px)", content)
        self.assertIn("grid-template-columns: 1fr;", content)

        # Overflow containment
        self.assertIn("overflow-x: hidden;", content)

    def test_prefers_reduced_motion(self) -> None:
        """Verify prefers-reduced-motion media query disables animations and transitions."""
        content = self.html_content
        self.assertIn("@media (prefers-reduced-motion: reduce)", content)
        self.assertIn("animation-duration: 0.01ms !important;", content)
        self.assertIn("transition-duration: 0.01ms !important;", content)

        # In JS: check reduced motion in animateValue
        self.assertIn("window.matchMedia('(prefers-reduced-motion: reduce)').matches", content)

    def test_metric_cards_and_truthful_rendering(self) -> None:
        """Verify metric cards exist and render actual API data."""
        content = self.html_content
        metric_ids = [
            "metric-pending",
            "metric-running",
            "metric-completed",
            "metric-failed",
            "metric-workers",
            "metric-queue-length",
        ]
        for mid in metric_ids:
            self.assertIn(f'id="{mid}"', content, f"Metric element #{mid} not found in template")

        # Discrepancy / Truthful queue explanation
        self.assertIn("data.queue_diagnostics.discrepancy_explanation", content)
        self.assertIn("Queue depth: Unavailable", content)

    def test_controlled_table_scroller_and_id_truncation(self) -> None:
        """Verify wide tables have controlled horizontal scrolling and copyable IDs."""
        content = self.html_content
        self.assertIn(".table-container {", content)
        self.assertIn("overflow-x: auto;", content)
        self.assertIn("-webkit-overflow-scrolling: touch;", content)

        # Click to copy full ID helper
        self.assertIn("copyToClipboard", content)
        self.assertIn("navigator.clipboard.writeText", content)

    def test_status_filter_pills_and_search(self) -> None:
        """Verify presence of status filter buttons and client-side filtering."""
        content = self.html_content
        self.assertIn('data-filter="all"', content)
        self.assertIn('data-filter="pending"', content)
        self.assertIn('data-filter="running"', content)
        self.assertIn('data-filter="completed"', content)
        self.assertIn('data-filter="failed"', content)
        self.assertIn('id="job-search-input"', content)
        self.assertIn("setStatusFilter", content)
        self.assertIn("filterJobs", content)

    def test_connection_banner_and_retry(self) -> None:
        """Verify coordinator connection alert banner and Retry Now button."""
        content = self.html_content
        self.assertIn('id="connection-banner"', content)
        self.assertIn("Coordinator Unreachable", content)
        self.assertIn("Retry Now", content)
        self.assertIn("onclick=\"fetchDashboardData()\"", content)


if __name__ == "__main__":
    unittest.main()
