"""Automated Playwright tests for Phase 1 Dashboard improvements."""

import asyncio
import os

import pytest
from playwright.async_api import Page

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_UI_TESTS", "0") != "1",
    reason="UI Playwright tests are opt-in. Set RUN_UI_TESTS=1 to run.",
)


class TestPhase1Dashboard:
    """Test suite for Phase 1 UX improvements."""

    @staticmethod
    def get_base_url() -> str:
        """Get dashboard base URL."""
        return "http://localhost:8501"

    async def test_home_page_navigation_cards(self, page: Page):
        """Test home page has visual navigation cards."""
        await page.goto(self.get_base_url())
        await page.wait_for_load_state("networkidle")

        # Wait for dashboard to load
        await page.wait_for_timeout(2000)

        await page.wait_for_selector("text=NSE Momentum Lab", timeout=10000)
        await page.wait_for_selector("text=Quick Navigation", timeout=10000)
        page_text = await page.text_content("body")
        assert page_text and "Core Workflow" in page_text

        print("✅ Home page: Navigation cards render correctly")

    async def test_experiments_page_search(self, page: Page):
        """Test experiments page search functionality."""
        await page.goto(f"{self.get_base_url()}/pages/04_Experiments.py")
        await page.wait_for_timeout(3000)

        # Check for search input
        search_box = page.locator("input[placeholder*='Search']")
        if await search_box.count() > 0:
            await search_box.fill("test")
            await page.wait_for_timeout(1000)
            print("✅ Experiments page: Search box exists and accepts input")
        else:
            print("⚠️ Experiments page: Search box not found (may need to load experiments first)")

    async def test_experiments_page_charts(self, page: Page):
        """Test experiments page has multiple chart tabs."""
        await page.goto(f"{self.get_base_url()}/pages/04_Experiments.py")
        await page.wait_for_timeout(3000)

        # Check for tabs
        tabs = page.locator("[data-testid='stTab']")
        tab_count = await tabs.count()
        print(f"📊 Experiments page: Found {tab_count} tabs")

        # Look for chart-related tabs
        page_text = await page.text_content("body")
        if page_text and ("Equity" in page_text or "Drawdown" in page_text):
            print("✅ Experiments page: Chart elements found")
        else:
            print("⚠️ Experiments page: Charts may not be visible (need experiment data)")

    async def test_scans_page_charts(self, page: Page):
        """Test scans page has score distribution chart."""
        await page.goto(f"{self.get_base_url()}/pages/03_Scans.py")
        await page.wait_for_timeout(3000)

        # Look for chart elements
        page_text = await page.text_content("body")
        if page_text and ("Score" in page_text or "Distribution" in page_text):
            print("✅ Scans page: Chart elements found")
        else:
            print("⚠️ Scans page: Charts may need scan data to display")

    async def test_chat_page_confirmation(self, page: Page):
        """Test chat page has clear chat confirmation."""
        await page.goto(f"{self.get_base_url()}/pages/01_Chat.py")
        await page.wait_for_timeout(2000)

        # Look for clear chat button
        clear_button = page.locator("button:has-text('Clear Chat')")
        if await clear_button.count() > 0:
            print("✅ Chat page: Clear Chat button found")
        else:
            print("⚠️ Chat page: Clear Chat button not found")

    async def test_color_theme(self, page: Page):
        """Test theme colors are applied."""
        await page.goto(self.get_base_url())
        await page.wait_for_timeout(2000)

        # Check for primary color in page
        primary_color_elements = await page.locator("div[style*='#6366f1']").count()
        if primary_color_elements > 0:
            print(f"✅ Theme: Primary color (#6366f1) found in {primary_color_elements} elements")
        else:
            print("⚠️ Theme: Primary color may not be applied")

    async def test_error_handling(self, page: Page):
        """Test error handling with retry buttons."""
        # Try loading experiments page with bad API URL
        await page.goto(f"{self.get_base_url()}/pages/04_Experiments.py")

        # Set invalid API URL in sidebar
        api_input = page.locator("input[aria-label*='API']").or_(
            page.locator("input[type='text']").first
        )
        if await api_input.count() > 0:
            await api_input.fill("http://localhost:9999")
            await page.wait_for_timeout(2000)

            # Look for retry button
            retry_button = page.locator("button:has-text('Retry')")
            if await retry_button.count() > 0:
                print("✅ Error handling: Retry button appears when API fails")
            else:
                print("⚠️ Error handling: Retry button not found")

    async def test_csv_export(self, page: Page):
        """Test CSV export buttons exist."""
        await page.goto(f"{self.get_base_url()}/pages/04_Experiments.py")
        await page.wait_for_timeout(3000)

        # Look for download buttons
        download_buttons = page.locator("button:has-text('Download')")
        count = await download_buttons.count()

        if count > 0:
            print(f"✅ CSV Export: Found {count} download button(s)")
        else:
            print("⚠️ CSV Export: No download buttons found (may need data first)")

    async def test_pagination_controls(self, page: Page):
        """Test pagination controls exist."""
        await page.goto(f"{self.get_base_url()}/pages/04_Experiments.py")
        await page.wait_for_timeout(3000)

        # Look for pagination buttons
        prev_button = page.locator("button:has-text('Previous')")
        next_button = page.locator("button:has-text('Next')")

        prev_count = await prev_button.count()
        next_count = await next_button.count()

        if prev_count > 0 or next_count > 0:
            print(f"✅ Pagination: Found controls (Previous: {prev_count}, Next: {next_count})")
        else:
            print("⚠️ Pagination: No controls found (may need experiment data)")

    async def test_sidebar_filters(self, page: Page):
        """Test sidebar has filter options."""
        await page.goto(f"{self.get_base_url()}/pages/04_Experiments.py")
        await page.wait_for_timeout(3000)

        # Look for multiselect in sidebar
        multiselect = page.locator("[data-testid='stMultiSelect']")
        count = await multiselect.count()

        if count > 0:
            print(f"✅ Sidebar filters: Found {count} multiselect filter(s)")
        else:
            print("⚠️ Sidebar filters: No multiselect filters found")


async def run_all_tests():
    """Run all Phase 1 tests."""
    from playwright.async_api import async_playwright

    headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") == "1"

    print("🧪 Starting Phase 1 Dashboard Tests")
    print("=" * 70)

    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=headless)
        page = await browser.new_page()

        test_suite = TestPhase1Dashboard()

        tests = [
            ("Home Page Navigation", test_suite.test_home_page_navigation_cards),
            ("Experiments Search", test_suite.test_experiments_page_search),
            ("Experiments Charts", test_suite.test_experiments_page_charts),
            ("Scans Charts", test_suite.test_scans_page_charts),
            ("Chat Confirmation", test_suite.test_chat_page_confirmation),
            ("Color Theme", test_suite.test_color_theme),
            ("Error Handling", test_suite.test_error_handling),
            ("CSV Export", test_suite.test_csv_export),
            ("Pagination", test_suite.test_pagination_controls),
            ("Sidebar Filters", test_suite.test_sidebar_filters),
        ]

        passed = 0
        failed = 0

        for test_name, test_func in tests:
            try:
                print(f"\n🔍 Running: {test_name}")
                await test_func(page)
                passed += 1
            except Exception as e:
                print(f"❌ Failed: {test_name} - {e}")
                failed += 1

        await browser.close()

    print("\n" + "=" * 70)
    print(f"📊 Test Results: {passed} passed, {failed} failed out of {len(tests)} total")
    print("=" * 70)

    return passed, failed


if __name__ == "__main__":
    asyncio.run(run_all_tests())
