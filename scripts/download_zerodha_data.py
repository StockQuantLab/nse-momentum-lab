#!/usr/bin/env python
"""
Download Zerodha equity data from Jio Cloud.

Data source: https://github.com/bh1rg1v/algorithmic-trading/tree/main/data/storage/raw/equity/zerodha
Jio Cloud link: https://www.jioaicloud.com/l/?u=nJeSTwHnU5GtuaLD7aYu97WZUO0E-HJCtLqWE-q4gD3VbsX1gBXZVMyTO5OGzLd-hkW
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


async def download_from_jiocloud():
    """Download Zerodha data from Jio Cloud using Playwright."""
    print("\n" + "=" * 70)
    print("Downloading Zerodha Equity Data from Jio Cloud")
    print("=" * 70)

    from playwright.async_api import async_playwright

    # Jio Cloud link
    share_url = "https://www.jioaicloud.com/l/?u=nJeSTwHnU5GtuaLD7aYu97WZUO0E-HJCtLqWE-q4gD3VbsX1gBXZVMyTO5OGzLd-hkW"

    # Download directory
    download_dir = Path("data/vendor/zerodha")
    download_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nShare URL: {share_url}")
    print(f"Download directory: {download_dir}")
    print("\nStarting browser (will take a moment to launch)...\n")

    try:
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(
                headless=False,  # Show browser for debugging
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                ],
            )

            # Create context with download handling
            context = await browser.new_context(
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={"width": 1920, "height": 1080},
            )

            # Page
            page = await context.new_page()
            page.set_default_timeout(120000)  # 2 minutes

            try:
                # Navigate to Jio Cloud share page
                print("Navigating to Jio Cloud...")
                await page.goto(share_url, wait_until="networkidle")
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(3000)

                print("Page loaded. Looking for download options...\n")

                # Try to find download button/link
                # Jio Cloud usually has a download button for shared folders

                # Strategy 1: Look for download button
                download_selectors = [
                    "button:has-text('Download')",
                    "a:has-text('Download')",
                    "button:has-text('Download folder')",
                    "a:has-text('Download as ZIP')",
                    "[class*='download']",
                    "[id*='download']",
                ]

                download_started = False

                for selector in download_selectors:
                    try:
                        elements = await page.query_selector_all(selector)
                        print(f"Found {len(elements)} elements with selector: {selector}")

                        for element in elements:
                            try:
                                text = await element.inner_text()
                                is_visible = await element.is_visible()

                                if is_visible and ("download" in text.lower()):
                                    print(f"Clicking: {text[:50]}")

                                    async with page.expect_download() as download_info:
                                        await element.click()
                                    download = await download_info.value

                                    print(
                                        f"\n[SUCCESS] Download started: {download.suggested_filename}"
                                    )

                                    # Save file
                                    save_path = download_dir / download.suggested_filename
                                    await download.save_as(save_path)

                                    size_mb = save_path.stat().st_size / (1024 * 1024)
                                    print(f"Saved to: {save_path}")
                                    print(f"Size: {size_mb:.1f} MB")

                                    download_started = True
                                    break
                            except Exception as e:
                                print(f"Error clicking element: {e}")
                                continue

                        if download_started:
                            break

                    except Exception as e:
                        print(f"Error with selector {selector}: {e}")
                        continue

                # Strategy 2: Look for file list and download individual files
                if not download_started:
                    print("\nNo direct download button found. Looking for file list...")

                    # Wait for file list to load
                    await page.wait_for_timeout(5000)

                    # Look for file items
                    file_items = await page.query_selector_all(
                        "[class*='file'], [class*='item'], tr, [role='row']"
                    )

                    print(f"Found {len(file_items)} potential file items")

                    # Filter for daily data files (smaller files)
                    for i, item in enumerate(file_items[:20]):  # Limit to first 20
                        try:
                            text = await item.inner_text()

                            # Look for daily data files (usually smaller)
                            if "day" in text.lower() or "daily" in text.lower():
                                print(f"\nFile {i + 1}: {text[:100]}")

                                # Look for download button in this row
                                download_btn = await item.query_selector(
                                    "button, a[class*='download'], [class*='download']"
                                )

                                if download_btn:
                                    async with page.expect_download() as download_info:
                                        await download_btn.click()
                                    download = await download_info.value

                                    print(f"Downloading: {download.suggested_filename}")
                                    save_path = download_dir / download.suggested_filename
                                    await download.save_as(save_path)
                                    print(f"Saved: {save_path}")

                        except Exception:
                            continue

                # Strategy 3: Wait and take screenshot to debug
                if not download_started:
                    print("\nNo automated download found.")
                    print("Taking screenshot for debugging...")

                    screenshot_path = Path("jiocloud_page.png")
                    await page.screenshot(path=screenshot_path)
                    print(f"Screenshot saved: {screenshot_path}")

                    print("\n" + "=" * 70)
                    print("MANUAL DOWNLOAD REQUIRED")
                    print("=" * 70)
                    print("\nBrowser window is open. Please:")
                    print("1. Look for the download button on the page")
                    print("2. Download the daily data ZIP file (approx 0.2 GB)")
                    print(f"3. Save it to: {download_dir.absolute()}")
                    print("\nPress Enter in this terminal when done...")
                    input()

                # Keep browser open for a bit to see what happened
                print("\nWaiting 10 seconds before closing browser...")
                print("(You can inspect the page while it's open)")
                await page.wait_for_timeout(10000)

            finally:
                await browser.close()

        print("\n" + "=" * 70)
        print("Download process complete")
        print("=" * 70 + "\n")

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback

        traceback.print_exc()
        return 1


async def main():
    """Main entry point."""
    return await download_from_jiocloud()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
