#!/usr/bin/env python
"""
Download Zerodha DAILY equity data from Jio Cloud.

Target folder: daily (0.2 GB)
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


async def download_zerodha_daily():
    """Download Zerodha daily data from Jio Cloud."""
    print("\n" + "=" * 70)
    print("Downloading Zerodha DAILY Data from Jio Cloud")
    print("=" * 70)

    from playwright.async_api import async_playwright

    share_url = "https://www.jioaicloud.com/l/?u=nJeSTwHnU5GtuaLD7aYu97WZUO0E-HJCtLqWE-q4gD3VbsX1gBXZVMyTO5OGzLd-hkW"
    download_dir = Path("data/vendor/zerodha")
    download_dir.mkdir(parents=True, exist_ok=True)

    print("\nTarget: daily folder (0.2 GB)")
    print(f"URL: {share_url}")
    print(f"Save to: {download_dir.absolute()}\n")

    async with async_playwright() as p:
        # Launch browser (NOT headless so you can see what's happening)
        browser = await p.chromium.launch(
            headless=False,  # Keep visible so you can help if needed
            args=["--no-sandbox"],
        )

        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1920, "height": 1080},
        )

        page = await context.new_page()
        page.set_default_timeout(120000)

        try:
            print("Opening browser... (please wait)")
            await page.goto(share_url, wait_until="networkidle")
            await page.wait_for_timeout(5000)

            print("\n" + "=" * 70)
            print("INSTRUCTIONS:")
            print("=" * 70)
            print("\n1. Look for 'daily' folder on the page")
            print("2. Click on the 'daily' folder")
            print("3. Look for download button (⬇️ or 'Download')")
            print("4. Download the ZIP file")
            print(f"5. Browser will auto-save to: {download_dir.absolute()}")
            print("\nWaiting for you to interact...")
            print("(Script will wait for download to start)")
            print("=" * 70 + "\n")

            # Wait for download to start
            try:
                download = await page.wait_for_event("download", timeout=300000)  # 5 minutes

                print("\n[SUCCESS] Download detected!")
                print(f"File: {download.suggested_filename}")

                # Save file
                save_path = download_dir / download.suggested_filename
                await download.save_as(save_path)

                size_mb = save_path.stat().st_size / (1024 * 1024)
                print(f"\nDownloaded: {save_path}")
                print(f"Size: {size_mb:.1f} MB")

                if size_mb > 100:  # Should be around 200 MB
                    print("\n[SUCCESS] Full daily data downloaded!")
                    print("\nNext steps:")
                    print(f"1. Extract: cd {download_dir} && unzip {save_path.name}")
                    print(
                        f"2. Ingest: doppler run -- uv run python scripts/ingest_vendor_candles.py {download_dir / 'day'} --timeframe day --vendor zerodha"
                    )

            except Exception:
                print("\n[TIMEOUT] Waiting too long for download.")
                print("\nPlease download manually:")
                print("1. Find 'daily' folder on the page")
                print("2. Download the ZIP")
                print(f"3. Save to: {download_dir.absolute()}")
                print("\nPress Enter when done...")
                input()

                # Check if file was downloaded manually
                zip_files = list(download_dir.glob("*.zip"))
                if zip_files:
                    print(f"\n[SUCCESS] Found downloaded file: {zip_files[0].name}")
                    print(f"Size: {zip_files[0].stat().st_size / (1024 * 1024):.1f} MB")

            # Keep browser open a bit more
            print("\nKeeping browser open for 30 seconds...")
            print("(Close it manually if needed)")
            await page.wait_for_timeout(30000)

        finally:
            await browser.close()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(download_zerodha_daily()))
