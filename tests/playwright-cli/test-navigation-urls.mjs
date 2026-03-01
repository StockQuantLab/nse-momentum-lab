// Playwright CLI test for dashboard navigation URL cleanliness
// Verifies that navigation cards use clean URLs without .py extensions

const { chromium } = require('playwright');

(async () => {
  // Launch browser
  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext();
  const page = await context.newPage();

  // Test configuration
  const baseUrl = 'http://localhost:8501';
  const navigationCards = [
    { name: 'Chat Assistant', expectedUrl: '/chat-assistant' },
    { name: 'Momentum Scans', expectedUrl: '/Scans' },
    { name: 'Backtest Experiments', expectedUrl: '/Backtest' },
    { name: 'Paper Ledger', expectedUrl: '/Ledger' },
    { name: 'Daily Summary', expectedUrl: '/Daily' },
    { name: 'Run Pipeline', expectedUrl: '/Pipeline' },
    { name: 'Pipeline Status', expectedUrl: '/PipelineStatus' },
    { name: 'Data Quality', expectedUrl: '/DataQuality' }
  ];

  console.log('🧪 Starting navigation URL cleanliness tests...\n');

  try {
    // 1. Navigate to home page and take initial screenshot
    console.log('📍 Step 1: Loading home page...');
    await page.goto(baseUrl);
    await page.waitForTimeout(3000); // Wait for Streamlit to load

    await page.screenshot({
      path: 'C:\\Users\\kanna\\github\\nse-momentum-lab\\tests\\playwright-cli\\screenshots\\home-page.png',
      fullPage: true
    });
    console.log('✅ Home page screenshot captured\n');

    // 2. Test each navigation card
    for (let i = 0; i < navigationCards.length; i++) {
      const card = navigationCards[i];
      console.log(`🔍 Step ${i + 2}: Testing "${card.name}" navigation card...`);

      try {
        // Click the navigation card
        const cardLocator = page.locator(`text=${card.name}`);
        await cardLocator.click();

        // Wait for navigation to complete
        await page.waitForLoadState('networkidle');
        await page.waitForTimeout(2000);

        // Get current URL
        const currentUrl = page.url();

        // Check if URL contains .py extension
        if (currentUrl.includes('.py')) {
          console.log(`❌ FAIL: "${card.name}" URL contains .py extension: ${currentUrl}`);
          process.exit(1);
        } else {
          console.log(`✅ PASS: "${card.name}" URL is clean: ${currentUrl}`);
        }

        // Verify page loaded successfully (no error messages)
        const errorText = await page.textContent('.stAlert, .stException, .stError');
        if (errorText && errorText.toLowerCase().includes('error')) {
          console.log(`❌ FAIL: "${card.name}" page has errors: ${errorText}`);
          process.exit(1);
        }

        // Take screenshot of the loaded page
        const screenshotPath = `C:\\Users\\kanna\\github\\nse-momentum-lab\\tests\\playwright-cli\\screenshots\\${card.name.toLowerCase().replace(/\s+/g, '-')}.png`;
        await page.screenshot({
          path: screenshotPath,
          fullPage: true
        });
        console.log(`✅ Screenshot captured for "${card.name}"\n`);

        // Navigate back to home for next test
        await page.goto(baseUrl);
        await page.waitForTimeout(2000);

      } catch (error) {
        console.log(`❌ FAIL: "${card.name}" navigation failed: ${error.message}`);
        process.exit(1);
      }
    }

    console.log('🎉 All navigation URL tests passed successfully!');
    console.log('✅ All 8 navigation cards use clean URLs without .py extensions');
    console.log('✅ All pages loaded successfully');
    console.log('✅ All screenshots captured');

  } catch (error) {
    console.log(`❌ Test suite failed: ${error.message}`);
    process.exit(1);
  } finally {
    // Close browser
    await browser.close();
  }
})();
