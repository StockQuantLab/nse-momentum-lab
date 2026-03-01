#!/usr/bin/env playwright-cli
// Test Home Page Navigation Cards

await page.goto("http://localhost:8501");
await page.waitForTimeout(2000); // Wait for page to load

console.log("🔍 Testing: Home Page Navigation Cards");

// Count navigation cards
const cards = await page.locator("div[style*='border: 1px solid #e5e7eb']").count();
console.log(`   Found ${cards} navigation cards`);

if (cards >= 6) {
  console.log("✅ PASS: Navigation cards render correctly");
} else {
  console.log("❌ FAIL: Expected at least 6 navigation cards");
}

// Check for specific card content
const pageText = await page.textContent("body");
if (pageText.includes("Chat Assistant") &&
    pageText.includes("Momentum Scans") &&
    pageText.includes("Backtest Experiments")) {
  console.log("✅ PASS: All expected navigation items present");
} else {
  console.log("⚠️  WARNING: Some navigation items missing");
}
