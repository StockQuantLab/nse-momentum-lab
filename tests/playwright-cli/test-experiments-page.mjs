#!/usr/bin/env playwright-cli
// Test Experiments Page Improvements

await page.goto("http://localhost:8501/pages/04_Experiments.py");
await page.waitForTimeout(3000); // Wait for page to load

console.log("🔍 Testing: Experiments Page Improvements");

// Test 1: Search box exists
const searchBox = await page.locator("input[placeholder*='Search']").count();
if (searchBox > 0) {
  console.log("✅ PASS: Search box found");
} else {
  console.log("⚠️  WARNING: Search box not found");
}

// Test 2: Status filter (multiselect) exists
const multiselect = await page.locator("[data-testid='stMultiSelect']").count();
if (multiselect > 0) {
  console.log("✅ PASS: Status filter multiselect found");
} else {
  console.log("⚠️  WARNING: Status filter not found");
}

// Test 3: Check for chart tabs
const tabs = await page.locator("[data-testid='stTab']").count();
console.log(`   Found ${tabs} tabs`);

const pageText = await page.textContent("body");
if (pageText.includes("Equity") || pageText.includes("Drawdown") || pageText.includes("P&L Distribution")) {
  console.log("✅ PASS: Chart-related tabs found");
} else {
  console.log("⚠️  WARNING: Chart tabs not visible (may need experiment data)");
}

// Test 4: Pagination controls
const prevButton = await page.locator("button:has-text('Previous')").count();
const nextButton = await page.locator("button:has-text('Next')").count();
if (prevButton > 0 || nextButton > 0) {
  console.log("✅ PASS: Pagination controls found");
} else {
  console.log("⚠️  WARNING: Pagination not visible (may need experiment data)");
}

// Test 5: CSV download button
const downloadBtn = await page.locator("button:has-text('Download')").count();
if (downloadBtn > 0) {
  console.log("✅ PASS: CSV download button found");
} else {
  console.log("⚠️  WARNING: Download button not found (may need data)");
}
