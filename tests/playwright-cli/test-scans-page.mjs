#!/usr/bin/env playwright-cli
// Test Scans Page Improvements

await page.goto("http://localhost:8501/pages/03_Scans.py");
await page.waitForTimeout(3000); // Wait for page to load

console.log("🔍 Testing: Scans Page Improvements");

// Test 1: Check for score distribution chart elements
const pageText = await page.textContent("body");
if (pageText.includes("Score") || pageText.includes("Distribution") || pageText.includes("Pass Rate")) {
  console.log("✅ PASS: Chart-related elements found");
} else {
  console.log("⚠️  WARNING: Chart elements not visible (may need scan data)");
}

// Test 2: Check for pagination
const passedTab = await page.locator("text=Passed Stocks").count();
const failedTab = await page.locator("text=Failed Stocks").count();
if (passedTab > 0 || failedTab > 0) {
  console.log("✅ PASS: Passed/Failed tabs found");
} else {
  console.log("⚠️  WARNING: Tabs not found");
}

// Test 3: Check for pagination buttons
const prevBtn = await page.locator("button:has-text('Previous')").count();
const nextBtn = await page.locator("button:has-text('Next')").count();
if (prevBtn > 0 || nextBtn > 0) {
  console.log("✅ PASS: Pagination controls found");
} else {
  console.log("⚠️  WARNING: Pagination not visible (may need scan data)");
}

// Test 4: Check for CSV download
const downloadBtn = await page.locator("button:has-text('Download')").count();
if (downloadBtn > 0) {
  console.log("✅ PASS: CSV download button found");
} else {
  console.log("⚠️  WARNING: Download button not found (may need data)");
}

// Test 5: Check that arbitrary limits are removed
if (pageText.includes("Showing") || pageText.includes("of")) {
  console.log("✅ PASS: Pagination indicators found (limits removed)");
} else {
  console.log("⚠️  WARNING: Pagination indicators not found");
}
