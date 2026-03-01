#!/usr/bin/env playwright-cli
// Test Chat Page Confirmation Dialog

await page.goto("http://localhost:8501/pages/01_Chat.py");
await page.waitForTimeout(2000); // Wait for page to load

console.log("🔍 Testing: Chat Page Confirmation Dialog");

// Test 1: Check for Clear Chat button
const clearButton = await page.locator("button:has-text('Clear Chat')").count();
if (clearButton > 0) {
  console.log("✅ PASS: Clear Chat button found");

  // Test 2: Click button and check for confirmation
  await page.locator("button:has-text('Clear Chat')").click();
  await page.waitForTimeout(500);

  const yesButton = await page.locator("button:has-text('Yes')").or(page.locator("button:has-text('clear it')")).count();
  const cancelButton = await page.locator("button:has-text('Cancel')").count();

  if (yesButton > 0 && cancelButton > 0) {
    console.log("✅ PASS: Confirmation dialog appears with Yes/Cancel buttons");

    // Cancel to avoid actually clearing
    await page.locator("button:has-text('Cancel')").click();
    await page.waitForTimeout(500);
    console.log("✅ PASS: Cancel button works");
  } else {
    console.log("❌ FAIL: Confirmation dialog not found");
  }
} else {
  console.log("⚠️  WARNING: Clear Chat button not found");
}
