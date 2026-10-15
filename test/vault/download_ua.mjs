import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import assert from "node:assert/strict";
import { VaultClient } from "../../weed/vault/assets/client.mjs";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || "playwright");
const cfg = JSON.parse(await readFile(process.argv[2], "utf8"));
const busyOnce = () => fetch(cfg.gateway + "/lab/control", { method: "POST", body: JSON.stringify({ require_empty_user_agent: true, busy_downloads: 1 }), headers: { "Content-Type": "application/json" } });
await busyOnce();
const native = new VaultClient({ url: cfg.url, space: "files", token: cfg.token, transport: "direct" });
assert.equal(createHash("sha256").update(await native.read("ua-contract")).digest("hex"), cfg.want);
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  await page.goto(cfg.browser_url);
  await busyOnce();
  const result = await page.evaluate(async cfg => {
    const { VaultClient } = await import("/sdk/client.mjs");
    const events = [];
    const client = new VaultClient({ url: cfg.url, space: "files", token: cfg.token, onEvent: e => events.push(e.type) });
    const bytes = await client.read("ua-contract");
    const hash = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)), b => b.toString(16).padStart(2, "0")).join("");
    return { events, hash };
  }, cfg);
  assert.equal(result.hash, cfg.want);
  assert(result.events.includes("header-fallback"));
  assert(result.events.includes("cdn-busy"));
  assert(!result.events.includes("link-refresh"));
} finally {
  await browser.close();
}
