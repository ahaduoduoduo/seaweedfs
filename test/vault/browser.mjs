import { readFile, rm } from "node:fs/promises";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
const { chromium } = await import(
  process.env.PLAYWRIGHT_MODULE || "playwright"
);
const config = JSON.parse(await readFile(process.argv[2], "utf8"));
const browser = await chromium.launch({ headless: true });
const errors = [];
const context = await browser.newContext({
  viewport: { width: 1320, height: 1080 },
});
try {
  const page = await context.newPage();
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(config.url);
  await page.locator("#token").fill(config.tokens.notes);
  await page.locator("#connect").click();
  await page.getByText("已连接 notes", { exact: true }).waitFor();
  await page
    .locator("#files li")
    .filter({ hasText: "article.json" })
    .first()
    .getByRole("button", { name: "读取", exact: true })
    .click();
  await page.getByText("读取完成，在客户端解密", { exact: true }).waitFor();
  assert.match(await page.locator("#preview").innerText(), /旅行笔记/);
  await page.screenshot({
    path: config.output + "/desktop.png",
    fullPage: true,
  });
  await page.locator("#group").fill("browser-note");
  await page.locator("#upload").setInputFiles(config.text);
  await page.locator("#send").click();
  await page
    .getByText("这组内容已保存至云端", { exact: true })
    .waitFor({ timeout: 60000 });
  const other = await context.newPage();
  await other.goto(config.url);
  await page.evaluate(async () => {
    const sdk = await import("/sdk/client.mjs");
    await sdk.clearCache();
  });
  const before = await (await fetch(config.gateway + "/lab/status")).json();
  const readPhoto = (target) =>
    target.evaluate(async (cfg) => {
      const { VaultClient } = await import("/sdk/client.mjs");
      const client = new VaultClient({
        url: cfg.url,
        space: "photos",
        token: cfg.tokens.photos,
        transport: "direct",
      });
      const bytes = await client.read("photo-0005");
      return Array.from(
        new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)),
        (x) => x.toString(16).padStart(2, "0"),
      ).join("");
    }, config);
  const cli = spawn(
    process.execPath,
    [
      fileURLToPath(new URL("../../examples/vault/cli.mjs", import.meta.url)),
      "get",
      "video",
      config.output + "/video-range.bin",
      "100",
      "1048676",
    ],
    {
      env: {
        ...process.env,
        VAULT_URL: config.url,
        VAULT_SPACE: "files",
        VAULT_TOKEN: config.tokens.files,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let cliError = "";
  cli.stderr.on("data", (b) => (cliError += b));
  const cliDone = new Promise((resolve, reject) => {
    cli.on("error", reject);
    cli.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(cliError)),
    );
  });
  const [a, b] = await Promise.all([
    readPhoto(page),
    readPhoto(other),
    cliDone,
  ]);
  const want = createHash("sha256")
    .update(await readFile(config.image))
    .digest("hex");
  assert.equal(a, want);
  assert.equal(b, want);
  const after = await (await fetch(config.gateway + "/lab/status")).json();
  assert.equal(
    after.downloads - before.downloads,
    2,
    "two tabs must share one photo pack download plus one CLI video pack",
  );
  await other.locator("#space").fill("photos");
  await other.locator("#token").fill(config.tokens.photos);
  await other.locator("#connect").click();
  await other.getByText("已连接 photos", { exact: true }).waitFor();
  await other
    .getByRole("button", { name: "读取", exact: true })
    .first()
    .click();
  await other.locator("#preview img").waitFor();
  await other.setViewportSize({ width: 390, height: 844 });
  await other.screenshot({
    path: config.output + "/mobile.png",
    fullPage: true,
  });
  assert.equal(
    await other.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
    true,
  );
  await fetch(config.gateway + "/lab/control", {
    method: "POST",
    body: JSON.stringify({ disable_cors: true, download_delay_ms: 100 }),
    headers: { "Content-Type": "application/json" },
  });
  const fallbackContext = await browser.newContext();
  const fallbackPage = await fallbackContext.newPage();
  await fallbackPage.goto(config.url);
  const events = await fallbackPage.evaluate(async (cfg) => {
    const { VaultClient } = await import("/sdk/client.mjs");
    const events = [];
    const client = new VaultClient({
      url: cfg.url,
      space: "notes",
      token: cfg.tokens.notes,
      onEvent: (e) => events.push(e.type),
    });
    const text = new TextDecoder().decode(await client.read("n002-json"));
    if (!text.includes("旅行笔记")) throw new Error("decryption failed");
    return events;
  }, config);
  assert(events.includes("cors-fallback"));
  await fallbackContext.close();
  await fetch(config.gateway + "/lab/control", {
    method: "POST",
    body: "{}",
    headers: { "Content-Type": "application/json" },
  });
  assert.deepEqual(errors, []);
  await rm(config.output + "/video-range.bin", { force: true });
  console.log(
    "Browser: upload, preview, mobile layout, two-tab cache, concurrent CLI and CORS fallback passed",
  );
} finally {
  await browser.close();
}
