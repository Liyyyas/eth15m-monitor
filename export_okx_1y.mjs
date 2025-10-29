// export_okx_1y.mjs
// 拉取 ETH/USDT 15m 最近 365 天K线，优先经你的 Cloudflare Worker，失败则直连 OKX。
// 输出仓库根目录 okx_eth_15m.csv。Node 20+ / ESM。

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

/* ============== 配置 ============== */
const PROXY_BASE = "https://eth-proxy.1053363050.workers.dev"; // 你的 Worker
const INST_ID = "ETH-USDT";
const BAR = "15m";
const DAYS = 365;
const PAGE_LIMIT = 300;                     // OKX 支持到 300
const OUT_CSV = "okx_eth_15m.csv";
/* ================================= */

// 时间边界
const nowMs = Date.now();
const cutoffMs = nowMs - DAYS * 24 * 60 * 60 * 1000;

// 小工具
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const okxHistUrl = (before, limit = PAGE_LIMIT) =>
  `https://www.okx.com/api/v5/market/history-candles?` +
  new URLSearchParams({ instId: INST_ID, bar: BAR, before: String(before), limit: String(limit) }).toString();

// 判定是否 HTML/非 JSON
const looksLikeHtmlOrText = (s) => {
  if (!s) return true;
  const t = s.trim().slice(0, 64).toLowerCase();
  return t.startsWith("<") || t.startsWith("okx proxy") || t.includes("<html");
};

async function fetchJson(url, useProxyFirst = true) {
  const headers = {
    "accept": "application/json, text/plain;q=0.5, */*;q=0.1",
    // 一些站点会对 UA 比较敏感，给个常见 UA 可以减少 403/验证页
    "user-agent":
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
  };

  // 两条线路：优先代理，失败走直连
  const routes = useProxyFirst
    ? [
        `${PROXY_BASE}/?url=${encodeURIComponent(url)}`,
        `${PROXY_BASE}/proxy?url=${encodeURIComponent(url)}`,
        url, // 直连
      ]
    : [url];

  let lastErr;
  for (const route of routes) {
    for (let i = 1; i <= 4; i++) {
      try {
        const resp = await fetch(route, { headers });
        const text = await resp.text();

        // 代理可能回 HTML/提示文本；直连也可能回 HTML（风控/错误页）
        if (looksLikeHtmlOrText(text)) throw new Error(`non-json from route: ${route}`);

        const data = JSON.parse(text);
        return data;
      } catch (e) {
        lastErr = e;
        await sleep(400 * i); // 线性退避
      }
    }
  }
  throw lastErr ?? new Error("All routes failed");
}

async function fetchPage(before) {
  const url = okxHistUrl(before, PAGE_LIMIT);
  const data = await fetchJson(url, true);

  if (data?.code === "0" && Array.isArray(data.data)) {
    return data.data; // [ [ts, open, high, low, close, vol, ...], ... ] 新->旧
  }
  throw new Error(`Bad payload: ${JSON.stringify(data).slice(0, 200)}`);
}

async function collectCandles() {
  const bucket = [];
  let before = nowMs;
  let pages = 0;

  while (true) {
    pages++;
    const page = await fetchPage(before);
    if (!page?.length) break;

    bucket.push(...page);

    const oldestTs = Number(page[page.length - 1][0]);
    if (!Number.isFinite(oldestTs)) break;

    before = oldestTs;
    if (oldestTs <= cutoffMs) break;
    if (pages > 1000) break; // 保险
  }

  // 去重 + 升序
  const map = new Map();
  for (const r of bucket) {
    const ts = Number(r[0]);
    if (Number.isFinite(ts)) map.set(ts, r);
  }
  const rows = Array.from(map.values()).sort((a, b) => Number(a[0]) - Number(b[0]));
  return rows.filter((r) => Number(r[0]) >= cutoffMs);
}

function writeCsv(rows) {
  const header = "ts,iso,open,high,low,close,vol";
  const lines = [header];

  for (const r of rows) {
    const ts = Number(r[0]);
    const iso = new Date(ts).toISOString();
    const [ , open, high, low, close, vol ] = r;
    lines.push([ts, iso, open, high, low, close, vol].join(","));
  }

  const content = lines.join("\n") + "\n";
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const outPath = path.join(__dirname, OUT_CSV);
  fs.writeFileSync(outPath, content, "utf8");
  return outPath;
}

(async () => {
  console.log(`Fetching ${INST_ID} ${BAR} for last ${DAYS} days...`);
  console.log(`Proxy first: ${PROXY_BASE}`);
  const rows = await collectCandles();
  console.log(`Rows within ${DAYS}d: ${rows.length}`);
  if (rows.length < 10) throw new Error("rows < 10，疑似仍然是空页/HTML。");

  const out = writeCsv(rows);
  console.log("CSV written:", out);
})().catch((e) => {
  console.error("Error:", e?.message || e);
  process.exit(1);
});
