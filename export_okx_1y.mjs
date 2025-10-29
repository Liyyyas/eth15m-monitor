// export_okx_1y.mjs
// Fetch ETH/USDT 15m candles for the last 365 days from OKX via your Cloudflare Worker,
// then write okx_eth_15m.csv in repo root.
// Node.js v20+ (fetch/URL/FS built-in). ESM module.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

/* ====================== CONFIG ====================== */
const PROXY_BASE = "https://eth-proxy.1053363050.workers.dev"; // 你的 Worker 根域名
const INST_ID = "ETH-USDT";   // 标的
const BAR     = "15m";        // K线周期
const DAYS    = 365;          // 拉取天数
const LIMIT   = 300;          // 每页条数（OKX 支持 100/200/300）
const OUT_CSV = "okx_eth_15m.csv"; // 输出文件名（仓库根目录）
/* ==================================================== */

// 计算时间边界
const nowMs = Date.now();
const cutoffMs = nowMs - DAYS * 24 * 60 * 60 * 1000;

// 小工具：休眠
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 通过 Worker 代理去拉 OKX
async function fetchViaProxy(targetUrl) {
  // 先试 /?url=，不行再试 /proxy?url=
  const candidates = [
    `${PROXY_BASE}/?url=${encodeURIComponent(targetUrl)}`,
    `${PROXY_BASE}/proxy?url=${encodeURIComponent(targetUrl)}`
  ];

  let lastErr;
  for (const url of candidates) {
    try {
      const resp = await fetch(url, { headers: { accept: "application/json" } });
      const text = await resp.text();

      // 必须是 JSON
      const data = JSON.parse(text);
      return data;
    } catch (e) {
      lastErr = e;
      // 继续尝试下一个候选
    }
  }
  throw lastErr ?? new Error("Proxy returned non-JSON response");
}

// 拉一页历史 K 线（OKX 返回从新到旧）
async function okxFetchPage({ before, limit = LIMIT }) {
  const q = new URLSearchParams({
    instId: INST_ID,
    bar: BAR,
    limit: String(limit),
    before: String(before)
  });
  const target = `https://www.okx.com/api/v5/market/history-candles?${q.toString()}`;

  // 带重试
  for (let i = 1; i <= 5; i++) {
    try {
      const payload = await fetchViaProxy(target);
      if (payload?.code === "0" && Array.isArray(payload.data)) {
        return payload.data; // data: Array<Array<string>> [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
      }
      throw new Error(`Bad payload: ${JSON.stringify(payload).slice(0, 200)}`);
    } catch (err) {
      if (i === 5) throw err;
      await sleep(500 * i);
    }
  }
}

// 主逻辑：翻页一直拉到一年之前
async function collectCandles() {
  const all = [];
  let before = nowMs;
  let pages = 0;

  while (true) {
    pages++;
    const page = await okxFetchPage({ before, limit: LIMIT });

    if (!page?.length) break;

    // OKX 返回按新→旧；拼到数组末尾
    all.push(...page);

    // 本页里最老的一根
    const oldestTs = Number(page[page.length - 1][0]);
    if (!Number.isFinite(oldestTs)) break;

    // 翻页游标：继续取更早的
    before = oldestTs;

    // 达到时间边界就停
    if (oldestTs <= cutoffMs) break;

    // 安全上限，防止死循环
    if (pages > 1000) break;
  }

  // 去重 & 排序（升序）
  const map = new Map(); // key: ts -> last row
  for (const row of all) {
    const ts = Number(row[0]);
    if (Number.isFinite(ts)) map.set(ts, row);
  }
  const rows = Array.from(map.values()).sort((a, b) => Number(a[0]) - Number(b[0]));

  // 仅保留一年内的数据
  const filtered = rows.filter((r) => Number(r[0]) >= cutoffMs);

  return filtered;
}

// 写 CSV：ts, iso, open, high, low, close, vol
function writeCsv(rows) {
  const header = "ts,iso,open,high,low,close,vol";
  const lines = [header];

  for (const r of rows) {
    const ts = Number(r[0]);
    const open  = r[1];
    const high  = r[2];
    const low   = r[3];
    const close = r[4];
    const vol   = r[5];

    const iso = new Date(ts).toISOString();
    lines.push([ts, iso, open, high, low, close, vol].join(","));
  }

  const content = lines.join("\n") + "\n";

  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const outPath = path.join(__dirname, OUT_CSV);

  fs.writeFileSync(outPath, content, "utf8");
  return outPath;
}

// 入口
(async () => {
  console.log(`Fetching ${INST_ID} ${BAR} for last ${DAYS} days...`);
  console.log(`Proxy: ${PROXY_BASE}`);
  const rows = await collectCandles();
  console.log(`Total rows (within ${DAYS}d):`, rows.length);

  if (rows.length < 10) {
    throw new Error("rows < 10，疑似拿到的是空页/HTML，请检查 Worker 与 PROXY_BASE。");
  }

  const out = writeCsv(rows);
  console.log("CSV written:", out);
})().catch((e) => {
  console.error("Error:", e?.message || e);
  process.exit(1);
});
