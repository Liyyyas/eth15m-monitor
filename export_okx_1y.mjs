// export_okx_1y.mjs
// ✅ 完整版：通过 Cloudflare Worker 抓取 OKX ETH/USDT 15m 全年K线
// 作者：ChatGPT 为 1053363050 定制版本
// 执行环境：Node.js 20+（GitHub Actions 默认）
// 输出文件：okx_eth_15m.csv
// 约 35,000～37,000 行 ≈ 一年数据

import fs from "fs/promises";

// ===== 固定配置（你的 Worker 域名） =====
const PROXY_BASE = "https://eth-proxy.1053363050.workers.dev/okx";

// ===== 抓取参数 =====
const INST_ID = "ETH-USDT-SWAP";  // 永续合约更活跃，如需现货改成 ETH-USDT
const BAR = "15m";                // 15分钟K线
const LIMIT = 300;                // OKX每页最大条数
const MAX_PAGES = 600;            // 最大翻页次数
const TARGET_DAYS = 365;          // 抓取目标时间跨度（天）
const OUT_CSV = "okx_eth_15m.csv";

// ===== 工具函数 =====
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const toIso = (tsMs) => new Date(Number(tsMs)).toISOString().replace(".000Z", "Z");

function buildUrl({ before }) {
  const u = new URL(PROXY_BASE);
  u.searchParams.set("instId", INST_ID);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", String(LIMIT));
  if (before) u.searchParams.set("before", String(before));
  return u.toString();
}

function normalizeCandles(json) {
  if (!json) return [];
  const arr = Array.isArray(json)
    ? json
    : Array.isArray(json.data)
    ? json.data
    : Array.isArray(json.candles)
    ? json.candles
    : [];

  const rows = arr
    .map(r => Array.isArray(r) ? r : [])
    .filter(r => r.length >= 6)
    .map(r => ({
      ts: Number(r[0]),
      open: Number(r[1]),
      high: Number(r[2]),
      low: Number(r[3]),
      close: Number(r[4]),
      vol: Number(r[5])
    }))
    .filter(r => Number.isFinite(r.ts));

  // OKX返回倒序，转换为升序
  rows.sort((a, b) => a.ts - b.ts);
  return rows;
}

async function fetchPage(before, attempt = 1) {
  const url = buildUrl({ before });
  try {
    const resp = await fetch(url, { method: "GET", headers: { accept: "application/json" } });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const text = await resp.text();

    if (text.trim().startsWith("<")) throw new Error("Proxy returned HTML (check Worker)");

    const json = JSON.parse(text);
    const rows = normalizeCandles(json);
    return rows;
  } catch (e) {
    if (attempt >= 5) throw e;
    const wait = 500 * attempt;
    console.log(`Retry ${attempt}/5 after ${wait}ms: ${e.message}`);
    await sleep(wait);
    return fetchPage(before, attempt + 1);
  }
}

function dedupAppend(dst, src) {
  const seen = new Set(dst.map(r => r.ts));
  for (const r of src) if (!seen.has(r.ts)) dst.push(r);
  dst.sort((a, b) => a.ts - b.ts);
  return dst;
}

function daysSpan(list) {
  if (list.length < 2) return 0;
  return (list.at(-1).ts - list[0].ts) / 86400000;
}

// ===== 主逻辑 =====
async function main() {
  console.log(`=== 开始抓取 ${INST_ID} ${BAR} 数据 (via ${PROXY_BASE}) ===`);
  let all = [];
  let before = undefined;
  let pages = 0;

  while (pages < MAX_PAGES) {
    pages++;
    console.log(`→ 第 ${pages} 页 ${before ? `(before=${before})` : ""}`);

    const page = await fetchPage(before);

    if (!page.length) {
      console.log("⚠️ 空页，停止。");
      break;
    }

    all = dedupAppend(all, page);
    const earliest = page[0].ts;
    before = earliest - 1;

    console.log(`已抓取 ${all.length} 根，时间跨度≈${daysSpan(all).toFixed(1)} 天`);

    if (daysSpan(all) >= TARGET_DAYS) {
      console.log(`✅ 达到目标 ${TARGET_DAYS} 天，结束。`);
      break;
    }

    await sleep(150);
  }

  if (all.length < 10000) {
    console.warn(`❌ 抓取量过少 (${all.length} 条)，可能代理异常。`);
  }

  const header = "ts,iso,open,high,low,close,vol";
  const lines = all.map(r =>
    [r.ts, toIso(r.ts), r.open, r.high, r.low, r.close, r.vol].join(",")
  );
  const csv = [header, ...lines].join("\n");
  await fs.writeFile(OUT_CSV, csv, "utf8");

  console.log(`✅ 完成导出: ${OUT_CSV}`);
  console.log(`总条数: ${all.length}`);
  console.log(`时间跨度≈${daysSpan(all).toFixed(1)} 天`);
}

main().catch(err => {
  console.error("FATAL:", err);
  process.exit(1);
});
