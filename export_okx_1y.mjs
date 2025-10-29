// export_okx_1y.mjs  —— 正向分页( after ) 版，专为 1053363050 定制
// 目标：抓满 365 天的 ETH/USDT 15m K线
// 输出：okx_eth_15m.csv
// 运行环境：Node.js 20+

import fs from "fs/promises";

const PROXY_BASE = "https://eth-proxy.1053363050.workers.dev/okx";

// ====== 可选：现货更长历史，把下面符号改为 'ETH-USDT' ======
const INST_ID = "ETH-USDT-SWAP";   // 或 'ETH-USDT'
const BAR = "15m";
const LIMIT = 300;                 // OKX 单页最大
const TARGET_DAYS = 365;
const MAX_PAGES = 2000;            // 放宽，确保能翻够一年
const OUT_CSV = "okx_eth_15m.csv";

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const toIso = (ms) => new Date(Number(ms)).toISOString().replace(".000Z", "Z");

function buildUrl({ after }) {
  const u = new URL(PROXY_BASE);
  u.searchParams.set("instId", INST_ID);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", String(LIMIT));
  if (after) u.searchParams.set("after", String(after));
  return u.toString();
}

function normalize(json) {
  const arr = Array.isArray(json)
    ? json
    : Array.isArray(json.data) ? json.data
    : Array.isArray(json.candles) ? json.candles
    : [];

  const rows = arr
    .map(r => Array.isArray(r) ? r : [])
    .filter(r => r.length >= 6)
    .map(r => ({
      ts: Number(r[0]),             // ms
      open: Number(r[1]),
      high: Number(r[2]),
      low: Number(r[3]),
      close: Number(r[4]),
      vol: Number(r[5]),
    }))
    .filter(r => Number.isFinite(r.ts));

  // OKX 返回一般是倒序，这里统一升序
  rows.sort((a, b) => a.ts - b.ts);
  return rows;
}

async function fetchPage(after, attempt = 1) {
  const url = buildUrl({ after });
  try {
    const resp = await fetch(url, { headers: { accept: "application/json" } });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const text = await resp.text();
    if (text.trim().startsWith("<")) throw new Error("Proxy returned HTML");

    const json = JSON.parse(text);
    return normalize(json);
  } catch (e) {
    if (attempt >= 5) throw e;
    const wait = 600 * attempt;
    console.log(`Retry ${attempt}/5 after ${wait}ms: ${e.message}`);
    await sleep(wait);
    return fetchPage(after, attempt + 1);
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

async function main() {
  const now = Date.now();
  const startTs = now - TARGET_DAYS * 86400000; // 从 365 天前开始
  console.log(`=== 抓取 ${INST_ID} ${BAR} 正向分页 from ${toIso(startTs)} to ${toIso(now)} ===`);

  let all = [];
  let after = startTs;   // 用 after 正向推进
  let pages = 0;

  while (pages < MAX_PAGES) {
    pages++;
    console.log(`→ 第 ${pages} 页 (after=${after})`);
    const page = await fetchPage(after);

    if (!page.length) {
      console.log("⚠️ 空页，可能到达最早/代理忽略 after。尝试微步前进。");
      after += 1; // 微步前进，避免卡住
      if (after >= now) break;
      await sleep(120);
      continue;
    }

    all = dedupAppend(all, page);
    const lastTs = page.at(-1).ts;
    after = lastTs + 1; // 下一页从本页最后一根之后开始

    console.log(`累计 ${all.length} 根，跨度≈${daysSpan(all).toFixed(1)} 天，最早 ${toIso(all[0].ts)} 最晚 ${toIso(all.at(-1).ts)}`);

    if (after >= now) {
      console.log("✅ 已到当前时间。");
      break;
    }

    // 节流
    await sleep(120);
  }

  const header = "ts,iso,open,high,low,close,vol";
  const lines = all.map(r => [r.ts, toIso(r.ts), r.open, r.high, r.low, r.close, r.vol].join(","));
  const csv = [header, ...lines].join("\n");
  await fs.writeFile(OUT_CSV, csv, "utf8");

  console.log(`✅ 导出完成: ${OUT_CSV}`);
  console.log(`总条数: ${all.length}，时间跨度≈${daysSpan(all).toFixed(1)} 天`);
}

main().catch(e => {
  console.error("FATAL:", e);
  process.exit(1);
});
