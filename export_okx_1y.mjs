// export_okx_1y.mjs
// 拉取 OKX ETH/USDT 15m 全年K线（经 Cloudflare Worker 代理），输出 okx_eth_15m.csv
// 直接替换本文件即可。Node 20+（GitHub Actions 默认环境）。

import fs from "fs/promises";

// === 需要你确认的唯一参数（把下面地址改成你自己的 Worker 域名） ===
const PROXY_BASE = "https://eth-proxy.1053363050.workers.dev/okx"; 
// 你的 Worker 我们此前配过：转发到 https://www.okx.com/api/v5/market/candles

// === 拉取配置 ===
const INST_ID   = "ETH-USDT-SWAP";
const BAR       = "15m";      // 15 分钟K
const LIMIT     = 300;        // OKX单页最大300
const MAX_PAGES = 500;        // 安全上限（足够覆盖 >1 年）
const TARGET_DAYS = 365;      // 期望抓取天数
const OUT_CSV   = "okx_eth_15m.csv";

// === 工具方法 ===
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function toIso(tsMs) {
  // OKX返回是毫秒字符串；我们保证按 UTC ISO 输出
  return new Date(Number(tsMs)).toISOString().replace(".000Z", "Z");
}

function buildUrl({ before }) {
  const u = new URL(PROXY_BASE);
  u.searchParams.set("instId", INST_ID);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", String(LIMIT));
  if (before) u.searchParams.set("before", String(before));
  // 你的 Worker 会把这些参数原样转给 OKX
  return u.toString();
}

// 兼容多种返回形态：
// 1) { code, data: [ [ts, o, h, l, c, vol, ...], ... ] }
// 2) { data: ... }  /  [ [ts, o, h, l, c, vol], ... ]
function normalizeCandles(json) {
  if (!json) return [];
  const arr = Array.isArray(json) ? json
    : Array.isArray(json.data) ? json.data
    : Array.isArray(json.candles) ? json.candles
    : [];

  // OKX 的 /market/candles 默认**最近在前**（倒序）；我们统一转成升序
  const rows = arr
    .map(r => Array.isArray(r) ? r : [])
    .filter(r => r.length >= 6)
    .map(r => ({
      ts:   Number(r[0]),           // 毫秒
      open: Number(r[1]),
      high: Number(r[2]),
      low:  Number(r[3]),
      close:Number(r[4]),
      vol:  Number(r[5]),
    }))
    .filter(r => Number.isFinite(r.ts));

  rows.sort((a,b) => a.ts - b.ts);
  return rows;
}

// 带重试的抓取
async function fetchPage(before, attempt = 1) {
  const url = buildUrl({ before });
  try {
    const resp = await fetch(url, { method: "GET", headers: { "accept": "application/json" } });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const text = await resp.text();

    // 可能代理层返回 HTML/错误页；尽量防御
    if (text.trim().startsWith("<")) throw new Error("Proxy returned HTML");

    const json = JSON.parse(text);
    const rows = normalizeCandles(json);

    // OKX 常见：返回空页/极少条 => 认为到头
    return rows;
  } catch (e) {
    if (attempt >= 5) throw e;
    const backoff = 400 * attempt;
    console.log(`Retry ${attempt}/5 after ${backoff}ms: ${e.message}`);
    await sleep(backoff);
    return fetchPage(before, attempt + 1);
  }
}

function dedupAppend(dst, src) {
  // 用 ts 去重并合并（dst、src 均为升序）
  const existed = new Set(dst.map(r => r.ts));
  for (const r of src) if (!existed.has(r.ts)) dst.push(r);
  dst.sort((a,b) => a.ts - b.ts);
  return dst;
}

function daysSpan(list) {
  if (list.length < 2) return 0;
  return (list.at(-1).ts - list[0].ts) / 86400000;
}

async function main() {
  console.log(`Fetching ${INST_ID} ${BAR} via ${PROXY_BASE} ...`);
  let all = [];
  let before = undefined;   // 第一页不带 before
  let pages = 0;

  while (pages < MAX_PAGES) {
    pages++;
    console.log(`→ Page ${pages} ${before ? `(before=${before})` : ""}`);

    const page = await fetchPage(before);

    if (page.length === 0) {
      console.log("Empty page, stop.");
      break;
    }

    // 追加去重
    all = dedupAppend(all, page);

    // 更新 before 为本页最早一根的 ts - 1ms（继续往更早翻页）
    const earliestTs = page[0].ts;
    before = earliestTs - 1;

    // 进度日志
    const span = daysSpan(all).toFixed(1);
    console.log(`  accumulated rows=${all.length}, span≈${span} days`);

    // 达到目标天数就退出
    if (daysSpan(all) >= TARGET_DAYS) {
      console.log(`Reached target ${TARGET_DAYS} days, stop.`);
      break;
    }

    // 轻微节流，避免触发频控
    await sleep(120);
  }

  if (all.length < 10000) {
    throw new Error(`rows=${all.length} 看起来像没翻页成功，请检查 Worker 或代理/网络`);
  }

  // 生成 CSV（升序）
  const header = "ts,iso,open,high,low,close,vol";
  const lines = all.map(r => [
    r.ts,
    toIso(r.ts),
    r.open,
    r.high,
    r.low,
    r.close,
    r.vol
  ].join(","));
  const csv = [header, ...lines].join("\n");

  await fs.writeFile(OUT_CSV, csv, "utf8");

  console.log(`Done: ${OUT_CSV}`);
  console.log(`rows=${all.length}, span≈${daysSpan(all).toFixed(1)} days`);
}

main().catch(err => {
  console.error("FATAL:", err.message);
  process.exit(1);
});
