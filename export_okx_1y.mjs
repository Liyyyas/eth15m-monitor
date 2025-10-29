// eth15m-monitor/export_okx_1y.mjs
// 拉取 ETH-USDT 15m 近 365 天，分页稳定、节流、去重，自动写入 okx_eth_15m.csv

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const PROXY = "https://eth-proxy.1053363050.workers.dev"; // 你的 Worker
const INST_ID = "ETH-USDT";
const BAR = "15m";
const LIMIT = 300;               // OKX 单页上限
const SLEEP_MS = 150;            // 节流：视速率可调 100~300
const RETRY = 3;                 // 单页重试
const DAYS = 365;                // 抓取天数
const PAGE_GUARD = 200;          // 安全页数上限（15m≈117 页，200 足够防止死循环）

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 输出到仓库根下的 eth15m-monitor/okx_eth_15m.csv
const repoRoot = path.resolve(__dirname, "..");
const outDir = path.join(repoRoot, "eth15m-monitor");
const outFile = path.join(outDir, "okx_eth_15m.csv");
fs.mkdirSync(outDir, { recursive: true });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = Date.now();
const yearAgo = now - DAYS * 24 * 60 * 60 * 1000;

// Worker 白名单固定：/api/v5/market/history-candles
function buildUrl(beforeTs) {
  const u = new URL(PROXY + "/api/v5/market/history-candles");
  u.searchParams.set("instId", INST_ID);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", String(LIMIT));
  if (beforeTs) u.searchParams.set("before", String(beforeTs));
  return u.toString();
}

async function fetchPage(beforeTs) {
  const url = buildUrl(beforeTs);
  for (let i = 1; i <= RETRY; i++) {
    try {
      const r = await fetch(url, { headers: { accept: "application/json" } });
      const t = await r.text();
      let j;
      try {
        j = JSON.parse(t);
      } catch (e) {
        console.log(`WARN parse fail (${i}/${RETRY}) sample=`, t.slice(0, 120));
        await sleep(400 * i);
        continue;
      }
      if (!j || j.code !== "0" || !Array.isArray(j.data)) {
        console.log(`WARN non-zero code (${i}/${RETRY}) resp=`, j);
        await sleep(400 * i);
        continue;
      }
      return j.data; // OKX 数据为数组，时间倒序（近->远）
    } catch (err) {
      console.log(`WARN fetch error (${i}/${RETRY}):`, err.message);
      await sleep(400 * i);
    }
  }
  return [];
}

function toCsvRows(rows) {
  // rows: [ts, open, high, low, close, vol, ...]
  const header = "ts,iso,open,high,low,close,vol\n";
  const body = rows
    .map((d) => {
      const ts = Number(d[0]);
      const iso = new Date(ts).toISOString();
      return [ts, iso, d[1], d[2], d[3], d[4], d[5]].join(",");
    })
    .join("\n");
  return header + body + "\n";
}

async function main() {
  console.log(`Start fetching ${INST_ID} ${BAR} for last ${DAYS} days...`);
  let before = now;
  let pages = 0;
  const bag = [];
  const seen = new Set(); // 按 ts 去重

  while (true) {
    if (pages >= PAGE_GUARD) {
      console.log(`Guard stop: pages=${pages} >= ${PAGE_GUARD}`);
      break;
    }
    const page = await fetchPage(before);
    if (!page || page.length === 0) {
      console.log(`Empty page, stop. before=${before}`);
      break;
    }

    // 统计 + 去重
    let added = 0;
    for (const d of page) {
      const ts = Number(d[0]);
      if (!Number.isFinite(ts)) continue;
      if (!seen.has(ts)) {
        seen.add(ts);
        bag.push(d);
        added++;
      }
    }
    pages++;
    const last = page[page.length - 1];
    const lastTs = Number(last[0]);
    console.log(`page #${pages}: +${added} / ${page.length}, total=${bag.length}, lastTs=${lastTs} -> ${new Date(lastTs).toISOString()}`);

    // 下一轮 before：严格递减，减 1ms 防止重复
    const nextBefore = lastTs - 1;
    if (!(nextBefore < before)) {
      console.log(`Stop: nextBefore(${nextBefore}) !< before(${before})`);
      break;
    }
    before = nextBefore;

    if (lastTs < yearAgo) {
      console.log(`Reached >= ${DAYS} days. Stop.`);
      break;
    }
    await sleep(SLEEP_MS);
  }

  if (bag.length === 0) {
    console.log("❗No data fetched. Check Worker/OKX availability.");
  }

  // OKX 返回倒序，改为顺序（旧->新），并裁剪到一年窗口
  bag.sort((a, b) => Number(a[0]) - Number(b[0]));
  const clipped = bag.filter((d) => Number(d[0]) >= yearAgo);

  // 落盘
  const csv = toCsvRows(clipped);
  fs.writeFileSync(outFile, csv);
  console.log(`✅ Done. Saved rows=${clipped.length}, file=${outFile}`);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
