// export_okx_1y.mjs
// Node 20+ (GitHub Actions 默认)；通过 Cloudflare Worker 代理拉 OKX K线，抓满近 1 年（15m）

import fs from "node:fs/promises";

const WORKER_URL =
  // ① 优先用环境变量（你也可以在 workflow 里设置 WORKER_URL）
  process.env.WORKER_URL
  // ② 或直接把下面这行替换成你的 workers.dev 地址（保留 /okx 路径）
  || "https://eth-proxy.1053363050.workers.dev/okx";

const INST_ID = "ETH-USDT";
const BAR = "15m";
const LIMIT = 300;                 // OKX 单页上限
const MAX_PAGES = 500;             // 安全兜底，最多翻 500 页（≈ 150k 根）
const NOW = Date.now();
const ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;
const CUTOFF = NOW - ONE_YEAR_MS;  // 抓到这一时刻（含）为止
const OUT_CSV = "okx_eth_15m.csv";

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchPage(beforeTs) {
  // 你的 Worker 会把这个请求转发到 OKX：/api/v5/market/candles
  // 并返回 OKX 原样的 JSON：{ code:"0", msg:"", data:[[ts, o, h, l, c, vol, ...], ...] }
  const u = new URL(WORKER_URL);
  u.searchParams.set("instId", INST_ID);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", String(LIMIT));
  if (beforeTs) u.searchParams.set("before", String(beforeTs));

  for (let attempt = 1; attempt <= 5; attempt++) {
    const resp = await fetch(u, { headers: { "accept": "application/json" } });
    const text = await resp.text();
    // Worker 可能返回 text/plain；先尝试 JSON 解析
    try {
      const json = JSON.parse(text);
      if (json.code !== "0") {
        throw new Error(`OKX code ${json.code} msg ${json.msg || ""}`);
      }
      const arr = Array.isArray(json.data) ? json.data : [];
      // 返回格式：每行是数组：[ts, open, high, low, close, vol, ...]，ts 为毫秒
      return arr;
    } catch (e) {
      if (attempt === 5) throw new Error(`Parse/Fetch failed: ${e.message}, raw=${text.slice(0, 200)}`);
      await sleep(500 * attempt);
    }
  }
  return [];
}

function toRow(line) {
  // 只取我们要的 6 列：ts, iso, open, high, low, close, vol
  const ts = Number(line[0]);
  const iso = new Date(ts).toISOString();
  const open = Number(line[1]);
  const high = Number(line[2]);
  const low  = Number(line[3]);
  const close= Number(line[4]);
  const vol  = Number(line[5]); // OKX 返回的是 baseCcy 成交量
  return { ts, iso, open, high, low, close, vol };
}

(async () => {
  console.log(`Fetching ${INST_ID} ${BAR} for ~1y via Worker: ${WORKER_URL}`);
  let before = NOW + 60_000; // 从“现在之后”开始，保证拿到最新一页
  const map = new Map();     // ts -> row 去重
  let pages = 0;
  let oldestTs = Infinity;

  while (pages < MAX_PAGES) {
    pages++;
    console.log(`→ Page ${pages}, before=${before}`);
    const data = await fetchPage(before);

    if (!data.length) {
      console.log("Empty page, stop.");
      break;
    }

    // OKX 返回是按时间**倒序**（最新在前）
    for (const line of data) {
      const row = toRow(line);
      if (!Number.isFinite(row.ts)) continue;
      // 只保留 >= CUTOFF 的数据；再老就不需要了
      if (row.ts < CUTOFF) continue;
      if (!map.has(row.ts)) map.set(row.ts, row);
      if (row.ts < oldestTs) oldestTs = row.ts;
    }

    // 下一轮往更早翻页：把 before=本页最后一条（最老）的 ts
    const last = data[data.length - 1];
    const lastTs = Number(last?.[0]);
    if (!Number.isFinite(lastTs)) {
      console.log("No valid lastTs, stop.");
      break;
    }
    before = lastTs;

    // 够老了就停
    if (lastTs <= CUTOFF) {
      console.log("Reached cutoff, stop.");
      break;
    }

    // 轻微限速，避免 429
    await sleep(200);
  }

  // 收尾：排序、落盘
  const rows = Array.from(map.values()).sort((a, b) => a.ts - b.ts);

  if (rows.length < 10000) {
    console.warn(`Warning: rows=${rows.length}，明显少于一整年（理论≈35k）。可能是代理/API 限制，或未翻到更早数据。`);
  }

  const header = "ts,iso,open,high,low,close,vol";
  const body = rows.map(r =>
    [r.ts, r.iso, r.open, r.high, r.low, r.close, r.vol].join(",")
  ).join("\n");
  const csv = `${header}\n${body}\n`;

  await fs.writeFile(OUT_CSV, csv, "utf8");
  const spanDays = (rows.at(-1)?.ts - rows[0]?.ts) / 86400000;
  console.log(`Done: ${OUT_CSV}, rows=${rows.length}, span≈${spanDays?.toFixed(1)} days`);
})().catch(e => {
  console.error(e);
  process.exit(1);
});
