// export_okx_1y.mjs
// 拉取 OKX ETH-USDT 15m 最近 365 天历史K线，自动分页、防死循环、去重，支持 Cloudflare Worker 代理。
// 产出：项目根目录 okx_eth_15m.csv

import fs from "fs/promises";

const SYMBOL = "ETH-USDT";
const BAR = "15m";
const DAYS = 365;
const OUT_CSV = "okx_eth_15m.csv";

// ====== 你的代理（可换成直连）======
const PROXY = "https://eth-proxy.1053363050.workers.dev/"; // 末尾带 /
// ====================================

const DIRECT = "https://www.okx.com";
const BASE = PROXY || DIRECT;

const START_TS = Date.now() - DAYS * 24 * 60 * 60 * 1000; // 起点：365天前
const LIMIT = 300;            // OKX 单页最大 300
const MAX_PAGES = 2000;       // 双保险：最多翻 2000 页（按常识一年 15m ~ 35k 根，约 117 页）
const SLEEP_MS = 180;         // 分页小延时，降低风控概率
const RETRY = 5;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function httpGetJson(url) {
  for (let i = 1; i <= RETRY; i++) {
    try {
      const resp = await fetch(url, { headers: { "User-Agent": "eth1-exporter" } });
      const text = await resp.text();

      // 代理可能返回 HTML，直接挡掉
      if (text.startsWith("<")) throw new Error("HTML returned");

      const j = JSON.parse(text);
      if (j.code !== "0") throw new Error(`API code=${j.code}, msg=${j.msg}`);
      return j;
    } catch (e) {
      if (i === RETRY) throw e;
      await sleep(400 * i);
    }
  }
}

function buildUrl(beforeTs) {
  // OKX history-candles 文档：before/after 为毫秒时间戳
  // 这里用 before=上一页最早一根的ts-1，向更早翻页
  const u = new URL("/api/v5/market/history-candles", BASE);
  u.searchParams.set("instId", SYMBOL);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", String(LIMIT));
  if (beforeTs) u.searchParams.set("before", String(beforeTs));
  return u.toString();
}

function normalizeRow(arr) {
  // OKX 返回：[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
  const [ts, open, high, low, close, vol] = arr;
  return {
    ts: Number(ts),
    iso: new Date(Number(ts)).toISOString(),
    open: Number(open),
    high: Number(high),
    low: Number(low),
    close: Number(close),
    vol: Number(vol)
  };
}

function toCsv(rows) {
  const header = "ts,iso,open,high,low,close,vol";
  const body = rows.map(r =>
    [r.ts, r.iso, r.open, r.high, r.low, r.close, r.vol].join(",")
  );
  return [header, ...body].join("\n");
}

async function main() {
  console.log(`Fetching ${SYMBOL} ${BAR} for last ${DAYS} days...`);
  if (PROXY) console.log(`Proxy: ${PROXY}`);

  let before = undefined;
  let page = 0;
  let all = [];
  const seen = new Set();
  let lastMinTs = Infinity;

  while (page < MAX_PAGES) {
    page++;
    const url = buildUrl(before);
    console.log(`page ${page} -> ${url}`);

    const j = await httpGetJson(url);
    const data = j.data || [];
    if (data.length === 0) {
      console.log("empty page, break.");
      break;
    }

    // OKX 返回通常是按时间降序（最新在前），保险起见都放进来后再整体排序去重
    let batch = data.map(normalizeRow);

    // 去重
    const fresh = [];
    for (const r of batch) {
      if (!seen.has(r.ts)) {
        seen.add(r.ts);
        fresh.push(r);
      }
    }
    all.push(...fresh);

    // 更新游标：取这一页中**最早**的一根K线时间戳
    const minTs = Math.min(...batch.map(r => r.ts));

    // 防止游标不动导致死循环
    if (minTs >= lastMinTs) {
      console.log(`cursor not moving (minTs=${minTs}) → break`);
      break;
    }
    lastMinTs = minTs;

    // 如果已经翻到目标区间之前，就可以停止
    if (minTs <= START_TS) {
      console.log(`reached start (${new Date(START_TS).toISOString()})`);
      break;
    }

    // 下一页 before=这一页最早K线时间戳 - 1
    before = minTs - 1;

    await sleep(SLEEP_MS);
  }

  if (all.length === 0) {
    throw new Error("No rows fetched.");
  }

  // 过滤出最近 365 天
  all = all
    .filter(r => r.ts >= START_TS)
    .sort((a, b) => a.ts - b.ts); // 最终升序

  console.log(`rows within ${DAYS}d: ${all.length}`);
  if (all.length < 100) {
    throw new Error("rows < 100, 疑似抓取异常/被风控");
  }

  // 写 CSV
  const csv = toCsv(all);
  await fs.writeFile(OUT_CSV, csv, "utf8");
  console.log(`written: ${OUT_CSV}, size=${csv.length} chars`);
}

main().catch(err => {
  console.error("FATAL:", err.message || err);
  process.exit(1);
});
