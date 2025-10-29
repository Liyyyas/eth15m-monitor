/**
 * 拉取 OKX ETH/USDT 15m 一整年数据，生成 okx_eth_15m.csv
 * 自动分页、重试、错误保护。
 */
const fs = require("fs/promises");
const https = require("https");

const INST_ID = "ETH-USDT";
const BAR = "15m";
const BASE_URL = "https://www.okx.com";
const END_MS = Date.now();
const START_MS = END_MS - 365 * 24 * 60 * 60 * 1000; // 1年
const LIMIT = 300;

const agent = new https.Agent({ keepAlive: true });

async function fetchData(before = null) {
  const url = new URL("/api/v5/market/history-candles", BASE_URL);
  url.searchParams.set("instId", INST_ID);
  url.searchParams.set("bar", BAR);
  url.searchParams.set("limit", String(LIMIT));
  if (before) url.searchParams.set("before", String(before));

  for (let i = 0; i < 5; i++) {
    try {
      const res = await fetch(url, { agent, headers: { "User-Agent": "Mozilla/5.0" } });
      const json = await res.json();
      if (json.code !== "0") throw new Error(json.msg || "OKX Error");
      return json.data;
    } catch (e) {
      console.log(`Retry ${i + 1}/5: ${e.message}`);
      await new Promise((r) => setTimeout(r, 1000 * (i + 1)));
    }
  }
  throw new Error("Failed after retries");
}

(async () => {
  console.log("Fetching ETH/USDT 15m candles for last 1 year...");
  let cursor = END_MS;
  let all = [];
  let page = 0;

  while (cursor > START_MS) {
    page++;
    console.log(`→ Page ${page}, before=${cursor}`);
    const data = await fetchData(cursor);
    if (!data || data.length === 0) {
      console.log("No more data, break.");
      break;
    }

    for (const d of data) {
      const ts = Number(d[0]);
      if (ts < START_MS) continue;
      all.push({
        ts,
        iso: new Date(ts).toISOString(),
        open: d[1],
        high: d[2],
        low: d[3],
        close: d[4],
        vol: d[5],
      });
    }

    cursor = Number(data[data.length - 1][0]) - 1;
    await new Promise((r) => setTimeout(r, 200));
  }

  all = all.sort((a, b) => a.ts - b.ts);
  const header = "ts,iso,open,high,low,close,vol\n";
  const csv = header + all.map(o => `${o.ts},${o.iso},${o.open},${o.high},${o.low},${o.close},${o.vol}`).join("\n");
  await fs.writeFile("okx_eth_15m.csv", csv, "utf8");
  console.log(`✅ Done: ${all.length} rows saved to okx_eth_15m.csv`);
})();
