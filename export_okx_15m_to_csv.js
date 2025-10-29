/**
 * 拉取 ETH/USDT 15m 一年数据，经镜像中转接口，生成 okx_eth_15m.csv
 * 适用于 GitHub Actions 被 OKX 屏蔽的情况
 */
const fs = require("fs/promises");
const https = require("https");

const INST_ID = "ETH-USDT";
const BAR = "15m";
const MIRROR = "https://api.allorigins.win/raw?url=" + encodeURIComponent("https://www.okx.com");
const END_MS = Date.now();
const START_MS = END_MS - 365 * 24 * 60 * 60 * 1000;
const LIMIT = 300;
const agent = new https.Agent({ keepAlive: true });

async function fetchViaMirror(path, before) {
  const url = new URL(MIRROR + path);
  url.searchParams.set("instId", INST_ID);
  url.searchParams.set("bar", BAR);
  url.searchParams.set("limit", String(LIMIT));
  if (before) url.searchParams.set("before", String(before));

  for (let i = 0; i < 5; i++) {
    try {
      const res = await fetch(url, { agent });
      const text = await res.text();
      const json = JSON.parse(text);
      if (json.code !== "0") throw new Error(json.msg);
      return json.data;
    } catch (err) {
      console.log(`Retry ${i + 1}/5: ${err.message}`);
      await new Promise(r => setTimeout(r, 1000 * (i + 1)));
    }
  }
  return [];
}

(async () => {
  console.log("Fetching ETH/USDT 15m via OKX mirror...");
  let cursor = END_MS;
  let page = 0;
  const rows = [];

  while (cursor > START_MS) {
    page++;
    console.log(`→ Page ${page}, before=${cursor}`);
    const data = await fetchViaMirror("/api/v5/market/history-candles", cursor);
    if (!data || data.length === 0) {
      console.log("Empty, stop.");
      break;
    }

    for (const d of data) {
      const ts = Number(d[0]);
      if (ts < START_MS) continue;
      rows.push({
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
    await new Promise(r => setTimeout(r, 300));
  }

  rows.sort((a, b) => a.ts - b.ts);
  if (rows.length < 10) {
    console.log("❌ rows < 10，疑似风控仍未绕过。");
    process.exit(1);
  }

  const header = "ts,iso,open,high,low,close,vol\n";
  const body = rows.map(o => `${o.ts},${o.iso},${o.open},${o.high},${o.low},${o.close},${o.vol}`).join("\n");
  await fs.writeFile("okx_eth_15m.csv", header + body, "utf8");
  console.log(`✅ Done: ${rows.length} rows saved.`);
})();
