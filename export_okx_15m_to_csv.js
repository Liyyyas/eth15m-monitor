/**
 * OKX ETH/USDT 15m 一年数据导出脚本（支持 GitHub Action 直连）
 * 使用 cors.sh 公共代理防止 instId 丢失。
 */
const fs = require("fs/promises");
const https = require("https");

const INST_ID = "ETH-USDT";
const BAR = "15m";
const OKX_API = "https://www.okx.com";
const PROXY = "https://proxy.cors.sh"; // 稳定转发
const END_MS = Date.now();
const START_MS = END_MS - 365 * 24 * 60 * 60 * 1000; // 一年
const LIMIT = 300;
const agent = new https.Agent({ keepAlive: true });

async function fetchData(before) {
  const url = new URL(`${PROXY}${OKX_API}/api/v5/market/history-candles`);
  url.searchParams.set("instId", INST_ID);
  url.searchParams.set("bar", BAR);
  url.searchParams.set("limit", LIMIT);
  if (before) url.searchParams.set("before", String(before));

  for (let i = 0; i < 5; i++) {
    try {
      const res = await fetch(url, {
        agent,
        headers: {
          "x-cors-api-key": "temp_dbee97c80a6a0a8f9b1d2a2a0d97f60f", // 公共密钥
          "User-Agent": "Mozilla/5.0",
          "Accept": "application/json",
        },
      });
      const txt = await res.text();
      const json = JSON.parse(txt);
      if (json.code !== "0") throw new Error(json.msg || txt);
      return json.data;
    } catch (err) {
      console.log(`Retry ${i + 1}/5: ${err.message}`);
      await new Promise(r => setTimeout(r, 1000 * (i + 1)));
    }
  }
  return [];
}

(async () => {
  console.log("Fetching ETH/USDT 15m candles for 1 year via cors.sh proxy...");
  let cursor = END_MS;
  let page = 0;
  const all = [];

  while (cursor > START_MS) {
    page++;
    console.log(`→ Page ${page}, before=${cursor}`);
    const data = await fetchData(cursor);
    if (!data || data.length === 0) {
      console.log("No data, break.");
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
    await new Promise(r => setTimeout(r, 250));
  }

  all.sort((a, b) => a.ts - b.ts);

  if (all.length < 10) {
    console.log("❌ rows < 10，仍被封锁或返回空数据。");
    process.exit(1);
  }

  const csv =
    "ts,iso,open,high,low,close,vol\n" +
    all.map(o => `${o.ts},${o.iso},${o.open},${o.high},${o.low},${o.close},${o.vol}`).join("\n");
  await fs.writeFile("okx_eth_15m.csv", csv, "utf8");
  console.log(`✅ Done: ${all.length} rows saved.`);
})();
