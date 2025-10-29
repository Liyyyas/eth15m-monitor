// export_okx_1y.mjs — 完整抓取过去365天ETH-USDT 15mK线 (支持多页递归)
// 使用 Cloudflare Worker 代理，自动分页直至一年数据完整

import fs from "fs";

const proxy = "https://eth-proxy.1053363050.workers.dev";
const instId = "ETH-USDT";
const bar = "15m";
const outfile = "./eth15m-monitor/okx_eth_15m.csv";

let now = Date.now();
const yearAgo = now - 365 * 24 * 60 * 60 * 1000;
let before = now;
let all = [];

console.log(`Start fetching ${instId} ${bar} for 1 year...`);

while (true) {
  const url = `${proxy}/api/v5/market/history-candles?instId=${instId}&bar=${bar}&limit=300&before=${before}`;
  const resp = await fetch(url);
  const text = await resp.text();

  try {
    const json = JSON.parse(text);
    if (!json.data || json.data.length === 0) break;

    all.push(...json.data);
    const last = json.data[json.data.length - 1];
    before = last[0]; // timestamp for next page

    // 输出进度
    console.log(
      `Fetched ${all.length} rows, last ts=${before}, continue...`
    );

    // 如果超出一年，停止
    if (before < yearAgo) break;
  } catch (e) {
    console.error("Error parsing response:", text.slice(0, 200));
    break;
  }

  // 避免被限流
  await new Promise((r) => setTimeout(r, 100));
}

console.log(`✅ Done! Total rows: ${all.length}`);

// 写入CSV
const header = "ts,iso,open,high,low,close,vol\n";
const rows = all
  .map((d) => {
    const ts = d[0];
    const iso = new Date(Number(ts)).toISOString();
    return [ts, iso, d[1], d[2], d[3], d[4], d[5]].join(",");
  })
  .join("\n");

fs.writeFileSync(outfile, header + rows);
console.log(`Saved to ${outfile}`);
