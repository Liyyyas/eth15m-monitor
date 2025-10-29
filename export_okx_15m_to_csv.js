/**
 * 拉 OKX ETH/USDT 15m 一整年 → okx_eth_15m.csv
 * 先用 /candles 拉最新，再用 before 游标回溯；若为空自动切到 /history-candles 重试。
 * 少于 10 行则进程退出码 1，避免只提交表头。
 */
const fs = require("fs/promises");
const https = require("https");

const INST_ID = "ETH-USDT";
const BAR = "15m";
const BASE = "https://www.okx.com";
const END_MS = Date.now();
const START_MS = END_MS - 365 * 24 * 60 * 60 * 1000; // 最近 1 年
const LIMIT = 300;

const agent = new https.Agent({ keepAlive: true });

async function getJSON(url) {
  const headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.okx.com/",
  };
  const res = await fetch(url, { agent, headers });
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`Non-JSON response: ${text.slice(0, 200)}`);
  }
}

async function fetchPage(api, before) {
  const u = new URL(api, BASE);
  u.searchParams.set("instId", INST_ID);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", String(LIMIT));
  if (before) u.searchParams.set("before", String(before));

  // 线性重试
  for (let i = 0; i < 5; i++) {
    try {
      const j = await getJSON(u);
      if (j.code !== "0") throw new Error(j.msg || "OKX error");
      return j.data;
    } catch (err) {
      console.log(`[${api}] retry ${i + 1}/5: ${err.message}`);
      await new Promise(r => setTimeout(r, 800 * (i + 1)));
    }
  }
  throw new Error(`[${api}] failed after retries`);
}

async function fetchCandles(before) {
  // 先 /candles，再 /history-candles 兜底
  let data = await fetchPage("/api/v5/market/candles", before);
  if (!data || data.length === 0) {
    data = await fetchPage("/api/v5/market/history-candles", before);
  }
  return data;
}

(async () => {
  console.log("Start fetching ETH/USDT 15m for last 1y...");
  let cursor = END_MS;
  let page = 0;
  const rows = [];

  while (cursor > START_MS) {
    page++;
    console.log(`→ Page ${page}, before=${cursor}`);
    const data = await fetchCandles(cursor);

    if (!data || data.length === 0) {
      console.log("Empty page, stop.");
      break;
    }

    // OKX 返回倒序：最新在前
    for (const d of data) {
      const ts = Number(d[0]); // open time ms
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

    // 下一页游标（取本页最旧一根的 open time - 1ms）
    cursor = Number(data[data.length - 1][0]) - 1;

    // 轻微节流
    await new Promise(r => setTimeout(r, 200));
  }

  rows.sort((a, b) => a.ts - b.ts);

  if (rows.length < 10) {
    console.log("❌ rows < 10，疑似被风控或参数异常，不提交空文件。");
    process.exit(1);
  }

  const header = "ts,iso,open,high,low,close,vol\n";
  const body = rows.map(o => `${o.ts},${o.iso},${o.open},${o.high},${o.low},${o.close},${o.vol}`).join("\n");
  await fs.writeFile("okx_eth_15m.csv", header + body, "utf8");
  console.log(`✅ Done. saved ${rows.length} rows → okx_eth_15m.csv`);
})();
