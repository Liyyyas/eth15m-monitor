// 拉取 OKX ETH/USDT 15m 一整年K线，输出 okx_eth_15m.csv（覆盖写入）

const fs = require("fs/promises");

const INST_ID = "ETH-USDT";
const BAR = "15m";
const LIMIT = 300; // OKX 单次最大条数（300 足够稳定）
const BASE_URL = "https://www.okx.com/api/v5/market/history-candles";

// 目标时间范围：过去 365 天
const END_MS = Date.now();
const START_MS = END_MS - 365 * 24 * 60 * 60 * 1000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getBatch(beforeMs) {
  const u = new URL(BASE_URL);
  // history-candles 从最近往更早翻页，使用 before 游标倒序回溯
  u.searchParams.set("instId", INST_ID);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", String(LIMIT));
  if (beforeMs) u.searchParams.set("before", String(beforeMs));

  const res = await fetch(u, {headers: { "User-Agent": "gh-actions" }});
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  const json = await res.json();
  if (json.code !== "0") throw new Error(`OKX error: ${json.code} ${json.msg}`);

  // 返回的数据是二维数组，按「最新→最旧」排列
  // 每条形如：[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
  return json.data || [];
}

async function main() {
  console.log(`Exporting ${INST_ID} ${BAR} candles for last 1 year...`);
  console.log(
    `Range: ${new Date(START_MS).toISOString()} ~ ${new Date(END_MS).toISOString()}`
  );

  let cursor = END_MS;       // 从现在开始往回翻
  const items = [];
  let page = 0;

  while (cursor > START_MS) {
    page++;
    console.log(`Fetching page ${page} before=${cursor} ...`);
    let batch = [];
    // 简单重试机制
    for (let t = 0; t < 3; t++) {
      try {
        batch = await getBatch(cursor);
        break;
      } catch (e) {
        console.warn(`  fetch failed (${t + 1}/3): ${e.message}`);
        await sleep(800);
      }
    }
    if (!batch.length) {
      console.log("No more data, stop.");
      break;
    }

    // OKX 返回是「最新→最旧」，遍历时筛选在时间窗内的
    for (const row of batch) {
      const ts = Number(row[0]);
      if (ts >= START_MS && ts <= END_MS) {
        items.push({
          ts,
          iso: new Date(ts).toISOString(),
          open: row[1],
          high: row[2],
          low: row[3],
          close: row[4],
          vol: row[5],
        });
      }
    }

    // 下一页游标：取这一页里最旧K线时间 - 1ms 继续向更早翻
    const oldestTs = Number(batch[batch.length - 1][0]);
    if (oldestTs >= cursor) {
      // 保护：如果排序异常，防止死循环
      console.log("Pagination guard hit, stop.");
      break;
    }
    cursor = oldestTs - 1;

    // 轻微休眠，友好一些
    await sleep(150);
  }

  // 去重（同一ts以最后一次为准），并按时间正序
  const map = new Map();
  for (const it of items) map.set(it.ts, it);
  const dedup = Array.from(map.values()).sort((a, b) => a.ts - b.ts);

  // 生成CSV
  const header = "ts,iso,open,high,low,close,vol";
  const lines = dedup.map(
    (r) => `${r.ts},${r.iso},${r.open},${r.high},${r.low},${r.close},${r.vol}`
  );
  const csv = [header, ...lines].join("\n");

  await fs.writeFile("okx_eth_15m.csv", csv, "utf8");
  console.log(`Done. Rows: ${dedup.length}. Saved -> okx_eth_15m.csv`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
