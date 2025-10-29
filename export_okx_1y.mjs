// eth15m-monitor/export_okx_1y.mjs
// 抓取 OKX ETH-USDT 15m 近 365 天，经过 Cloudflare Worker 代理，自动分页+重试，稳定落盘 CSV

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const PROXY = "https://eth-proxy.1053363050.workers.dev"; // 你的 Worker
const INST_ID = "ETH-USDT";
const BAR = "15m";
const MAX_PER_PAGE = 300;           // OKX 单页上限
const PAUSE_MS = 120;               // 每页间隔，降低限流
const RETRY = 3;                    // 单页失败重试
const DAYS = 365;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 无论从哪个工作目录执行，都写到仓库根下的 eth15m-monitor/okx_eth_15m.csv
const repoRoot = path.resolve(__dirname, "..");
const outDir = path.join(repoRoot, "eth15m-monitor");
const outFile = path.join(outDir, "okx_eth_15m.csv");

// 确保目录存在
fs.mkdirSync(outDir, { recursive: true });

// 工具：sleep
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 时间区间（毫秒时间戳）
const now = Date.now();
const yearAgo = now - DAYS * 24 * 60 * 60 * 1000;

// 拉取一页
async function fetchPage(beforeTs) {
  const url =
    `${PROXY}/api/v5/market/history-candles?` +
    `instId=${encodeURIComponent(INST_ID)}` +
    `&bar=${encodeURIComponent(BAR)}` +
    `&limit=${MAX_PER_PAGE}` +
    (beforeTs ? `&before=${beforeTs}` : "");

  for (let i = 1; i <= RETRY; i++) {
    try {
      const resp = await fetch(url, { headers: { "accept": "application/json" } });
      const text = await resp.text();

      // 解析 JSON
      let json;
      try {
        json = JSON.parse(text);
      } catch (e) {
        console.log(`WARN parse fail (try ${i}/${RETRY}):`, text.slice(0, 120));
        await sleep(500 * i);
        continue;
      }

      if (!json || json.code !== "0") {
        console.log(`WARN non-zero code (try ${i}/${RETRY}):`, json);
        await sleep(500 * i);
        continue;
      }

      const data = json.data || [];
      return data;
    } catch (err) {
      console.log(`WARN fetch error (try ${i}/${RETRY}):`, err.message);
      await sleep(500 * i);
    }
  }
  return []; // 重试失败返回空
}

async function main() {
  console.log(`Start fetching ${INST_ID} ${BAR} for last ${DAYS} days...`);
  let before = now;      // 从现在开始往前翻页
  let total = 0;
  const all = [];

  while (true) {
    const page = await fetchPage(before);

    if (!page || page.length === 0) {
      console.log(`Empty page, stop paging. before=${before}`);
      break;
    }

    // 累加
    all.push(...page);
    total += page.length;

    // 下一页的 before（使用当前页最后一条K线的时间戳）
    const last = page[page.length - 1];
    const lastTs = Number(last[0]);
    before = lastTs;

    console.log(`+${page.length} rows, total=${total}, lastTs=${lastTs} (${new Date(lastTs).toISOString()})`);

    // 到达一年之前就停
    if (lastTs < yearAgo) {
      console.log(`Reached >= ${DAYS} days. Stop.`);
      break;
    }

    await sleep(PAUSE_MS);
  }

  if (all.length === 0) {
    console.log("❗ No data fetched. Check Worker 或 API 可用性/限流。");
  }

  // OKX 返回时间倒序（近→远），写入前倒序成从旧到新更直观
  all.reverse();

  // 仅保留一年范围内（避免多翻几页溢出）
  const filtered = all.filter((d) => Number(d[0]) >= yearAgo);

  // 写 CSV
  const header = "ts,iso,open,high,low,close,vol\n";
  const body = filtered
    .map((d) => {
      const ts = d[0];
      const iso = new Date(Number(ts)).toISOString();
      return [ts, iso, d[1], d[2], d[3], d[4], d[5]].join(",");
    })
    .join("\n");

  fs.writeFileSync(outFile, header + body);
  console.log(`✅ Done. Saved ${filtered.length} rows -> ${outFile}`);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
