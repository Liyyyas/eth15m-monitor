// export_okx_1y.mjs
// 目标：抓取 ETH/USDT 15m 最近 365 天K线；第1页走 /market/candles，往前翻用 /market/history-candles
// 若直连空/异常则自动切到 Cloudflare Worker。
// 产物：okx_eth_15m.csv（ts, iso, open, high, low, close, vol）
// 运行环境：Node 20+（ESM）

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

/* ===== 你的配置 ===== */
const PROXY_BASE = "https://eth-proxy.1053363050.workers.dev"; // 你的 Worker
const INST_ID = "ETH-USDT";
const BAR = "15m";
const DAYS = 365;
const PAGE_LIMIT = 100;             // 建议 100，稳
const OUT_CSV = "okx_eth_15m.csv";
/* ==================== */

const nowMs = Date.now();
const cutoffMs = nowMs - DAYS * 24 * 60 * 60 * 1000;
const UA =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 生成 OKX URL */
const okxUrl = ({ history, before }) => {
  const base = history
    ? "https://www.okx.com/api/v5/market/history-candles"
    : "https://www.okx.com/api/v5/market/candles";
  const p = new URLSearchParams({
    instId: INST_ID,
    bar: BAR,
    limit: String(PAGE_LIMIT),
  });
  if (history && before) p.set("before", String(before));
  return `${base}?${p.toString()}`;
};

const looksNonJson = (s) => {
  if (!s) return true;
  const t = s.trim().slice(0, 80).toLowerCase();
  return t.startsWith("<") || t.startsWith("okx proxy") || t.includes("<html");
};

async function fetchText(url) {
  const resp = await fetch(url, {
    headers: {
      accept: "application/json, text/plain;q=0.5, */*;q=0.1",
      "user-agent": UA,
    },
  });
  return await resp.text();
}

/** 拉一页（先直连，再 Worker 回退） */
async function getPage({ history, before }) {
  const direct = okxUrl({ history, before });
  const proxy = `${PROXY_BASE}/?url=${encodeURIComponent(direct)}`;

  for (const route of [direct, proxy]) {
    for (let i = 1; i <= 4; i++) {
      try {
        const txt = await fetchText(route);
        if (looksNonJson(txt)) throw new Error(`non-json: ${txt.slice(0, 120)}`);
        const data = JSON.parse(txt);
        if (data?.code !== "0" || !Array.isArray(data?.data)) {
          throw new Error(`bad payload: ${txt.slice(0, 120)}`);
        }
        return { arr: data.data, route, sample: txt.slice(0, 120) };
      } catch (e) {
        await sleep(300 * i);
        if (i === 4) break; // 切换线路
      }
    }
  }
  throw new Error("all routes failed");
}

async function collectAll() {
  const bucket = [];

  // 第 1 页：最新
  {
    const { arr, route, sample } = await getPage({ history: false, before: undefined });
    if (!arr || arr.length === 0) {
      console.log(`Empty first page from /market/candles. route=${route}\nsample=${sample}`);
      return [];
    }
    bucket.push(...arr);
  }

  // 往前翻页：history-candles
  let page = 1;
  while (true) {
    const last = bucket[bucket.length - 1];
    const oldestTs = Number(last?.[0]); // ts 在 index 0
    if (!Number.isFinite(oldestTs)) break;
    if (oldestTs <= cutoffMs) break;

    page++;
    const { arr, route, sample } = await getPage({ history: true, before: oldestTs });
    if (!arr || arr.length === 0) {
      console.log(
        `Empty page. route=${route}\nsample=${sample ?? "(empty)"}`
      );
      break;
    }
    bucket.push(...arr);

    if (page > 2000) break; // 保险
    await sleep(120);
  }

  // 去重 + 升序 + 截到 cutoff
  const map = new Map();
  for (const r of bucket) {
    const ts = Number(r[0]);
    if (Number.isFinite(ts)) map.set(ts, r);
  }
  const rows = Array.from(map.values()).sort((a, b) => Number(a[0]) - Number(b[0]));
  return rows.filter((r) => Number(r[0]) >= cutoffMs);
}

function writeCsv(rows) {
  const header = "ts,iso,open,high,low,close,vol";
  const lines = [header];
  for (const r of rows) {
    const ts = Number(r[0]);
    const iso = new Date(ts).toISOString();
    const [, open, high, low, close, vol] = r;
    lines.push([ts, iso, open, high, low, close, vol].join(","));
  }
  const content = lines.join("\n") + "\n";

  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const outPath = path.join(__dirname, OUT_CSV);
  fs.writeFileSync(outPath, content, "utf8");
  return outPath;
}

(async () => {
  console.log(`Fetching ${INST_ID} ${BAR} for last ${DAYS} days... (/market/candles → /market/history-candles)`);
  const rows = await collectAll();
  console.log(`Rows within ${DAYS}d: ${rows.length}`);
  if (rows.length < 10) {
    throw new Error("rows < 10，疑似仍为空页/风控；上面日志已输出 sample。");
  }
  const out = writeCsv(rows);
  console.log("CSV written:", out);
})().catch((e) => {
  console.error("Error:", e?.message || e);
  process.exit(1);
});
