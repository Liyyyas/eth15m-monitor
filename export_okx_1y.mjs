// export_okx_1y.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

/* ===== 配置 ===== */
const PROXY_BASE = "https://eth-proxy.1053363050.workers.dev"; // 你的 Worker
const INST_ID = "ETH-USDT";
const BAR = "15m";
const DAYS = 365;
const PAGE_LIMIT = 300;                 // OKX 允许到 300，页数更少
const OUT_CSV = "okx_eth_15m.csv";
const FORCE_PROXY_FIRST = true;         // 直接走 Worker，减少直连空页
/* ================= */

const nowMs = Date.now();
const cutoffMs = nowMs - DAYS * 24 * 60 * 60 * 1000;
const UA = "Mozilla/5.0 Chrome/124 Safari/537.36";

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const looksNonJson = (s) => !s || s.trim().startsWith("<") || s.toLowerCase().includes("<html");

const okxUrl = ({ history, cursor }) => {
  const base = history
    ? "https://www.okx.com/api/v5/market/history-candles"
    : "https://www.okx.com/api/v5/market/candles";
  const p = new URLSearchParams({ instId: INST_ID, bar: BAR, limit: String(PAGE_LIMIT) });
  if (history && cursor) p.set("before", String(cursor));
  return `${base}?${p.toString()}`;
};

async function fetchText(url) {
  const resp = await fetch(url, { headers: { accept: "application/json", "user-agent": UA } });
  return await resp.text();
}

async function getPage({ history, cursor }) {
  const direct = okxUrl({ history, cursor });
  const proxy = `${PROXY_BASE}/?url=${encodeURIComponent(direct)}`;
  const routes = FORCE_PROXY_FIRST ? [proxy, direct] : [direct, proxy];

  for (const route of routes) {
    for (let i = 1; i <= 3; i++) {
      try {
        const txt = await fetchText(route);
        if (looksNonJson(txt)) throw new Error(`non-json: ${txt.slice(0,80)}`);
        const j = JSON.parse(txt);
        if (j?.code !== "0" || !Array.isArray(j?.data)) throw new Error(`bad payload: ${txt.slice(0,100)}`);
        return { arr: j.data, route, sample: txt.slice(0,120) };
      } catch (e) {
        await sleep(250 * i);
      }
    }
  }
  return { arr: [], route: "all_failed", sample: "" };
}

async function collectAll() {
  const bucket = [];

  // 第1页：最新
  {
    const { arr, route, sample } = await getPage({ history: false, cursor: undefined });
    console.log(`page 1 via /candles -> ${arr.length} rows, route=${route}`);
    if (!arr?.length) {
      console.log("Empty first page sample:", sample);
      return [];
    }
    bucket.push(...arr);
  }

  // 往前翻页：history-candles（游标 = 最老 ts - 1）
  let page = 1;
  while (true) {
    const last = bucket[bucket.length - 1];
    const oldestTs = Number(last?.[0]);
    if (!Number.isFinite(oldestTs) || oldestTs <= cutoffMs) break;

    const cursor = oldestTs - 1; // 关键：减1，避免重复导致空页
    page++;

    const { arr, route, sample } = await getPage({ history: true, cursor });
    console.log(`page ${page} via /history -> ${arr.length} rows, route=${route}, cursor=${cursor}`);
    if (!arr?.length) {
      console.log("Empty page sample:", sample);
      break;
    }
    bucket.push(...arr);

    if (page > 5000) break; // 保险
    await sleep(120);
  }

  // 去重+升序+截断
  const map = new Map();
  for (const r of bucket) {
    const ts = Number(r[0]);
    if (Number.isFinite(ts)) map.set(ts, r);
  }
  const rows = Array.from(map.values()).sort((a,b)=>Number(a[0])-Number(b[0]));
  return rows.filter(r => Number(r[0]) >= cutoffMs);
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
  const out = path.join(__dirname, OUT_CSV);
  fs.writeFileSync(out, content, "utf8");
  return out;
}

(async () => {
  console.log(`Fetching ${INST_ID} ${BAR} for last ${DAYS} days... (proxy-first)`);
  const rows = await collectAll();
  console.log(`Rows within ${DAYS}d: ${rows.length}`);
  if (rows.length < 1000) throw new Error("rows 太少，仍未翻页成功（看上面的每页日志）。");
  const out = writeCsv(rows);
  console.log("CSV written:", out);
})().catch(e => { console.error("Error:", e?.message || e); process.exit(1); });
