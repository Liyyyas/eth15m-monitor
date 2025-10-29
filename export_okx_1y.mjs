// export_okx_1y.mjs
// 拉取 ETH/USDT 15m 最近 365 天K线；优先直连 OKX，必要时回退到你的 Worker。
// 输出 okx_eth_15m.csv。Node 20+ (ESM)。

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

/* ===== 配置 ===== */
const PROXY_BASE = "https://eth-proxy.1053363050.workers.dev";
const INST_ID = "ETH-USDT";
const BAR = "15m";
const DAYS = 365;
const PAGE_LIMIT = 100; // 保守，用 100，稳定一些
const OUT_CSV = "okx_eth_15m.csv";
/* =============== */

const nowMs = Date.now();
const cutoffMs = nowMs - DAYS * 24 * 60 * 60 * 1000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const okxHistUrl = (before) =>
  "https://www.okx.com/api/v5/market/history-candles?" +
  new URLSearchParams({
    instId: INST_ID,
    bar: BAR,
    before: String(before),
    limit: String(PAGE_LIMIT),
  }).toString();

const UA =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36";

const looksNonJson = (s) => {
  if (!s) return true;
  const t = s.trim().slice(0, 64).toLowerCase();
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

async function getPage(before) {
  const directUrl = okxHistUrl(before);
  const proxyUrl = `${PROXY_BASE}/?url=${encodeURIComponent(directUrl)}`;

  // 1) 直连优先
  for (const route of [directUrl, proxyUrl]) {
    for (let i = 1; i <= 4; i++) {
      try {
        const txt = await fetchText(route);

        if (looksNonJson(txt)) throw new Error(`non-json: ${txt.slice(0, 120)}`);

        const data = JSON.parse(txt);
        if (data?.code !== "0" || !Array.isArray(data?.data)) {
          throw new Error(`bad payload: ${txt.slice(0, 120)}`);
        }
        return { arr: data.data, rawSample: txt.slice(0, 120), route };
      } catch (e) {
        await sleep(350 * i);
        if (i === 4) {
          // 切换到下条线路
          break;
        }
      }
    }
  }
  throw new Error("all routes failed");
}

async function collect() {
  const bucket = [];
  let before = nowMs;
  let pageNo = 0;

  while (true) {
    pageNo++;
    const { arr, rawSample, route } = await getPage(before);

    if (!arr || arr.length === 0) {
      console.log(
        `Empty page. route=${route}\nsample=${rawSample ?? "(empty)"}`
      );
      break;
    }

    bucket.push(...arr);

    const oldestTs = Number(arr[arr.length - 1][0]);
    if (!Number.isFinite(oldestTs)) break;
    before = oldestTs;

    if (oldestTs <= cutoffMs) break;
    if (pageNo > 1200) break; // 保险
    await sleep(120); // 轻微节流
  }

  // 去重+升序
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
    const [ , open, high, low, close, vol ] = r;
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
  console.log(`Fetching ${INST_ID} ${BAR} for last ${DAYS} days... (direct first)`);
  const rows = await collect();
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
