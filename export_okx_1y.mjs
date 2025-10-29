// export_okx_1y.mjs —— 从 Cloudflare Worker 拉取 ETH-USDT 15m 一年历史K线
import fs from "fs";
import path from "path";

const PROXY = "https://eth-proxy.1053363050.workers.dev"; // 你的 Worker
const INST_ID = "ETH-USDT";
const BAR = "15m";
const LIMIT = 300;
const SLEEP_MS = 150;
const RETRY = 3;
const DAYS = 365;
const PAGE_GUARD = 200;

const now = Date.now();
const yearAgo = now - DAYS * 24 * 60 * 60 * 1000;
const outDir = "eth15m-monitor";
const outFile = path.join(outDir, "okx_eth_15m.csv");
fs.mkdirSync(outDir, { recursive: true });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function buildUrl(beforeTs) {
  const u = new URL(PROXY + "/api/v5/market/history-candles");
  u.searchParams.set("instId", INST_ID);
  u.searchParams.set("bar", BAR);
  u.searchParams.set("limit", LIMIT);
  if (beforeTs) u.searchParams.set("before", beforeTs);
  return u.toString();
}

async function fetchPage(beforeTs) {
  const url = buildUrl(beforeTs);
  for (let i = 1; i <= RETRY; i++) {
    try {
      const r = await fetch(url);
      const t = await r.text();
      let j;
      try {
        j = JSON.parse(t);
      } catch {
        console.log(`⚠️ Parse fail (${i}/${RETRY})`);
        await sleep(400 * i);
        continue;
      }
      if (!j || j.code !== "0" || !Array.isArray(j.data)) {
        console.log(`⚠️ Non-zero code (${i}/${RETRY})`);
        await sleep(400 * i);
        continue;
      }
      return j.data;
    } catch (err) {
      console.log(`⚠️ Fetch error (${i}/${RETRY}):`, err.message);
      await sleep(400 * i);
    }
  }
  return [];
}

function toCsvRows(rows) {
  const header = "ts,iso,open,high,low,close,vol\n";
  const body = rows
    .map((d) => {
      const ts = +d[0];
      const iso = new Date(ts).toISOString();
      return [ts, iso, d[1], d[2], d[3], d[4], d[5]].join(",");
    })
    .join("\n");
  return header + body + "\n";
}

async function main() {
  console.log(`📊 Fetching ${INST_ID} ${BAR} for last ${DAYS} days...`);
  let before = now;
  let pages = 0;
  const bag = [];
  const seen = new Set();

  while (true) {
    if (pages >= PAGE_GUARD) {
      console.log(`🛑 Guard stop at ${PAGE_GUARD} pages`);
      break;
    }
    const page = await fetchPage(before);
    if (!page?.length) {
      console.log(`🛑 Empty page stop. before=${before}`);
      break;
    }
    let added = 0;
    for (const d of page) {
      const ts = +d[0];
      if (!seen.has(ts)) {
        seen.add(ts);
        bag.push(d);
        added++;
      }
    }
    const lastTs = +page[page.length - 1][0];
    pages++;
    console.log(`✅ page ${pages}: +${added} (${bag.length} total) → ${new Date(lastTs).toISOString()}`);

    if (lastTs < yearAgo) break;
    before = lastTs - 1;
    await sleep(SLEEP_MS);
  }

  bag.sort((a, b) => +a[0] - +b[0]);
  const filtered = bag.filter((d) => +d[0] >= yearAgo);
  fs.writeFileSync(outFile, toCsvRows(filtered));
  console.log(`✅ Done. Saved ${filtered.length} rows → ${outFile}`);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
