// 一年 ETH/USDT 15m 历史K线抓取（含代理回退、防反爬、重试、分页）
// 输出到: eth15m-monitor/okx_eth_15m.csv
import fs from 'fs/promises';

// 基本配置
const INST_ID = 'ETH-USDT';
const BAR = '15m';
const PAGE_LIMIT = 300; // OKX单页上限
const EXPECT_MIN_ROWS = 30000; // 一年约35000根
const MAX_PAGES = 1500;
const RETRY = 4;
const SLEEP_MS = 400;
const OUT = 'eth15m-monitor/okx_eth_15m.csv';

// ⚠️ 使用你能正常访问的 Cloudflare Worker 地址
const PROXY_BASE = 'https://053363050.workers.dev';

// 请求头
const UA =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36';
const HEADERS = {
  'User-Agent': UA,
  'Accept': 'application/json,text/plain,*/*',
  'Referer': 'https://www.okx.com/',
  'Origin': 'https://www.okx.com',
  'Accept-Language': 'en-US,en;q=0.9',
};

// OKX原始接口（before往回翻页）
const okxRoute = (beforeTs) =>
  `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(
    INST_ID
  )}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}` +
  (beforeTs ? `&before=${beforeTs}` : '');

// 代理接口
const proxyRoute = (beforeTs) =>
  `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(
    INST_ID
  )}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}` +
  (beforeTs ? `&before=${beforeTs}` : '');

// 判断是否被反爬返回了HTML
const maybeHtml = (txt) => txt && /^\s*<!DOCTYPE html/i.test(txt);

// 延迟函数
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 先走代理，失败再走直连
async function fetchJsonWithFallback(urlProxy, urlDirect) {
  try {
    const r1 = await fetch(urlProxy, { headers: HEADERS });
    const t1 = await r1.text();
    if (maybeHtml(t1)) throw new Error('HTML_FROM_PROXY');
    return JSON.parse(t1);
  } catch (e) {
    const r2 = await fetch(urlDirect, { headers: HEADERS });
    const t2 = await r2.text();
    if (maybeHtml(t2)) throw new Error('HTML_FROM_OKX');
    return JSON.parse(t2);
  }
}

// 时间戳转ISO
const toISO = (ms) => new Date(Number(ms)).toISOString();

async function main() {
  console.log(`Node ${process.versions.node}`);
  console.log(`Start fetching ${INST_ID} ${BAR} for ~1 year...`);

  let beforeCursor = null; // 第一页不带before
  let page = 0;
  const rows = [];
  const seen = new Set();

  while (page < MAX_PAGES) {
    page++;
    const direct = okxRoute(beforeCursor);
    const viaProxy = proxyRoute(beforeCursor);
    if (page === 1) console.log('page 1 urls:', { direct, viaProxy });

    let data = null;
    let ok = false;

    for (let tr = 1; tr <= RETRY; tr++) {
      try {
        const j = await fetchJsonWithFallback(viaProxy, direct);
        if (j && j.code === '0' && Array.isArray(j.data)) {
          data = j.data;
          ok = true;
          break;
        }
        throw new Error(`BAD_JSON code=${j?.code}`);
      } catch (err) {
        const backoff = SLEEP_MS * tr * 2;
        console.log(`Retry ${tr}/${RETRY} for page ${page} (${backoff}ms)`);
        await sleep(backoff);
        if (tr === RETRY)
          console.error(`Page ${page} failed after ${RETRY} retries: ${err.message}`);
      }
    }

    if (!ok || !Array.isArray(data) || data.length === 0) {
      console.log(`page ${page} empty; stopping.`);
      break;
    }

    for (const c of data) {
      if (!c || c.length < 6) continue;
      const ts = Number(c[0]);
      const confirm = String(c[8] ?? '1');
      if (confirm !== '1' || seen.has(ts)) continue;
      seen.add(ts);

      rows.push({
        ts,
        iso: toISO(ts),
        open: Number(c[1]),
        high: Number(c[2]),
        low: Number(c[3]),
        close: Number(c[4]),
        vol: Number(c[5]),
      });
    }

    const last = data[data.length - 1];
    beforeCursor = Number(last[0]);
    console.log(`page ${page} done -> ${rows.length} total`);
    await sleep(200);
  }

  // 排序
  rows.sort((a, b) => a.ts - b.ts);
  console.log(`DONE fetch: total rows = ${rows.length}`);

  // 写入CSV
  const header = 'ts,iso,open,high,low,close,vol\n';
  const body = rows
    .map((r) => [r.ts, r.iso, r.open, r.high, r.low, r.close, r.vol].join(','))
    .join('\n');
  await fs.writeFile(OUT, header + body, 'utf8');
  console.log(`✅ Wrote CSV to ./${OUT}`);

  if (rows.length < EXPECT_MIN_ROWS) {
    console.error(`⚠️ WARNING: fetched rows ${rows.length} < expected ${EXPECT_MIN_ROWS}`);
    process.exit(1);
  }
}

main().catch((e) => {
  console.error('FATAL:', e);
  process.exit(1);
});
