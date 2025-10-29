// export_okx_ly.mjs —— OKX ETH/USDT 15m 历史K线（约1年），自动代理回退/退避/去重/排序/CSV输出
import fs from 'fs/promises';

const INST_ID = 'ETH-USDT';
const BAR = '15m';
const PAGE_LIMIT = 300;          // OKX 单页上限
const EXPECT_MIN_ROWS = 35000;   // 15m*365 ≈ 35040
const MAX_PAGES = 1800;          // 冗余上限
const SLEEP_MS = 200;            // 基础退避
const RETRY = 4;                 // 同一页最多尝试（直连+代理）
const OUT = 'eth15m-monitor/okx_eth_15m.csv';

// 你的 Cloudflare Worker 代理
const PROXY_BASE = 'https://eth-proxy.1053363050.workers.dev';

// UA & 基本头
const UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';
const HEADERS = { 'User-Agent': UA, 'Accept': 'application/json,text/plain,*/*' };

// 路由构造
const okxRoute = (beforeTs) =>
  `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}` +
  (beforeTs ? `&before=${beforeTs}` : '');

const proxyRoute = (beforeTs) =>
  `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}` +
  (beforeTs ? `&before=${beforeTs}` : '');

// 粗判 HTML（被风控/验证码时 OKX 会回 HTML）
const maybeHTML = (txt) => txt && /^\s*<!DOCTYPE html/i.test(txt);

// 休眠
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// 先直连，失败自动走代理（包括：HTML、code!=0、数据不是数组、第一页空页）
async function fetchJsonWithFallback(urlDirect, urlProxy, { allowEmpty = false }) {
  // 尝试直连
  try {
    const r = await fetch(urlDirect, { headers: HEADERS });
    const t = await r.text();
    if (!r.ok || maybeHTML(t)) throw new Error('HTML_OR_HTTP_ERR');
    const j = JSON.parse(t);
    if (j.code !== '0' || !Array.isArray(j.data)) throw new Error(`BAD_JSON code=${j.code}`);
    if (!allowEmpty && j.data.length === 0) throw new Error('EMPTY_DIRECT');
    return j;
  } catch (e) {
    // 走代理
    const r2 = await fetch(urlProxy, { headers: HEADERS });
    const t2 = await r2.text();
    if (!r2.ok || maybeHTML(t2)) throw new Error('HTML_OR_HTTP_ERR_PROXY');
    const j2 = JSON.parse(t2);
    if (j2.code !== '0' || !Array.isArray(j2.data)) throw new Error(`BAD_JSON_PROXY code=${j2.code}`);
    return j2;
  }
}

// 主流程：从“现在”开始用 before= 向过去翻页，直到拿满一年或触顶
async function main() {
  console.log(`Run node -v`); 
  console.log(process.version);
  console.log(`Start fetching ${INST_ID} ${BAR} for last ~365 days...`);

  const now = Date.now();
  const oneYearAgo = now - 365 * 24 * 60 * 60 * 1000;

  let cursor = now;              // before=cursor，从“现在”向过去翻
  let page = 0;
  const seenTs = new Set();
  const all = [];

  while (page < MAX_PAGES) {
    page++;

    const direct = okxRoute(cursor);
    const viaProxy = proxyRoute(cursor);

    let json = null;
    let backoff = 0;

    // 页级重试（直连→代理，指数退避）
    for (let t = 1; t <= RETRY; t++) {
      try {
        // 第1页如果空也要强制走代理重试，所以 allowEmpty=false
        const allowEmpty = false;
        json = await fetchJsonWithFallback(direct, viaProxy, { allowEmpty });
        break;
      } catch (err) {
        backoff = SLEEP_MS * t * 5;
        await sleep(backoff);
        if (t === RETRY) throw err;
      }
    }

    const rows = json?.data ?? [];
    if (rows.length === 0) {
      console.log(`page ${page}: empty -> stopping`);
      break;
    }

    // OKX 返回通常按时间倒序（新→旧），用最后一条作为下一页 before 游标
    let took = 0;
    for (const k of rows) {
      // k: [ts, o, h, l, c, volCcy, volQty, volQuote, confirm, ...]
      if (!Array.isArray(k) || k.length < 5) continue;
      const ts = Number(k[0]);
      const open = Number(k[1]);
      const high = Number(k[2]);
      const low = Number(k[3]);
      const close = Number(k[4]);
      const confirm = k[8] ?? '1';

      // 丢弃未确认K线
      if (String(confirm) !== '1') continue;
      // 去重
      if (seenTs.has(ts)) continue;
      seenTs.add(ts);

      all.push({ ts, iso: new Date(ts).toISOString(), open, high, low, close, vol: k[6] ?? '' });
      took++;
    }

    // 更新 before 游标：取本页“最老”的一条时间（通常是最后一条），减1ms 防止重复
    const oldest = Number(rows[rows.length - 1]?.[0]);
    if (Number.isFinite(oldest)) cursor = oldest - 1;

    console.log(`page ${page}: got ${took} rows, cursor -> ${cursor}`);

    // 到达一年前
    if (cursor <= oneYearAgo) {
      console.log('Reached one year boundary.');
      break;
    }

    // 基础节流
    await sleep(SLEEP_MS);
  }

  // 过滤到一年前（含）之后的 1 年数据，并按时间升序
  const filtered = all.filter(r => r.ts >= oneYearAgo).sort((a, b) => a.ts - b.ts);
  console.log(`DONE fetch: unique rows = ${filtered.length}`);

  // 输出 CSV
  const header = 'ts,iso,open,high,low,close,vol\n';
  const body = filtered.map(r =>
    [r.ts, r.iso, r.open, r.high, r.low, r.close, r.vol].join(',')
  ).join('\n') + '\n';

  await fs.writeFile(OUT, header + body, 'utf8');
  console.log(`Wrote CSV to ./${OUT}`);

  if (filtered.length < EXPECT_MIN_ROWS) {
    console.log(`WARNING: fetched rows ${filtered.length} < expected ${EXPECT_MIN_ROWS}.`);
    process.exit(1);
  }
}

main().catch(e => {
  console.error('FATAL:', e?.message || e);
  process.exit(1);
});
