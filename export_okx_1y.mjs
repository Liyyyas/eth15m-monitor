// 一年 ETH/USDT 15m 历史K线抓取（带直连/代理回退、反爬检测、重试、行数校验）
// 输出: eth15m-monitor/okx_eth_15m.csv
import fs from 'fs/promises';

const INST_ID   = 'ETH-USDT';
const BAR       = '15m';
const PAGE_LIMIT = 300;     // OKX 单页上限
const EXPECT_MIN_ROWS = 30000;  // 15m*365 ≈ 35040
const MAX_PAGES = 1500;     // 安全上限
const SLEEP_MS  = 400;      // 基础等待
const RETRY     = 4;        // 每页最大重试

const OUT = 'eth15m-monitor/okx_eth_15m.csv';

// 你的 Cloudflare Worker 代理（纯前缀，不带 /api/...）
const PROXY_BASE = 'https://eth-proxy.1053363050.workers.dev';

// UA 与请求头
const UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36';
const HEADERS = { 'User-Agent': UA, 'Accept': 'application/json,text/plain,*/*' };

// OKX 直连路由（使用 before 游标，往回翻页）
const okxRoute = (beforeTs) =>
  `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}` +
  (beforeTs ? `&before=${beforeTs}` : '');

// 代理路由（把 OKX 的 path 拼到 Worker 后面）
const proxyRoute = (beforeTs) =>
  `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}` +
  (beforeTs ? `&before=${beforeTs}` : '');

// 判定是否被反爬返回了 HTML
const maybeHtml = (txt) => txt && /^\s*<!DOCTYPE html/i.test(txt);

// 简单 sleep
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// 先直连，失败再走代理
async function fetchJsonWithFallback(urlDirect, urlProxy) {
  try {
    const r1 = await fetch(urlDirect, { headers: HEADERS });
    const t1 = await r1.text();
    if (maybeHtml(t1)) throw new Error('HTML_FROM_OKX');
    return JSON.parse(t1);
  } catch (e) {
    const r2 = await fetch(urlProxy, { headers: HEADERS });
    const t2 = await r2.text();
    if (maybeHtml(t2)) throw new Error('HTML_FROM_PROXY');
    return JSON.parse(t2);
  }
}

// 时间戳(ms)转ISO
const toISO = (ms) => new Date(Number(ms)).toISOString();

async function main() {
  console.log(process.versions.node);
  console.log(`Start fetching ${INST_ID} ${BAR} for last ~365 days...`);

  const nowMs = Date.now();
  let beforeCursor = nowMs;   // 关键点：用 before 往回翻页
  let page = 0;

  const rows = [];
  const seen = new Set();

  while (page < MAX_PAGES) {
    page++;
    const direct = okxRoute(beforeCursor);
    const viaProxy = proxyRoute(beforeCursor);

    let data = null;
    let ok = false;

    for (let tr = 1; tr <= RETRY; tr++) {
      try {
        const j = await fetchJsonWithFallback(direct, viaProxy);
        if (j && j.code === '0' && Array.isArray(j.data)) {
          data = j.data;
          ok = true;
          break;
        }
        throw new Error(`BAD_JSON code=${j?.code}`);
      } catch (err) {
        // 429/HTML等，退避加长
        const backoff = SLEEP_MS * tr * 2;
        await sleep(backoff);
        if (tr === RETRY) {
          console.error(`Page ${page} failed after ${RETRY} retries: ${err.message}`);
          // 继续下一轮，以免整个任务卡死
        }
      }
    }

    if (!ok || !Array.isArray(data) || data.length === 0) {
      console.log(`page ${page} empty; stopping.`);
      break;
    }

    // OKX 返回是从新到旧；逐条处理
    for (const c of data) {
      // c: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
      if (!c || c.length < 7) continue;
      const ts = Number(c[0]);     // 毫秒
      const confirm = String(c[8] ?? '1'); // 有些返回在[8]，没有就默认1

      // 跳过未确认K线
      if (confirm !== '1') continue;

      if (seen.has(ts)) continue;
      seen.add(ts);

      rows.push({
        ts,
        iso: toISO(ts),
        open:  Number(c[1]),
        high:  Number(c[2]),
        low:   Number(c[3]),
        close: Number(c[4]),
        vol:   Number(c[5]),
      });
    }

    // 下一页游标：取本页里“最旧”的一根的时间戳（即数组最后一个元素）
    const last = data[data.length - 1];
    beforeCursor = Number(last[0]);

    // 小歇避免节流
    await sleep(200);
  }

  // 去重 & 排序（按时间升序）
  rows.sort((a, b) => a.ts - b.ts);

  console.log(`DONE fetch: total rows = ${rows.length}`);

  // 输出 CSV
  const header = 'ts,iso,open,high,low,close,vol\n';
  const body = rows.map(r => [r.ts, r.iso, r.open, r.high, r.low, r.close, r.vol].join(',')).join('\n');
  await fs.writeFile(OUT, header + body, 'utf8');
  console.log(`Wrote CSV to ./${OUT}`);

  if (rows.length < EXPECT_MIN_ROWS) {
    console.error(`WARNING: fetched rows ${rows.length} < expected ${EXPECT_MIN_ROWS}.`);
    process.exit(1);
  }
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
