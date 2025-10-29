// export_okx_1y.mjs  —— 一年ETH/USDT 15m历史K线导出（带分页/反爬处理/进度提示）
import fs from 'fs/promises';

const INST_ID = 'ETH-USDT';
const BAR = '15m';
const PAGE_LIMIT = 300;              // OKX单页上限
const EXPECT_MIN_ROWS = 30000;       // 低于这个认为不合格（理论值~35040）
const MAX_PAGES = 1500;              // 安全上限（> 一年所需页数）
const SLEEP_MS = 120;                // 基础节流
const RETRY = 4;                     // 每页最大重试
const OUT = 'eth15m-monitor/okx_eth_15m.csv';

// 你的 Worker 代理（必须 https）
const PROXY_BASE = 'https://eth-proxy.1053363050.workers.dev';

const UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari';
const HEADERS = { 'User-Agent': UA, 'Accept': 'application/json,text/plain,*/*', 'Referer': 'https://www.okx.com' };

const okxRoute = (beforeTs) =>
  `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}&before=${beforeTs}`;

const proxyRoute = (beforeTs) =>
  `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}&before=${beforeTs}`;

// 判断是不是 OKX 返回了 HTML（被风控/跳验证码页）
const maybeHtml = (txt) => txt && /^\s*<!DOCTYPE html/i.test(txt);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchJsonWithFallback(urlDirect, urlProxy) {
  // 先直连 OKX
  try {
    const r = await fetch(urlDirect, { headers: HEADERS });
    const t = await r.text();
    if (maybeHtml(t)) throw new Error('HTML_FROM_OKX');
    const j = JSON.parse(t);
    return j;
  } catch (e) {
    // 退回 Worker 代理
    const r2 = await fetch(urlProxy, { headers: HEADERS });
    const t2 = await r2.text();
    if (maybeHtml(t2)) throw new Error('HTML_FROM_PROXY');
    const j2 = JSON.parse(t2);
    return j2;
  }
}

function toIso(ms) {
  return new Date(Number(ms)).toISOString();
}

async function main() {
  console.log(`v${process.versions.node}`);
  console.log(`Start fetching ${INST_ID} ${BAR} for last 365 days...`);

  // 游标从“现在”开始向前翻
  let cursor = Date.now();
  let all = [];
  let page = 0;

  while (page < MAX_PAGES) {
    page++;

    // 期望用“365 天前”的大致下限控制结束：拿满就停
    // 15m 一年大约 35040 根，拿到 >= EXPECT_MIN_ROWS 就够
    if (all.length >= EXPECT_MIN_ROWS) break;

    const direct = okxRoute(cursor);
    const viaProxy = proxyRoute(cursor);

    let data = null;
    let ok = false;

    for (let tr = 1; tr <= RETRY; tr++) {
      try {
        const j = await fetchJsonWithFallback(direct, viaProxy);
        if (j && j.code === '0' && Array.isArray(j.data)) {
          data = j.data;
          ok = true;
          break;
        } else {
          throw new Error(`BAD_JSON code=${j?.code}`);
        }
      } catch (err) {
        // 429/HTML/网络异常 -> 渐进退避
        const backoff = SLEEP_MS * tr * 5;
        console.log(`page ${page} fetch error: ${err.message || err}; backoff ${backoff}ms`);
        await sleep(backoff);
      }
    }

    if (!ok) {
      console.log(`page ${page} failed after retries; stop.`);
      break;
    }

    if (!data || data.length === 0) {
      console.log(`page ${page} empty; stop.`);
      break;
    }

    // OKX返回是最新在前的数组，保持原样先 push，最后统一反转排序
    all.push(...data);

    // 更新游标到本页最旧蜡烛的时间戳 - 1(ms) 避免重复
    const lastRow = data[data.length - 1];
    const lastTs = Number(lastRow[0]);
    cursor = lastTs - 1;

    // 进度提示
    const approxDays = (all.length * 15) / (60 * 24);
    console.log(`page ${page}: +${data.length} rows, total=${all.length}, ~${approxDays.toFixed(1)}d`);

    // 基础节流
    await sleep(SLEEP_MS);
  }

  console.log(`DONE fetch: ${all.length} rows`);

  if (all.length < EXPECT_MIN_ROWS) {
    throw new Error(`TOO_FEW_ROWS: ${all.length} (<${EXPECT_MIN_ROWS})`);
  }

  // 去重 & 排序（时间升序）
  const map = new Map();
  for (const r of all) map.set(r[0], r); // key = ts
  const rows = Array.from(map.values()).sort((a, b) => Number(a[0]) - Number(b[0]));

  // 输出 CSV
  const header = 'ts,iso,open,high,low,close,vol\n';
  const lines = rows.map(r => {
    const [ts, o, h, l, c, vol] = [r[0], r[1], r[2], r[3], r[4], r[5]];
    return `${ts},${toIso(ts)},${o},${h},${l},${c},${vol}`;
  });
  const csv = header + lines.join('\n') + '\n';

  await fs.writeFile(OUT, csv, 'utf8');
  console.log(`WROTE ${OUT}: ${rows.length} lines`);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
