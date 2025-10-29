// export_okx_1y.mjs  —  OKX ETH/USDT 15m，抓最近 ~365 天
import fs from 'node:fs/promises';

const INST_ID = 'ETH-USDT';
const BAR = '15m';
const PAGE_LIMIT = 300;        // OKX 单页上限
const EXPECT_MIN_ROWS = 30000; // 15m*365≈35040，给个保底
const MAX_PAGES = 2000;        // 安全上限
const SLEEP_MS = 400;          // 每页间隔，友好一点
const RETRY = 3;               // 每页最多重试 3 次
const OUT = './eth15m-monitor/okx_eth_15m.csv';

// 直连 & 代理（你的 Cloudflare Worker）
const DIRECT = (before) =>
  `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${BAR}&limit=${PAGE_LIMIT}${before ? `&before=${before}` : ''}`;
const PROXY_BASE = 'https://eth-proxy.1053363050.workers.dev';
const VIA_PROXY = (before) =>
  `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${BAR}&limit=${PAGE_LIMIT}${before ? `&before=${before}` : ''}`;

const UA = 'Mozilla/5.0 (Linux; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36';
const HEADERS = { 'User-Agent': UA, 'Accept': 'application/json,text/plain,*/*' };

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function getJson(url) {
  // 先直连，失败/HTML 再走代理
  for (let attempt = 1; attempt <= RETRY; attempt++) {
    try {
      let res = await fetch(url, { headers: HEADERS });
      let txt = await res.text();
      // 有些情况下会返回 HTML（风控页），直接抛错切代理
      if (!res.ok || /^\s*<!DOCTYPE/i.test(txt)) throw new Error('HTML_OR_HTTP');
      const j = JSON.parse(txt);
      if (j.code !== '0' || !Array.isArray(j.data)) throw new Error(`BAD_JSON_${j.code ?? 'x'}`);
      return j.data;
    } catch (_) {
      // 换路由再试：直连→代理
      if (!url.startsWith(PROXY_BASE)) url = url.replace('https://www.okx.com', PROXY_BASE);
      await sleep(SLEEP_MS * attempt);
    }
  }
  return []; // 多次失败，当作空页处理
}

function toISO(ts) { return new Date(Number(ts)).toISOString(); }

async function main() {
  console.log(process.versions.node);
  console.log(`Start fetching ${INST_ID} ${BAR} for ~365 days...`);

  let before = Date.now();     // 从“现在”往回翻
  let page = 0;

  const allData = [];          // ★ 只在这里声明一次
  const seenTs = new Set();    // ★ 只在这里声明一次

  while (page < MAX_PAGES) {
    page++;
    const url = DIRECT(before);
    const data = await getJson(url);

    if (!data || data.length === 0) {
      console.log(`page ${page} empty; stopping.`);
      break;
    }

    // OKX 返回“新在前旧在后”，我们要从旧到新处理更稳妥
    data.slice().reverse().forEach(c => {
      // c: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
      if (!c || c.length < 8) return;
      const ts = Number(c[0]);
      const confirm = `${c[8] ?? '1'}`;
      if (confirm !== '1') return;     // 丢弃未确认 K 线（历史一般都是 1）
      if (seenTs.has(ts)) return;      // 去重
      seenTs.add(ts);
      allData.push({
        ts,
        iso: toISO(ts),
        open: Number(c[1]),
        high: Number(c[2]),
        low:  Number(c[3]),
        close:Number(c[4]),
        vol:  Number(c[5])
      });
    });

    console.log(`page ${page} done -> ${allData.length} total`);

    // 下一页“before”取本页最早一根 K 线的 ts（即 data 数组最后一项的 ts）
    const oldest = Number(data[data.length - 1]?.[0]);
    if (!oldest || oldest >= before) {
      // 正常 oldest 必须 < before，否则说明翻页卡住了，强行回退一点
      before = before - 60 * 1000; // 退 1 分钟兜底
    } else {
      before = oldest;
    }

    await sleep(SLEEP_MS);
  }

  // 写 CSV
  // 先按 ts 排一下（防止偶发乱序），再输出
  allData.sort((a, b) => a.ts - b.ts);
  const header = 'ts,iso,open,high,low,close,vol\n';
  const lines = allData.map(r => `${r.ts},${r.iso},${r.open},${r.high},${r.low},${r.close},${r.vol}`).join('\n');
  await fs.mkdir('./eth15m-monitor', { recursive: true });
  await fs.writeFile(OUT, header + lines, 'utf8');

  console.log(`DONE fetch: total rows = ${allData.length}`);
  console.log(`Wrote CSV to ${OUT}`);

  if (allData.length < EXPECT_MIN_ROWS) {
    console.warn(`WARNING: fetched rows ${allData.length} < expected ${EXPECT_MIN_ROWS}.`);
    process.exitCode = 1; // 让 Actions 显示黄色/红色，提醒你核查
  }
}

main().catch(e => {
  console.error('FATAL:', e?.stack || e);
  process.exit(1);
});
