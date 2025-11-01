// export_okx_1y.mjs —— OKX ETH/USDT 15m，抓最近 ~365 天
import fs from 'node:fs/promises';

// ===== 可按需改动的参数 =====
const INST_ID = 'ETH-USDT';
const BAR     = '15m';

const PAGE_LIMIT     = 300;     // OKX 单页上限
const MAX_PAGES      = 2000;    // 安全上限，够 1 年
const RETRY          = 3;       // 单页重试
const SLEEP_MS       = 3000;    // 429/backoff 基础等待
const OUT            = 'okx_eth_15m.csv';   // 输出到仓库根目录

// 可选：你的 Cloudflare Worker 代理（留空即不用）
const PROXY_BASE     = '';      // 例如 'https://xxxx.workers.dev'

// ===== 常量 & 公共函数 =====
const DIRECT = (beforeTs) =>
  `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${BAR}&limit=${PAGE_LIMIT}${beforeTs ? `&before=${beforeTs}` : ''}`;

const VIA_PROXY = (beforeTs) =>
  `${PROXY_BASE ? `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${BAR}&limit=${PAGE_LIMIT}${beforeTs ? `&before=${beforeTs}` : ''}` : ''}`;

const UA = 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36';
const HEADERS = { 'User-Agent': UA, 'Accept': 'application/json,text/plain,*/*' };

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function looksLikeHTML(txt) {
  return txt && /^\s*<!DOCTYPE html/i.test(txt);
}

async function fetchJSON(url) {
  const res = await fetch(url, { headers: HEADERS });
  const txt = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  if (looksLikeHTML(txt)) throw new Error('HTML_FROM_OKX');
  const j = JSON.parse(txt);
  if (j.code !== '0' || !Array.isArray(j.data)) {
    throw new Error(`BAD_JSON code=${j.code ?? 'x'}`);
  }
  return j.data; // 数组
}

async function fetchPage(beforeTs) {
  // 先直连，失败再走代理（如果配置了）
  const urls = [DIRECT(beforeTs)];
  if (PROXY_BASE) urls.push(VIA_PROXY(beforeTs));

  let lastErr;
  for (const url of urls) {
    for (let t = 1; t <= RETRY; t++) {
      try {
        return await fetchJSON(url);
      } catch (e) {
        lastErr = e;
        // 对 429/HTML/网络错误做指数退避
        await sleep(SLEEP_MS * t);
      }
    }
  }
  throw lastErr ?? new Error('FETCH_FAILED');
}

function toISO(ts) {
  return new Date(Number(ts)).toISOString();
}

// ===== 主流程 =====
async function main() {
  console.log(`Node ${process.version}`);
  console.log(`Start fetching ${INST_ID} ${BAR} (~365 days)…`);

  let page = 0;
  let before = undefined;             // 从“现在”往回翻
  const seen = new Set();             // 去重（按毫秒时间戳）
  const rows = [];

  while (page < MAX_PAGES) {
    page++;
    let data = [];
    try {
      data = await fetchPage(before);
    } catch (e) {
      console.log(`page ${page} error: ${e.message}`);
      break;
    }

    if (!data.length) {
      console.log(`page ${page} empty; stopping.`);
      break;
    }

    // OKX 数据是从最近到最远，反转为时间正序
    data.reverse();

    // 记录下一页的 before（=本页最早K线的时间戳）
    before = data[0][0];

    // 处理每条 K 线：只保留 confirm == "1"
    for (const c of data) {
      // c = [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
      if (!c || c.length < 9) continue;
      const ts = c[0];
      const confirm = c[8] ?? '1';
      if (confirm !== '1') continue;
      if (seen.has(ts)) continue;
      seen.add(ts);

      rows.push({
        ts: Number(ts),
        iso: toISO(ts),
        open: Number(c[1]),
        high: Number(c[2]),
        low:  Number(c[3]),
        close:Number(c[4]),
        vol:  Number(c[5]),
      });
    }

    // 每 120 页休息一下，避免限流
    if (page % 120 === 0) await sleep(1500);
  }

  // 写 CSV（有表头）
  const header = 'ts,iso,open,high,low,close,vol\n';
  const body = rows.map(r =>
    [r.ts, r.iso, r.open, r.high, r.low, r.close, r.vol].join(',')
  ).join('\n') + '\n';

  await fs.writeFile(OUT, header + body, 'utf8');
  console.log(`Wrote CSV to ./${OUT}`);
  console.log(`DONE: fetched rows ${rows.length}`);
}

main().catch(e => {
  console.error('FATAL:', e.message);
  process.exit(1);
});
