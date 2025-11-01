// export_okx_1y.mjs —— OKX ETH/USDT 15m，近 365 天
import fs from 'node:fs/promises';

// === 可调参数 ===
const INST_ID = 'ETH-USDT';
const BAR = '15m';
const PAGE_LIMIT = 300;          // OKX 上限 300
const EXPECT_MIN_ROWS = 32000;   // 近一年理论 35040，<32000 仅警告
const MAX_PAGES = 2000;          // 2000*300=60w 安全上限
const RETRY = 3;                 // 单页 3 次尝试（直连/代理+退避）
const BASE_SLEEP_MS = 180;       // 基础退避(带抖动)，过小易 429
const OUT = './okx_eth_15m.csv'; // 输出在仓库根
const TMP = './okx_eth_15m.tmp'; // 原子写临时文件

// 你的 Cloudflare Worker 代理（必须 GET 返回 OKX 同构 JSON）
const PROXY_BASE = 'https://eth-proxy.053363050.workers.dev';

// === URL 生成（含 cache-bust） ===
const okxDirect = (before) =>
  `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${BAR}&limit=${PAGE_LIMIT}&before=${before}&_=${Date.now()}`;

const okxProxy = (before) =>
  `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${BAR}&limit=${PAGE_LIMIT}&before=${before}&_=${Date.now()}`;

// === 简易 sleep（含[0,1)抖动）===
const sleep = (ms) => new Promise(r => setTimeout(r, ms * (1 + Math.random() * 0.3)));

// === 判断是不是 HTML（被风控/重定向）===
const maybeHTML = (txt) => txt && /^\s*<!DOCTYPE html/i.test(txt);

// === 带超时的 fetch（Node20 原生支持 AbortSignal.timeout）===
async function getJSON(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36',
      'Accept': 'application/json,text/plain,*/*'
    },
    signal: AbortSignal.timeout(15000)
  });
  const txt = await res.text();
  if (!res.ok || maybeHTML(txt)) throw new Error(`HTTP_OR_HTML status=${res.status}`);
  let j;
  try { j = JSON.parse(txt); } catch { throw new Error('BAD_JSON'); }
  if (j.code !== '0' || !Array.isArray(j.data)) throw new Error(`BAD_CODE_${j.code ?? 'X'}`);
  return j.data;
}

// === 直连→代理回退 + 指数退避 ===
async function getPage(before) {
  for (let t = 1; t <= RETRY; t++) {
    try {
      // 优先直连
      return await getJSON(okxDirect(before));
    } catch (_) {
      try {
        // 再走代理
        return await getJSON(okxProxy(before));
      } catch (e2) {
        if (t === RETRY) throw e2;
        await sleep(BASE_SLEEP_MS * Math.pow(2, t));
      }
    }
  }
  throw new Error('UNREACHABLE');
}

// === 工具 ===
const toISO = (ts) => new Date(Number(ts)).toISOString();

// === 主流程 ===
async function main() {
  console.log('Node', process.versions.node);
  console.log(`Start: ${INST_ID} ${BAR} for ~365 days …`);

  const now = Date.now();
  let before = now;   // 从“现在”往回翻页
  let page = 0;

  const seen = new Set();
  const rows = [];

  while (page < MAX_PAGES) {
    page++;
    let data;
    try {
      data = await getPage(before);
    } catch (e) {
      console.log(`page ${page} error, stop.`, e.message);
      break;
    }

    if (!data || data.length === 0) {
      console.log(`page ${page} empty; stopping.`);
      break;
    }

    // OKX 返回从新到旧，翻转为旧→新以便确定 next before
    data.slice().reverse().forEach(c => {
      // c = [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
      const ts = Number(c?.[0]);           // 毫秒
      const confirm = (c?.[8] ?? '1') + ''; // 未给时视为已确认
      if (!Number.isFinite(ts)) return;
      if (confirm !== '1') return;         // 丢弃未收盘
      if (seen.has(ts)) return;            // 去重
      seen.add(ts);
      rows.push({
        ts,
        iso: toISO(ts),
        open: Number(c?.[1]),
        high: Number(c?.[2]),
        low:  Number(c?.[3]),
        close:Number(c?.[4]),
        vol:  Number(c?.[5] ?? 0)
      });
    });

    // 计算下一页游标：取本页最早 ts，减 1（避免等值边界）
    const minTs = Math.min(...data.map(d => Number(d?.[0])).filter(Number.isFinite));
    if (!Number.isFinite(minTs)) {
      console.log(`page ${page} bad data; stopping.`);
      break;
    }
    const nextBefore = minTs - 1;
    const kept = data.length;

    if (page % 10 === 1) {
      console.log(`page ${page} done → kept=${kept}, total=${rows.length}, nextBefore=${nextBefore}`);
    }

    // 365 天达到即可退出（15m≈96根/天）
    if (rows.length >= 365 * 96) break;

    before = nextBefore;
    await sleep(80); // 轻微节流
  }

  // 升序写盘（老→新）
  rows.sort((a, b) => a.ts - b.ts);

  // 原子写：先 .tmp 再 rename
  let csv = 'ts,iso,open,high,low,close,vol\n';
  for (const r of rows) {
    csv += `${r.ts},${r.iso},${r.open},${r.high},${r.low},${r.close},${r.vol}\n`;
  }
  await fs.writeFile(TMP, csv, 'utf8');
  await fs.rename(TMP, OUT);

  const n = rows.length;
  console.log(`DONE: wrote ${OUT}, rows=${n}`);

  // 阈值校验（默认仅警告，不 fail）
  if (n < EXPECT_MIN_ROWS) {
    console.log(`WARNING: fetched rows ${n} < expected ${EXPECT_MIN_ROWS}`);
    // 若要严格失败，请取消下一行注释：
    // process.exit(1);
  }
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
