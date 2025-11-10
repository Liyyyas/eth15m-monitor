// export_eth_15m_multi.mjs
// 目标：抓满最近 1 年 ETH/USDT 15m K线
// 顺序：Binance → Bybit → KuCoin → Gate → CryptoDataDownload(分钟线聚合)
// 输出：okx_eth_15m.csv（根目录）

import fs from 'node:fs/promises';

// ======== 参数 ========
const SYMBOL_BINANCE = 'ETHUSDT';
const SYMBOL_BYBIT   = 'ETHUSDT';
const SYMBOL_KUCOIN  = 'ETH-USDT';
const SYMBOL_GATE    = 'ETH_USDT';

const INTERVAL_MIN = 15;
const INTERVAL_MS  = INTERVAL_MIN * 60 * 1000;
const ONE_YEAR_MS  = 365 * 24 * 60 * 60 * 1000;
const END_TS       = Date.now();
const START_TS     = END_TS - ONE_YEAR_MS;

const TARGET_ROWS     = Math.floor(ONE_YEAR_MS / INTERVAL_MS); // ≈35040
const HARD_MIN_ROWS   = 32000;      // 行数硬阈值（不到就失败换源）
const COVER_MARGIN_MS = 2 * INTERVAL_MS; // 覆盖度边界允许误差（30 分钟）
const BATCH_LIMIT     = 1000;       // 单批最大条数
const TIMEOUT_MS      = 15000;
const RETRIES         = 2;          // 批次级重试次数（每批至多重试 2 次）
const EMPTY_STREAK_LIMIT = 5;       // 连续空批 N 次视为停滞
const OUT_CSV         = 'okx_eth_15m.csv';

// CryptoDataDownload 分钟线兜底（公开地址）
const MINUTE_CSV_URL = 'https://www.cryptodatadownload.com/cdd/Binance_ETHUSDT_minute.csv';

// UA
const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Android 14; ETH 15m multi-source)',
  'Accept': 'application/json,text/plain,*/*'
};

// ======== 工具函数 ========
const sleep  = (ms) => new Promise(r => setTimeout(r, ms));
const fmtISO = (ts) => new Date(Number(ts)).toISOString();
const asc    = (a, b) => a.ts - b.ts;

async function fetchText(url) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(url, { headers: HEADERS, signal: ac.signal });
    const txt = await res.text();
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    if (/^\s*<!DOCTYPE\s+html/i.test(txt)) throw new Error('HTML_BODY');
    return txt;
  } finally {
    clearTimeout(timer);
  }
}
async function fetchJson(url) {
  const txt = await fetchText(url);
  try { return JSON.parse(txt); } catch { throw new Error('JSON_PARSE'); }
}

// —— 批次级重试（并入 #11/#22）
async function fetchJsonRetry(url, maxRetries = RETRIES) {
  let lastErr;
  for (let r = 0; r <= maxRetries; r++) {
    try { return await fetchJson(url); }
    catch (e) {
      lastErr = e;
      if (r === maxRetries) break;
      await sleep(500 * (r + 1));
    }
  }
  throw lastErr;
}
async function fetchTextRetry(url, maxRetries = RETRIES) {
  let lastErr;
  for (let r = 0; r <= maxRetries; r++) {
    try { return await fetchText(url); }
    catch (e) {
      lastErr = e;
      if (r === maxRetries) break;
      await sleep(500 * (r + 1));
    }
  }
  throw lastErr;
}

// —— 排序去重
function dedupeSort(rows) {
  rows.sort(asc);
  const clean = [];
  let prev = -Infinity;
  for (const r of rows) {
    if (!Number.isFinite(r.ts)) continue;
    if (r.ts <= prev) continue;
    prev = r.ts;
    clean.push(r);
  }
  return clean;
}

// —— 写 CSV
async function writeCSV(cleanRows) {
  const header = 'ts,iso,open,high,low,close,vol\n';
  const body = cleanRows
    .map(r => `${r.ts},${fmtISO(r.ts)},${r.open},${r.high},${r.low},${r.close},${r.vol}`)
    .join('\n');
  await fs.writeFile(OUT_CSV, header + body + '\n', 'utf8');
}

// —— 行数 & 覆盖度硬验收（并入 #5/#22）
function ensureQuality(rows, srcName) {
  if (rows.length < HARD_MIN_ROWS) {
    throw new Error(`TOO_FEW_ROWS from ${srcName}: ${rows.length} < ${HARD_MIN_ROWS}`);
  }
  // 覆盖度：最早/最晚时间必须贴近窗口边界
  let minTs = Infinity, maxTs = -Infinity;
  for (const r of rows) {
    if (!Number.isFinite(r.ts)) continue;
    if (r.ts < minTs) minTs = r.ts;
    if (r.ts > maxTs) maxTs = r.ts;
  }
  if (minTs === Infinity || maxTs === -Infinity) {
    throw new Error(`COVERAGE_FAIL(${srcName}): invalid ts range`);
  }
  if (minTs > START_TS + COVER_MARGIN_MS || maxTs < END_TS - COVER_MARGIN_MS) {
    throw new Error(`COVERAGE_FAIL(${srcName}): earliest=${fmtISO(minTs)} latest=${fmtISO(maxTs)} window=${fmtISO(START_TS)}~${fmtISO(END_TS)}`);
  }
}

// ======== 数据源实现 ========

// 1) Binance
async function fetchFromBinance() {
  console.log('[Binance] start');
  const base = 'https://api.binance.com/api/v3/klines';
  const rows = [];

  let start = START_TS;
  const step = BATCH_LIMIT * INTERVAL_MS;
  let emptyStreak = 0;
  let prevLastTs = -Infinity;

  while (start < END_TS) {
    const end = Math.min(start + step - 1, END_TS);
    const url = `${base}?symbol=${SYMBOL_BINANCE}&interval=15m&limit=${BATCH_LIMIT}&startTime=${start}&endTime=${end}`;

    let data = await fetchJsonRetry(url);
    if (!Array.isArray(data) || data.length === 0) {
      emptyStreak++;
      if (emptyStreak >= EMPTY_STREAK_LIMIT) throw new Error('STALLED_EMPTY');
      start = end + 1;
      continue;
    }
    emptyStreak = 0;

    for (const c of data) {
      const ts = Number(c[0]);
      rows.push({ ts, open:+c[1], high:+c[2], low:+c[3], close:+c[4], vol:+c[5] });
    }
    const lastTs = Number(data[data.length - 1][0]);
    if (!(lastTs > prevLastTs)) throw new Error(`CURSOR_STALLED lastTs=${lastTs} <= prevLastTs=${prevLastTs}`);
    prevLastTs = lastTs;

    start = lastTs + INTERVAL_MS;
    if (rows.length % 5000 < 15) console.log(`[Binance] rows=${rows.length}`);
    await sleep(120);
  }

  console.log(`[Binance] done rows=${rows.length}, target≈${TARGET_ROWS}`);
  ensureQuality(rows, 'Binance');
  return rows;
}

// 2) Bybit
async function fetchFromBybit() {
  console.log('[Bybit] start');
  const base = 'https://api.bybit.com/v5/market/kline';
  const rows = [];

  let start = START_TS;
  const step = BATCH_LIMIT * INTERVAL_MS;
  let emptyStreak = 0;
  let prevLastTs = -Infinity;

  while (start < END_TS) {
    const end = Math.min(start + step - 1, END_TS);
    const url = `${base}?category=spot&symbol=${SYMBOL_BYBIT}&interval=15&limit=${BATCH_LIMIT}&start=${start}&end=${end}`;

    const j = await fetchJsonRetry(url);
    const list = j?.result?.list;
    if (!Array.isArray(list) || list.length === 0) {
      emptyStreak++;
      if (emptyStreak >= EMPTY_STREAK_LIMIT) throw new Error('STALLED_EMPTY');
      start = end + 1;
      continue;
    }
    emptyStreak = 0;

    list.reverse(); // 新->旧 转 旧->新
    for (const c of list) {
      const ts = Number(c[0]);
      rows.push({ ts, open:+c[1], high:+c[2], low:+c[3], close:+c[4], vol:+c[5] });
    }
    const lastTs = Number(list[list.length - 1][0]);
    if (!(lastTs > prevLastTs)) throw new Error(`CURSOR_STALLED lastTs=${lastTs} <= prevLastTs=${prevLastTs}`);
    prevLastTs = lastTs;

    start = lastTs + INTERVAL_MS;
    if (rows.length % 5000 < 15) console.log(`[Bybit] rows=${rows.length}`);
    await sleep(150);
  }

  console.log(`[Bybit] done rows=${rows.length}`);
  ensureQuality(rows, 'Bybit');
  return rows;
}

// 3) KuCoin
async function fetchFromKuCoin() {
  console.log('[KuCoin] start');
  const base = 'https://api.kucoin.com/api/v1/market/candles';
  const rows = [];

  let startSec = Math.floor(START_TS / 1000);
  const endSec   = Math.floor(END_TS / 1000);
  const stepSec  = Math.floor((BATCH_LIMIT * INTERVAL_MS) / 1000);
  let emptyStreak = 0;
  let prevLastTs  = -Infinity;

  while (startSec < endSec) {
    const end = Math.min(startSec + stepSec - 1, endSec);
    const url = `${base}?type=15min&symbol=${SYMBOL_KUCOIN}&startAt=${startSec}&endAt=${end}`;

    const j = await fetchJsonRetry(url);
    let arr = j?.data;
    if (!Array.isArray(arr) || arr.length === 0) {
      emptyStreak++;
      if (emptyStreak >= EMPTY_STREAK_LIMIT) throw new Error('STALLED_EMPTY');
      startSec = end + 1;
      continue;
    }
    emptyStreak = 0;

    arr.reverse(); // 新->旧 转 旧->新
    for (const c of arr) {
      const ts = Number(c[0]) * 1000; // 秒→毫秒
      rows.push({ ts, open:+c[1], high:+c[3], low:+c[4], close:+c[2], vol:+c[5] });
    }
    const lastTs = Number(arr[arr.length - 1][0]) * 1000;
    if (!(lastTs > prevLastTs)) throw new Error(`CURSOR_STALLED lastTs=${lastTs} <= prevLastTs=${prevLastTs}`);
    prevLastTs = lastTs;

    startSec = Math.floor(lastTs / 1000) + Math.floor(INTERVAL_MS / 1000);
    if (rows.length % 5000 < 15) console.log(`[KuCoin] rows=${rows.length}`);
    await sleep(150);
  }

  console.log(`[KuCoin] done rows=${rows.length}`);
  ensureQuality(rows, 'KuCoin');
  return rows;
}

// 4) Gate
async function fetchFromGate() {
  console.log('[Gate] start');
  const base = 'https://api.gateio.ws/api/v4/spot/candlesticks';
  const rows = [];

  let fromSec = Math.floor(START_TS / 1000);
  const endSec = Math.floor(END_TS / 1000);
  const stepSec = Math.floor((BATCH_LIMIT * INTERVAL_MS) / 1000);
  let emptyStreak = 0;
  let prevLastTs  = -Infinity;

  while (fromSec < endSec) {
    const to = Math.min(fromSec + stepSec - 1, endSec);
    const url = `${base}?currency_pair=${SYMBOL_GATE}&interval=15m&limit=${BATCH_LIMIT}&from=${fromSec}&to=${to}`;

    const txt = await fetchTextRetry(url);
    const arr = JSON.parse(txt);
    if (!Array.isArray(arr) || arr.length === 0) {
      emptyStreak++;
      if (emptyStreak >= EMPTY_STREAK_LIMIT) throw new Error('STALLED_EMPTY');
      fromSec = to + 1;
      continue;
    }
    emptyStreak = 0;

    // arr: [t, vol, close, high, low, open] 新->旧
    arr.reverse();
    for (const c of arr) {
      const ts = Number(c[0]) * 1000;
      rows.push({ ts, open:+c[5], high:+c[3], low:+c[4], close:+c[2], vol:+c[1] });
    }
    const lastTs = Number(arr[arr.length - 1][0]) * 1000;
    if (!(lastTs > prevLastTs)) throw new Error(`CURSOR_STALLED lastTs=${lastTs} <= prevLastTs=${prevLastTs}`);
    prevLastTs = lastTs;

    fromSec = Math.floor(lastTs / 1000) + Math.floor(INTERVAL_MS / 1000);
    if (rows.length % 5000 < 15) console.log(`[Gate] rows=${rows.length}`);
    await sleep(200);
  }

  console.log(`[Gate] done rows=${rows.length}`);
  ensureQuality(rows, 'Gate');
  return rows;
}

// —— 分钟线兜底（并入 #7：强制 UTC 解析）
function toUTC(dateStr) {
  // 'YYYY-MM-DD HH:mm:ss' → 'YYYY-MM-DDTHH:mm:ssZ'
  return Date.parse(/\dT/.test(dateStr) || /Z$/.test(dateStr)
    ? dateStr
    : dateStr.replace(' ', 'T') + 'Z');
}
async function fetchFromMinuteCSV() {
  console.log('[MinuteCSV] start ->', MINUTE_CSV_URL);
  const text = await fetchTextRetry(MINUTE_CSV_URL);
  const lines = text.split(/\r?\n/).filter(Boolean);

  const headerIdx = lines.findIndex(l => /date/i.test(l) && /open/i.test(l) && /close/i.test(l));
  if (headerIdx < 0) throw new Error('CSV_HEADER_NOT_FOUND');

  const dataLines = lines.slice(headerIdx + 1);
  const points = [];

  for (const line of dataLines) {
    const parts = line.split(',').map(s => s.trim());
    if (parts.length < 6) continue;

    // CryptoDataDownload 常见：Date,Symbol,Open,High,Low,Close,Volume,...
    const ts = toUTC(parts[0]);
    if (!Number.isFinite(ts)) continue;
    if (ts < START_TS || ts > END_TS) continue;

    const open = +parts[2], high = +parts[3], low = +parts[4], close = +parts[5];
    const vol  = +parts[6] || 0;
    if (![open, high, low, close].every(Number.isFinite)) continue;
    points.push({ ts, open, high, low, close, vol });
  }
  if (!points.length) throw new Error('NO_POINTS_FROM_MINUTE_CSV');

  // 聚合为 15m
  points.sort(asc);
  const rows = [];
  let bucket = Math.floor(points[0].ts / INTERVAL_MS) * INTERVAL_MS;
  let cur = null;
  for (const p of points) {
    const b = Math.floor(p.ts / INTERVAL_MS) * INTERVAL_MS;
    if (!cur || b !== bucket) {
      if (cur) rows.push(cur);
      bucket = b;
      cur = { ts: b, open: p.open, high: p.high, low: p.low, close: p.close, vol: p.vol };
    } else {
      cur.high = Math.max(cur.high, p.high);
      cur.low  = Math.min(cur.low,  p.low);
      cur.close= p.close;
      cur.vol += p.vol;
    }
  }
  if (cur) rows.push(cur);

  console.log(`[MinuteCSV] 15m rows=${rows.length}`);
  ensureQuality(rows, 'MinuteCSV');
  return rows;
}

// ======== 主流程 ========
async function main() {
  console.log(`Target: ETH/USDT 15m`);
  console.log(`Window: ${fmtISO(START_TS)} → ${fmtISO(END_TS)} (~${TARGET_ROWS} bars)`);

  const sources = [
    { name: 'Binance',   fn: fetchFromBinance },
    { name: 'Bybit',     fn: fetchFromBybit },
    { name: 'KuCoin',    fn: fetchFromKuCoin },
    { name: 'Gate',      fn: fetchFromGate },
    { name: 'MinuteCSV', fn: fetchFromMinuteCSV }, // 最后兜底
  ];

  let lastErr = null;

  for (const s of sources) {
    try {
      console.log(`\n=== Try source: ${s.name} ===`);
      const raw = await s.fn();
      const clean = dedupeSort(raw);                 // 单调去重
      ensureQuality(clean, s.name);                  // 行数 + 覆盖度硬验收
      await writeCSV(clean);
      console.log(`SUCCESS via ${s.name}: wrote ${clean.length} rows -> ./${OUT_CSV}`);
      console.log(`Range: ${fmtISO(clean[0].ts)} ~ ${fmtISO(clean[clean.length-1].ts)}`);
      return;
    } catch (e) {
      console.log(`Source ${s.name} failed: ${e.message}`);
      lastErr = e;
    }
  }

  throw lastErr || new Error('NO_SOURCE_AVAILABLE');
}

main().catch(e => {
  console.error('FATAL:', e?.stack || e?.message || e);
  process.exit(1);
});
