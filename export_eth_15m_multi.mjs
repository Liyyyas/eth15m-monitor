// export_eth_15m_multi.mjs
// 目标：抓满「最近1年」ETH/USDT 15m K线，优先 Binance，兜底 Bybit→KuCoin→Gate
// 可选：Kaggle / CryptoDataDownload （仅当提供密钥/URL时启用）
// 产物：仓库根目录生成 okx_eth_15m.csv

import fs from 'node:fs/promises';

// ======== 可调参数 ========
const SYMBOL_BINANCE = 'ETHUSDT';
const SYMBOL_BYBIT   = 'ETHUSDT';
const SYMBOL_KUCOIN  = 'ETH-USDT';
const SYMBOL_GATE    = 'ETH_USDT';

const INTERVAL_MIN = 15;
const INTERVAL_MS  = INTERVAL_MIN * 60 * 1000;
const ONE_YEAR_MS  = 365 * 24 * 60 * 60 * 1000;
const START_TS     = Date.now() - ONE_YEAR_MS;
const END_TS       = Date.now();

const TARGET_ROWS  = Math.floor(ONE_YEAR_MS / INTERVAL_MS); // ≈35040
const HARD_MIN_ROWS= 32000; // 达不到就判失败换下一个源（保守阈值）
const BATCH_LIMIT  = 1000;  // 各家API单次最多条数（Binance/Bybit/Gate都是1000，OK）
const TIMEOUT_MS   = 15000; // 单次请求超时

const OUT_CSV      = 'okx_eth_15m.csv';

// 可选兜底（仅当配置存在时启用）
const KAGGLE_USERNAME = process.env.KAGGLE_USERNAME || '';
const KAGGLE_KEY      = process.env.KAGGLE_KEY || '';
// 若你手动提供一个分钟线CSV直链（CryptoDataDownload等），脚本会尝试聚合成15m
const FALLBACK_MINUTE_CSV_URL = process.env.MINUTE_CSV_URL || '';

// UA
const HEADERS = { 'User-Agent': 'Mozilla/5.0 (Android 14; ETH 15m fetcher)', 'Accept': 'application/json,text/plain,*/*' };

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const fmtISO = (ts) => new Date(Number(ts)).toISOString();
const asc = (a,b)=>a.ts-b.ts;

async function fetchText(url){
  const ac = new AbortController();
  const id = setTimeout(()=>ac.abort(), TIMEOUT_MS);
  try{
    const r = await fetch(url, {headers: HEADERS, signal: ac.signal});
    const t = await r.text();
    if(!r.ok) throw new Error(`HTTP ${r.status} ${r.statusText}`);
    if (/^\s*<!DOCTYPE\s+html/i.test(t)) throw new Error('HTML_BODY');
    return t;
  } finally { clearTimeout(id); }
}
async function fetchJson(url){
  const t = await fetchText(url);
  try{ return JSON.parse(t); }catch(e){ throw new Error('JSON_PARSE'); }
}

// 统一写CSV
async function writeCSV(rows){
  rows.sort(asc);
  // 去重+单调校验
  const clean = [];
  let prev = -Infinity;
  for(const r of rows){
    if(!Number.isFinite(r.ts)) continue;
    if(r.ts<=prev) continue;
    prev = r.ts;
    clean.push(r);
  }
  const header = 'ts,iso,open,high,low,close,vol\n';
  const body = clean.map(r => `${r.ts},${fmtISO(r.ts)},${r.open},${r.high},${r.low},${r.close},${r.vol}`).join('\n');
  await fs.writeFile(OUT_CSV, header + body + '\n', 'utf8');
  return clean.length;
}

function ensureEnough(rows, srcName){
  if(rows.length < HARD_MIN_ROWS) {
    throw new Error(`TOO_FEW_ROWS from ${srcName}: ${rows.length} < ${HARD_MIN_ROWS}`);
  }
}

// ======== 各源实现 ========

// 1) Binance 优先
async function fetchFromBinance(){
  console.log('[Binance] start');
  // /api/v3/klines?symbol=ETHUSDT&interval=15m&limit=1000&startTime&endTime
  const base = 'https://api.binance.com/api/v3/klines';
  const rows = [];
  let start = START_TS;
  const step = BATCH_LIMIT * INTERVAL_MS; // 每批 ≈ 10.4 天
  while (start < END_TS){
    const end = Math.min(start + step - 1, END_TS);
    const url = `${base}?symbol=${SYMBOL_BINANCE}&interval=15m&limit=${BATCH_LIMIT}&startTime=${start}&endTime=${end}`;
    let data = [];
    try{
      data = await fetchJson(url);
    }catch(e){
      console.log(`[Binance] batch error: ${e.message}; backoff`);
      await sleep(1000);
      continue; // 重试用下一批窗口推进，避免卡死
    }
    if(!Array.isArray(data) || data.length===0){
      // 没数据，推进窗口
      start = end + 1;
      continue;
    }
    for(const c of data){
      // [ openTime, open, high, low, close, volume, closeTime, ...]
      const ts = Number(c[0]);
      rows.push({
        ts,
        open: +c[1], high:+c[2], low:+c[3], close:+c[4], vol:+c[5],
      });
    }
    start = Number(data[data.length-1][0]) + INTERVAL_MS; // 前进
    if(rows.length % 5000 < 15) console.log(`[Binance] rows=${rows.length}`);
    await sleep(120); // 温柔点
  }
  console.log(`[Binance] done rows=${rows.length}`);
  ensureEnough(rows, 'Binance');
  return rows;
}

// 2) Bybit 兜底
async function fetchFromBybit(){
  console.log('[Bybit] start');
  // v5: /v5/market/kline?category=spot&symbol=ETHUSDT&interval=15&start=...&end=...&limit=1000
  const base = 'https://api.bybit.com/v5/market/kline';
  const rows = [];
  let start = START_TS;
  const step = BATCH_LIMIT * INTERVAL_MS;
  while (start < END_TS){
    const end = Math.min(start + step - 1, END_TS);
    const url = `${base}?category=spot&symbol=${SYMBOL_BYBIT}&interval=15&limit=${BATCH_LIMIT}&start=${start}&end=${end}`;
    try{
      const j = await fetchJson(url);
      const list = j?.result?.list;
      if(!Array.isArray(list) || list.length===0){
        start = end + 1;
        continue;
      }
      // list 数组通常是新->旧，反转为旧->新
      list.reverse();
      for(const c of list){
        // c: [start, open, high, low, close, volume, turnover]
        const ts = Number(c[0]);
        rows.push({ ts, open:+c[1], high:+c[2], low:+c[3], close:+c[4], vol:+c[5] });
      }
      start = Number(list[list.length-1][0]) + INTERVAL_MS;
      if(rows.length % 5000 < 15) console.log(`[Bybit] rows=${rows.length}`);
      await sleep(150);
    }catch(e){
      console.log(`[Bybit] batch error: ${e.message}; backoff`);
      await sleep(600);
    }
  }
  console.log(`[Bybit] done rows=${rows.length}`);
  ensureEnough(rows, 'Bybit');
  return rows;
}

// 3) KuCoin 兜底
async function fetchFromKuCoin(){
  console.log('[KuCoin] start');
  // /api/v1/market/candles?type=15min&symbol=ETH-USDT&startAt=sec&endAt=sec
  const base = 'https://api.kucoin.com/api/v1/market/candles';
  const rows = [];
  let startSec = Math.floor(START_TS/1000);
  const endSec   = Math.floor(END_TS/1000);
  const stepSec  = Math.floor((BATCH_LIMIT * INTERVAL_MS)/1000);
  while (startSec < endSec){
    const end = Math.min(startSec + stepSec - 1, endSec);
    const url = `${base}?type=15min&symbol=${SYMBOL_KUCOIN}&startAt=${startSec}&endAt=${end}`;
    try{
      const j = await fetchJson(url);
      let arr = j?.data;
      if(!Array.isArray(arr) || arr.length===0){
        startSec = end + 1;
        continue;
      }
      // KuCoin 返回新->旧
      arr.reverse();
      for(const c of arr){
        // [time, open, close, high, low, volume, turnover]  time为秒字符串
        const ts = Number(c[0])*1000;
        rows.push({ ts, open:+c[1], high:+c[3], low:+c[4], close:+c[2], vol:+c[5] });
      }
      startSec = Math.floor(rows[rows.length-1].ts/1000) + Math.floor(INTERVAL_MS/1000);
      if(rows.length % 5000 < 15) console.log(`[KuCoin] rows=${rows.length}`);
      await sleep(150);
    }catch(e){
      console.log(`[KuCoin] batch error: ${e.message}; backoff`);
      await sleep(600);
    }
  }
  console.log(`[KuCoin] done rows=${rows.length}`);
  ensureEnough(rows, 'KuCoin');
  return rows;
}

// 4) Gate 兜底
async function fetchFromGate(){
  console.log('[Gate] start');
  // /api/v4/spot/candlesticks?currency_pair=ETH_USDT&interval=15m&limit=1000&from=sec&to=sec
  const base = 'https://api.gateio.ws/api/v4/spot/candlesticks';
  const rows = [];
  let fromSec = Math.floor(START_TS/1000);
  const endSec= Math.floor(END_TS/1000);
  const stepSec = Math.floor((BATCH_LIMIT * INTERVAL_MS)/1000);
  while (fromSec < endSec){
    const to = Math.min(fromSec + stepSec - 1, endSec);
    const url = `${base}?currency_pair=${SYMBOL_GATE}&interval=15m&limit=${BATCH_LIMIT}&from=${fromSec}&to=${to}`;
    try{
      const arr = JSON.parse(await fetchText(url));
      if(!Array.isArray(arr) || arr.length===0){
        fromSec = to + 1;
        continue;
      }
      // 返回新->旧，元素：[t, vol, close, high, low, open]
      arr.reverse();
      for(const c of arr){
        const ts = Number(c[0])*1000;
        rows.push({ ts, open:+c[5], high:+c[3], low:+c[4], close:+c[2], vol:+c[1] });
      }
      fromSec = Math.floor(rows[rows.length-1].ts/1000) + Math.floor(INTERVAL_MS/1000);
      if(rows.length % 5000 < 15) console.log(`[Gate] rows=${rows.length}`);
      await sleep(200);
    }catch(e){
      console.log(`[Gate] batch error: ${e.message}; backoff`);
      await sleep(600);
    }
  }
  console.log(`[Gate] done rows=${rows.length}`);
  ensureEnough(rows, 'Gate');
  return rows;
}

// 5) Kaggle（可选，需密钥+你指定的数据集逻辑；此处仅占位，默认跳过）
async function fetchFromKaggle(){
  if(!KAGGLE_USERNAME || !KAGGLE_KEY) throw new Error('KAGGLE_NOT_CONFIGURED');
  // 这里通常需要先下载到本地再解析；出于通用性，本脚本不内置具体数据集名。
  // 你可将数据放到公开直链，再用 MINUTE_CSV_URL 或直接替换某个 fetch。
  throw new Error('KAGGLE_PLACEHOLDER');
}

// 6) CryptoDataDownload 分钟线聚合（可选，提供直链才启用）
async function fetchFromMinuteCSV(){
  if(!FALLBACK_MINUTE_CSV_URL) throw new Error('MINUTE_CSV_URL_NOT_SET');
  console.log('[MinuteCSV] start download & aggregate -> 15m');
  const text = await fetchText(FALLBACK_MINUTE_CSV_URL);
  // 该站CSV一般顶部有说明行与表头，这里做“尽力而为”的兼容解析
  const lines = text.split(/\r?\n/).filter(Boolean);
  // 找到表头行（含 Date / Open / High / Low / Close），其后为数据（通常新->旧）
  const headerIdx = lines.findIndex(l => /date|time/i.test(l) && /open/i.test(l) && /close/i.test(l));
  if(headerIdx < 0) throw new Error('CSV_HEADER_NOT_FOUND');
  const dataLines = lines.slice(headerIdx+1);
  const points = [];
  for(const line of dataLines){
    const parts = line.split(',').map(s=>s.trim());
    if(parts.length < 6) continue;
    // 尝试识别：Date,Symbol,Open,High,Low,Close,Volume..., (多种格式)
    const dateStr = parts[0];
    const ts = Date.parse(dateStr);
    if(!Number.isFinite(ts)) continue;
    if(ts < START_TS || ts > END_TS) continue; // 仅取最近一年
    const open = +parts[2], high=+parts[3], low=+parts[4], close=+parts[5];
    const vol  = +parts[6] || 0;
    if(![open,high,low,close].every(Number.isFinite)) continue;
    points.push({ ts, open, high, low, close, vol });
  }
  // 按 15m 聚合
  points.sort(asc);
  const rows = [];
  let bucketStart = Math.floor(points[0]?.ts / INTERVAL_MS) * INTERVAL_MS;
  let cur = null;
  for(const p of points){
    const b = Math.floor(p.ts / INTERVAL_MS) * INTERVAL_MS;
    if(!cur || b !== bucketStart){
      if(cur) rows.push(cur);
      bucketStart = b;
      cur = { ts: b, open: p.open, high: p.high, low: p.low, close: p.close, vol: p.vol };
    }else{
      cur.high = Math.max(cur.high, p.high);
      cur.low  = Math.min(cur.low,  p.low);
      cur.close= p.close;
      cur.vol += p.vol;
    }
  }
  if(cur) rows.push(cur);
  console.log(`[MinuteCSV] rows=${rows.length}`);
  ensureEnough(rows, 'MinuteCSV');
  return rows;
}

// ======== 主流程：多源级联 ========
async function main(){
  console.log(`Target: ETH/USDT 15m, from ${fmtISO(START_TS)} to ${fmtISO(END_TS)}`);
  const sources = [
    {name:'Binance', fn: fetchFromBinance},
    {name:'Bybit',   fn: fetchFromBybit},
    {name:'KuCoin',  fn: fetchFromKuCoin},
    {name:'Gate',    fn: fetchFromGate},
    // 下面两个为可选，默认跳过；配置了才会命中
    {name:'Kaggle',  fn: fetchFromKaggle},
    {name:'MinuteCSV', fn: fetchFromMinuteCSV},
  ];

  let lastErr = null;
  for(const s of sources){
    try{
      const rows = await s.fn();
      if(rows.length >= HARD_MIN_ROWS){
        const n = await writeCSV(rows);
        console.log(`SUCCESS via ${s.name}: wrote ${n} rows -> ./${OUT_CSV}`);
        return;
      } else {
        throw new Error(`Rows too few from ${s.name}: ${rows.length}`);
      }
    }catch(e){
      console.log(`Source ${s.name} failed: ${e.message}`);
      lastErr = e;
    }
  }
  throw lastErr || new Error('NO_SOURCE_AVAILABLE');
}

main().catch(e=>{ console.error('FATAL:', e?.stack || e?.message || e); process.exit(1); });
