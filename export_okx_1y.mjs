import fs from 'node:fs/promises';

// ================================
// 配置区域（只需改这里 ↓↓↓）
const PROXY_BASE = 'https://eth-proxy.1053363050.workers.dev'; // ← 改成你的 Worker 地址
const INST_ID = 'ETH-USDT';
const BAR = '15m';
const OUT = 'okx_eth_15m.csv';
const ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000; // 一年周期
// ================================

const now = Date.now();
const since = now - ONE_YEAR_MS;
const header = 'ts,iso,open,high,low,close,vol\n';
const rows = [];
let before = null;
let page = 0;

async function fetchPage() {
  const p = new URLSearchParams({
    instId: INST_ID,
    bar: BAR,
    limit: '100',
  });
  if (before) p.set('before', String(before));
  const url = `${PROXY_BASE}/proxy/api/v5/market/candles?${p.toString()}`;
  const resp = await fetch(url, { headers: { 'accept': 'application/json' } });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const js = await resp.json();
  if (js.code !== '0') throw new Error(`OKX code=${js.code} msg=${js.msg}`);
  return js.data ?? [];
}

while (true) {
  page++;
  const data = await fetchPage();
  if (!Array.isArray(data) || data.length === 0) break;
  for (const k of data) {
    const ts = Number(k[0]);
    if (Number.isNaN(ts) || ts < since) continue;
    const iso = new Date(ts).toISOString().replace('.000Z', 'Z');
    const [ , open, high, low, close, vol ] = k;
    rows.push([ts, iso, open, high, low, close, vol].join(','));
  }
  before = Number(data[data.length - 1][0]);
  if (page > 2000 || before < since) break;
}

if (rows.length < 10) {
  console.error('rows < 10，疑似抓取失败，取消写入。');
  process.exit(1);
}

rows.sort((a, b) => Number(a.split(',')[0]) - Number(b.split(',')[0]));
const csv = header + rows.join('\n') + '\n';
await fs.writeFile(OUT, csv, 'utf8');
console.log(`✅ 写入 ${OUT}，共 ${rows.length} 条记录。`);
