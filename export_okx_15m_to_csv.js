// Export ~1 year of ETH/USDT 15m candles from OKX to okx_eth_15m.csv
// 兼容 GitHub Actions (Node 20 自带 fetch)

const fs = require('fs');
const path = require('path');

const INST_ID   = 'ETH-USDT';
const BAR       = '15m';
const LIMIT     = 200;                 // 每页条数：OKX一般支持 100~300，200较稳
const DAYS      = 365;                 // 拉取天数：一年
const OUT_CSV   = path.join(process.cwd(), 'okx_eth_15m.csv');
const UA        = 'okx-eth15m-export/1.0 (+github actions)';

const nowUtcMs   = Date.now();
const startUtcMs = nowUtcMs - DAYS * 24 * 60 * 60 * 1000;
const needBars   = Math.ceil((DAYS * 24 * 60) / 15); // 约 35040

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchPage(beforeTs) {
  const params = new URLSearchParams({
    instId: INST_ID,
    bar: BAR,
    limit: String(LIMIT),
  });
  if (beforeTs) params.set('before', String(beforeTs)); // 从 before 之前的更早数据往回翻
  const url = `https://www.okx.com/api/v5/market/candles?${params.toString()}`;

  const res = await fetch(url, { headers: { 'User-Agent': UA } });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  const j = await res.json();
  if (j.code !== '0') throw new Error(`OKX error: ${j.code} ${j.msg || ''}`);
  // j.data: 数组，单项结构一般为: [ts, o, h, l, c, vol, volCcy, ...]
  return j.data || [];
}

(async () => {
  console.log(`Start export ${INST_ID} ${BAR}, last ${DAYS} days...`);
  let all = [];
  let before = undefined;

  while (true) {
    const batch = await fetchPage(before);
    if (!batch.length) break;

    all.push(...batch);
    // OKX 返回通常是按时间倒序（最新→最旧）
    const oldestTs = Math.min(...batch.map(r => +r[0]));
    before = oldestTs; // 下一页继续往更早翻

    console.log(`Fetched page: ${batch.length} bars, oldest=${new Date(oldestTs).toISOString()}, total=${all.length}`);

    if (oldestTs <= startUtcMs) break;        // 已经翻到一年之前
    if (all.length > needBars * 1.5) break;   // 安全边界，防止无限翻页
    await sleep(150);                          // 给 API 一点喘息，降低限速风险
  }

  // 去重、过滤到一年范围、按时间正序
  const map = new Map();
  for (const r of all) map.set(+r[0], r);
  all = Array.from(map.values())
    .filter(r => +r[0] >= startUtcMs)
    .sort((a, b) => +a[0] - +b[0]);

  console.log(`After clean: ${all.length} bars (expect ~${needBars})`);

  // 生成 CSV
  const header = 'ts,iso,open,high,low,close,vol';
  const lines = [header];
  for (const r of all) {
    const ts    = +r[0];
    const iso   = new Date(ts).toISOString();
    const open  = r[1];
    const high  = r[2];
    const low   = r[3];
    const close = r[4];
    const vol   = r[5]; // 基于张/币的成交量，OKX定义参见其文档
    lines.push([ts, iso, open, high, low, close, vol].join(','));
  }

  fs.writeFileSync(OUT_CSV, lines.join('\n'));
  console.log(`Saved: ${OUT_CSV} (${lines.length - 1} rows)`);
})().catch(e => {
  console.error(e);
  process.exit(1);
});
