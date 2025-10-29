// export_okx_15m_to_csv.js
// 拉取 OKX ETH-USDT 15m 历史K线并输出为 CSV：okx_eth_15m.csv
// Node 20，无需第三方依赖

const fs = require('fs');

async function fetchPage(params) {
  const url = new URL('https://www.okx.com/api/v5/market/history-candles');
  url.searchParams.set('instId', 'ETH-USDT-SWAP');
  url.searchParams.set('bar', '15m');
  url.searchParams.set('limit', params.limit || '100');
  if (params.before) url.searchParams.set('before', String(params.before));
  // history-candles 返回更早的数据；不传 before 则从最近开始往回翻
  const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
  if (!res.ok) throw new Error(`OKX ${res.status} ${res.statusText}`);
  const j = await res.json();
  if (!j.data || !Array.isArray(j.data) || j.data.length === 0) return [];
  // data: [[ts, o,h,l,c,vol, volCcy, volCcyQuote, confirm], ...] 倒序(新→旧)
  return j.data.map(r => ({
    ts: Number(r[0]),
    open: Number(r[1]),
    high: Number(r[2]),
    low:  Number(r[3]),
    close:Number(r[4]),
    volume:Number(r[5]),
  }));
}

async function main() {
  const MAX_BARS = 3000;         // 约 31 天（15m 一天 96 根）; 想更久可改大
  const PAGE_LIMIT = 100;        // OKX 单页上限
  let all = [];
  let before = undefined;
  while (all.length < MAX_BARS) {
    const page = await fetchPage({ limit: PAGE_LIMIT, before });
    if (page.length === 0) break;
    all = all.concat(page);
    // 下一轮往更早翻：取这一页里“最旧”的时间
    const oldest = page[page.length - 1].ts;
    before = oldest;
    // 防止 API 速率限制，稍微歇一歇
    await new Promise(r => setTimeout(r, 200));
  }
  // OKX 返回每页倒序，这里把总数据按时间升序
  all.sort((a,b)=> a.ts - b.ts);

  const lines = ['ts,open,high,low,close,volume'];
  for (const k of all) {
    const iso = new Date(k.ts).toISOString();
    lines.push(`${iso},${k.open},${k.high},${k.low},${k.close},${k.volume}`);
  }
  fs.writeFileSync('okx_eth_15m.csv', lines.join('\n'), 'utf8');
  console.log(`Wrote okx_eth_15m.csv with ${all.length} rows`);
}

main().catch(e => { console.error(e); process.exit(1); });
