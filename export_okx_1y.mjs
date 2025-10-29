// export_okx_1y.mjs
// 拉取 OKX ETH/USDT 15m K线：整整 1 年，history-candles 全量回溯
// 运行环境：Node 20（Actions 已是 node20，自带 fetch）
// 输出：okx_eth_15m.csv  (ts, iso, open, high, low, close, vol)

const PROXY_BASE = 'https://eth-proxy.1053363050.workers.dev'; // 你的 Cloudflare Worker
const INST_ID = 'ETH-USDT';          // 现货历史更完整；若要永续换成 'ETH-USDT-SWAP'
const BAR = '15m';
const CSV_PATH = 'okx_eth_15m.csv';

// 拉 365 天；如需更久改这个天数
const DAYS = 365;

// OKX 返回的蜡烛字段顺序：
// [0] ts, [1] o, [2] h, [3] l, [4] c, [5] vol, [6] volCcy, [7] volCcyQuote, [8] confirm
// 返回时间为毫秒时间戳，且数组是“新到旧”或“旧到新”视接口而定；history-candles 通常是“新到旧”
async function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

async function okxFetchHistory({before, limit = 300}) {
  // 用 history-candles，按 before 从现在往过去翻页
  const q = new URLSearchParams({
    instId: INST_ID,
    bar: BAR,
    limit: String(limit),
    before: String(before),
  });
  const target = `https://www.okx.com/api/v5/market/history-candles?${q.toString()}`;
  const url = `${PROXY_BASE}/proxy?url=${encodeURIComponent(target)}`;

  for (let i = 1; i <= 5; i++) {
    try {
      const resp = await fetch(url, { headers: { 'accept': 'application/json' } });
      const txt = await resp.text();
      // Worker 直接原样转发 OKX 的 JSON；容错下
      const json = JSON.parse(txt);
      if (json && json.code === '0' && Array.isArray(json.data)) return json.data;
      throw new Error(`bad payload: ${txt.slice(0, 200)}`);
    } catch (e) {
      if (i === 5) throw e;
      await sleep(600 * i);
    }
  }
}

function msToIso(ms) {
  const d = new Date(Number(ms));
  return d.toISOString().replace('.000', '');
}

async function main() {
  const now = Date.now();
  const start = now - DAYS * 24 * 60 * 60 * 1000;

  console.log(`Start fetching ${INST_ID} ${BAR} for last ${DAYS} days...`);
  console.log(`Target range: ${msToIso(start)}  ->  ${msToIso(now)}`);

  let before = now;      // 翻页锚点：每次取 “before” 之前的数据（更早）
  const all = [];
  const seen = new Set(); // 去重 (ts)

  // 估算最多页数：一年 35k 根，limit=300 => ~120 页，留足保险
  for (let page = 1; page <= 300; page++) {
    const batch = await okxFetchHistory({ before, limit: 300 });
    if (!batch || batch.length === 0) {
      console.log('Empty batch, stop.');
      break;
    }

    // OKX history-candles 通常按“新 -> 旧”返回；我们反转成“旧 -> 新”更好处理
    batch.reverse();

    let earliestTsInBatch = null;

    for (const k of batch) {
      const [ts, o, h, l, c, vol] = k; // 只取前六个字段
      const t = Number(ts);
      earliestTsInBatch = earliestTsInBatch == null ? t : Math.min(earliestTsInBatch, t);

      if (t < start) continue; // 超过目标范围就丢
      if (t > now) continue;

      if (!seen.has(t)) {
        seen.add(t);
        all.push({
          ts: t,
          iso: msToIso(t),
          open: o, high: h, low: l, close: c, vol: vol
        });
      }
    }

    console.log(`Page ${page}: +${batch.length} (kept ${all.length}), earliest=${msToIso(earliestTsInBatch)}`);

    // 下一页锚点：比这一页最早的再早一点点
    before = (earliestTsInBatch ?? before) - 1;

    // 终止判断
    if (earliestTsInBatch != null && earliestTsInBatch <= start) {
      console.log('Reached start boundary.');
      break;
    }

    // 轻微限速
    await sleep(700);
  }

  // 按时间升序排一下
  all.sort((a, b) => a.ts - b.ts);

  // 写 CSV
  const header = 'ts,iso,open,high,low,close,vol\n';
  const lines = all.map(r => [
    r.ts, r.iso, r.open, r.high, r.low, r.close, r.vol
  ].join(','));
  const fs = await import('node:fs/promises');
  await fs.writeFile(CSV_PATH, header + lines.join('\n'), 'utf8');

  console.log(`DONE. rows=${all.length}, file=${CSV_PATH}`);
  if (all.length < 30000) {
    console.log('WARN: rows < 30k，可能没覆盖满一年（上游限流/网络中断）。');
    process.exitCode = 0; // 仍然提交，但你会在日志里看到 WARN
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
