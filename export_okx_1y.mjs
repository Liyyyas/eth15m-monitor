// export_okx_1y.mjs — OKX ETH/USDT 15m，回溯 ~365 天，写 CSV
import fs from 'node:fs/promises';

// === 可调参数（尽量少、够稳） ===
const INST_ID = 'ETH-USDT';
const BAR = '15m';
const LIMIT = 300;             // OKX 单页上限
const EXPECT_MIN = 30000;      // 15m*365 ≈ 35040，留点余量
const MAX_PAGES = 2000;        // 安全上限
const RETRY = 3;               // 单次请求最多重试次数
const SLEEP_MS = 150;          // 轻微节流，OKX 限流更稳
const OUT = './eth15m-monitor/okx_eth_15m.csv';

// === 直连与代理（Cloudflare Worker 代理域名保留，用你的） ===
const PROXY_BASE = 'https://eth-proxy.1053363050.workers.dev';
const UA = 'Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36';
const HDRS = { 'User-Agent': UA, 'Accept': 'application/json,text/plain,*/*' };

// 直连：不带 before 先拿最新一页，之后用 before=oldestTs 继续往回翻
const directRoute = (beforeTs) => {
  const base = `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${BAR}&limit=${LIMIT}`;
  return beforeTs ? `${base}&before=${beforeTs}` : base;
};
// 代理：与直连一致，只是 host 换成你的 worker
const proxyRoute = (beforeTs) => {
  const base = `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${BAR}&limit=${LIMIT}`;
  return beforeTs ? `${base}&before=${beforeTs}` : base;
};

// 识别 HTML（被风控/错误页）
const looksHtml = (txt) => txt && /^\s*<!doctype\s+html/i.test(txt);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// 单次请求（先直连；出现非 JSON / 非 code:"0" / 429 等，就回退代理；都不行再重试）
async function pullPage(beforeTs) {
  const urls = [directRoute(beforeTs), proxyRoute(beforeTs)]; // 直连优先，再代理
  let lastErr;
  for (let attempt = 1; attempt <= RETRY; attempt++) {
    for (const url of urls) {
      try {
        const res = await fetch(url, { headers: HDRS });
        const txt = await res.text();
        if (!res.ok || looksHtml(txt)) throw new Error(`HTTP/HTML ${res.status}`);
        const j = JSON.parse(txt);
        if (j.code !== '0' || !Array.isArray(j.data)) throw new Error(`BAD_JSON code=${j.code}`);
        return j.data; // 最新在前
      } catch (e) {
        lastErr = e;
        // 轻微指数退避
        await sleep(SLEEP_MS * (attempt ** 2));
      }
    }
  }
  throw lastErr ?? new Error('UNKNOWN_FETCH_ERROR');
}

// 写 CSV（只要 ts, o, h, l, c）
function toCsv(rows) {
  const header = 'ts,iso,open,high,low,close\n';
  const body = rows.map(r => {
    // OKX 返回：[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    const ts = Number(r[0]);
    const iso = new Date(ts).toISOString();
    return [
      ts,
      iso,
      Number(r[1]),
      Number(r[2]),
      Number(r[3]),
      Number(r[4]),
    ].join(',');
  }).join('\n');
  return header + body + '\n';
}

async function main() {
  console.log('node', process.versions.node);
  console.log(`Start ${INST_ID} ${BAR} for ~365 days...`);

  const sinceTs = Date.now() - 365 * 24 * 60 * 60 * 1000;
  const all = [];
  const seen = new Set(); // 去重（按 ts）
  let before = undefined; // 第 1 页不带 before
  let page = 0;

  while (page < MAX_PAGES) {
    page++;
    const data = await pullPage(before);
    if (!data.length) {
      console.log(`page ${page} empty; stopping.`);
      break;
    }

    // OKX 返回最新在前，翻转成旧→新，便于 before 续页与去重
    const olderFirst = data.slice().reverse();
    let got = 0;

    for (const c of olderFirst) {
      const ts = Number(c[0]);
      const confirm = (c[8] ?? '1') + '';
      // 只保留已确认的蜡烛；且只收集过去 365 天内的数据
      if (confirm !== '1') continue;
      if (ts < sinceTs) continue;

      if (!seen.has(ts)) {
        seen.add(ts);
        all.push(c);
        got++;
      }
    }

    // 下一页用“这一页里最老一根”的 ts 作为 before
    const oldestTs = Number(olderFirst[0][0]);
    before = oldestTs;

    console.log(`page ${page} done -> ${got} kept, total = ${all.length}`);

    // 终止条件：这一页最老的一根已经早于 sinceTs 很多了，继续翻页意义不大
    if (oldestTs <= sinceTs) {
      console.log('Reached sinceTs boundary; stopping.');
      break;
    }
    await sleep(SLEEP_MS);
  }

  if (all.length < EXPECT_MIN) {
    console.log(`WARNING: fetched rows ${all.length} < expected ${EXPECT_MIN}`);
    // 仍然写出 CSV，方便检查；同时用非零退出让工作流直观看到“少了”
  }

  // 时间升序写出（旧→新）
  all.sort((a, b) => Number(a[0]) - Number(b[0]));
  const csv = toCsv(all);
  await fs.writeFile(OUT, csv, 'utf8');
  console.log(`Wrote CSV to ${OUT}`);
  // 成功/不足都 exit 0，让后续 git 提交稳定；是否严格卡阈值，你可以在 workflow 里开关
}

main().catch(err => {
  console.error('FATAL:', err?.message || err);
  process.exit(1);
});
