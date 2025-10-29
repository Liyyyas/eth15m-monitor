// export_okx_1y.mjs
// 完整版本：从 OKX 拉取 15m K 线，1 年左右（或直到达到 EXPECT_MIN_ROWS）
// 放在 repository 根目录，workflow 中运行 `node export_okx_1y.mjs`

import fs from 'node:fs/promises';
import path from 'node:path';

const INST_ID = 'ETH-USDT';          // 仓库里是 ETH-USDT 还是 ETH_USDT，请与 OKX API 使用一致
const BAR = '15m';
const PAGE_LIMIT = 300;              // OKX 单页最大（保持 300）
const EXPECT_MIN_ROWS = 35000;       // 期望的最少行数（约 1 年 15m -> ~35040）
const MAX_PAGES = 2000;              // 防止无限循环（安全上限）
const SLEEP_MS_BASE = 1000;          // 重试基数
const RETRY_MAX = 5;
const OUT_PATH = './okx_eth_15m.csv'; // 输出路径（相对 workflow 工作目录）
const PROXY_BASE = 'https://eth-proxy.1053363050.workers.dev'; // 若无代理可设为 '' 空串

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36';
const HEADERS = {
  'User-Agent': UA,
  'Accept': 'application/json,text/html,application/xhtml+xml,*/*',
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isHtml(txt) {
  if (!txt) return false;
  // 简单判断，如果里面有 <!DOCTYPE html 或 <html 或 <script> 且没有 JSON 结构，则判定为 HTML
  const small = txt.slice(0, 200).toLowerCase();
  return /<\s*!doctype|<\s*html|<\s*script/.test(small);
}

function okxDirectUrl(beforeTs) {
  // OKX v5 history-candles 示例： /api/v5/market/history-candles?instId=ETH-USDT&bar=15m&limit=300&before=timestamp
  const b = encodeURIComponent(String(beforeTs));
  return `https://www.okx.com/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}&before=${b}`;
}

function proxyUrl(beforeTs) {
  if (!PROXY_BASE) return null;
  // 我们期望 worker 能把 query 转发，如果你的 worker 路径不同可自行调整
  const b = encodeURIComponent(String(beforeTs));
  return `${PROXY_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(INST_ID)}&bar=${encodeURIComponent(BAR)}&limit=${PAGE_LIMIT}&before=${b}`;
}

async function fetchJsonWithFallback(urlDirect, urlProxy, attempt = 1) {
  // 先直连，失败或返回 HTML 则尝试 proxy（如果有）
  let lastErr = null;
  // try direct
  try {
    const r = await fetch(urlDirect, { headers: HEADERS });
    const txt = await r.text();
    if (isHtml(txt)) {
      lastErr = new Error('HTML_FROM_OKX');
      lastErr.raw = txt;
      throw lastErr;
    }
    // try parse
    const j = JSON.parse(txt);
    return j;
  } catch (e) {
    lastErr = e;
    // 如果有 proxy，尝试 proxy
    if (urlProxy) {
      try {
        const r2 = await fetch(urlProxy, { headers: HEADERS });
        const txt2 = await r2.text();
        if (isHtml(txt2)) {
          const err = new Error('HTML_FROM_PROXY');
          err.raw = txt2;
          throw err;
        }
        const j2 = JSON.parse(txt2);
        return j2;
      } catch (e2) {
        lastErr = e2;
      }
    }
  }

  // 重试或抛出
  if (attempt < RETRY_MAX) {
    const backoff = SLEEP_MS_BASE * Math.pow(2, attempt);
    await sleep(backoff);
    return fetchJsonWithFallback(urlDirect, urlProxy, attempt + 1);
  }
  throw lastErr;
}

function csvEscapeCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

async function ensureOutDir(outPath) {
  const dir = path.dirname(outPath);
  if (dir && dir !== '.') {
    await fs.mkdir(dir, { recursive: true });
  }
}

async function main() {
  console.log(`Start fetching ${INST_ID} ${BAR} for last ~365 days...`);
  const now = Date.now();
  const oneDayMs = 24 * 60 * 60 * 1000;
  const startTime = now - 365 * oneDayMs;
  let beforeTs = Date.now(); // 从现在开始往回
  let all = [];
  const seen = new Set();
  let page = 0;

  while (all.length < EXPECT_MIN_ROWS && page < MAX_PAGES) {
    page++;
    const direct = okxDirectUrl(beforeTs);
    const proxy = proxyUrl(beforeTs);

    console.log(`page ${page} -> fetching before=${beforeTs} (direct)`);
    let j;
    try {
      j = await fetchJsonWithFallback(direct, proxy);
    } catch (err) {
      console.error(`Failed to fetch page ${page}:`, err && err.message ? err.message : err);
      // 如果收到 route not allowed 或 code !== '0' 或 data empty，则可能是被限流或 worker 未允许该 route
      // 等待一段时间再试（退避）
      if (page >= MAX_PAGES) break;
      await sleep(SLEEP_MS_BASE * 5);
      continue;
    }

    // OKX typical response: { code: '0', msg:'', data: [[ts, open,...,confirm], ...] }
    if (!j || (typeof j !== 'object')) {
      console.warn('Bad json response, skip page');
      await sleep(SLEEP_MS_BASE * 2);
      continue;
    }

    // Some proxies/wrappers return { code: -1, msg: 'Route not allowed', data: [] }
    if ('code' in j && j.code !== '0' && j.code !== 0) {
      console.warn('OKX returned non-zero code:', j.code, j.msg || '');
      // 如果 route not allowed，说明 worker 需要配置允许该路径，等待并跳 proxy/direct 切换
      // 重试小延时
      await sleep(SLEEP_MS_BASE * 3);
      continue;
    }

    const data = Array.isArray(j.data) ? j.data : (j.data && Array.isArray(j.data.candles) ? j.data.candles : null);
    if (!data || data.length === 0) {
      console.log(`page ${page} empty; stopping.`);
      break;
    }

    // process candles
    // each candle: [ts, open, high, low, close, volume, ... maybe confirm '1' or '0' at index 8]
    let lastTsOnPage = null;
    for (const candle of data) {
      if (!Array.isArray(candle) || candle.length < 6) continue;
      const ts = Number(candle[0]);
      if (!ts) continue;
      // Some APIs append confirm flag at index 8 (as string '1'); if present and confirm !== '1' => skip
      const confirm = (candle.length > 8 ? String(candle[8]) : '1');
      if (confirm !== '1') {
        // 跳过未确认 K 线（可能是正在生成的当前 candle）
        continue;
      }
      if (seen.has(ts)) continue;
      seen.add(ts);
      lastTsOnPage = ts;
      const row = {
        ts,
        iso: new Date(Number(ts)).toISOString(),
        open: candle[1],
        high: candle[2],
        low: candle[3],
        close: candle[4],
        vol: candle[5]
      };
      all.push(row);
    }

    console.log(`page ${page} fetched, got ${data.length} raw, ${all.length} total after dedupe.`);

    // 准备下一页：使用最后一根 candle 的时间戳 - 1
    if (!lastTsOnPage) {
      console.log('No confirmed candles found on page; stop.');
      break;
    }
    beforeTs = lastTsOnPage - 1;

    // 防止短时间内过快请求
    await sleep(300); // 0.3s 短暂间隔
  }

  // 排序保序（降序 - 我们是从最新往旧取的，按时间戳升序存 CSV 更直观）
  all.sort((a, b) => a.ts - b.ts);

  console.log(`Done fetch: total rows = ${all.length}`);

  // 如果数据不足，仍旧写出（但返回非零以便 workflow 注意）
  await ensureOutDir(OUT_PATH);

  // 写 CSV
  const header = ['ts', 'iso', 'open', 'high', 'low', 'close', 'vol'].join(',') + '\n';
  const lines = [header];
  for (const r of all) {
    const row = [
      csvEscapeCell(r.ts),
      csvEscapeCell(r.iso),
      csvEscapeCell(r.open),
      csvEscapeCell(r.high),
      csvEscapeCell(r.low),
      csvEscapeCell(r.close),
      csvEscapeCell(r.vol)
    ].join(',') + '\n';
    lines.push(row);
  }
  await fs.writeFile(OUT_PATH, lines.join(''), 'utf8');
  console.log(`Wrote CSV to ${OUT_PATH}`);

  if (all.length < EXPECT_MIN_ROWS) {
    console.warn(`WARNING: fetched rows ${all.length} < expected ${EXPECT_MIN_ROWS}.`);
    // 退出码 0 也可以，但为了提醒 workflow 我们用非零
    process.exitCode = 1;
  } else {
    process.exitCode = 0;
  }
}

main().catch((err) => {
  console.error('Fatal error:', err && err.stack ? err.stack : err);
  process.exitCode = 2;
});
