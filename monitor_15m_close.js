// ETH 15m close monitor (OKX) → status.json + ntfy
// Node 20+, no deps

const fs = require('fs');

const NTFY_SERVER = (process.env.NTFY_SERVER || 'https://ntfy.sh').replace(/\/+$/, '');
const NTFY_TOPIC  = process.env.NTFY_TOPIC || 'ETH15_DUI';
const TARGET_URL  = process.env.TARGET_URL || 'https://example.com/';

const STATUS_FILE = 'status.json';

// ---- Utils ----
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));

function ema(values, period) {
  // returns full EMA series (same length)
  if (values.length === 0) return [];
  const k = 2 / (period + 1);
  const out = new Array(values.length);
  // seed with SMA
  let sum = 0;
  for (let i = 0; i < period && i < values.length; i++) sum += values[i];
  out[period - 1] = sum / period;
  for (let i = period; i < values.length; i++) {
    out[i] = values[i] * k + out[i - 1] * (1 - k);
  }
  // fill head with first known
  for (let i = 0; i < period - 1 && i < values.length; i++) out[i] = out[period - 1];
  return out;
}

function pct(a, b) {
  return b === 0 ? 0 : (a - b) / b; // as fraction
}

function toISO(tsMs) {
  return new Date(tsMs).toISOString();
}

async function fetchOkxKlines() {
  // 200 根 15m K
  const url = 'https://www.okx.com/api/v5/market/candles?instId=ETH-USDT-SWAP&bar=15m&limit=200';
  const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
  if (!res.ok) throw new Error(`OKX ${res.status} ${res.statusText}`);
  const json = await res.json();
  if (!json.data || !Array.isArray(json.data) || json.data.length === 0) {
    throw new Error('OKX no data');
  }
  // OKX 返回时间倒序： [ [ts, o,h,l,c,vol,...], ... ]
  const rows = json.data.map(r => ({
    ts: Number(r[0]),
    open: Number(r[1]),
    high: Number(r[2]),
    low:  Number(r[3]),
    close:Number(r[4])
  })).sort((a,b)=> a.ts - b.ts); // 升序
  return rows;
}

function decideSignal(rows) {
  const closes = rows.map(r => r.close);
  const ema34 = ema(closes, 34);
  const ema144 = ema(closes, 144);

  const last = rows[rows.length - 1];
  const prev = rows[rows.length - 2];

  const c = last.close;
  const e34 = ema34[ema34.length - 1];
  const e144 = ema144[ema144.length - 1];

  const e34_10 = ema34[ema34.length - 11];
  const e144_10 = ema144[ema144.length - 11];

  const dist34 = pct(c, e34);
  const dist144 = pct(c, e144);

  const slope34 = pct(e34, e34_10);     // 10 根内 EMA%变化
  const slope144 = pct(e144, e144_10);

  const between = (c >= Math.min(e34, e144) && c <= Math.max(e34, e144));
  const nearAny = Math.min(Math.abs(dist34), Math.abs(dist144)) <= 0.005; // <=0.5%
  const flat = Math.abs(slope34) <= 0.003 && Math.abs(slope144) <= 0.002; // 0.3% / 0.2%

  const ok = (between || nearAny) && flat;

  const direction = e34 >= e144 ? 'ETH 向上' : 'ETH 向下';

  return {
    ok,
    direction,
    last_candle_iso: toISO(last.ts),
    price: c,
    ema34: e34,
    ema144: e144,
    dist34,
    dist144,
    slope34,
    slope144
  };
}

function readStatus() {
  try {
    const t = fs.readFileSync(STATUS_FILE, 'utf8');
    return JSON.parse(t);
  } catch {
    return null;
  }
}

function writeStatus(obj) {
  fs.writeFileSync(STATUS_FILE, JSON.stringify(obj, null, 2), 'utf8');
}

async function pushNtfy(payload) {
  // 头部必须 ASCII；中文放 body
  const titleAscii = payload.ok ? 'OPEN BOTH' : 'NO ENTRY';
  const tagsAscii  = payload.ok ? 'white_check_mark' : 'x';

  const body =
`信号：${payload.ok ? '✅ 可开双向' : '❌ 暂不建议'}
方向：${payload.direction}
收盘(UTC)：${payload.last_candle_iso}
当前价=${payload.price.toFixed(2)}，EMA34=${payload.ema34.toFixed(2)}，EMA144=${payload.ema144.toFixed(2)}
均线距离：${(payload.dist34 * 100).toFixed(3)}% / ${(payload.dist144 * 100).toFixed(3)}%
斜率(10根)：${(payload.slope34 * 100).toFixed(3)}% / ${(payload.slope144 * 100).toFixed(3)}%
对冲 Meme：${payload.meme || '-'}
规则：ETH止损6%/止盈10%；Meme止损10%/止盈10%；+8%保本，+15%启用2%移动止盈。
查看：${TARGET_URL}`;

  const res = await fetch(`${NTFY_SERVER}/${encodeURIComponent(NTFY_TOPIC)}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
      'X-Title': titleAscii,
      'X-Tags': tagsAscii,
      'X-Priority': '4'
    },
    body
  });
  if (!res.ok) {
    const t = await res.text().catch(()=>'');
    throw new Error(`ntfy push failed: ${res.status} ${res.statusText} ${t}`);
  }
}

function getMemeChoice() {
  // 这里先占位（固定列表，未来你要接交易所 API 可再扩展）
  // 简单策略：按固定优先顺序选择
  const list = ['PEPE', 'DOGE', 'SHIB', 'FLOKI'];
  return list[0];
}

async function main() {
  // 1) 拉数
  const rows = await fetchOkxKlines();

  // 2) 只在 15m 收盘之后的 1 分钟内触发：判断 now 距离 last.ts
  const now = Date.now();
  const lastTs = rows[rows.length - 1].ts;
  const sinceMs = now - lastTs;
  const fifteenMin = 15 * 60 * 1000;

  // 若最新 K 线还在进行中（即距离上根收盘 < 15min），不推送，但仍更新“最近检测时间”
  // 为了只在收盘后 1 分钟窗口内发，做一个窗口限制（<= 70 秒），避免重复。
  let canNotify = false;
  if (sinceMs >= 0 && sinceMs <= 70 * 1000) {
    canNotify = true;
  }

  // 3) 计算信号
  const s = decideSignal(rows);
  s.meme = getMemeChoice();

  // 4) 读取旧状态，判断是否“状态变化”
  const old = readStatus();
  const prevSignal = old ? (old.ok ? 'ok' : 'no') : 'none';
  const currSignal = s.ok ? 'ok' : 'no';
  const changed = prevSignal !== currSignal;

  // 5) 准备写入 status.json
  const out = {
    ok: s.ok,
    direction: s.direction,
    last_candle_iso: s.last_candle_iso,
    price: s.price,
    ema34: s.ema34,
    ema144: s.ema144,
    dist34: s.dist34,
    dist144: s.dist144,
    slope34: s.slope34,
    slope144: s.slope144,
    meme: s.meme,
    last_check_iso: toISO(Date.now()),
    last_signal: currSignal
  };
  writeStatus(out);

  // 6) 仅当 (状态变化 && 确认是收盘后窗口内) 才推送
  if (changed && canNotify) {
    await pushNtfy(out);
  }
}

main().catch(async (err) => {
  console.error(err);
  // 出错也尽量写入一个可读状态，方便页面提示
  const fallback = {
    ok: false,
    direction: '-',
    last_candle_iso: '1970-01-01T00:00:00Z',
    price: 0,
    ema34: 0,
    ema144: 0,
    dist34: 0,
    dist144: 0,
    slope34: 0,
    slope144: 0,
    meme: '-',
    last_check_iso: toISO(Date.now()),
    last_signal: 'no',
    error: String(err && err.message || err)
  };
  try { writeStatus(fallback); } catch {}
  process.exit(1);
});
