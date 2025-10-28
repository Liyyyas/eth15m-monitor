// monitor_15m_close.js
// 云端每分钟跑：仅在“出现新收盘的15m K线”时计算 & 推送到 ntfy，并把状态写入 status.json

const fs = require('fs');
const path = require('path');

const NTFY_SERVER = process.env.NTFY_SERVER || 'https://ntfy.sh';
const NTFY_TOPIC  = process.env.NTFY_TOPIC || '';      // 为空则不推送

const INST_ID = 'ETH-USDT';
const BAR = '15m';
const STATE_DIR = path.join(process.cwd(), '.state');
const STATE_FILE = path.join(STATE_DIR, 'last_ts.txt');
const STATUS_JSON = path.join(process.cwd(), 'status.json');

const pct = (a,b)=> (a-b)/b*100;
const fmt = (n,d=2)=> Number(n).toFixed(d);

function ema(vals, p){
  if (vals.length < p) return [];
  const k = 2/(p+1);
  const out = [];
  const sma = vals.slice(0,p).reduce((a,b)=>a+b,0)/p;
  out[p-1]=sma;
  for (let i=p;i<vals.length;i++) out[i] = vals[i]*k + out[i-1]*(1-k);
  return out;
}

async function okxJSON(url){
  const r = await fetch(url, { headers: { 'cache-control':'no-cache' }});
  const j = await r.json();
  if (!j || j.code !== '0') throw new Error('OKX API error: '+JSON.stringify(j));
  return j.data;
}

async function getCandles(instId=INST_ID, bar=BAR, limit=210){
  const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${bar}&limit=${limit}`;
  const data = await okxJSON(url);
  // OKX candles 是倒序，这里翻为正序
  return data.map(x=>({
    ts:+x[0], open:+x[1], high:+x[2], low:+x[3], close:+x[4]
  })).reverse();
}

async function pickMeme(){
  // 候选池：成交额最高者；若API字段缺失，回退按 volCcy/volCcyQuote；都没有则默认PEPE
  const candidates = ['PEPE-USDT','DOGE-USDT','SHIB-USDT','FLOKI-USDT'];
  const tickers = await okxJSON('https://www.okx.com/api/v5/market/tickers?instType=SPOT');

  let best = null;
  for (const sym of candidates){
    const row = tickers.find(x=>x.instId===sym);
    if (!row) continue;
    const vol = parseFloat(row.volCcyQuote || row.volCcy || row.vol || '0'); // 多重兜底
    if (!best || vol > best.vol) best = { sym: sym.split('-')[0], vol };
  }
  return best?.sym || 'PEPE';
}

function computeSignal(closes){
  if (closes.length < 160) return { ready:false };
  const e34 = ema(closes,34);
  const e144= ema(closes,144);
  const i = closes.length - 1;

  const c = closes[i];
  const a = e34[i];
  const b = e144[i];

  const d34 = pct(c,a);
  const d144= pct(c,b);
  const s34 = pct(e34[i], e34[i-10]);
  const s144= pct(e144[i], e144[i-10]);

  const near = Math.abs(d34)<=0.5 && Math.abs(d144)<=0.5;
  const flat = Math.abs(s34)<=0.3 && Math.abs(s144)<=0.2;
  const use  = near && flat;
  const direction = a>=b ? 'ETH 多' : 'ETH 空';

  return { ready:true, use, direction, c,a,b, d34,d144, s34,s144 };
}

async function pushNtfy(title, body){
  if (!NTFY_TOPIC) return;
  const url = `${NTFY_SERVER.replace(/\/+$/,'')}/${encodeURIComponent(NTFY_TOPIC)}`;
  const r = await fetch(url, {
    method:'POST',
    headers:{
      'Title': title,
      'Tags': title.includes('✅') ? 'white_check_mark,rocket' : 'x',
      'Content-Type':'text/plain; charset=utf-8'
    },
    body
  });
  if (!r.ok) throw new Error(`ntfy push failed: ${r.status} ${r.statusText}`);
}

function ensureDir(p){ if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive:true }); }

function readLastTs(){
  try{
    if (fs.existsSync(STATE_FILE)) {
      const s = fs.readFileSync(STATE_FILE,'utf8').trim();
      const n = Number(s);
      return Number.isFinite(n) ? n : 0;
    }
  }catch(e){}
  return 0;
}

function writeLastTs(ts){
  ensureDir(STATE_DIR);
  fs.writeFileSync(STATE_FILE, String(ts));
}

function writeStatusJSON(payload){
  fs.writeFileSync(STATUS_JSON, JSON.stringify(payload, null, 2));
}

(async ()=>{
  // 1) 拉K线
  const candles = await getCandles();
  if (candles.length < 2) {
    console.log('Not enough candles');
    return;
  }

  // 2) 仅处理“上一根已收盘”的K线
  const lastClosed = candles[candles.length-2];
  const lastTs = readLastTs();

  // 3) 若这根已处理过，则仅更新 status.json 的“最近检测时间”（不重复推送/不提交）
  const nowIso = new Date().toISOString();
  if (lastClosed.ts === lastTs) {
    // 读取原状态（如有）并刷新最近检测时间
    let prev = {};
    try { prev = JSON.parse(fs.readFileSync(STATUS_JSON,'utf8')); } catch {}
    prev.last_check_iso = nowIso;
    writeStatusJSON(prev);
    console.log('Same closed candle. Updated last_check only.');
    return;
  }

  // 4) 计算指标（只到收盘那根为止）
  const closes = candles.map(c=>c.close);
  const idx = candles.length-2;
  const closesTillClosed = closes.slice(0, idx+1);
  const sig = computeSignal(closesTillClosed);
  if (!sig.ready) {
    console.log('Not ready for EMA calc.');
    return;
  }

  // 5) 选择对冲 Meme
  const meme = await pickMeme();

  // 6) 生成消息 & 推送
  const candleIso = new Date(lastClosed.ts).toISOString();
  const title = sig.use ? '✅ 可开双向' : '❌ 暂不建议';
  const body =
`${title} ｜ ${sig.direction} ｜ 对冲：${meme}
收盘时间(UTC)：${candleIso}
close=${fmt(sig.c)}, EMA34=${fmt(sig.a)}, EMA144=${fmt(sig.b)}
距离：${fmt(sig.d34)}% / ${fmt(sig.d144)}% · 斜率10：${fmt(sig.s34)}% / ${fmt(sig.s144)}%
规则：ETH止损6%/止盈10%；Meme止损10%/止盈10%；+8%保本，+15%启用2%移动止盈
最近检测：${nowIso}`;

  await pushNtfy(title, body);

  // 7) 写入状态文件与网页用的 JSON（用于“最近检测”展示）
  const statusPayload = {
    last_candle_ts: lastClosed.ts,
    last_candle_iso: candleIso,
    last_check_iso: nowIso,
    use: sig.use,
    direction: sig.direction,
    close: +fmt(sig.c,2),
    ema34: +fmt(sig.a,2),
    ema144:+fmt(sig.b,2),
    d34: +fmt(sig.d34,4),
    d144:+fmt(sig.d144,4),
    s34: +fmt(sig.s34,4),
    s144:+fmt(sig.s144,4),
    meme
  };
  writeStatusJSON(statusPayload);
  writeLastTs(lastClosed.ts);

  console.log('Pushed & updated status for closed candle:', lastClosed.ts);
})().catch(e=>{
  console.error('[FATAL]', e.stack || e.message || e);
  process.exit(1);
});
