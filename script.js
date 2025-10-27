// ============ 配置 ============
const REFRESH_MINUTES = 15;            // 自动刷新周期（分钟）
const BARS_FOR_EMA34  = 34;
const BARS_FOR_EMA144 = 144;
const SLOPE_LOOKBACK  = 10;

const MEME_LIST = ['PEPE','DOGE','SHIB','FLOKI'];

// OKX API
const OKX_KLINES = 'https://www.okx.com/api/v5/market/candles?instId=ETH-USDT&bar=15m&limit=200';
const OKX_TICKER = sym => `https://www.okx.com/api/v5/market/ticker?instId=${sym}-USDT`;
const PROXY = 'https://api.allorigins.win/raw?url=';

// ============ DOM ============
const elSignal   = document.getElementById('signal');
const elPrice    = document.getElementById('price');
const elHL       = document.getElementById('hl');
const elDist     = document.getElementById('dist');
const elSlope    = document.getElementById('slope');
const elHedge    = document.getElementById('hedge');
const elBtnNow   = document.getElementById('btnNow');
const elBtnNotif = document.getElementById('btnNotify');
const elNextAt   = document.getElementById('nextAt');
const elCountdown= document.getElementById('countdown');

// ============ 工具 ============
const fmt2 = n => Number(n).toFixed(2);
const pct  = n => (n*100).toFixed(3) + '%';
function hhmm(d=new Date()){return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');}

function ema(values, period){
  const k = 2/(period+1);
  let e = values[0];
  for(let i=1;i<values.length;i++){ e = values[i]*k + e*(1-k); }
  return e;
}

async function ensureNotifyPermission(){
  if (!('Notification' in window)) return false;
  if (Notification.permission === 'granted') return true;
  if (Notification.permission !== 'denied'){
    const p = await Notification.requestPermission(); return p === 'granted';
  }
  return false;
}
function pushNotify(title, body){
  if (('Notification' in window) && Notification.permission === 'granted'){
    new Notification(title, {body});
  }
}

// ============ 数据 ============
async function fetchKlines(){
  const res = await fetch(PROXY + encodeURIComponent(OKX_KLINES), {cache:'no-store'});
  if (!res.ok) throw new Error('网络错误: '+res.status);
  const json = await res.json();
  const rows = json.data || [];
  if (!rows.length) throw new Error('无数据');
  return rows.slice().reverse().map(r=>({
    ts: Number(r[0]),
    open: Number(r[1]),
    high: Number(r[2]),
    low:  Number(r[3]),
    close:Number(r[4]),
  }));
}

// 拉取单个 ticker（返回 last 与 24h 成交额估算 USDT）
async function fetchOneTicker(sym){
  const r = await fetch(PROXY + encodeURIComponent(OKX_TICKER(sym)), {cache:'no-store'});
  if (!r.ok) throw new Error('ticker错误');
  const j = await r.json();
  const d = (j.data && j.data[0]) || {};
  const last = Number(d.last || d.lastPx || d.askPx || d.bidPx || 0);
  // vol24h 是 base 数量，这里用 base量 * last 估算 24h USDT 成交额
  const volBase = Number(d.vol24h || d.vol || 0);
  const volUsd  = volBase * last;
  return {sym,last,volUsd};
}

// 选出 24h 成交额最大的 meme
async function pickHedgeMeme(){
  const all = await Promise.allSettled(MEME_LIST.map(fetchOneTicker));
  const ok = all.filter(x=>x.status==='fulfilled').map(x=>x.value);
  if (!ok.length) throw new Error('Meme 数据获取失败');
  ok.sort((a,b)=> b.volUsd - a.volUsd);
  return {best: ok[0], list: ok};
}

// ============ 分析 ============
const SIGNAL_KEY = 'last_signal_state'; // 'USE'|'NO_USE'

async function analyze(){
  try{
    elSignal.textContent = '加载中…';
    elHedge.textContent  = '加载中…';

    const [kl, hedge] = await Promise.all([fetchKlines(), pickHedgeMeme()]);
    const closes = kl.map(k=>k.close);

    if (closes.length < Math.max(BARS_FOR_EMA34,BARS_FOR_EMA144)+SLOPE_LOOKBACK+1)
      throw new Error('样本不足');

    const latest = kl[kl.length-1];
    const price  = latest.close;

    const ema34  = ema(closes, BARS_FOR_EMA34);
    const ema144 = ema(closes, BARS_FOR_EMA144);

    const dist34  = Math.abs(price-ema34)/price;
    const dist144 = Math.abs(price-ema144)/price;

    const closesAgo = closes.slice(0, closes.length - SLOPE_LOOKBACK);
    const ema34Ago  = ema(closesAgo, BARS_FOR_EMA34);
    const ema144Ago = ema(closesAgo, BARS_FOR_EMA144);
    const slope34   = (ema34 - ema34Ago)/ema34;
    const slope144  = (ema144 - ema144Ago)/ema144;

    const near34   = dist34  <= 0.005;
    const near144  = dist144 <= 0.005;
    const nearBand = (price>=Math.min(ema34,ema144) && price<=Math.max(ema34,ema144));
    const flat34   = Math.abs((ema34-ema34Ago)/ema34)   <= 0.003;
    const flat144  = Math.abs((ema144-ema144Ago)/ema144)<= 0.002;

    const useCondition = ( (nearBand || (near34&&near144)) && flat34 && flat144 );

    // ===== 对冲 Meme 展示 =====
    const best = hedge.best;
    const listLine = hedge.list
      .map(x=>`${x.sym}: 24h≈$${fmt2(x.volUsd/1e6)}M`)
      .join('  ·  ');
    elHedge.textContent = `推荐：${best.sym}（24h≈$${fmt2(best.volUsd/1e6)}M） | 明细：${listLine}`;

    // ===== 页面展示 =====
    if (useCondition){
      elSignal.innerHTML =
        `<span class="ok">✅ 可开双向</span> | ` +
        `close=${fmt2(price)}, EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}, 距离=${pct((ema34-price)/price)}/${pct((ema144-price)/price
