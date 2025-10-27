/************ 配置 ************/
const REFRESH_MINUTES = 15;
const BARS_FOR_EMA34  = 34;
const BARS_FOR_EMA144 = 144;
const SLOPE_LOOKBACK  = 10;

const MEME_LIST = ['PEPE','DOGE','SHIB','FLOKI'];

// OKX API (HTTPS)
const API_KLINES = 'https://www.okx.com/api/v5/market/candles?instId=ETH-USDT&bar=15m&limit=200';
const API_TICKER = (sym)=> `https://www.okx.com/api/v5/market/ticker?instId=${sym}-USDT`;

/************ 代理链（按顺序尝试） ************/
// 1) 直连（有时能过 CORS；GitHub Pages 下多半不行，但先试一次）
// 2) AllOrigins raw
// 3) AllOrigins get（需要多一层 JSON 解析）
// 4) isomorphic-git CORS 代理
// 5) thingproxy（免费代理，偶尔会慢）
function buildProxyChain(url){
  const enc = encodeURIComponent(url);
  return [
    { kind:'direct', url, parse:'json' },
    { kind:'allorigins-raw', url:`https://api.allorigins.win/raw?url=${enc}`, parse:'json' },
    { kind:'allorigins-get', url:`https://api.allorigins.win/get?url=${enc}`,  parse:'allorigins' },
    { kind:'iso-cors',       url:`https://cors.isomorphic-git.org/${url}`,     parse:'json' },
    { kind:'thingproxy',     url:`https://thingproxy.freeboard.io/fetch/${url}`, parse:'json' },
  ];
}

/************ DOM ************/
const elSignal    = document.getElementById('signal');
const elPrice     = document.getElementById('price');
const elHL        = document.getElementById('hl');
const elDist      = document.getElementById('dist');
const elSlope     = document.getElementById('slope');
const elHedge     = document.getElementById('hedge');
const elBtnNow    = document.getElementById('btnNow');
const elBtnNotify = document.getElementById('btnNotify');
const elNextAt    = document.getElementById('nextAt');
const elCountdown = document.getElementById('countdown');

const fmt2 = n => Number(n).toFixed(2);
const pct  = n => (n*100).toFixed(3) + '%';
function hhmm(d=new Date()){return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');}

/************ 超时 fetch + 代理回退 ************/
async function fetchWithTimeout(url, {timeout=10000, noStore=true}={}){
  const ctrl = new AbortController();
  const id = setTimeout(()=>ctrl.abort(new Error('timeout')), timeout);
  const res = await fetch(url, {signal: ctrl.signal, cache: noStore ? 'no-store' : 'default'});
  clearTimeout(id);
  return res;
}
async function fetchJsonViaProxies(url){
  const chain = buildProxyChain(url);
  const errors = [];
  for (const hop of chain){
    try{
      const res = await fetchWithTimeout(hop.url, {timeout: 10000, noStore:true});
      if (!res.ok) throw new Error(`${hop.kind} HTTP ${res.status}`);
      if (hop.parse === 'json'){
        return await res.json();
      }else if (hop.parse === 'allorigins'){
        const wr = await res.json();
        if (!wr || !wr.contents) throw new Error(`${hop.kind} empty`);
        return JSON.parse(wr.contents);
      }else{
        // 兜底
        return await res.json();
      }
    }catch(e){
      errors.push(`${hop.kind}: ${e.message||e}`);
    }
  }
  throw new Error('所有代理均失败：\n' + errors.join('\n'));
}

/************ 计算 ************/
function ema(values, period){
  const k = 2/(period+1);
  let e = values[0];
  for(let i=1;i<values.length;i++){ e = values[i]*k + e*(1-k); }
  return e;
}

/************ 通知 ************/
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
    new Notification(title, { body });
  }
}

/************ 拉数据 ************/
async function getKlines(){
  const json = await fetchJsonViaProxies(API_KLINES);
  const rows = json.data || [];
  if (!rows.length) throw new Error('K线无数据');
  // OKX 返回按时间倒序，这里翻转为正序
  return rows.slice().reverse().map(r=>({
    ts: Number(r[0]),
    open: Number(r[1]),
    high: Number(r[2]),
    low:  Number(r[3]),
    close:Number(r[4]),
  }));
}
async function getTicker(sym){
  const json = await fetchJsonViaProxies(API_TICKER(sym));
  const d = (json.data && json.data[0]) || {};
  const last   = Number(d.last || d.lastPx || d.askPx || d.bidPx || 0);
  const volBase= Number(d.vol24h || d.vol || 0);
  const volUsd = volBase * last;
  return {sym, last, volUsd};
}
async function pickHedgeMeme(){
  const all = await Promise.allSettled(MEME_LIST.map(getTicker));
  const ok = all.filter(x=>x.status==='fulfilled').map(x=>x.value);
  if (!ok.length) throw new Error('Meme 24h 成交额获取失败');
  ok.sort((a,b)=> b.volUsd - a.volUsd);
  return { best: ok[0], list: ok };
}

/************ 主分析 ************/
const SIGNAL_KEY = 'last_signal_state'; // 'USE'|'NO_USE'

async function analyze(){
  try{
    elSignal.textContent = '加载中…';
    elHedge.textContent  = '—';

    const [kl, hedge] = await Promise.all([ getKlines(), pickHedgeMeme() ]);

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

    // 对冲展示
    const best = hedge.best;
    const listLine = hedge.list.map(x=>`${x.sym}: 24h≈$${fmt2(x.volUsd/1e6)}M`).join('  ·  ');
    elHedge.textContent = `推荐：${best.sym}（24h≈$${fmt2(best.volUsd/1e6)}M） | 明细：${listLine}`;

    // 页面信息
    if (useCondition){
      elSignal.innerHTML =
        `<span class="ok">✅ 可开双向</span> | ` +
        `close=${fmt2(price)}, EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}, 距离=${pct((ema34-price)/price)}/${pct((ema144-price)/price)} | ` +
        `斜率：EMA34 ${slope34>=0?'上':'下'}（${pct(slope34)}），EMA144 ${slope144>=0?'上':'下'}（${pct(slope144)}） | ` +
        `对冲币：${best.sym} | ETH止损6%/止盈10%，Meme止损10%/止盈10%，+8%保本，+15%启用2%移动止盈`;
    }else{
      elSignal.innerHTML =
        `<span class="no">❌ 暂不建议</span> | ` +
        `close=${fmt2(price)}, EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}, 距离=${pct((ema34-price)/price)}/${pct((ema144-price)/price)} | ` +
        `斜率：EMA34 ${slope34>=0?'上':'下'}（${pct(slope34)}），EMA144 ${slope144>=0?'上':'下'}（${pct(slope144)}） | ` +
        `对冲币：${best.sym}（仅记录，不建议入场）`;
    }

    elPrice.textContent = `$${fmt2(price)}`;
    elHL.textContent    = `H: $${fmt2(latest.high)}  |  L: $${fmt2(latest.low)}`;
    elDist.textContent  = `EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}， 距离 = ${pct((ema34-price)/price)} / ${pct((ema144-price)/price)}`;
    elSlope.textContent = `EMA34 ${slope34>=0?'上':'下'}（${pct(slope34)}）  ·  EMA144 ${slope144>=0?'上':'下'}（${pct(slope144)}）`;

    // 通知（状态切换）
    const newSignal = useCondition ? 'USE' : 'NO_USE';
    const lastSignal= localStorage.getItem(SIGNAL_KEY);
    if (lastSignal !== newSignal){
      ensureNotifyPermission().then(ok=>{
        if (!ok) return;
        if (newSignal==='USE'){
          pushNotify('✅ 可开双向', `对冲：${best.sym} · 现价 $${fmt2(price)}`);
        }else{
          pushNotify('❌ 暂不建议', `对冲：${best.sym} · 现价 $${fmt2(price)}`);
        }
      });
      localStorage.setItem(SIGNAL_KEY, newSignal);
    }

  }catch(err){
    console.error(err);
    elSignal.innerHTML = `<span class="no">❌ 加载失败</span>：<span class="small">${(err && err.message) || err}</span>`;
    elHedge.textContent = '—';
  }
}

/************ 定时与交互 ************/
let timer=null, nextTs=0;
function schedule(){
  clearInterval(timer);
  nextTs = Date.now() + REFRESH_MINUTES*60*1000;
  elNextAt.textContent = hhmm(new Date(nextTs));
  timer = setInterval(()=>{
    const remain = Math.max(0, nextTs - Date.now());
    const m = Math.floor(remain/60000);
    const s = Math.floor(remain/1000)%60;
    elCountdown.textContent = `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
    if (remain<=0){
      analyze().finally(schedule);
    }
  }, 1000);
}

elBtnNow.addEventListener('click', ()=>{ analyze().finally(schedule); });
elBtnNotify.addEventListener('click', async ()=>{
  const ok = await ensureNotifyPermission();
  elBtnNotify.textContent = ok ? '通知已开启' : '无法开启通知';
});

/************ 启动 ************/
analyze().finally(schedule);
