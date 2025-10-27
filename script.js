// ============ 配置 ============
const REFRESH_MINUTES = 15;            // 自动刷新周期（分钟）
const BARS_FOR_EMA34 = 34;
const BARS_FOR_EMA144 = 144;
const SLOPE_LOOKBACK = 10;

// OKX 15m K线（取 200 根），用 AllOrigins 代理绕过 CORS
const OKX_KLINES = 'https://www.okx.com/api/v5/market/candles?instId=ETH-USDT&bar=15m&limit=200';
const PROXY = 'https://api.allorigins.win/raw?url=';

// ============ DOM 引用 ============
const elSignal   = document.getElementById('signal');
const elPrice    = document.getElementById('price');
const elHL       = document.getElementById('hl');
const elDist     = document.getElementById('dist');
const elSlope    = document.getElementById('slope');
const elBtnNow   = document.getElementById('btnNow');
const elBtnNotif = document.getElementById('btnNotify');
const elNextAt   = document.getElementById('nextAt');
const elCountdown= document.getElementById('countdown');

// ============ 小工具 ============
const fmt2 = n => Number(n).toFixed(2);
const pct = n => (n*100).toFixed(3) + '%';
function hhmm(d=new Date()){return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');}

// 计算 EMA
function ema(values, period){
  const k = 2/(period+1);
  let emaVal = values[0];
  for(let i=1;i<values.length;i++){
    emaVal = values[i]*k + emaVal*(1-k);
  }
  return emaVal;
}

// 请求权限
async function ensureNotifyPermission(){
  if (!('Notification' in window)) return false;
  if (Notification.permission === 'granted') return true;
  if (Notification.permission !== 'denied'){
    const p = await Notification.requestPermission();
    return p === 'granted';
  }
  return false;
}
function pushNotify(title, body){
  if (!('Notification' in window)) return;
  if (Notification.permission === 'granted'){
    new Notification(title, {body});
  }
}

// ============ 核心：拉数据 & 计算 ============
async function fetchKlines(){
  const url = PROXY + encodeURIComponent(OKX_KLINES);
  const res = await fetch(url, {cache:'no-store'});
  if (!res.ok) throw new Error('网络错误: '+res.status);
  const json = await res.json();
  // OKX 返回 data: [ [ts, o, h, l, c, vol, ...], ...] 逆序（最近在前）
  const rows = json.data || [];
  if (!rows.length) throw new Error('无数据');
  // 转为从旧到新
  const klines = rows.slice().reverse().map(r=>({
    ts: Number(r[0]),
    open: Number(r[1]),
    high: Number(r[2]),
    low:  Number(r[3]),
    close:Number(r[4]),
  }));
  return klines;
}

// 记录上次信号用于去重
const SIGNAL_KEY = 'last_signal_state'; // 'USE' | 'NO_USE'

// 主分析函数
async function analyze(){
  try{
    elSignal.textContent = '加载中…';

    const kl = await fetchKlines();
    const closes = kl.map(k=>k.close);
    if (closes.length < Math.max(BARS_FOR_EMA34,BARS_FOR_EMA144)+SLOPE_LOOKBACK+1){
      throw new Error('样本不足');
    }

    const latest = kl[kl.length-1];
    const latestPrice = latest.close;

    // 计算 EMA34 / EMA144（用全部收盘价计算最新值）
    const ema34 = ema(closes, BARS_FOR_EMA34);
    const ema144= ema(closes, BARS_FOR_EMA144);

    // 距离：相对 close 的百分比
    const dist34 = Math.abs(latestPrice-ema34)/latestPrice;
    const dist144= Math.abs(latestPrice-ema144)/latestPrice;

    // 斜率：近 N 根 EMA 的变化占比（这里用近 N+period 的数据滚动 EMA 近似）
    // 简化做法：取 SLOPE_LOOKBACK 前的 close 序列分别求一次 EMA
    const closesAgo = closes.slice(0, closes.length - SLOPE_LOOKBACK);
    const ema34Ago  = ema(closesAgo, BARS_FOR_EMA34);
    const ema144Ago = ema(closesAgo, BARS_FOR_EMA144);
    const slope34   = (ema34 - ema34Ago)/ema34;     // 相对当前 EMA 比例
    const slope144  = (ema144 - ema144Ago)/ema144;

    // 规则
    const near34  = dist34 <= 0.005;  // 0.5%
    const near144 = dist144<= 0.005;  // 0.5%
    const nearBand= (latestPrice>=Math.min(ema34,ema144) && latestPrice<=Math.max(ema34,ema144));
    const flat34  = Math.abs((ema34-ema34Ago)/ema34) <= 0.003; // 0.3%
    const flat144 = Math.abs((ema144-ema144Ago)/ema144) <= 0.002; // 0.2%

    const useCondition = ( (nearBand || (near34&&near144)) && flat34 && flat144 );

    // ====== 呈现 ======
    if (useCondition){
      elSignal.innerHTML =
        `<span class="ok">✅ 可开双向</span> | ` +
        `close=${fmt2(latestPrice)}, EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}, 距离=${pct((ema34-latestPrice)/latestPrice)}/${pct((ema144-latestPrice)/latestPrice)} | ` +
        `斜率：EMA34 ${slope34>=0?'上':'下'}（${pct(slope34)}），EMA144 ${slope144>=0?'上':'下'}（${pct(slope144)}）`;
    }else{
      elSignal.innerHTML =
        `<span class="no">❌ 暂不建议</span> | ` +
        `close=${fmt2(latestPrice)}, EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}, 距离=${pct((ema34-latestPrice)/latestPrice)}/${pct((ema144-latestPrice)/latestPrice)} | ` +
        `斜率：EMA34 ${slope34>=0?'上':'下'}（${pct(slope34)}），EMA144 ${slope144>=0?'上':'下'}（${pct(slope144)}）`;
    }

    elPrice.textContent = `$${fmt2(latestPrice)}`;
    elHL.textContent    = `H: $${fmt2(latest.high)}  |  L: $${fmt2(latest.low)}`;
    elDist.textContent  =
      `EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}， 距离 = ${pct((ema34-latestPrice)/latestPrice)} / ${pct((ema144-latestPrice)/latestPrice)}`;
    elSlope.textContent =
      `EMA34 ${slope34>=0?'上':'下'}（${pct(slope34)}）  ·  EMA144 ${slope144>=0?'上':'下'}（${pct(slope144)}）`;

    // ====== 通知：仅在状态切换时推送 ======
    const newSignal = useCondition ? 'USE' : 'NO_USE';
    const lastSignal= localStorage.getItem(SIGNAL_KEY);
    if (lastSignal !== newSignal){
      ensureNotifyPermission().then(ok=>{
        if (!ok) return;
        if (newSignal==='USE'){
          pushNotify('✅ 可开双向', `close=${fmt2(latestPrice)}, EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}`);
        }else{
          pushNotify('❌ 暂不建议', `close=${fmt2(latestPrice)}, EMA34=${fmt2(ema34)}, EMA144=${fmt2(ema144)}`);
        }
      });
      localStorage.setItem(SIGNAL_KEY, newSignal);
    }

  }catch(err){
    console.error(err);
    elSignal.innerHTML = `<span class="no">❌ 加载失败</span>：${err.message||err}`;
  }
}

// ============ 刷新 & 倒计时 ============
let timer = null;
let nextTs = 0;

function schedule(){
  clearInterval(timer);
  nextTs = Date.now() + REFRESH_MINUTES*60*1000;
  elNextAt.textContent = hhmm(new Date(nextTs));
  timer = setInterval(()=>{
    const remain = Math.max(0, nextTs - Date.now());
    const m = Math.floor(remain/60000);
    const s = Math.floor(remain/1000)%60;
    elCountdown.textContent = `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
    if (remain <= 0){
      analyze().finally(schedule);
    }
  }, 1000);
}

// ============ 事件 ============
elBtnNow.addEventListener('click', ()=>{
  analyze().finally(schedule);
});
elBtnNotif.addEventListener('click', async ()=>{
  const ok = await ensureNotifyPermission();
  elBtnNotif.textContent = ok ? '通知已开启' : '无法开启通知';
});

// ============ 启动 ============
analyze().finally(schedule);
