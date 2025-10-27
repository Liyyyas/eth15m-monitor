/* ========= ETH 15m 双向策略监控（OKX + AllOrigins 代理） =========
 * 1) 取 OKX 15m K线，计算 EMA34 / EMA144 与近10根斜率
 * 2) 满足“近均线 & 走平”则输出 ✅ 文案，否则 ❌ 文案
 * 3) Meme 选择：DOGE/SHIB/PEPE/FLOKI 中，优先 24h 成交额最高且 |Δ|≤20%
 * 4) 支持 “立即刷新” 按钮 & 每15分钟自动刷新
 * --------------------------------------------------------------- */

const PROXY = "https://api.allorigins.win/raw?url="; // AllOrigins 代理
const OKX_BASE = "https://www.okx.com";
const INST = "ETH-USDT";
const BAR = "15m";
const LIMIT = 400; // 多取点：EMA144 需要足够样本

// DOM
const $ = (sel) => document.querySelector(sel);
const elSignal = $("#signal");
const elPrice  = $("#price");
const elDist   = $("#dist");
const elSlope  = $("#slope");
const elMeme   = $("#meme");
const elNext   = $("#nextTick");
const elBtnNow = $("#btn-refresh");
const elBtnNoti= $("#btn-notify");

// ------------ 工具 ------------
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const pct = (a)=> (a*100).toFixed(3) + "%";
const fmt = (n)=> Number(n).toLocaleString(undefined,{maximumFractionDigits:2});

// EMA 计算
function ema(values, period){
  const k = 2/(period+1);
  let emaArr = [];
  // 用前period个的SMA作为初始值
  const seed = values.slice(0, period).reduce((a,b)=>a+b,0)/period;
  emaArr[period-1] = seed;
  for (let i=period;i<values.length;i++){
    emaArr[i] = values[i]*k + emaArr[i-1]*(1-k);
  }
  return emaArr;
}

// 通过代理 GET JSON
async function getJSON(url){
  const full = PROXY + encodeURIComponent(url);
  const res = await fetch(full, {cache:"no-store"});
  if(!res.ok) throw new Error("网络错误："+res.status);
  return await res.json();
}

// 取 OKX 15m K线（返回升序数组）
async function fetchKlines(){
  const url = `${OKX_BASE}/api/v5/market/candles?instId=${INST}&bar=${BAR}&limit=${LIMIT}`;
  const data = await getJSON(url);
  // OKX 返回 [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm] 倒序
  const arr = data.data.map(row=>({
    ts: Number(row[0]),
    o: Number(row[1]),
    h: Number(row[2]),
    l: Number(row[3]),
    c: Number(row[4])
  })).reverse();
  return arr;
}

// 取 24h ticker（单币）
async function fetchTicker(instId){
  const url = `${OKX_BASE}/api/v5/market/ticker?instId=${instId}`;
  const data = await getJSON(url);
  const t = data.data?.[0];
  return {
    last: Number(t.last),
    high24h: Number(t.high24h),
    low24h: Number(t.low24h),
    volCcy24h: Number(t.volCcy24h || 0), // 以计价币计（USDT）
    change24h: Number(t.change24h || 0)  // 比例，如 -0.0345
  };
}

// 选 Meme 对冲
async function pickMeme(){
  const pool = ["DOGE","SHIB","PEPE","FLOKI"];
  let best = {sym:"DOGE", vol: -1}; // 默认 DOGE
  for (const s of pool){
    try{
      const t = await fetchTicker(`${s}-USDT`);
      const vol = t.volCcy24h;           // 成交额(USDT)
      const chgAbs = Math.abs(t.change24h); // 比例
      if (chgAbs <= 0.20 && vol > best.vol){
        best = {sym: s, vol};
      }
      // 慢一点，避免被限频
      await sleep(150);
    }catch(e){ /* 忽略失败，继续下一个 */ }
  }
  return best.sym;
}

// 主逻辑
async function analyze(){
  try{
    // UI reset
    elSignal.textContent = "加载中…";
    elPrice.textContent  = "—";
    elDist.textContent   = "—";
    elSlope.textContent  = "—";
    elMeme.textContent   = "—";

    const kl = await fetchKlines();
    const closes = kl.map(k=>k.c);
    const high = Math.max(...kl.slice(-1*96).map(k=>k.h)); // 近24h高(96根)
    const low  = Math.min(...kl.slice(-1*96).map(k=>k.l)); // 近24h低

    // 计算 EMA34 / EMA144
    if (closes.length < 160){
      elSignal.textContent = "样本不足，稍后再试";
      return;
    }
    const ema34Arr  = ema(closes, 34);
    const ema144Arr = ema(closes,144);
    const last      = closes.at(-1);
    const ema34     = ema34Arr.at(-1);
    const ema144    = ema144Arr.at(-1);

    // 距离
    const d34 = Math.abs(last-ema34)/last;   // 与EMA34相对距离
    const d144= Math.abs(last-ema144)/last;

    // 近10根“斜率”（按说明：|EMA_now-EMA_10bars_ago|/EMA_now）
    const ema34_ago10  = ema34Arr.at(-11);
    const ema144_ago10 = ema144Arr.at(-11);
    const slope34 = Math.abs(ema34-ema34_ago10)/ema34;
    const slope144= Math.abs(ema144-ema144_ago10)/ema144;

    // 条件
    const nearMA = ( (last>Math.min(ema34,ema144) && last<Math.max(ema34,ema144)) ||
                     (d34<=0.005 && d144<=0.005) );
    const flat   = ( slope34<=0.003 && slope144<=0.002 );
    const useOK  = nearMA && flat;

    // UI: 价格区
    elPrice.textContent = `close=${fmt(last)}   H: ${fmt(high)} | L: ${fmt(low)}`;

    // UI: 距离
    elDist.textContent = `EMA34=${fmt(ema34)},  EMA144=${fmt(ema144)}， 距离=${pct(d34)}/${pct(d144)}`;

    // UI: 斜率
    const dir34 = (ema34-ema34_ago10);
    const dir144= (ema144-ema144_ago10);
    const txtDir = (x)=> x>0 ? "上" : (x<0 ? "下" : "走平");
    elSlope.textContent = `EMA34 ${pct(slope34)}（${txtDir(dir34)}） ，EMA144 ${pct(slope144)}（${txtDir(dir144)}）`;

    if (useOK){
      const hedge = await pickMeme();
      elSignal.textContent =
        `✅ 可开双向  | close=${fmt(last)}, EMA34=${fmt(ema34)}, EMA144=${fmt(ema144)}, 距离=${pct(d34)}/${pct(d144)}  | ` +
        `方向：ETH任一向，Meme反向  | 对冲币：${hedge}  | ` +
        `ETH止损6%、止盈10%；Meme止损10%、止盈10%；+8%移保本，+15%启用2%移动止盈。`;
      elMeme.textContent = `候选池[DOGE, SHIB, PEPE, FLOKI]，已选：${hedge}`;
    }else{
      elSignal.textContent =
        `❌ 暂不建议  | close=${fmt(last)}, EMA34=${fmt(ema34)}, EMA144=${fmt(ema144)}, 距离=${pct(d34)}/${pct(d144)}  | ` +
        `斜率=${pct(slope34)} / ${pct(slope144)}（${txtDir(dir34)} / ${txtDir(dir144)}）`;
      elMeme.textContent = "—";
    }

    // 下一次刷新时间（15:00 格式）
    const next = new Date(Date.now()+15*60*1000);
    const hh = String(next.getHours()).padStart(2,"0");
    const mm = String(next.getMinutes()).padStart(2,"0");
    elNext.textContent = `${hh}:${mm}`;

  }catch(err){
    elSignal.textContent = "数据获取失败（可能网络或代理不稳定），下拉刷新或稍后再试。";
    console.error(err);
  }
}

// 绑定按钮 & 定时器
let ticking = false;
elBtnNow?.addEventListener("click", async ()=>{
  if (ticking) return;
  ticking = true;
  elBtnNow.disabled = true;
  await analyze();
  await sleep(800);
  elBtnNow.disabled = false;
  ticking = false;
});

// 首次运行 & 15分钟自动刷新
analyze();
setInterval(analyze, 15 * 60 * 1000);

// 通知按钮（仅在支持的浏览器有效，不强制）
elBtnNoti?.addEventListener("click", async ()=>{
  try{
    const perm = await Notification.requestPermission();
    if (perm !== "granted") return alert("请允许通知权限后再试");
    new Notification("ETH 15m 监控", { body: "通知已开启：满足条件会在页面内提示" });
  }catch{ alert("此浏览器不支持通知"); }
});
