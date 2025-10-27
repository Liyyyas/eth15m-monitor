// ====== 可调参数 ======
const INST = "ETH-USDT";
const BAR  = "15m";         // 15 分钟
const CANDLE_LIMIT = 200;   // 够算 EMA144
const AUTO_INTERVAL_MS = 15 * 60 * 1000; // 15 分钟
const MEME_CANDS = ["DOGE-USDT","SHIB-USDT","PEPE-USDT","FLOKI-USDT"];

// AllOrigins 代理（适合 GitHub Pages）
const prox = (u) => `https://api.allorigins.win/raw?url=${encodeURIComponent(u)}`;
// 也可以尝试 Cloudflare Workers 版：
// const prox = (u) => `https://api.allorigins.workers.dev/raw?url=${encodeURIComponent(u)}`;

// ====== DOM ======
const $ = s => document.querySelector(s);
const btnNow = $("#btn-now");
const btnNotify = $("#btn-notify");
const signalText = $("#signalText");
const priceLine = $("#priceLine");
const hlLine = $("#hlLine");
const maLine = $("#maLine");
const slopeLine = $("#slopeLine");
const memeLine = $("#memeLine");
const countdown = $("#countdown");
const nextTick = $("#nextTick");

let ticking = false;
let timerId = null;
let nextAt = 0;

// ====== 工具 ======
function ema(arr, period) {
  const k = 2 / (period + 1);
  let emaVal = arr[0];
  const out = [emaVal];
  for (let i = 1; i < arr.length; i++) {
    emaVal = arr[i] * k + emaVal * (1 - k);
    out.push(emaVal);
  }
  return out;
}

const pct = (v) => (v * 100).toFixed(3) + "%";
const f2 = (n) => Number(n).toFixed(2);

// ====== 数据获取（OKX + 代理）======
async function fetchCandles(instId, bar, limit) {
  const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${bar}&limit=${limit}`;
  const r = await fetch(prox(url));
  if (!r.ok) throw new Error("candles fetch failed");
  const j = await r.json();
  // OKX: data 是数组 [ts, o, h, l, c, vol, volCcy, volCcyQuote]，最近在前
  const rows = j.data || j;
  const k = rows.map(x => ({
    ts: Number(x[0]),
    o: Number(x[1]),
    h: Number(x[2]),
    l: Number(x[3]),
    c: Number(x[4]),
  })).reverse();
  return k;
}

async function fetchTicker(instId) {
  const url = `https://www.okx.com/api/v5/market/ticker?instId=${instId}`;
  const r = await fetch(prox(url));
  if (!r.ok) throw new Error("ticker fetch failed");
  const j = await r.json();
  const d = (j.data && j.data[0]) || j;
  const last = Number(d.last);
  const high24h = Number(d.high24h);
  const low24h  = Number(d.low24h);
  const open24h = Number(d.open24h);
  const vol24h  = Number(d.vol24h || d.volCcy24h || 0); // 先取张数，取不到再取币量
  const chgPct  = open24h ? Math.abs((last - open24h) / open24h) : 1;
  return { last, high24h, low24h, open24h, vol24h, chgPct };
}

// ====== 主逻辑 ======
async function analyze() {
  if (ticking) return;
  ticking = true;

  try {
    signalText.textContent = "加载中…";

    // 1) K 线与 EMA
    const k = await fetchCandles(INST, BAR, CANDLE_LIMIT);
    const closes = k.map(x => x.c);
    const e34 = ema(closes, 34);
    const e144 = ema(closes, 144);
    const latest = k.at(-1);
    const latestE34 = e34.at(-1);
    const latestE144 = e144.at(-1);
    const e34_ago10 = e34[e34.length - 11];
    const e144_ago10 = e144[e144.length - 11];

    // 2) 斜率（10 根差值的相对幅度）
    const slope34 = Math.abs((latestE34 - e34_ago10) / latestE34);
    const slope144 = Math.abs((latestE144 - e144_ago10) / latestE144);

    // 3) 价格靠近均线
    const between = (latest.c >= Math.min(latestE34, latestE144)) &&
                    (latest.c <= Math.max(latestE34, latestE144));
    const near34 = Math.abs(latest.c - latestE34) / latest.c <= 0.005;
    const near144 = Math.abs(latest.c - latestE144) / latest.c <= 0.005;
    const nearCond = between || (near34 && near144);

    // 4) 均线走平
    const flatCond = (slope34 <= 0.003) && (slope144 <= 0.002);

    // 5) Meme 选择
    let hedge = "DOGE-USDT";
    let bestVol = -1;
    for (const s of MEME_CANDS) {
      try {
        const t = await fetchTicker(s);
        if (t.chgPct <= 0.20 && t.vol24h > bestVol) {
          bestVol = t.vol24h;
          hedge = s;
        }
      } catch {}
    }

    // 6) 输出
    priceLine.textContent = `$${f2(latest.c)}`;
    hlLine.textContent = `H: $${f2(latest.h)} | L: $${f2(latest.l)}`;
    maLine.innerHTML = `EMA34=${f2(latestE34)}，EMA144=${f2(latestE144)}，距离=${pct((latest.c - latestE34)/latest.c)} / ${pct((latest.c - latestE144)/latest.c)}`;
    const slTxt = `EMA34 ${slope34 === 0 ? "走平" : (latestE34 >= e34_ago10 ? "上" : "下")}（${pct((latestE34 - e34_ago10)/latestE34)}），` +
                  `EMA144 ${slope144 === 0 ? "走平" : (latestE144 >= e144_ago10 ? "上" : "下")}（${pct((latestE144 - e144_ago10)/latestE144)}）`;
    slopeLine.textContent = slTxt;
    memeLine.textContent = hedge.replace("-USDT","");

    const ok = nearCond && flatCond;
    if (ok) {
      const msg = `✅ 可开双向 | close=${f2(latest.c)}，EMA34=${f2(latestE34)}，EMA144=${f2(latestE144)}，距离=${pct((latest.c-latestE34)/latest.c)}/${pct((latest.c-latestE144)/latest.c)} | 方向：ETH任一向，Meme反向 | 对冲币：${hedge.replace("-USDT","")} | ETH止损6%、止盈10%；Meme止损10%、止盈10%；+8%移保本，+15%启用2%移动止盈。`;
      signalText.textContent = msg;
      signalText.classList.remove("bad"); signalText.classList.add("ok");
      maybeNotify("可开双向", msg);
    } else {
      const dir34 = latestE34 >= e34_ago10 ? "上" : "下";
      const dir144 = latestE144 >= e144_ago10 ? "上" : "下";
      const msg = `❌ 暂不建议 | close=${f2(latest.c)}，EMA34=${f2(latestE34)}，EMA144=${f2(latestE144)}，距离=${pct((latest.c-latestE34)/latest.c)}/${pct((latest.c-latestE144)/latest.c)} | 斜率=${dir34}/${dir144}（上/下/走平）`;
      signalText.textContent = msg;
      signalText.classList.remove("ok"); signalText.classList.add("bad");
    }

  } catch (e) {
    signalText.textContent = `加载失败：${e.message || e}`;
    signalText.classList.remove("ok"); signalText.classList.add("bad");
  } finally {
    ticking = false;
  }
}

// ====== 通知相关 ======
function ensureNotify() {
  if (Notification?.permission === "granted") return true;
  return false;
}
function maybeNotify(title, body) {
  if (!ensureNotify()) return;
  try { new Notification(title, { body }); } catch {}
}

// ====== 定时 & 交互 ======
function schedule() {
  clearInterval(timerId);
  nextAt = Date.now() + AUTO_INTERVAL_MS;
  timerId = setInterval(() => {
    const remain = Math.max(0, nextAt - Date.now());
    const mm = Math.floor(remain / 60000);
    const ss = Math.floor((remain % 60000) / 1000);
    countdown.textContent = `下次刷新：${String(mm).padStart(2,"0")}:${String(ss).padStart(2,"0")}`;
    nextTick.textContent = `—  ${String(mm).padStart(2,"0")}:${String(ss).padStart(2,"0")}`;
    if (remain <= 0) {
      nextAt = Date.now() + AUTO_INTERVAL_MS;
      analyze();
    }
  }, 1000);
}

btnNow.addEventListener("click", async () => {
  await analyze();
  nextAt = Date.now() + AUTO_INTERVAL_MS; // 立即刷新后，重新计时 15 分钟
});

btnNotify.addEventListener("click", async () => {
  try {
    const p = await Notification.requestPermission();
    if (p === "granted") {
      btnNotify.textContent = "通知已开启";
    } else {
      alert("通知未授权");
    }
  } catch {
    alert("此浏览器不支持通知或已被禁用");
  }
});

// ====== 启动 ======
analyze();
schedule();
