import fetch from 'node-fetch';

const NTFY_TOPIC = process.env.NTFY_TOPIC;              // 你的 ntfy 主题

async function getOkxCandles(instId='ETH-USDT', bar='15m', limit=210){
  const r = await fetch(`https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${bar}&limit=${limit}`);
  const j = await r.json(); if (j.code!=='0') throw new Error('OKX candles error');
  return j.data.map(x=>({t:+x[0], c:+x[4]})).reverse(); // 只用 close
}
function ema(vals,p){ const k=2/(p+1); const arr=[]; const sma=vals.slice(0,p).reduce((a,b)=>a+b,0)/p;
  arr[p-1]=sma; for(let i=p;i<vals.length;i++) arr[i]=vals[i]*k+arr[i-1]*(1-k); return arr; }
const pct=(a,b)=> (a-b)/b*100;
function signal(closes, idx){
  if (idx<160) throw new Error('Not enough bars');
  const e34=ema(closes,34), e144=ema(closes,144);
  const c=closes[idx], a=e34[idx], b=e144[idx];
  const d34=pct(c,a), d144=pct(c,b);
  const s34=pct(e34[idx],e34[idx-10]), s144=pct(e144[idx],e144[idx-10]);
  const use = Math.abs(d34)<=0.5 && Math.abs(d144)<=0.5 && Math.abs(s34)<=0.3 && Math.abs(s144)<=0.2;
  const direction = a>=b ? 'ETH 多' : 'ETH 空';
  return {use, direction, c,a,b, d34,d144, s34,s144};
}
async function pickMeme(){
  const syms=['PEPE-USDT','DOGE-USDT','SHIB-USDT','FLOKI-USDT'];
  const r=await fetch('https://www.okx.com/api/v5/market/tickers?instType=SPOT');
  const j=await r.json(); if (j.code!=='0') throw new Error('OKX tickers error');
  let best=null; for(const s of syms){ const row=j.data.find(x=>x.instId===s); if(!row) continue;
    const vol=parseFloat(row.volCcyQuote||row.volCcy||'0'); if(!best||vol>best.vol) best={sym:s.split('-')[0], vol}; }
  return best?.sym || 'PEPE';
}
const fmt=(n,d=2)=>Number(n).toFixed(d);

async function pushNtfy(title, body){
  const url = `https://ntfy.sh/${encodeURIComponent(NTFY_TOPIC)}`;
  await fetch(url, {
    method:'POST',
    headers:{
      'Title': title,
      'Priority': '4',        // 1-5；需要更醒目可设5
      'Tags': 'chart_with_upwards_trend'
    },
    body: body
  });
}

(async ()=>{
  if (!NTFY_TOPIC) throw new Error('Missing NTFY_TOPIC');
  const candles = await getOkxCandles();
  const closes  = candles.map(x=>x.c);
  const i = closes.length-1, ip=i-1;

  const now  = signal(closes, i);
  const prev = signal(closes, ip);

  const changed = (now.use!==prev.use) || (now.use && prev.use && now.direction!==prev.direction);
  if (!changed){ console.log('No state change'); return; }

  const meme = await pickMeme();
  const title = now.use ? '✅ 可开双向' : '❌ 暂不建议';
  const msg = [
    `${title}｜${now.direction}｜对冲：${meme}`,
    `close=${fmt(now.c)}, EMA34=${fmt(now.a)}, EMA144=${fmt(now.b)}`,
    `距离：${fmt(now.d34)}% / ${fmt(now.d144)}% · 斜率10：${fmt(now.s34)}% / ${fmt(now.s144)}%`,
    `规则：ETH止损6%/止盈10%，Meme止损10%/止盈10%，+8%保本，+15%启用2%移动止盈`
  ].join('\n');

  await pushNtfy(title, msg);
  console.log('Pushed (state changed).');
})().catch(e=>{ console.error(e); process.exit(1); });
