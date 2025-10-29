// export_okx_1y.mjs  —— 获取 OKX ETH/USDT 15m 最近365天K线并写入 okx_eth_15m.csv
import fs from "node:fs/promises";

const INST_ID = "ETH-USDT";
const BAR = "15m";
const PAGE_LIMIT = 300;              // OKX 单页上限
const DAYS = 365;                    // 目标天数
const PROXY = "https://eth-proxy.1053363050.workers.dev"; // 你的 Worker
const DIRECT = "https://www.okx.com";                      // 直连兜底
const OUT = "okx_eth_15m.csv";

const startMs = Date.now() - DAYS * 24 * 60 * 60 * 1000;

async function okxFetch(base, before) {
  const qs = new URLSearchParams({
    instId: INST_ID,
    bar: BAR,
    limit: String(PAGE_LIMIT),
  });
  if (before) qs.set("before", String(before));
  const url = `${base}/api/v5/market/history-candles?${qs.toString()}`;
  const r = await fetch(url, { headers: { "accept": "application/json" } });
  const t = await r.text(); // 先拿文本避免 HTML 混入不报错
  let j;
  try { j = JSON.parse(t); } catch { 
    throw new Error(`NOT_JSON: ${t.slice(0,120)}`);
  }
  if (j.code !== "0" || !Array.isArray(j.data)) {
    throw new Error(`BAD_RESP code=${j.code} msg=${j.msg ?? ""}`);
  }
  return j.data; // 每项: [ts, o, h, l, c, vol, ...]
}

async function fetchAll() {
  let cursor = undefined; // 第一页不带 before
  let rows = [];
  let page = 0;

  while (true) {
    page++;
    let data;
    try {
      data = await okxFetch(PROXY, cursor);
    } catch (e) {
      // 代理失败就直连一次
      data = await okxFetch(DIRECT, cursor);
    }

    if (!data.length) break;
    // OKX 返回新到旧，累加后统一排序去重
    rows.push(...data);

    // 计算下一页 before：取本页最后一条(最旧)时间戳 - 1
    const lastTs = Number(data[data.length - 1][0]);
    if (Number.isFinite(lastTs)) cursor = lastTs - 1;
    else break;

    // 达到时间边界：最旧一条已经早于 startMs，结束
    if (lastTs <= startMs) break;

    // 防御：最多翻 2000 页（远超一年所需）
    if (page >= 2000) break;
    // 轻微节流
    await new Promise(r => setTimeout(r, 120));
  }

  // 去重、升序、截到一年范围内
  const map = new Map();
  for (const it of rows) map.set(it[0], it);
  const uniq = Array.from(map.values())
    .map(a => [Number(a[0]), ...a.slice(1)])
    .filter(a => a[0] >= startMs)
    .sort((a, b) => a[0] - b[0]);

  return uniq;
}

function toCSV(arr) {
  const header = ["ts","iso","open","high","low","close","vol"];
  const lines = [header.join(",")];
  for (const a of arr) {
    const [ts, o, h, l, c, vol] = a;
    const iso = new Date(ts).toISOString();
    lines.push([ts, iso, o, h, l, c, vol].join(","));
  }
  return lines.join("\n") + "\n";
}

(async () => {
  const all = await fetchAll();
  if (all.length < 10000) {
    throw new Error(`TOO_FEW_ROWS: ${all.length}（应≈35000）`);
  }
  const csv = toCSV(all);
  await fs.writeFile(OUT, csv, "utf8");
  console.log(`DONE: ${all.length} rows -> ${OUT}`);
})();
