// ntfy_notifier.js —— 不依赖 node-fetch，直接用 Node 18+ 内置 fetch
const crypto = require("crypto");

const TARGET_URL = process.env.TARGET_URL;     // 例如 https://liyyyas.github.io/eth15m-monitor/
const NTFY_SERVER = process.env.NTFY_SERVER;   // https://ntfy.sh
const NTFY_TOPIC  = process.env.NTFY_TOPIC;    // 你的topic名

if (!TARGET_URL || !NTFY_SERVER || !NTFY_TOPIC) {
  console.error("[FATAL] Missing env. TARGET_URL/NTFY_SERVER/NTFY_TOPIC are required.");
  process.exit(1);
}

(async () => {
  try {
    // 1) 抓页面
    const res = await fetch(TARGET_URL, { headers: { "cache-control": "no-cache" } });
    if (!res.ok) throw new Error(`Fetch failed: ${res.status} ${res.statusText}`);
    const html = await res.text();

    // 2) 粗略解析 “信号”卡片文本（含“可开/不建议”等）
    const m = html.match(/信号[\s\S]*?<\/div>[\s\S]*?<\/div>/i);
    const signalBlock = m ? m[0] : "";
    const text = signalBlock.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();

    if (!text) {
      console.error("[ERROR] Cannot parse signal text from page.");
      process.exit(1);
    }

    // 3) 判定状态
    let status = "UNKNOWN";
    if (/可开双向|可开/.test(text)) status = "OPEN";
    else if (/暂不建议|不建议/.test(text)) status = "NO";

    // 4) 组装消息
    const title = (status === "OPEN")
      ? "✅ 可开双向"
      : (status === "NO" ? "❌ 暂不建议" : "ℹ️ 信号更新");

    const short = text.length > 400 ? text.slice(0, 400) + "…" : text;

    // 5) 发送到 ntfy
    const url = `${NTFY_SERVER.replace(/\/+$/, "")}/${encodeURIComponent(NTFY_TOPIC)}`;
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Title": title,
        "Tags": (status === "OPEN") ? "white_check_mark,rocket" : (status === "NO" ? "x" : "bell"),
        "Content-Type": "text/plain; charset=utf-8",
      },
      body: short
    });
    if (!resp.ok) throw new Error(`ntfy send failed: ${resp.status} ${resp.statusText}`);
    console.log(`[OK] ${title} pushed.`);
    process.exit(0);
  } catch (e) {
    console.error("[FATAL]", e.message || e);
    process.exit(1);
  }
})();
