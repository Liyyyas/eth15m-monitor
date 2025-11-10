// 审计 okx_eth_15m.csv（或任何同结构 15m K 线CSV）
// 统计时间跨度、行数、缺口、重复，输出日志与 gaps CSV/JSON
import fs from 'fs/promises';
import path from 'path';

const CSV_PATH = process.env.CSV_PATH || 'okx_eth_15m.csv';
const STEP_MS = 15 * 60 * 1000;

function parseLine(line) {
  // 简单逗号分割（你的文件没有引号/逗号嵌套，足够用）
  // 期望表头：ts,iso,open,high,low,close,vol
  const arr = line.split(',');
  return arr;
}

function toMsFromIso(s) {
  // 尽量稳妥：Date.parse 返回 ms；若失败返回 NaN
  const t = Date.parse(s);
  return Number.isNaN(t) ? NaN : t;
}

function toMsFromTs(s) {
  const n = Number(s);
  if (!Number.isFinite(n)) return NaN;
  // 自动判别秒/毫秒
  return n > 1e12 ? n : Math.round(n * 1000);
}

function fmtUtc(ms) {
  return new Date(ms).toISOString().replace('.000Z', 'Z');
}

async function main() {
  const raw = await fs.readFile(CSV_PATH, 'utf8');

  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (lines.length === 0) throw new Error('CSV 是空的');
  const header = parseLine(lines[0]).map(s => s.trim().toLowerCase());
  const body = lines.slice(1);

  const iTs  = header.indexOf('ts');
  const iIso = header.indexOf('iso');

  if (iTs === -1 && iIso === -1) {
    throw new Error('缺少 ts / iso 列，无法解析时间');
  }

  // 读取时间戳
  const times = [];
  for (const ln of body) {
    const cols = parseLine(ln);
    let ms = NaN;

    if (iIso !== -1 && cols[iIso]) {
      ms = toMsFromIso(cols[iIso]);
    }
    if ((!Number.isFinite(ms)) && iTs !== -1 && cols[iTs]) {
      ms = toMsFromTs(cols[iTs]);
    }
    if (Number.isFinite(ms)) times.push(ms);
  }

  if (times.length === 0) throw new Error('没有任何可解析的时间戳');

  // 排序 + 去重
  times.sort((a,b) => a - b);
  const unique = [];
  for (let i=0;i<times.length;i++){
    if (i===0 || times[i] !== times[i-1]) unique.push(times[i]);
  }
  const duplicatesRemoved = times.length - unique.length;

  const first = unique[0];
  const last  = unique[unique.length - 1];
  const span  = last - first;
  const expectedExact = Math.floor(span / STEP_MS) + 1; // 首尾都计入
  const actual = unique.length;
  const missingTotal = Math.max(0, expectedExact - actual);

  // 缺口检测
  const gaps = [];
  let shortIntervals = 0; // < 15m 的异常（重叠/乱序导致）
  for (let i=1;i<unique.length;i++) {
    const delta = unique[i] - unique[i-1];
    if (delta > STEP_MS) {
      const miss = Math.round(delta / STEP_MS) - 1; // 四舍五入规避毫秒级误差
      if (miss > 0) gaps.push({
        gap_start_utc: fmtUtc(unique[i-1]),
        gap_end_utc:   fmtUtc(unique[i]),
        missing_15m_bars: miss
      });
    } else if (delta < STEP_MS) {
      shortIntervals++;
    }
  }

  // 输出 gaps CSV/JSON
  const gapsCsv = ['gap_start_utc,gap_end_utc,missing_15m_bars']
    .concat(gaps.map(g => `${g.gap_start_utc},${g.gap_end_utc},${g.missing_15m_bars}`))
    .join('\n');
  await fs.writeFile('okx_eth_15m_gaps.csv', gapsCsv, 'utf8');

  const summary = {
    file: CSV_PATH,
    header,
    total_lines_including_header: lines.length,
    parsed_rows: body.length,
    usable_rows_after_time_parse: times.length,
    rows_after_dedup: actual,
    duplicates_removed: duplicatesRemoved,
    first_utc: fmtUtc(first),
    last_utc:  fmtUtc(last),
    span_ms: span,
    step_ms: STEP_MS,
    expected_by_span: expectedExact,
    missing_by_span: missingTotal,
    gap_segments: gaps.length,
    short_intervals_lt_15m: shortIntervals
  };
  await fs.writeFile('okx_eth_15m_audit.json', JSON.stringify(summary, null, 2), 'utf8');

  // 控制台总结（日志里一眼能看懂）
  console.log('=== CSV 审计结果 ===');
  console.log('文件：', CSV_PATH);
  console.log('时间范围(UTC)：', summary.first_utc, '->', summary.last_utc);
  console.log(`实际蜡烛数：${actual} | 按跨度应有：${expectedExact} | 缺口(按跨度)：${missingTotal}`);
  console.log(`去重移除：${duplicatesRemoved} | 异常间隔>15m 段数：${gaps.length} | 异常间隔<15m 段数：${shortIntervals}`);
  if (gaps.length) {
    console.log('前10个缺口：');
    gaps.slice(0,10).forEach(g => console.log('-', g.gap_start_utc, '->', g.gap_end_utc, '缺', g.missing_15m_bars));
  } else {
    console.log('未发现 >15m 的时间缺口。');
  }
  console.log('已输出：okx_eth_15m_gaps.csv, okx_eth_15m_audit.json');
}

main().catch(e => {
  console.error('审计失败：', e?.message || e);
  process.exit(1);
});
