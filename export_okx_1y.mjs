import fs from 'node:fs/promises';

const baseUrl = 'https://www.okx.com/api/v5/market/history-candles?instId=ETH-USDT&bar=15m&limit=300';
const oneDayMs = 24 * 60 * 60 * 1000;
const now = Date.now();
const startTime = now - 365 * oneDayMs;
const intervalMs = 15 * 60 * 1000;
const maxPages = 1500;
const outputPath = 'eth15m-monitor/okx_eth_15m.csv';

async function fetchAllData() {
    let currentTime = startTime;
    let page = 0;
    const seenTs = new Set();
    const allData = [];

    while (currentTime < now && page < maxPages) {
        page++;
        const url = `${baseUrl}&after=${currentTime}`;
        let responseData;
        // Retry mechanism for each page request
        for (let attempt = 1; attempt <= 3; attempt++) {
            try {
                const res = await fetch(url);
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status} error`);
                }
                const data = await res.json();
                if (data.code !== '0') {
                    throw new Error(`API error ${data.code}: ${data.msg || 'Unknown error'}`);
                }
                responseData = data.data;
                break; // fetched successfully
            } catch (err) {
                if (attempt < 3) {
                    // wait an increasing interval before retrying
                    await new Promise(res => setTimeout(res, attempt * 1000));
                } else {
                    throw new Error(`Failed to fetch page ${page}: ${err.message}`);
                }
            }
        }

        const candles = responseData || [];
        if (candles.length === 0) {
            // No more data
            break;
        }

        // If the last candle is incomplete (confirm == 0), remove it and mark as end reached
        let reachedEnd = false;
        const lastCandle = candles[candles.length - 1];
        if (lastCandle && lastCandle.length > 7 && lastCandle[8] === '0') {
            // Drop the incomplete candle
            candles.pop();
            reachedEnd = true;
        }

        // Process and store each complete candle
        for (const candle of candles) {
            if (candle.length > 7) {
                const ts = candle[0];
                const confirm = candle[8];
                if (confirm !== '1') {
                    // Skip incomplete candles (should not occur here after removal)
                    continue;
                }
                if (seenTs.has(ts)) {
                    // Skip duplicate timestamp (if any)
                    continue;
                }
                seenTs.add(ts);
                // Store necessary fields for CSV
                allData.push({
                    ts: Number(ts),
                    open: candle[1],
                    high: candle[2],
                    low: candle[3],
                    close: candle[4],
                    vol: candle[5]
                });
            }
        }

        if (reachedEnd) {
            // Reached the latest available data
            break;
        }

        // Update currentTime for next page (skip the last candle timestamp to avoid overlap)
        const lastTs = Number(candles[candles.length - 1][0]);
        currentTime = lastTs + intervalMs;
    }

    // Check data volume completeness
    if (allData.length < 30000) {
        throw new Error(`Data incomplete: expected >= 30000 records, got ${allData.length}`);
    }

    // Sort data by timestamp ascending
    allData.sort((a, b) => a.ts - b.ts);

    // Prepare CSV content
    let csv = 'ts,iso,open,high,low,close,vol\n';
    for (const entry of allData) {
        const date = new Date(entry.ts);
        // Convert to ISO 8601 (UTC) without milliseconds
        const iso = date.toISOString().slice(0, 19) + 'Z';
        csv += `${entry.ts},${iso},${entry.open},${entry.high},${entry.low},${entry.close},${entry.vol}\n`;
    }

    // Ensure output directory exists
    await fs.mkdir('eth15m-monitor', { recursive: true });
    // Write to CSV file
    await fs.writeFile(outputPath, csv);
}

fetchAllData().catch(err => {
    console.error(err);
    process.exit(1);
});
