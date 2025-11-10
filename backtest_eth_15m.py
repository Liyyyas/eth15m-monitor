#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd

# ===== 基本参数 =====
CSV_PATH = "okx_eth_15m.csv"
INITIAL_EQUITY = 50.0
MARGIN_PER_TRADE = 25.0
LEVERAGE = 5.0
FEE_RATE = 0.0007

STOP_LOSS_PCT = 0.03
TRAIL_1_TRIGGER = 0.08
TRAIL_1_PCT = 0.05
TRAIL_2_TRIGGER = 0.15
TRAIL_2_PCT = 0.02

EMA_SHORT = 144
EMA_LONG = 288


# ===== 数据读取 =====
def load_data(path):
    df = pd.read_csv(path)
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True)
    elif "timestamp" in df.columns:
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    else:
        first_col = df.columns[0]
        df["dt"] = pd.to_datetime(df[first_col], unit="s", utc=True)
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")
    return df


# ===== 计算均线 =====
def add_trend_columns(df):
    close = df["close"]
    df["ema144"] = close.ewm(span=EMA_SHORT, adjust=False).mean()
    df["ema288"] = close.ewm(span=EMA_LONG, adjust=False).mean()
    df.dropna(subset=["ema144", "ema288"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ===== 判断趋势与回踩开仓 =====
def is_uptrend(row):
    return row["ema144"] > row["ema288"]

def is_pullback(prev_row, row):
    return prev_row["close"] <= prev_row["ema144"] and row["close"] > row["ema144"]


# ===== 回测逻辑 =====
def backtest(df):
    equity = INITIAL_EQUITY
    in_pos = False
    entry_price = None
    entry_time = None
    high_since_entry = None
    stop_price = None
    trail_mode = 0

    trades = []

    for i in range(1, len(df)):
        row, prev_row = df.iloc[i], df.iloc[i-1]
        dt, o, h, l, c = row["dt"], row["open"], row["high"], row["low"], row["close"]

        # 管理仓位
        if in_pos:
            high_since_entry = max(high_since_entry, h)
            gain_pct = (high_since_entry - entry_price) / entry_price

            # 启动跟踪
            if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_1_PCT)
                trail_mode = 1
            if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_2_PCT)
                trail_mode = 2

            if trail_mode > 0:
                new_stop = high_since_entry * (1 - (TRAIL_1_PCT if trail_mode == 1 else TRAIL_2_PCT))
                stop_price = max(stop_price, new_stop)

            fixed_stop = entry_price * (1 - STOP_LOSS_PCT)
            stop_price = max(stop_price or 0, fixed_stop)

            exit_price, exit_reason = None, None
            if l <= stop_price:
                exit_price = stop_price
                exit_reason = "stop_or_trail"

            if exit_price:
                margin = MARGIN_PER_TRADE
                notional = margin * LEVERAGE
                size = notional / entry_price
                gross_pnl = (exit_price - entry_price) * size
                fee_open = notional * FEE_RATE
                fee_close = abs(exit_price * size) * FEE_RATE
                pnl_net = gross_pnl - fee_open - fee_close
                equity += pnl_net
                trades.append({
                    "entry_time": entry_time, "exit_time": dt,
                    "entry_price": entry_price, "exit_price": exit_price,
                    "pnl_net": pnl_net, "equity_after": equity
                })
                in_pos = False
                entry_price = None

        # 开仓逻辑
        if not in_pos and equity >= MARGIN_PER_TRADE:
            if is_uptrend(row) and is_pullback(prev_row, row):
                in_pos = True
                entry_price = c
                entry_time = dt
                high_since_entry = c
                stop_price = c * (1 - STOP_LOSS_PCT)
                trail_mode = 0

    return equity, trades


# ===== 输出结果 =====
def summarize(df, equity, trades):
    n = len(trades)
    wins = sum(t["pnl_net"] > 0 for t in trades)
    losses = n - wins
    total_pnl = sum(t["pnl_net"] for t in trades)
    win_rate = (wins / n * 100) if n else 0

    avg_win = sum(t["pnl_net"] for t in trades if t["pnl_net"] > 0) / wins if wins else 0
    avg_loss = sum(t["pnl_net"] for t in trades if t["pnl_net"] < 0) / losses if losses else 0

    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}\n")
    print("========== 回测结果（轻趋势过滤 + 盈亏优化） ==========")
    print(f"总交易数: {n}")
    print(f"胜率: {win_rate:.2f}%")
    print(f"总盈亏: {total_pnl:.4f} U")
    print(f"期末资金: {equity:.4f} U (初始 {INITIAL_EQUITY})")
    print(f"平均盈利单: {avg_win:.4f} U")
    print(f"平均亏损单: {avg_loss:.4f} U")


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_trend_columns(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
