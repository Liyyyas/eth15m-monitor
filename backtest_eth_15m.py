#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
新基础版 v2.0
EMA34 / EMA144 趋势方向 + ATR(34)*3.5 动态止损 + 浮盈≥6%启动3%回撤追踪 + 动态仓位(50%)
"""

import pandas as pd
import math

# === 基本参数 ===
CSV_PATH = "okx_eth_15m.csv"
INITIAL_EQUITY = 50.0
RISK_FRACTION = 0.5
LEVERAGE = 5.0
FEE_RATE = 0.0007

# === ATR 与止损相关参数 ===
ATR_PERIOD = 34
ATR_STOP_MULT = 3.5

# === 浮盈追踪止盈参数 ===
FLOAT_TRIGGER = 0.06  # 启动阈值：6%
FLOAT_TRAIL_PCT = 0.03  # 回撤幅度：3%

# === 数据读取 ===
def load_data(path):
    df = pd.read_csv(path)

    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit=unit, utc=True)
    else:
        first_col = df.columns[0]
        med = pd.to_numeric(df[first_col], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first_col], errors="coerce"), unit=unit, utc=True)

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df

# === 添加指标 ===
def add_indicators(df):
    close = df["close"]
    high = df["high"]
    low = df["low"]

    df["ema34"] = close.ewm(span=34, adjust=False).mean()
    df["ema144"] = close.ewm(span=144, adjust=False).mean()

    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1/ATR_PERIOD, adjust=False).mean()

    return df.dropna(subset=["ema34", "ema144", "atr"]).reset_index(drop=True)

# === 回测逻辑 ===
def backtest(df):
    equity = INITIAL_EQUITY
    in_pos = False
    direction = 0  # 1=多单, -1=空单
    entry_price = None
    pos_size = 0
    margin_used = 0
    best_price = None
    entry_time = None

    trades = []

    for i, row in df.iterrows():
        dt, o, h, l, c = row["dt"], row["open"], row["high"], row["low"], row["close"]
        ema34, ema144, atr = row["ema34"], row["ema144"], row["atr"]

        if math.isnan(atr) or atr <= 0:
            continue

        # === 平仓逻辑 ===
        if in_pos:
            stop_price = None
            exit_price = None
            exit_reason = None

            if direction == 1:  # 多单
                best_price = max(best_price, h)
                gain_pct = (best_price - entry_price) / entry_price
                base_stop = entry_price - ATR_STOP_MULT * atr
                if gain_pct >= FLOAT_TRIGGER:
                    trail_stop = best_price * (1 - FLOAT_TRAIL_PCT)
                    stop_price = max(base_stop, trail_stop)
                else:
                    stop_price = base_stop
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            elif direction == -1:  # 空单
                best_price = min(best_price, l)
                gain_pct = (entry_price - best_price) / entry_price
                base_stop = entry_price + ATR_STOP_MULT * atr
                if gain_pct >= FLOAT_TRIGGER:
                    trail_stop = best_price * (1 + FLOAT_TRAIL_PCT)
                    stop_price = min(base_stop, trail_stop)
                else:
                    stop_price = base_stop
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            if exit_price is not None:
                notional = abs(entry_price * pos_size)
                gross_pnl = (exit_price - entry_price) * pos_size
                fee = notional * FEE_RATE + abs(exit_price * pos_size) * FEE_RATE
                pnl_net = gross_pnl - fee
                equity += pnl_net
                trades.append({
                    "entry_time": entry_time, "exit_time": dt,
                    "entry_price": entry_price, "exit_price": exit_price,
                    "direction": direction, "margin_used": margin_used,
                    "pnl_net": pnl_net, "pnl_pct_on_margin": pnl_net / margin_used if margin_used else 0,
                    "equity_after": equity, "exit_reason": exit_reason
                })
                in_pos = False
                direction = 0
                if equity <= 0:
                    break

        # === 开仓逻辑 ===
        if not in_pos and equity > 0:
            if ema34 > ema144:
                new_dir = 1
            elif ema34 < ema144:
                new_dir = -1
            else:
                continue
            margin = equity * RISK_FRACTION
            notional = margin * LEVERAGE
            pos_size = notional / c
            in_pos = True
            direction = new_dir
            entry_price = c
            entry_time = dt
            margin_used = margin
            best_price = c

    return equity, trades

# === 汇总结果 ===
def summarize(df, equity, trades):
    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}\n")

    n = len(trades)
    wins = sum(1 for t in trades if t["pnl_net"] > 0)
    losses = n - wins
    total_pnl = sum(t["pnl_net"] for t in trades)
    avg_win = sum(t["pnl_net"] for t in trades if t["pnl_net"] > 0) / wins if wins else 0
    avg_loss = sum(t["pnl_net"] for t in trades if t["pnl_net"] < 0) / losses if losses else 0

    eq_curve = [INITIAL_EQUITY] + [t["equity_after"] for t in trades]
    peak, max_dd = eq_curve[0], 0
    for x in eq_curve:
        peak = max(peak, x)
        max_dd = min(max_dd, (x - peak) / peak)
    total_ret = (equity - INITIAL_EQUITY) / INITIAL_EQUITY

    print("========== 回测结果（新基础版 v2.0） ==========")
    print(f"总交易数: {n}")
    print(f"胜率: {wins/n*100:.2f}%")
    print(f"总盈亏: {total_pnl:.4f} U")
    print(f"期末资金: {equity:.4f} U (初始 {INITIAL_EQUITY})")
    print(f"平均盈利单: {avg_win:.4f} U  | 平均亏损单: {avg_loss:.4f} U")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"总收益率: {total_ret*100:.2f}%")
    print("\n前5笔交易示例:")
    for t in trades[:5]:
        print(t)

# === 主程序 ===
if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
