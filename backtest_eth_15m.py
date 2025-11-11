#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
新基础版 · 低杠杆验证版（1x 杠杆 / 20% 仓位）
回测目标：验证新基础版策略逻辑在低风险参数下是否仍有边缘正期望。
"""

import pandas as pd
import numpy as np

CSV_PATH       = "okx_eth_15m.csv"
INITIAL_EQUITY = 50.0
LEVERAGE       = 1.0           # ✅ 杠杆降低到 1x
FEE_RATE       = 0.0007        # ✅ 保留手续费（跑完后改成 0 再测第二组）
MARGIN_FRACTION = 0.2          # ✅ 每次仅用 20% 资金入场

# ATR 参数
ATR_LEN        = 34
ATR_MULT_SL    = 3.5           # ATR*3.5 止损
TRAIL_TRIGGER  = 0.06          # 浮盈 ≥6%
TRAIL_BACK     = 0.03          # 回撤3% 平仓

# ========== 加载数据 ==========
def load_data(path: str):
    df = pd.read_csv(path)
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="ms", utc=True, errors="coerce")
    else:
        df["dt"] = pd.to_datetime(df.iloc[:,0], utc=True, errors="coerce")
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df

# ========== 计算指标 ==========
def add_indicators(df):
    df["ema34"] = df["close"].ewm(span=34, adjust=False).mean()
    df["ema144"] = df["close"].ewm(span=144, adjust=False).mean()
    df["atr"] = (
        pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs()
        ], axis=1).max(axis=1)
    ).ewm(span=34, adjust=False).mean()
    df["trend_dir"] = np.sign(df["ema34"] - df["ema144"])
    return df.dropna().reset_index(drop=True)

# ========== 回测逻辑 ==========
def backtest(df):
    equity = INITIAL_EQUITY
    in_pos = False
    direction = 0
    entry_price = 0
    atr_entry = 0
    best_price = 0
    margin_used = 0
    trades = []

    for i, row in df.iterrows():
        c = row["close"]; h = row["high"]; l = row["low"]; atr = row["atr"]; dt = row["dt"]
        trend_dir = int(row["trend_dir"])

        # 管理已有仓位
        if in_pos:
            if direction == 1:
                best_price = max(best_price, h)
                atr_stop = entry_price - atr_entry * ATR_MULT_SL
                gain = (best_price - entry_price) / entry_price
                trail_stop = best_price * (1 - TRAIL_BACK) if gain >= TRAIL_TRIGGER else None
                stop = max(atr_stop, trail_stop) if trail_stop else atr_stop
                if l <= stop:
                    exit_price = stop
                elif trend_dir == -1:
                    exit_price = c
                else:
                    continue
            else:
                best_price = min(best_price, l)
                atr_stop = entry_price + atr_entry * ATR_MULT_SL
                gain = (entry_price - best_price) / entry_price
                trail_stop = best_price * (1 + TRAIL_BACK) if gain >= TRAIL_TRIGGER else None
                stop = min(atr_stop, trail_stop) if trail_stop else atr_stop
                if h >= stop:
                    exit_price = stop
                elif trend_dir == 1:
                    exit_price = c
                else:
                    continue

            size = margin_used * LEVERAGE / entry_price
            pnl = (exit_price - entry_price) * size * direction
            fees = (entry_price * size + exit_price * size) * FEE_RATE
            pnl_net = pnl - fees
            equity += pnl_net

            trades.append({
                "entry_time": entry_time, "exit_time": dt,
                "entry_price": entry_price, "exit_price": exit_price,
                "direction": direction, "pnl_net": pnl_net,
                "equity_after": equity
            })
            in_pos = False
            if equity <= 0: break

        # 开仓条件：仅当趋势明确
        elif trend_dir != 0 and equity > 0:
            margin_used = equity * MARGIN_FRACTION
            entry_price = c; atr_entry = atr; entry_time = dt
            direction = trend_dir
            best_price = h if direction == 1 else l
            in_pos = True

    return equity, trades

# ========== 输出结果 ==========
def summarize(df, equity, trades):
    wins = sum(1 for t in trades if t["pnl_net"] > 0)
    losses = sum(1 for t in trades if t["pnl_net"] < 0)
    total_pnl = sum(t["pnl_net"] for t in trades)
    winrate = (wins / len(trades)) * 100 if trades else 0
    avg_win = np.mean([t["pnl_net"] for t in trades if t["pnl_net"] > 0]) if wins else 0
    avg_loss = np.mean([t["pnl_net"] for t in trades if t["pnl_net"] < 0]) if losses else 0
    eq_curve = [INITIAL_EQUITY] + [t["equity_after"] for t in trades]
    peak, max_dd = eq_curve[0], 0
    for x in eq_curve:
        if x > peak: peak = x
        max_dd = min(max_dd, (x - peak) / peak)
    print(f"========== 新基础版·低杠杆验证 ==========")
    print(f"总交易数: {len(trades)} | 胜率: {winrate:.2f}%")
    print(f"总盈亏: {total_pnl:.4f} U | 期末资金: {equity:.4f} U")
    print(f"平均盈利单: {avg_win:.4f} | 平均亏损单: {avg_loss:.4f}")
    print(f"最大回撤: {max_dd*100:.2f}%")

if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
