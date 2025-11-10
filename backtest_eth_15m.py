#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import timedelta

CSV_PATH = "okx_eth_15m.csv"

# ========= 策略参数 =========
INITIAL_EQUITY = 50.0     # 初始资金
MARGIN_PER_TRADE = 25.0   # 每笔占用保证金
LEVERAGE = 5.0            # 杠杆
FEE_RATE = 0.0003         # 手续费万3，每边
STOP_LOSS_PCT = 0.05      # 固定止损 5%
TRAIL_1_TRIGGER = 0.05    # 浮盈 >= 5% 启动 5% 跟踪
TRAIL_2_TRIGGER = 0.10    # 浮盈 >= 10% 换 2% 跟踪
TRAIL_1_PCT = 0.05        # 5% 跟踪
TRAIL_2_PCT = 0.02        # 2% 跟踪

# ========= 读取数据 =========
df = pd.read_csv(CSV_PATH)

# 解析时间
if "iso" in df.columns:
    df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
elif "ts" in df.columns:
    med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
    unit = "ms" if med > 1e11 else "s"
    df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                              unit=unit, utc=True, errors="coerce")
else:
    first_col = df.columns[0]
    med = pd.to_numeric(df[first_col], errors="coerce").dropna().median()
    if pd.notna(med):
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first_col], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    else:
        df["dt"] = pd.to_datetime(df[first_col], utc=True, errors="coerce")

df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

# 确保有需要的价格列
for col in ["open", "high", "low", "close"]:
    if col not in df.columns:
        raise ValueError(f"CSV 缺少列: {col}")

print("数据行数:", len(df))
print("时间范围:", df["dt"].iloc[0], "->", df["dt"].iloc[-1])

# ========= 回测 =========
equity = INITIAL_EQUITY
equity_curve = [equity]
equity_time = [df["dt"].iloc[0]]

in_pos = False
entry_price = None
entry_time = None
high_since_entry = None
stop_price = None
trail_mode = 0  # 0 无, 1:5% 跟踪, 2:2% 跟踪

trades = []

for i, row in df.iterrows():
    o = float(row["open"])
    h = float(row["high"])
    l = float(row["low"])
    c = float(row["close"])
    t = row["dt"]

    # 不在仓位 -> 开多
    if not in_pos:
        if equity < MARGIN_PER_TRADE:
            # 钱不够开新仓，后面就当休息
            equity_curve.append(equity)
            equity_time.append(t)
            continue

        entry_price = c
        entry_time = t
        high_since_entry = c

        # 固定止损线：跌 5%
        stop_price = entry_price * (1.0 - STOP_LOSS_PCT)
        trail_mode = 0

        # 开仓手续费
        notional = MARGIN_PER_TRADE * LEVERAGE
        fee_open = notional * FEE_RATE
        equity -= fee_open
        fee_accum = fee_open

        in_pos = True
        equity_curve.append(equity)
        equity_time.append(t)
        continue

    # 在仓位中：更新最高价
    if h > high_since_entry:
        high_since_entry = h

    # 当前浮盈（按最高点算）
    profit_from_entry = (high_since_entry - entry_price) / entry_price

    # 跟踪止盈模式切换
    if trail_mode == 0 and profit_from_entry >= TRAIL_1_TRIGGER:
        trail_mode = 1
    if trail_mode == 1 and profit_from_entry >= TRAIL_2_TRIGGER:
        trail_mode = 2

    # 计算最新止盈价
    if trail_mode == 1:
        new_stop = high_since_entry * (1.0 - TRAIL_1_PCT)
        if new_stop > stop_price:
            stop_price = new_stop
    elif trail_mode == 2:
        new_stop = high_since_entry * (1.0 - TRAIL_2_PCT)
        if new_stop > stop_price:
            stop_price = new_stop

    # 检查本根K线是否击穿止损/止盈
    exit_price = None
    exit_reason = None

    # 用 low 判断是否触及 stop_price（简化假设：穿了就按 stop_price 成交）
    if l <= stop_price:
        exit_price = stop_price
        exit_reason = "stop_or_trail"

    if exit_price is not None:
        # 平仓
        notional = MARGIN_PER_TRADE * LEVERAGE
        fee_close = notional * FEE_RATE

        # 杠杆后的收益百分比（按保证金）
        pnl_pct_on_margin = (exit_price - entry_price) / entry_price * LEVERAGE

        # 这笔交易对保证金的盈亏
        pnl_gross = MARGIN_PER_TRADE * pnl_pct_on_margin
        pnl_net = pnl_gross - fee_close  # 开仓手续费已经在之前扣了

        equity += pnl_net

        trades.append({
            "entry_time": entry_time,
            "exit_time": t,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "pnl_net": pnl_net,
            "pnl_pct_on_margin": pnl_pct_on_margin,
            "equity_after": equity
        })

        # 重置仓位状态
        in_pos = False
        entry_price = None
        entry_time = None
        high_since_entry = None
        stop_price = None
        trail_mode = 0

    equity_curve.append(equity)
    equity_time.append(t)

# ========= 统计结果 =========
n_trades = len(trades)
wins = sum(1 for tr in trades if tr["pnl_net"] > 0)
losses = sum(1 for tr in trades if tr["pnl_net"] < 0)
flats = n_trades - wins - losses

total_pnl = sum(tr["pnl_net"] for tr in trades)
end_equity = equity

avg_win = (sum(tr["pnl_net"] for tr in trades if tr["pnl_net"] > 0) / wins) if wins > 0 else 0.0
avg_loss = (sum(tr["pnl_net"] for tr in trades if tr["pnl_net"] < 0) / losses) if losses > 0 else 0.0

win_rate = (wins / n_trades * 100.0) if n_trades > 0 else 0.0

# 最大回撤
import numpy as np
eq_arr = np.array(equity_curve)
peak = np.maximum.accumulate(eq_arr)
drawdown = (eq_arr - peak) / peak
max_dd = drawdown.min() if len(drawdown) > 0 else 0.0

# 年化收益估算
days_span = (df["dt"].iloc[-1] - df["dt"].iloc[0]).total_seconds() / 86400.0
total_return = (end_equity / INITIAL_EQUITY - 1.0)
if days_span > 0:
    annual_return = (1.0 + total_return) ** (365.0 / days_span) - 1.0
else:
    annual_return = 0.0

print("\n========== 回测结果 ==========")
print(f"总交易数: {n_trades}")
print(f"胜: {wins}  负: {losses}  和: {flats}")
print(f"胜率: {win_rate:.2f}%")
print(f"总盈亏: {total_pnl:.4f} U")
print(f"期末资金: {end_equity:.4f} U (初始 {INITIAL_EQUITY} U)")
print(f"平均盈利单: {avg_win:.4f} U")
print(f"平均亏损单: {avg_loss:.4f} U")
print(f"最大回撤: {max_dd*100:.2f}%")
print(f"总收益率: {total_return*100:.2f}%  | 年化收益率估计: {annual_return*100:.2f}%")

# 如需看前几笔交易：
if n_trades > 0:
    print("\n前 5 笔交易示例:")
    for tr in trades[:5]:
        print(tr)
