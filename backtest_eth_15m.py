#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import timedelta

# ========= 参数区域 =========
CSV_PATH       = "okx_eth_15m.csv"   # 你的 15m ETH CSV
INITIAL_EQUITY = 50.0                # 初始资金
MARGIN_FRAC    = 0.5                 # 每次用多少资金做保证金（0.5 = 一半）
LEVERAGE       = 5.0                 # 杠杆倍数
FEE_RATE       = 0.0005              # 单边手续费率（0.05%），会收开仓 + 平仓两次

STOP_LOSS_PCT  = 0.05                # 止损 5%
TRAIL_1_TRIGGER = 0.05               # 浮盈 5% 开始第一档跟踪
TRAIL_1_PCT     = 0.05               # 第一档：5% 跟踪止盈
TRAIL_2_TRIGGER = 0.10               # 浮盈 10% 切换到第二档
TRAIL_2_PCT     = 0.02               # 第二档：2% 跟踪止盈

# ========= 读取 & 预处理数据 =========
df = pd.read_csv(CSV_PATH)

# 兼容你之前的格式：有 iso 就用 iso，没有就用 ts
if "iso" in df.columns:
    df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
elif "ts" in df.columns:
    # ts 一般是毫秒时间戳
    df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="ms", utc=True)
else:
    raise ValueError("CSV 里找不到 iso 或 ts 列，用来当时间戳。")

# 丢掉时间解析失败的行，按时间排序
df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

# 检查必须存在的列
for col in ["open", "high", "low", "close"]:
    if col not in df.columns:
        raise ValueError(f"CSV 缺少必要列: {col}")

print("数据行数:", len(df))
print("时间范围:", df["dt"].iloc[0], "->", df["dt"].iloc[-1])

# ========= 回测状态变量 =========
equity = INITIAL_EQUITY        # 当前资金
equity_curve = [equity]        # 资金曲线
equity_time  = [df["dt"].iloc[0]]

in_pos = False                 # 是否有仓位
entry_price = None
entry_time  = None
qty         = None             # 持仓张数（ETH 数量）
margin_used = None             # 实际占用保证金（用来计手续费）
high_since  = None             # 开仓以来最高价
low_since   = None             # 开仓以来最低价
trail_mode  = 0                # 0=未触发，1=5% 跟踪，2=2% 跟踪
stop_price  = None             # 当前止损/止盈线

trades = []                    # 用来记录每一笔交易

# ========= 主循环：逐根 K 线回测 =========
for i, row in df.iterrows():
    t   = row["dt"]
    o   = float(row["open"])
    h   = float(row["high"])
    l   = float(row["low"])
    c   = float(row["close"])

    # 记录资金曲线
    equity_curve.append(equity)
    equity_time.append(t)

    # 1）如果当前没有仓位：马上开新仓（只要还有钱）
    if not in_pos:
        if equity <= 0:
            # 已经爆光了，后面也不用模拟了
            continue

        # 按比例用资金的一部分做保证金（例如 0.5 = 一半）
        margin_used = equity * MARGIN_FRAC
        if margin_used <= 0:
            continue

        # 按市价 o 开多
        entry_price = o
        entry_time  = t

        # 持仓张数：保证金 * 杠杆 / 价格
        qty = margin_used * LEVERAGE / entry_price

        # 入场手续费：按照「名义保证金」来算
        fee_entry = margin_used * FEE_RATE
        equity -= fee_entry

        # 初始化价格跟踪
        high_since = h
        low_since  = l
        trail_mode = 0
        stop_price = entry_price * (1 - STOP_LOSS_PCT)

        in_pos = True
        continue  # 这一根 K 线只负责开仓，不考虑在同一根里就触发止损 / 止盈

    # 2）如果已经有仓位：更新最高/最低价，检查是否需要平仓
    high_since = max(high_since, h)
    low_since  = min(low_since, l)

    # 先根据浮盈决定跟踪模式
    up_pct = (high_since - entry_price) / entry_price

    if trail_mode == 0 and up_pct >= TRAIL_1_TRIGGER:
        trail_mode = 1
    if trail_mode == 1 and up_pct >= TRAIL_2_TRIGGER:
        trail_mode = 2

    # 根据模式更新 stop_price
    if trail_mode == 0:
        stop_price = entry_price * (1 - STOP_LOSS_PCT)
    elif trail_mode == 1:
        # 5% 跟踪：止盈价 = 最高价 * (1 - 5%)
        stop_price = max(stop_price, high_since * (1 - TRAIL_1_PCT))
    else:
        # 2% 跟踪：止盈价 = 最高价 * (1 - 2%)
        stop_price = max(stop_price, high_since * (1 - TRAIL_2_PCT))

    exit_reason = None
    exit_price  = None

    # 用最低价判断是否击穿 stop_price
    if l <= stop_price:
        exit_reason = "stop_or_trail"
        exit_price  = stop_price

    # 如果这一根没有触发出场，继续持仓
    if exit_reason is None:
        continue

    # ========= 计算平仓结果 =========
    entry_notional = qty * entry_price
    exit_notional  = qty * exit_price

    pnl_gross = exit_notional - entry_notional

    # 平仓手续费：按占用保证金来算（和开仓一样用 margin_used）
    fee_exit = margin_used * FEE_RATE

    pnl_net = pnl_gross - fee_exit

    # 更新资金：归还保证金 + 盈亏
    equity += margin_used + pnl_net

    trades.append({
        "entry_time": entry_time,
        "exit_time":  t,
        "entry_price": entry_price,
        "exit_price":  exit_price,
        "exit_reason": exit_reason,
        "pnl_net":     pnl_net,
        "pnl_pct_on_margin": pnl_net / margin_used if margin_used > 0 else 0.0,
        "equity_after": equity,
    })

    # 平仓后，本根 K 线结束，下一根会按「没有仓位」的逻辑立刻再开一单
    in_pos = False
    entry_price = None
    qty         = None
    margin_used = None
    high_since  = None
    low_since   = None
    trail_mode  = 0
    stop_price  = None

# ========= 统计结果 =========
n_trades = len(trades)
wins  = [t for t in trades if t["pnl_net"] > 0]
losses = [t for t in trades if t["pnl_net"] < 0]
flats  = [t for t in trades if abs(t["pnl_net"]) < 1e-8]

total_pnl = sum(t["pnl_net"] for t in trades)
end_equity = equity

win_rate = len(wins) / n_trades * 100 if n_trades > 0 else 0.0
avg_win  = sum(t["pnl_net"] for t in wins) / len(wins) if wins else 0.0
avg_loss = sum(t["pnl_net"] for t in losses) / len(losses) if losses else 0.0

# 最大回撤
eq_series = pd.Series(equity_curve, index=equity_time)
roll_max  = eq_series.cummax()
drawdown  = (eq_series - roll_max) / roll_max
max_dd    = drawdown.min() * 100 if len(drawdown) > 0 else 0.0

total_return = (end_equity / INITIAL_EQUITY - 1) * 100

print()
print("========== 回测结果 ==========")
print(f"总交易数: {n_trades}")
print(f"胜: {len(wins)}  负: {len(losses)}  和: {len(flats)}")
print(f"胜率: {win_rate:.2f}%")
print(f"总盈亏: {total_pnl:.4f} U")
print(f"期末资金: {end_equity:.4f} U (初始 {INITIAL_EQUITY} U)")
print(f"平均盈利单: {avg_win:.4f} U")
print(f"平均亏损单: {avg_loss:.4f} U")
print(f"最大回撤: {max_dd:.2f}%")
print(f"总收益率: {total_return:.2f}%")

print("\n前 5 笔交易示例:")
for t in trades[:5]:
    print(t)
