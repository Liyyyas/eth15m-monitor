#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ========= 参数区域 =========
CSV_PATH       = "okx_eth_15m.csv"   # 你的 15m ETH CSV
INITIAL_EQUITY = 50.0                # 初始资金
MARGIN_FRAC    = 0.5                 # 每次用多少资金做保证金（0.5 = 一半）
LEVERAGE       = 5.0                 # 杠杆倍数
FEE_RATE       = 0.0005              # 单边手续费率（按名义仓位计费）

STOP_LOSS_PCT   = 0.05               # 止损 5%
TRAIL_1_TRIGGER = 0.05               # 浮盈 5% 开始第一档跟踪
TRAIL_1_PCT     = 0.05               # 第一档：5% 跟踪止盈
TRAIL_2_TRIGGER = 0.10               # 浮盈 10% 切换到第二档
TRAIL_2_PCT     = 0.02               # 第二档：2% 跟踪止盈

MIN_MARGIN      = 5.0                # 最小开仓保证金，太小就不再开新仓


# ========= 读取 & 预处理数据 =========
df = pd.read_csv(CSV_PATH)

if "iso" in df.columns:
    df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
elif "ts" in df.columns:
    df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                              unit="ms", utc=True)
else:
    raise ValueError("CSV 里找不到 iso 或 ts 列。")

df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

for col in ["open", "high", "low", "close"]:
    if col not in df.columns:
        raise ValueError(f"CSV 缺少必要列: {col}")

print("数据行数:", len(df))
print("时间范围:", df["dt"].iloc[0], "->", df["dt"].iloc[-1])

# ========= 回测状态 =========
equity = INITIAL_EQUITY
equity_curve = [equity]
equity_time  = [df["dt"].iloc[0]]

in_pos       = False
entry_price  = None
entry_time   = None
qty          = None
margin_used  = None
notional_in  = None       # 入场名义仓位
high_since   = None
low_since    = None
trail_mode   = 0
stop_price   = None

trades = []


# ========= 主循环 =========
for i, row in df.iterrows():
    t = row["dt"]
    o = float(row["open"])
    h = float(row["high"])
    l = float(row["low"])
    c = float(row["close"])

    # 记录资金曲线
    equity_curve.append(equity)
    equity_time.append(t)

    # 1）没有仓位 -> 开新仓
    if not in_pos:
        if equity <= MIN_MARGIN:
            # 钱太少，不值得继续开仓，但还能继续画资金曲线
            continue

        margin_used = equity * MARGIN_FRAC
        if margin_used < MIN_MARGIN:
            # 比例太小也不开了
            continue

        entry_price = o
        entry_time  = t

        # 名义仓位 = 保证金 * 杠杆
        notional_in = margin_used * LEVERAGE
        qty         = notional_in / entry_price

        # 入场手续费：按名义仓位算
        fee_entry = notional_in * FEE_RATE

        # 把保证金 + 手续费从资金里扣掉
        equity -= (margin_used + fee_entry)

        # 初始化止损/跟踪
        high_since = h
        low_since  = l
        trail_mode = 0
        stop_price = entry_price * (1 - STOP_LOSS_PCT)

        in_pos = True
        continue   # 同一根只负责开仓，不在这一根同时平仓

    # 2）已有仓位 -> 更新高低点 & 判断是否触发平仓
    high_since = max(high_since, h)
    low_since  = min(low_since, l)

    # 浮盈百分比（按价格算）
    up_pct = (high_since - entry_price) / entry_price

    # 切换跟踪档位
    if trail_mode == 0 and up_pct >= TRAIL_1_TRIGGER:
        trail_mode = 1
    if trail_mode == 1 and up_pct >= TRAIL_2_TRIGGER:
        trail_mode = 2

    # 更新 stop_price
    if trail_mode == 0:
        stop_price = entry_price * (1 - STOP_LOSS_PCT)
    elif trail_mode == 1:
        stop_price = max(stop_price, high_since * (1 - TRAIL_1_PCT))
    else:
        stop_price = max(stop_price, high_since * (1 - TRAIL_2_PCT))

    exit_reason = None
    exit_price  = None

    # 用最低价判断是否击穿 stop_price
    if l <= stop_price:
        exit_reason = "stop_or_trail"
        exit_price  = stop_price

    if exit_reason is None:
        continue

    # ========= 平仓计算 =========
    notional_out = qty * exit_price

    # 毛利润 = 名义平仓 - 名义开仓
    pnl_gross = notional_out - notional_in

    # 平仓手续费
    fee_exit = notional_out * FEE_RATE

    pnl_net = pnl_gross - fee_exit

    # 归还保证金 + 盈亏
    equity += (margin_used + pnl_net)

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

    # 清空仓位状态，下一根自动再开新仓
    in_pos      = False
    entry_price = None
    entry_time  = None
    qty         = None
    margin_used = None
    notional_in = None
    high_since  = None
    low_since   = None
    trail_mode  = 0
    stop_price  = None


# ========= 统计 =========
n_trades = len(trades)
wins   = [t for t in trades if t["pnl_net"] > 0]
losses = [t for t in trades if t["pnl_net"] < 0]
flats  = [t for t in trades if abs(t["pnl_net"]) < 1e-8]

total_pnl = sum(t["pnl_net"] for t in trades)
end_equity = equity

win_rate = len(wins) / n_trades * 100 if n_trades > 0 else 0.0
avg_win  = sum(t["pnl_net"] for t in wins) / len(wins) if wins else 0.0
avg_loss = sum(t["pnl_net"] for t in losses) / len(losses) if losses else 0.0

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
