#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ===== 基本配置 =====
CSV_PATH           = "okx_eth_15m.csv"   # 你的 ETH 15m CSV
INITIAL_EQUITY     = 50.0                # 初始资金
TARGET_MARGIN      = 25.0                # 目标保证金（会动态下调）
LEVERAGE           = 5.0                 # 杠杆
FEE_RATE           = 0.0007              # 单边手续费率（0.07%）

STOP_LOSS_PCT      = 0.05                # 固定止损 5%
TRAIL_1_TRIGGER    = 0.05                # 浮盈 >= 5% 启动第一档跟踪
TRAIL_1_PCT        = 0.05                # 第一档：5% 回撤止盈
TRAIL_2_TRIGGER    = 0.10                # 浮盈 >= 10% 启动第二档
TRAIL_2_PCT        = 0.02                # 第二档：2% 回撤止盈

MAX_HOLD_BARS      = 96                  # 保险：最多持仓 1 天（15m*96）
TZ_NAME            = "Asia/Singapore"    # 以新加坡时区计算“每天一单”
ONE_TRADE_PER_DAY  = True                # 每天最多一单（必须开到一单）


# ===== 数据读取 & 预处理 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列：iso / ts / timestamp / 第一列兜底
    if "iso" in df.columns:
        dt = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                            unit=unit, utc=True, errors="coerce")
    elif "timestamp" in df.columns:
        med = pd.to_numeric(df["timestamp"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"),
                            unit=unit, utc=True, errors="coerce")
    else:
        first_col = df.columns[0]
        med = pd.to_numeric(df[first_col], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df[first_col], errors="coerce"),
                            unit=unit, utc=True, errors="coerce")

    df["dt"] = dt
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    # 新加坡本地日界
    df["dt_sgt"] = df["dt"].dt.tz_convert(TZ_NAME)
    df["date_sgt"] = df["dt_sgt"].dt.date
    return df


# ===== 回测主体：每天必须开一单（动态仓位），SGT跨日强平 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    entry_price = None
    entry_time = None
    entry_date_sgt = None
    high_since_entry = None
    stop_price = None
    trail_mode = 0
    bars_in_trade = 0
    margin_used = 0.0

    trades = []

    current_date_sgt = None
    opened_today = False
    last_close = None  # 用于跨日强平的上一根收盘价

    for _, row in df.iterrows():
        dt = row["dt"]
        dt_sgt = row["dt_sgt"]
        date_sgt = row["date_sgt"]

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        # 新的一天（按SGT）
        if current_date_sgt is None or date_sgt != current_date_sgt:
            current_date_sgt = date_sgt
            opened_today = False

        # ===== 有持仓：优先管理仓位 =====
        if in_pos:
            bars_in_trade += 1

            # 更新入场后的最高价
            high_since_entry = h if high_since_entry is None else max(high_since_entry, h)
            gain_pct = (high_since_entry - entry_price) / entry_price

            # 启动追踪
            if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_1_PCT)
                trail_mode = 1
            if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_2_PCT)
                trail_mode = 2

            # 跟随最高价抬高止损
            if trail_mode == 1:
                new_stop = high_since_entry * (1 - TRAIL_1_PCT)
                if stop_price is None or new_stop > stop_price:
                    stop_price = new_stop
            elif trail_mode == 2:
                new_stop = high_since_entry * (1 - TRAIL_2_PCT)
                if stop_price is None or new_stop > stop_price:
                    stop_price = new_stop

            # 固定止损保护
            fixed_stop = entry_price * (1 - STOP_LOSS_PCT)
            stop_price = fixed_stop if stop_price is None else max(stop_price, fixed_stop)

            exit_price = None
            exit_reason = None

            # ① 触发止损 / 追踪止盈
            if l <= stop_price:
                exit_price = stop_price
                exit_reason = "stop_or_trail"

            # ② SGT 跨日强平（上一根收盘价），确保“每天能开新单”
            if exit_price is None and date_sgt != entry_date_sgt and last_close is not None:
                exit_price = last_close
                exit_reason = "day_end"

            # ③ 保险：持仓超过阈值也平
            if exit_price is None and bars_in_trade >= MAX_HOLD_BARS:
                exit_price = c
                exit_reason = "max_hold"

            # 结算
            if exit_price is not None:
                notional = margin_used * LEVERAGE
                size = notional / entry_price

                gross_pnl = (exit_price - entry_price) * size
                fee_open = notional * FEE_RATE
                fee_close = abs(exit_price * size) * FEE_RATE
                pnl_net = gross_pnl - fee_open - fee_close

                equity += pnl_net

                trades.append({
                    "entry_time": entry_time,
                    "exit_time": dt,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "exit_reason": exit_reason,
                    "margin_used": margin_used,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": (pnl_net / margin_used) if margin_used > 0 else 0.0,
                    "equity_after": equity,
                    "bars_held": bars_in_trade,
                    "date_sgt": str(entry_date_sgt)
                })

                # 清空
                in_pos = False
                entry_price = None
                entry_time = None
                entry_date_sgt = None
                high_since_entry = None
                stop_price = None
                trail_mode = 0
                bars_in_trade = 0
                margin_used = 0.0

        # ===== 没持仓：今天还没开单 → 强制开（动态仓位） =====
        if (not in_pos) and (not opened_today):
            # 动态保证金：哪怕本金只剩 7U，也用 7U 开小仓
            # 这样可以保证“每天都有一单”
            margin_used = min(TARGET_MARGIN, max(0.0, equity))
            if margin_used > 0.0:
                in_pos = True
                entry_price = c
                entry_time = dt
                entry_date_sgt = date_sgt
                high_since_entry = c
                stop_price = entry_price * (1 - STOP_LOSS_PCT)
                trail_mode = 0
                bars_in_trade = 0
                opened_today = True  # 当天只开这一单
            # 如果 equity <= 0，实在没法开了（自然停止）

        # 更新上一根收盘
        last_close = c

    return equity, trades


# ===== 结果统计 =====
def summarize(df: pd.DataFrame, equity, trades):
    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}")
    print()

    n = len(trades)
    wins = sum(1 for t in trades if t["pnl_net"] > 0)
    losses = sum(1 for t in trades if t["pnl_net"] < 0)
    flats = n - wins - losses

    total_pnl = sum(t["pnl_net"] for t in trades)
    win_pnls = [t["pnl_net"] for t in trades if t["pnl_net"] > 0]
    loss_pnls = [t["pnl_net"] for t in trades if t["pnl_net"] < 0]
    avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
    avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0

    # 最大回撤
    eq_curve = [INITIAL_EQUITY]
    for t in trades:
        eq_curve.append(t["equity_after"])
    peak = eq_curve[0]
    max_dd = 0.0
    for x in eq_curve:
        if x > peak:
            peak = x
        dd = (x - peak) / peak
        if dd < max_dd:
            max_dd = dd

    # 统计“按SGT日期开单”的覆盖率
    unique_days = df["date_sgt"].nunique()
    trade_days = len(set(t["date_sgt"] for t in trades))
    coverage = trade_days / unique_days * 100 if unique_days > 0 else 0.0

    total_ret = (equity - INITIAL_EQUITY) / INITIAL_EQUITY

    print("========== 回测结果（每天强制一单 + 动态仓位 + 日内止损/追踪） ==========")
    print(f"总交易数: {n}  | 覆盖率(有开单的天/总天): {trade_days}/{unique_days} ({coverage:.2f}%)")
    print(f"胜: {wins}  负: {losses}  和: {flats}")
    print(f"胜率: {wins/n*100:.2f}%" if n>0 else "胜率: 0.00%")
    print(f"总盈亏: {total_pnl:.4f} U")
    print(f"期末资金: {equity:.4f} U (初始 {INITIAL_EQUITY} U)")
    print(f"平均盈利单: {avg_win:.4f} U")
    print(f"平均亏损单: {avg_loss:.4f} U")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"总收益率: {total_ret*100:.2f}%")
    print()
    print("前 5 笔交易示例:")
    for t in trades[:5]:
        print(t)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
