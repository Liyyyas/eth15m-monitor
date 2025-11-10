#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ===== 基本配置 =====
CSV_PATH         = "okx_eth_15m.csv"   # 你的 ETH 15m CSV
INITIAL_EQUITY   = 50.0                # 初始资金
MARGIN_PER_TRADE = 25.0                # 每笔保证金（想拖长寿命可以改成 10）
LEVERAGE         = 5.0                 # 杠杆
FEE_RATE         = 0.0007              # 单边手续费率（0.07% 自己按交易所改）

STOP_LOSS_PCT    = 0.05                # 固定止损 5%
TRAIL_1_TRIGGER  = 0.05                # 浮盈 >= 5% 启动第一档跟踪
TRAIL_1_PCT      = 0.05                # 第一档：5% 回撤止盈
TRAIL_2_TRIGGER  = 0.10                # 浮盈 >= 10% 启动第二档
TRAIL_2_PCT      = 0.02                # 第二档：2% 回撤止盈

# 最多持仓多少根 15mK（保险用，理论上有“按天强平”就够了）
MAX_HOLD_BARS    = 96                  # ≈ 1 天


# ===== 数据读取 & 预处理 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列：iso / ts / timestamp / 第一列兜底
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    elif "timestamp" in df.columns:
        med = pd.to_numeric(df["timestamp"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    else:
        first_col = df.columns[0]
        med = pd.to_numeric(df[first_col], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first_col], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


# ===== 回测主体：每天尽量开一单，日内止损/止盈，跨日强平 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    entry_price = None
    entry_time = None
    entry_date = None  # 记录是哪一天开的仓
    high_since_entry = None
    stop_price = None
    trail_mode = 0
    bars_in_trade = 0

    trades = []

    current_date = None
    opened_today = False
    last_close = None  # 用来在跨日时按“前一根K线收盘价”强平

    for _, row in df.iterrows():
        dt = row["dt"]
        date = dt.date()
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        # 新的一天，重置“今日是否已经开过单”
        if current_date is None or date != current_date:
            current_date = date
            opened_today = False

        # ===== 有持仓：先管理这个仓位 =====
        if in_pos:
            bars_in_trade += 1

            # 先看日内止损 / 跟踪止盈
            if high_since_entry is None:
                high_since_entry = h
            else:
                high_since_entry = max(high_since_entry, h)

            gain_pct = (high_since_entry - entry_price) / entry_price

            # 1档：盈亏 >= 5% 启动 5% 回撤
            if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_1_PCT)
                trail_mode = 1

            # 2档：盈亏 >= 10% 启动 2% 回撤
            if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_2_PCT)
                trail_mode = 2

            # 更新跟踪止损价格
            if trail_mode == 1:
                new_stop = high_since_entry * (1 - TRAIL_1_PCT)
                if stop_price is None or new_stop > stop_price:
                    stop_price = new_stop
            elif trail_mode == 2:
                new_stop = high_since_entry * (1 - TRAIL_2_PCT)
                if stop_price is None or new_stop > stop_price:
                    stop_price = new_stop

            # 固定止损 5% 永远生效
            fixed_stop = entry_price * (1 - STOP_LOSS_PCT)
            if stop_price is None:
                stop_price = fixed_stop
            else:
                stop_price = max(stop_price, fixed_stop)

            exit_price = None
            exit_reason = None

            # ① 日内价格打到止损 / 止盈
            if l <= stop_price:
                exit_price = stop_price
                exit_reason = "stop_or_trail"

            # ② 跨日强平：只要日期变化（进入新的一天），但当前单还没出场
            if exit_price is None and date != entry_date and last_close is not None:
                # 按“上一根 K 线收盘价”平仓
                exit_price = last_close
                exit_reason = "day_end"

            # ③ 保险：持仓超过 MAX_HOLD_BARS 也强平
            if exit_price is None and bars_in_trade >= MAX_HOLD_BARS:
                exit_price = c
                exit_reason = "max_hold"

            # 真正平仓
            if exit_price is not None:
                margin = MARGIN_PER_TRADE
                notional = margin * LEVERAGE
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
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin,
                    "equity_after": equity,
                    "bars_held": bars_in_trade,
                })

                in_pos = False
                entry_price = None
                entry_time = None
                entry_date = None
                high_since_entry = None
                stop_price = None
                trail_mode = 0
                bars_in_trade = 0

        # ===== 没有持仓：今天还没开单 → 强制找机会开一单 =====
        if (not in_pos) and (not opened_today) and equity >= MARGIN_PER_TRADE:
            # 不看趋势、不看形态，只要账户还有钱，就今天开一单
            in_pos = True
            entry_price = c
            entry_time = dt
            entry_date = date
            high_since_entry = c
            stop_price = entry_price * (1 - STOP_LOSS_PCT)
            trail_mode = 0
            bars_in_trade = 0
            opened_today = True

        # 更新 last_close（给下一根 K 线用）
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

    total_ret = (equity - INITIAL_EQUITY) / INITIAL_EQUITY
    ann_ret = total_ret  # 一年数据，年化≈总收益率

    print("========== 回测结果（每天尽量开一单 + 日内止损止盈） ==========")
    print(f"总交易数: {n}")
    print(f"胜: {wins}  负: {losses}  和: {flats}")
    win_rate = wins / n * 100 if n > 0 else 0.0
    print(f"胜率: {win_rate:.2f}%")
    print(f"总盈亏: {total_pnl:.4f} U")
    print(f"期末资金: {equity:.4f} U (初始 {INITIAL_EQUITY} U)")
    print(f"平均盈利单: {avg_win:.4f} U")
    print(f"平均亏损单: {avg_loss:.4f} U")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"总收益率: {total_ret*100:.2f}%  | 年化收益率估计: {ann_ret*100:.2f}%")
    print()
    print("前 5 笔交易示例:")
    for t in trades[:5]:
        print(t)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
