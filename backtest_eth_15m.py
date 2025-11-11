#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
新基础版 + 仓位动态调整版

- 数据: okx_eth_15m.csv, 需要包含列: open, high, low, close, 以及时间列 iso 或 ts
- 方向: 通过 EMA34 / EMA144 判定多头/空头，只做一个方向
- 止损: ATR(34) * 3.5
- 浮盈: 浮盈 >= 6% 时启用 3% 回撤移动止损
- 仓位: 资金 < 30 U 用 30% 仓；其他情况用 50% 仓
"""

import pandas as pd
import numpy as np

# ===== 基本参数 =====
CSV_PATH = "okx_eth_15m.csv"

INITIAL_EQUITY = 50.0      # 初始资金
LEVERAGE = 5.0             # 杠杆
FEE_RATE = 0.0007          # 单边手续费率（按名义本金）

EMA_FAST = 34
EMA_SLOW = 144

ATR_PERIOD = 34
ATR_MULT = 3.5             # ATR 止损倍数

TRAIL_TRIGGER = 0.06       # 浮盈 >= 6% 启动移动止损
TRAIL_PCT = 0.03           # 回撤 3% 平仓


# ===== 读取 CSV =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 解析时间列：优先 iso，其次 ts，再不行用第一列尝试
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
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first_col], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


# ===== 指标计算：EMA34/144 + ATR(34) =====
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR 计算
    high = df["high"]
    low = df["low"]

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df["atr"] = tr.rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()

    df = df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)
    return df


def get_trend(row) -> int:
    """
    返回方向:
    1  -> 多头 (EMA34 > EMA144)
    -1 -> 空头 (EMA34 < EMA144)
    0  -> 无交易
    """
    if row["ema_fast"] > row["ema_slow"]:
        return 1
    elif row["ema_fast"] < row["ema_slow"]:
        return -1
    else:
        return 0


# ===== 动态仓位：根据当前资金决定使用多少保证金 =====
def calc_margin(equity: float) -> float:
    """
    - equity < 30 U → 30% 仓
    - equity >= 30 U → 50% 仓
    """
    if equity <= 0:
        return 0.0
    if equity < 30.0:
        risk_pct = 0.30
    else:
        risk_pct = 0.50
    return equity * risk_pct


# ===== 回测主体：原始策略 + ATR(34)*3.5 止损 + 6%→3% 回撤 + 动态仓位 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0      # 1 多 -1 空
    entry_price = None
    entry_time = None
    high_since = None  # 多单用
    low_since = None   # 空单用
    margin_used = 0.0

    trades = []

    for _, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        atr = float(row["atr"])

        trend_dir = get_trend(row)

        # ===== 有持仓：更新止损 & 判断是否出场 =====
        if in_pos:
            # 更新极值
            if direction == 1:
                high_since = max(high_since, h) if high_since is not None else h
            else:
                low_since = min(low_since, l) if low_since is not None else l

            stop_price = None

            if direction == 1:
                # 多单 ATR 止损
                atr_stop = entry_price - atr * ATR_MULT

                # 是否触发浮盈 ≥ 6%（用 high_since）
                trail_stop = None
                if high_since is not None:
                    gain_from_entry = (high_since - entry_price) / entry_price
                    if gain_from_entry >= TRAIL_TRIGGER:
                        trail_stop = high_since * (1 - TRAIL_PCT)

                candidates = [atr_stop]
                if trail_stop is not None:
                    candidates.append(trail_stop)

                # 对多单，止损越高越“保守”，取最大值
                stop_price = max(candidates)

                exit_price = None
                exit_reason = None
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            else:  # direction == -1 空单
                # 空单 ATR 止损
                atr_stop = entry_price + atr * ATR_MULT

                trail_stop = None
                if low_since is not None:
                    gain_from_entry = (entry_price - low_since) / entry_price
                    if gain_from_entry >= TRAIL_TRIGGER:
                        trail_stop = low_since * (1 + TRAIL_PCT)

                candidates = [atr_stop]
                if trail_stop is not None:
                    candidates.append(trail_stop)

                # 对空单，止损越低越“保守”，取最小值
                stop_price = min(candidates)

                exit_price = None
                exit_reason = None
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            if stop_price is not None and exit_price is not None:
                # 结算
                notional = margin_used * LEVERAGE
                size = notional / entry_price

                price_diff = (exit_price - entry_price) * direction
                gross_pnl = price_diff * size

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
                    "direction": direction,
                    "margin_used": margin_used,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin_used if margin_used > 0 else 0.0,
                    "equity_after": equity,
                })

                # 清空持仓
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                high_since = None
                low_since = None
                margin_used = 0.0

                if equity <= 0:
                    equity = 0.0
                    break  # 归零直接停止

        # ===== 没有持仓：根据趋势开仓 =====
        if (not in_pos) and trend_dir != 0 and equity > 0:
            margin = calc_margin(equity)
            if margin <= 0:
                continue

            in_pos = True
            direction = trend_dir
            entry_price = c
            entry_time = dt
            margin_used = margin

            high_since = c
            low_since = c

    # 数据结束时如果还在持仓，按最后收盘价平仓
    if in_pos and equity > 0:
        last_row = df.iloc[-1]
        last_price = float(last_row["close"])
        dt = last_row["dt"]

        notional = margin_used * LEVERAGE
        size = notional / entry_price
        price_diff = (last_price - entry_price) * direction
        gross_pnl = price_diff * size
        fee_open = notional * FEE_RATE
        fee_close = abs(last_price * size) * FEE_RATE
        pnl_net = gross_pnl - fee_open - fee_close
        equity += pnl_net

        trades.append({
            "entry_time": entry_time,
            "exit_time": dt,
            "entry_price": entry_price,
            "exit_price": last_price,
            "exit_reason": "end_of_data",
            "direction": direction,
            "margin_used": margin_used,
            "pnl_net": pnl_net,
            "pnl_pct_on_margin": pnl_net / margin_used if margin_used > 0 else 0.0,
            "equity_after": equity,
        })

    return equity, trades


# ===== 结果统计 =====
def summarize(df: pd.DataFrame, equity: float, trades):
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
    ann_ret = total_ret  # 一年数据，近似 = 总收益率

    print("========== 回测结果（新基础版 + ATR(34)*3.5 + 6%→3%回撤 + 动态仓位） ==========")
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
    df = add_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
