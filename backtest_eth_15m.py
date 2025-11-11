#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
双状态切换回测：
- 数据：ETH 15m，文件名 okx_eth_15m.csv
- 趋势环境：ADX(14) > 22 且 |EMA34-EMA144|/close >= 0.4%
    · 策略：Donchian(55) 突破顺势（上破做多，下破做空）
    · 止损：ATR(34)*3
    · 追踪止损：ATR(34)*2
    · 最长持仓：3天 ≈ 288 根

- 震荡环境：不满足趋势条件时
    · 策略：布林带(20,2.2) + RSI(2) 均值回归
        · close <= 下轨 且 RSI2 < 10 → 做多
        · close >= 上轨 且 RSI2 > 90 → 做空
    · 止盈：中轨
    · 止损：轨外 ± 0.5*ATR(34)
    · 最长持仓：12小时 ≈ 48 根

- 风控：
    · 初始资金：50 U
    · 每次固定保证金：25 U
    · 杠杆：5x
    · 手续费：单边 0.07%（可自行调整）
"""

import pandas as pd
import numpy as np

CSV_PATH         = "okx_eth_15m.csv"
INITIAL_EQUITY   = 50.0
MARGIN_PER_TRADE = 25.0
LEVERAGE         = 5.0
FEE_RATE         = 0.0007  # 单边手续费率

# === 工具函数：加载数据 ===
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列处理：优先 iso，其次 ts，否则用首列
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

    # 必须字段
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


# === 指标计算：EMA / ATR / ADX / BB / Donchian / RSI2 ===
def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    high = df["high"].astype(float)
    low  = df["low"].astype(float)
    close = df["close"].astype(float)

    # EMA34 / EMA144
    df["ema34"] = close.ewm(span=34, adjust=False).mean()
    df["ema144"] = close.ewm(span=144, adjust=False).mean()

    # True Range & ATR(14, 34)
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    df["atr14"] = tr.rolling(window=14, min_periods=14).mean()
    df["atr34"] = tr.rolling(window=34, min_periods=34).mean()

    # ADX(14)
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr14 = tr.rolling(window=14, min_periods=14).sum()
    plus_di14 = 100 * pd.Series(plus_dm).rolling(window=14, min_periods=14).sum() / tr14
    minus_di14 = 100 * pd.Series(minus_dm).rolling(window=14, min_periods=14).sum() / tr14

    dx = ( (plus_di14 - minus_di14).abs() / (plus_di14 + minus_di14).replace(0, np.nan) ) * 100
    df["adx14"] = dx.rolling(window=14, min_periods=14).mean()

    # Bollinger Bands(20, 2.2)
    ma20 = close.rolling(window=20, min_periods=20).mean()
    std20 = close.rolling(window=20, min_periods=20).std()
    df["bb_mid"] = ma20
    df["bb_up"]  = ma20 + 2.2 * std20
    df["bb_low"] = ma20 - 2.2 * std20

    # Donchian (55)
    df["donchian_high_55"] = high.rolling(window=55, min_periods=55).max().shift(1)
    df["donchian_low_55"]  = low.rolling(window=55, min_periods=55).min().shift(1)

    # RSI(2)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    roll_up = gain.rolling(window=2, min_periods=2).mean()
    roll_down = loss.rolling(window=2, min_periods=2).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    df["rsi2"] = 100 - (100 / (1 + rs))

    # 丢掉指标尚未齐全的前面部分
    df = df.dropna(subset=["ema34", "ema144", "atr34", "adx14",
                           "bb_mid", "bb_up", "bb_low",
                           "donchian_high_55", "donchian_low_55", "rsi2"]).reset_index(drop=True)
    return df


def is_trend_row(row) -> bool:
    """
    趋势环境：
    ADX(14) > 22 且 |EMA34-EMA144|/close >= 0.4%
    """
    close = float(row["close"])
    ema34 = float(row["ema34"])
    ema144 = float(row["ema144"])
    adx14 = float(row["adx14"])
    ema_gap = abs(ema34 - ema144) / close
    return (adx14 > 22.0) and (ema_gap >= 0.004)


# === 回测主体 ===
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0   # 1 多单，-1 空单
    entry_price = None
    entry_time = None
    trade_type = None  # "trend" or "range"
    stop_price = None
    take_profit = None
    bars_in_trade = 0
    extreme_price = None  # 多单最高价 / 空单最低价（用于趋势追踪）

    trades = []

    for i, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        atr = float(row["atr34"])
        donchian_high = float(row["donchian_high_55"])
        donchian_low  = float(row["donchian_low_55"])
        bb_mid = float(row["bb_mid"])
        bb_up  = float(row["bb_up"])
        bb_low = float(row["bb_low"])
        rsi2   = float(row["rsi2"])
        in_trend = is_trend_row(row)

        exit_price = None
        exit_reason = None

        # ==== 先管理已有持仓 ====
        if in_pos:
            bars_in_trade += 1

            # 趋势单：ATR 追踪 + 时间止盈（最多3天 ≈ 288根）
            if trade_type == "trend":
                if direction == 1:
                    # 多单：更新 extreme 为最高价
                    extreme_price = max(extreme_price, h)
                    # 跟踪止损：最高价 - ATR*2
                    trail_stop = extreme_price - atr * 2.0
                    # 初始硬止损：进场价 - ATR*3
                    hard_stop = entry_price - atr * 3.0
                    # 当前有效止损：两者取较大（离价格更近）
                    new_stop = max(trail_stop, hard_stop)
                    if stop_price is None:
                        stop_price = new_stop
                    else:
                        stop_price = max(stop_price, new_stop)
                    # 触发止损
                    if l <= stop_price:
                        exit_price = stop_price
                        exit_reason = "trend_atr_sl_or_trail"
                    # 时间止盈：持仓>=288根
                    elif bars_in_trade >= 288:
                        exit_price = c
                        exit_reason = "trend_time_exit"

                elif direction == -1:
                    # 空单：更新 extreme 为最低价
                    extreme_price = min(extreme_price, l)
                    trail_stop = extreme_price + atr * 2.0
                    hard_stop = entry_price + atr * 3.0
                    new_stop = min(trail_stop, hard_stop)
                    if stop_price is None:
                        stop_price = new_stop
                    else:
                        stop_price = min(stop_price, new_stop)
                    if h >= stop_price:
                        exit_price = stop_price
                        exit_reason = "trend_atr_sl_or_trail"
                    elif bars_in_trade >= 288:
                        exit_price = c
                        exit_reason = "trend_time_exit"

            # 震荡单：中轨止盈 + 轨外±0.5ATR止损 + 时间止盈（48根）
            elif trade_type == "range":
                # 多单
                if direction == 1:
                    # 先检查止损（保守：先止损后止盈）
                    stop_price = bb_low - 0.5 * atr
                    if l <= stop_price:
                        exit_price = stop_price
                        exit_reason = "range_sl"
                    else:
                        # 止盈：高价触及中轨
                        if h >= bb_mid:
                            exit_price = bb_mid
                            exit_reason = "range_tp"
                        elif bars_in_trade >= 48:
                            exit_price = c
                            exit_reason = "range_time_exit"

                # 空单
                elif direction == -1:
                    stop_price = bb_up + 0.5 * atr
                    if h >= stop_price:
                        exit_price = stop_price
                        exit_reason = "range_sl"
                    else:
                        if l <= bb_mid:
                            exit_price = bb_mid
                            exit_reason = "range_tp"
                        elif bars_in_trade >= 48:
                            exit_price = c
                            exit_reason = "range_time_exit"

            # 如有 exit_price，立即结算
            if exit_price is not None:
                margin = MARGIN_PER_TRADE
                notional = margin * LEVERAGE
                size = notional / entry_price

                if direction == 1:
                    gross_pnl = (exit_price - entry_price) * size
                else:
                    gross_pnl = (entry_price - exit_price) * size

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
                    "trade_type": trade_type,
                    "margin_used": margin,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin,
                    "equity_after": equity,
                    "bars_held": bars_in_trade
                })

                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                trade_type = None
                stop_price = None
                take_profit = None
                extreme_price = None
                bars_in_trade = 0

        # ==== 没有持仓 → 寻找入场 ====
        if (not in_pos) and equity > 0:

            # 1) 趋势环境：ADX高 + EMA34/144 张口足够大 → Donchian 突破
            if in_trend:
                # 上破 Donchian55 高点 → 做多
                if c > donchian_high:
                    direction = 1
                    trade_type = "trend"
                    entry_price = c
                    entry_time = dt
                    in_pos = True
                    bars_in_trade = 0
                    extreme_price = c
                    # 初始止损：价差 - ATR*3
                    stop_price = entry_price - atr * 3.0

                # 下破 Donchian55 低点 → 做空
                elif c < donchian_low:
                    direction = -1
                    trade_type = "trend"
                    entry_price = c
                    entry_time = dt
                    in_pos = True
                    bars_in_trade = 0
                    extreme_price = c
                    stop_price = entry_price + atr * 3.0

            # 2) 震荡环境：布林带 + RSI2 → 均值回归
            else:
                # 多：跌出下轨 + RSI2 超卖
                if (c <= bb_low) and (rsi2 < 10):
                    direction = 1
                    trade_type = "range"
                    entry_price = c
                    entry_time = dt
                    in_pos = True
                    bars_in_trade = 0
                    stop_price = bb_low - 0.5 * atr
                    take_profit = bb_mid

                # 空：顶出上轨 + RSI2 超买
                elif (c >= bb_up) and (rsi2 > 90):
                    direction = -1
                    trade_type = "range"
                    entry_price = c
                    entry_time = dt
                    in_pos = True
                    bars_in_trade = 0
                    stop_price = bb_up + 0.5 * atr
                    take_profit = bb_mid

    return equity, trades


# === 结果统计 ===
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
    ann_ret = total_ret  # 一年数据，简单视为年化

    print("========== 回测结果（双状态：趋势/震荡） ==========")
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
    print("前 5 笔已平仓交易示例:")
    for t in trades[:5]:
        print(t)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = calc_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
