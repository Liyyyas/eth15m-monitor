#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
新基础版 v2.0（整理版）

核心逻辑：
- 只做 ETH 15m K 线，双向多空都可以做
- EMA34 / EMA144 判断趋势方向
- 入场：顺着 EMA 方向，只要发生反转就上车（不做特别复杂过滤）
- 止损：ATR(34) * 3.5
- 浮盈 >= 6% 后启用 3% 回撤移动止盈
- 仓位：固定用当前资金的 50% 做保证金，5x 杠杆
"""

import pandas as pd
import numpy as np

# ===== 基础配置 =====
CSV_PATH = "okx_eth_15m.csv"

INITIAL_EQUITY = 50.0          # 初始资金
BASE_MARGIN_RATIO = 0.5        # 每笔使用账户资金的 50% 作为保证金
LEVERAGE = 5.0                 # 杠杆
FEE_RATE = 0.0007              # 单边手续费率（根据交易所改）

ATR_PERIOD = 34                # ATR 周期
ATR_STOP_MULT = 3.5            # ATR 止损倍数

TRAIL_TRIGGER_PCT = 0.06       # 浮盈 >= 6% 启用移动止盈
TRAIL_DRAW_PCT = 0.03          # 移动止盈回撤 3%

MIN_MARGIN = 5.0               # 资金太少就视为归零，停止开新仓


# ===== 读取 & 预处理 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 解析时间列：优先 iso，其次 ts，其它兜底
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(
            pd.to_numeric(df["ts"], errors="coerce"),
            unit=unit,
            utc=True,
            errors="coerce",
        )
    else:
        first_col = df.columns[0]
        med = pd.to_numeric(df[first_col], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(
            pd.to_numeric(df[first_col], errors="coerce"),
            unit=unit,
            utc=True,
            errors="coerce",
        )

    # 必要列检查
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df


# ===== 指标计算 =====
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    # EMA
    df["ema34"] = close.ewm(span=34, adjust=False).mean()
    df["ema144"] = close.ewm(span=144, adjust=False).mean()
    df["ema_gap"] = (df["ema34"] - df["ema144"]) / close

    # ATR(34)
    c_prev = close.shift(1)
    tr = np.maximum(
        df["high"] - df["low"],
        np.maximum((df["high"] - c_prev).abs(), (df["low"] - c_prev).abs()),
    )
    df["atr34"] = tr.ewm(alpha=1 / ATR_PERIOD, adjust=False).mean()

    # 趋势方向：1 多头，-1 空头，0 无趋势（gap 太小）
    df["trend_dir"] = 0
    strong_trend = df["ema_gap"].abs() > 0.001  # 大约 0.1% 的差距
    df.loc[strong_trend & (df["ema34"] > df["ema144"]), "trend_dir"] = 1
    df.loc[strong_trend & (df["ema34"] < df["ema144"]), "trend_dir"] = -1

    # 丢掉前期 NaN
    df = df.dropna(subset=["ema34", "ema144", "atr34"]).reset_index(drop=True)
    return df


# ===== 回测主体 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0       # 1 多，-1 空
    entry_price = None
    entry_time = None
    margin_used = 0.0

    best_price = None   # 入场以来，对多单是最高价，对空单是最低价
    stop_price = None

    trades = []
    eq_curve = [equity]

    prev_trend_dir = 0

    for _, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        trend_dir = int(row["trend_dir"])
        atr = float(row["atr34"])

        # ===== 有持仓先管风控 =====
        if in_pos:
            # 更新 best_price（根据方向）
            if best_price is None:
                best_price = c
            else:
                if direction == 1:
                    best_price = max(best_price, h)
                elif direction == -1:
                    best_price = min(best_price, l)

            # 浮盈百分比
            gain_pct = direction * (best_price - entry_price) / entry_price

            # ATR 固定止损
            if direction == 1:
                atr_stop = entry_price - ATR_STOP_MULT * atr
            else:
                atr_stop = entry_price + ATR_STOP_MULT * atr

            # 移动止盈：浮盈 >= 6% 后 启用 3% 回撤
            trail_stop = None
            if gain_pct >= TRAIL_TRIGGER_PCT:
                if direction == 1:
                    trail_stop = best_price * (1 - TRAIL_DRAW_PCT)
                else:
                    trail_stop = best_price * (1 + TRAIL_DRAW_PCT)

            # 组合止损价
            if trail_stop is not None:
                if direction == 1:
                    stop_price = max(atr_stop, trail_stop)
                else:
                    stop_price = min(atr_stop, trail_stop)
            else:
                stop_price = atr_stop

            exit_price = None
            exit_reason = None

            # 1) 先看 ATR / 移动止损
            if direction == 1 and l <= stop_price:
                exit_price = stop_price
                exit_reason = "atr_sl_or_trail"
            elif direction == -1 and h >= stop_price:
                exit_price = stop_price
                exit_reason = "atr_sl_or_trail"

            # 2) 再看 EMA 反转：趋势方向变号，下一根 K 收盘价平仓
            if exit_price is None and trend_dir != 0 and trend_dir != direction:
                # 用当前收盘价强平
                exit_price = c
                exit_reason = "ema_flip_close"

            # ==== 真正平仓 ====
            if exit_price is not None:
                notional = margin_used * LEVERAGE
                size = notional / entry_price

                gross_pnl = direction * (exit_price - entry_price) * size

                fee_open = notional * FEE_RATE
                fee_close = abs(exit_price * size) * FEE_RATE
                pnl_net = gross_pnl - fee_open - fee_close

                equity += pnl_net

                trades.append(
                    {
                        "entry_time": entry_time,
                        "exit_time": dt,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "exit_reason": exit_reason,
                        "direction": direction,
                        "margin_used": margin_used,
                        "pnl_net": pnl_net,
                        "pnl_pct_on_margin": pnl_net / margin_used
                        if margin_used > 0
                        else 0.0,
                        "equity_after": equity,
                    }
                )

                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                margin_used = 0.0
                best_price = None
                stop_price = None

                eq_curve.append(equity)

        # ===== 没仓位，考虑开仓 =====
        if (not in_pos) and equity > MIN_MARGIN:
            # 只在有明确趋势时交易
            if trend_dir != 0:
                # 开仓方向 = 趋势方向
                direction = trend_dir
                entry_price = c
                entry_time = dt

                margin_used = equity * BASE_MARGIN_RATIO
                notional = margin_used * LEVERAGE

                # 如果保证金太小，就不交易
                if margin_used < MIN_MARGIN:
                    direction = 0
                    entry_price = None
                    entry_time = None
                    margin_used = 0.0
                else:
                    in_pos = True
                    best_price = c
                    # 初始化 stop_price：按 ATR 固定止损
                    if direction == 1:
                        stop_price = entry_price - ATR_STOP_MULT * atr
                    else:
                        stop_price = entry_price + ATR_STOP_MULT * atr

        prev_trend_dir = trend_dir

    return equity, trades, eq_curve, df


# ===== 结果汇总 =====
def summarize(equity, trades, eq_curve, df):
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
    peak = eq_curve[0]
    max_dd = 0.0
    for x in eq_curve:
        if x > peak:
            peak = x
        dd = (x - peak) / peak
        if dd < max_dd:
            max_dd = dd

    total_ret = (equity - INITIAL_EQUITY) / INITIAL_EQUITY
    ann_ret = total_ret  # 一年数据近似年化收益

    print("========== 回测结果（新基础版 v2.0 整理版） ==========")
    print(f"总交易数: {n}")
    print(f"胜: {wins}  负: {losses}  和: {flats}")
    win_rate = wins / n * 100 if n > 0 else 0.0
    print(f"胜率: {win_rate:.2f}%")
    print(f"总盈亏: {total_pnl:.4f} U")
    print(f"期末资金: {equity:.4f} U (初始 {INITIAL_EQUITY} U)")
    print(f"平均盈利单: {avg_win:.4f} U")
    print(f"平均亏损单: {avg_loss:.4f} U")
    print(f"最大回撤: {max_dd * 100:.2f}%")
    print(f"总收益率: {total_ret * 100:.2f}%  | 年化收益率估计: {ann_ret * 100:.2f}%")
    print()
    print("前 5 笔已平仓交易示例:")
    for t in trades[:5]:
        print(t)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = enrich(df)
    equity, trades, eq_curve, df = backtest(df)
    summarize(equity, trades, eq_curve, df)
