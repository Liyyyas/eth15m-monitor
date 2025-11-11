#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import timedelta

# ========= 基本配置 =========
CSV_PATH = "okx_eth_15m.csv"

INITIAL_EQUITY   = 50.0      # 初始资金
LEVERAGE         = 5.0       # 杠杆
FEE_RATE         = 0.0007    # 单边手续费

RISK_PER_TRADE   = 0.80      # 每笔使用资金比例（69% 版本原来相当于 0.5，这里调到 0.8）

# 趋势均线
EMA_FAST = 34
EMA_SLOW = 144

# ATR 止损 / 追踪
ATR_PERIOD   = 34
ATR_SL_MULT  = 3.5    # 止损：入场价 ± ATR * 3.5
TRAIL_TRIGGER_PCT = 0.06  # 浮盈 ≥ 6% 启动追踪
TRAIL_PCT        = 0.03   # 3% 回撤止盈

# 回踩与右尾结构参数
PULLBACK_MAX_PCT = 0.01   # 回踩允许离 EMA34 的最大偏离（1%）
BREAKOUT_LOOKBACK = 8     # 右尾突破：突破最近 N 根的高/低点

# 时段过滤（用新加坡时间 SGT，UTC+8）
ACTIVE_START_HOUR = 8     # 08:00 SGT
ACTIVE_END_HOUR   = 23    # 23:00 SGT 之前允许开仓（含 22:45 这一根）


# ========= 工具函数 =========
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列
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


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high = df["high"]
    low = df["low"]

    # EMA 趋势
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()

    # SGT 时间列和小时
    df["dt_sgt"] = df["dt"] + timedelta(hours=8)
    df["hour_sgt"] = df["dt_sgt"].dt.hour

    return df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)


def in_active_session(row) -> bool:
    h = row["hour_sgt"]
    return ACTIVE_START_HOUR <= h < ACTIVE_END_HOUR


def trend_direction(row) -> int:
    """1 = 多头趋势, -1 = 空头趋势, 0 = 无趋势"""
    if row["ema_fast"] > row["ema_slow"] * 1.001:
        return 1
    elif row["ema_fast"] < row["ema_slow"] * 0.999:
        return -1
    else:
        return 0


# ========= 回踩 + 右尾结构信号 =========
def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    n = len(df)

    df["trend"] = 0
    df["pullback_long"] = False
    df["pullback_short"] = False
    df["long_signal"] = False
    df["short_signal"] = False

    for i in range(n):
        row = df.iloc[i]
        tdir = trend_direction(row)
        df.at[i, "trend"] = tdir

        # 只在活跃时段考虑开仓
        if not in_active_session(row):
            continue

        c = row["close"]
        ema_f = row["ema_fast"]
        ema_s = row["ema_slow"]

        # ===== 多头趋势逻辑 =====
        if tdir == 1:
            # 回踩：靠近 ema_fast，且不跌破 ema_slow 太多
            if ema_s < c < ema_f * (1 + PULLBACK_MAX_PCT):
                df.at[i, "pullback_long"] = True

            # 右尾突破：从 pullback 区之后，突破最近 N 根最高价
            if i > BREAKOUT_LOOKBACK:
                recent_high = df["high"].iloc[i - BREAKOUT_LOOKBACK : i].max()
                if c > recent_high and df["pullback_long"].iloc[i - 1]:
                    df.at[i, "long_signal"] = True

        # ===== 空头趋势逻辑 =====
        elif tdir == -1:
            if ema_f * (1 - PULLBACK_MAX_PCT) < c < ema_s:
                df.at[i, "pullback_short"] = True

            if i > BREAKOUT_LOOKBACK:
                recent_low = df["low"].iloc[i - BREAKOUT_LOOKBACK : i].min()
                if c < recent_low and df["pullback_short"].iloc[i - 1]:
                    df.at[i, "short_signal"] = True

    return df


# ========= 回测主逻辑（69% 版 + 更大仓位） =========
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY
    in_pos = False

    direction = 0  # 1 多, -1 空
    entry_price = None
    entry_time = None
    atr_at_entry = None
    stop_price = None

    high_since_entry = None
    low_since_entry = None
    trail_on = False

    trades = []

    for i, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        atr = float(row["atr"])
        ema_f = float(row["ema_fast"])
        ema_s = float(row["ema_slow"])
        trend = int(row["trend"])

        # === 持仓管理 ===
        if in_pos:
            # 更新浮动高低
            if direction == 1:
                high_since_entry = max(high_since_entry, h)
            else:
                low_since_entry = min(low_since_entry, l)

            # 计算浮盈百分比
            if direction == 1:
                best_price = high_since_entry
                gain_pct = (best_price - entry_price) / entry_price
            else:
                best_price = low_since_entry
                gain_pct = (entry_price - best_price) / entry_price

            # 启动追踪止盈：浮盈 ≥ 6%
            if not trail_on and gain_pct >= TRAIL_TRIGGER_PCT:
                trail_on = True

            # 更新止损价：ATR 固定止损 + 3% 回撤
            if direction == 1:
                base_sl = entry_price - atr_at_entry * ATR_SL_MULT
                if trail_on:
                    trail_sl = best_price * (1 - TRAIL_PCT)
                    stop_price = max(base_sl, trail_sl)
                else:
                    stop_price = base_sl

                exit_price = None
                exit_reason = None

                # 价格击穿止损
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

                # 趋势反转：ema_fast < ema_slow
                elif ema_f < ema_slow:
                    exit_price = c
                    exit_reason = "ema_flip_close"

            else:  # 空单
                base_sl = entry_price + atr_at_entry * ATR_SL_MULT
                if trail_on:
                    trail_sl = best_price * (1 + TRAIL_PCT)
                    stop_price = min(base_sl, trail_sl)
                else:
                    stop_price = base_sl

                exit_price = None
                exit_reason = None

                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"
                elif ema_f > ema_s:
                    exit_price = c
                    exit_reason = "ema_flip_close"

            if exit_price is not None:
                margin = equity * RISK_PER_TRADE
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
                    "margin_used": margin,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin if margin > 0 else 0.0,
                    "equity_after": equity,
                })

                # 清空仓位
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                atr_at_entry = None
                stop_price = None
                high_since_entry = None
                low_since_entry = None
                trail_on = False

        # === 空仓 → 找入场机会 ===
        if (not in_pos) and equity > 0:
            # 只在活跃时段开仓
            if not in_active_session(row):
                continue

            # 多头信号
            if row.get("long_signal", False) and trend == 1:
                direction = 1
                entry_price = c
                entry_time = dt
                atr_at_entry = atr
                in_pos = True
                high_since_entry = c
                low_since_entry = c
                trail_on = False

            # 空头信号
            elif row.get("short_signal", False) and trend == -1:
                direction = -1
                entry_price = c
                entry_time = dt
                atr_at_entry = atr
                in_pos = True
                high_since_entry = c
                low_since_entry = c
                trail_on = False

    return equity, trades


# ========= 统计输出 =========
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
    ann_ret = total_ret  # 一年数据，近似看成年化

    print("========== 回测结果（回踩 + 时段 + 右尾 + ATR + 6%→3% + 高仓位版） ==========")
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
    df = enrich(df)
    df = generate_signals(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
