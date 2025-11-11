#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

# ===== 基本参数 =====
CSV_PATH        = "okx_eth_15m.csv"  # 你的 15 分钟 ETH 数据
INITIAL_EQUITY  = 50.0
LEVERAGE        = 5.0
FEE_RATE        = 0.0007             # 单边手续费 0.07%

ATR_PERIOD      = 34
ATR_SL_MULT     = 3.5                # ATR 止损倍数

# 百分比移动止盈：浮盈 >= 6% → 3% 回撤
TRAIL_PCT_TRIGGER = 0.06
TRAIL_PCT_BACK    = 0.03

# 回踩带：相对 EMA34 回调 0.8 * ATR
PULLBACK_ATR_MULT = 0.8

# 交易时段（UTC）
SESSION_START_HOUR = 8
SESSION_END_HOUR   = 21   # 不含 21:00

# 持仓最长时间：96 根 15m K 线 ≈ 1 天
MAX_HOLD_BARS = 96

# RSI 仍然计算，但当前版本不强制使用
RSI_PERIOD      = 14
RSI_LONG_TH     = 55.0
RSI_SHORT_TH    = 45.0

# 自适应仓位
BASE_PCT        = 0.50
UP_PCT          = 0.55    # 上一单赢
DOWN_PCT        = 0.35    # 上一单亏

# 趋势 EMA
EMA_FAST        = 34
EMA_SLOW        = 144
TREND_CONFIRM_N = 3       # 连续 N 根同方向确认趋势


# ===== 工具函数 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

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
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first_col], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


def calc_atr(df: pd.DataFrame, period: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    return atr


def calc_rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    rsi = 100 - 100 / (1 + rs)
    return rsi


def prepare_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()
    df["atr"]      = calc_atr(df, ATR_PERIOD)
    df["rsi"]      = calc_rsi(close, RSI_PERIOD)

    # 原始趋势方向：ema_fast - ema_slow
    raw_dir = np.sign(df["ema_fast"] - df["ema_slow"])
    df["trend_raw"] = raw_dir

    # 连续 TREND_CONFIRM_N 根相同才算有效趋势
    trend_conf = np.zeros(len(df), dtype=int)
    for i in range(len(df)):
        if i < TREND_CONFIRM_N - 1:
            trend_conf[i] = 0
        else:
            window = raw_dir.iloc[i-TREND_CONFIRM_N+1:i+1]
            if (window == 1).all():
                trend_conf[i] = 1
            elif (window == -1).all():
                trend_conf[i] = -1
            else:
                trend_conf[i] = 0
    df["trend_dir"] = trend_conf

    return df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)


def in_session(dt) -> bool:
    h = dt.hour
    return SESSION_START_HOUR <= h < SESSION_END_HOUR


# ===== 回测主体 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY
    trades = []

    in_pos = False
    direction = 0          # 1 = 多，-1 = 空
    entry_price = None
    entry_time = None
    entry_idx = None
    margin_used = 0.0
    size = 0.0

    high_since_entry = None
    low_since_entry  = None

    last_trade_pnl = 0.0   # 用于自适应仓位

    for i, row in df.iterrows():
        dt = row["dt"]
        o  = float(row["open"])
        h  = float(row["high"])
        l  = float(row["low"])
        c  = float(row["close"])
        atr = float(row["atr"])
        trend_dir = int(row["trend_dir"])

        # === 持仓管理 ===
        if in_pos:
            bars_held = i - entry_idx

            if direction == 1:
                if high_since_entry is None:
                    high_since_entry = h
                else:
                    high_since_entry = max(high_since_entry, h)

                sl_atr = entry_price - ATR_SL_MULT * atr

                pct_gain = (high_since_entry - entry_price) / entry_price
                sl_pct = None
                if pct_gain >= TRAIL_PCT_TRIGGER:
                    sl_pct = high_since_entry * (1 - TRAIL_PCT_BACK)

                stops = [sl_atr]
                if sl_pct is not None:
                    stops.append(sl_pct)
                stop_price = max(stops)

                exit_price = None
                exit_reason = None

                if l <= stop_price <= h:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

                if exit_price is None and bars_held >= MAX_HOLD_BARS:
                    exit_price = c
                    exit_reason = "time_exit"

            else:  # 空单
                if low_since_entry is None:
                    low_since_entry = l
                else:
                    low_since_entry = min(low_since_entry, l)

                sl_atr = entry_price + ATR_SL_MULT * atr

                pct_gain = (entry_price - low_since_entry) / entry_price
                sl_pct = None
                if pct_gain >= TRAIL_PCT_TRIGGER:
                    sl_pct = low_since_entry * (1 + TRAIL_PCT_BACK)

                stops = [sl_atr]
                if sl_pct is not None:
                    stops.append(sl_pct)
                stop_price = min(stops)

                exit_price = None
                exit_reason = None

                if h >= stop_price >= l:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

                if exit_price is None and bars_held >= MAX_HOLD_BARS:
                    exit_price = c
                    exit_reason = "time_exit"

            if exit_price is not None:
                notional = margin_used * LEVERAGE
                if direction == 1:
                    gross_pnl = (exit_price - entry_price) * size
                else:
                    gross_pnl = (entry_price - exit_price) * size

                fee_open  = notional * FEE_RATE
                fee_close = abs(exit_price * size) * FEE_RATE
                pnl_net   = gross_pnl - fee_open - fee_close

                equity += pnl_net
                last_trade_pnl = pnl_net

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
                    "bars_held": bars_held,
                })

                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                entry_idx = None
                margin_used = 0.0
                size = 0.0
                high_since_entry = None
                low_since_entry = None

        # === 空仓 → 寻找入场机会 ===
        if not in_pos:
            if not in_session(dt):
                continue
            if trend_dir == 0:
                continue
            if np.isnan(atr) or atr <= 0:
                continue

            ema_fast = float(row["ema_fast"])

            # 回踩带：靠近 EMA34 的 ATR 区间
            if trend_dir == 1:
                upper = ema_fast
                lower = ema_fast - PULLBACK_ATR_MULT * atr
                pullback_ok = lower <= c <= upper
            else:
                lower = ema_fast
                upper = ema_fast + PULLBACK_ATR_MULT * atr
                pullback_ok = lower <= c <= upper

            if not pullback_ok:
                continue

            # RSI & 右尾 —— 当前版本不启用，只保留趋势 + 回踩 + 时段
            # 以后如果要再加回来，可以在这里加判断

            # 自适应仓位
            if last_trade_pnl > 0:
                margin_pct = UP_PCT
            elif last_trade_pnl < 0:
                margin_pct = DOWN_PCT
            else:
                margin_pct = BASE_PCT

            margin_used = equity * margin_pct
            if margin_used <= 0:
                continue

            notional = margin_used * LEVERAGE
            size = notional / c

            in_pos = True
            direction = trend_dir
            entry_price = c
            entry_time = dt
            entry_idx = i
            high_since_entry = c
            low_since_entry  = c

    return equity, trades


# ===== 结果统计 =====
def summarize(df: pd.DataFrame, equity: float, trades: list):
    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}")
    print()
    print("========== 回测结果（新基础版·回踩 + 时段 + ATR + 自适应仓位） ==========")

    n = len(trades)
    wins = sum(1 for t in trades if t["pnl_net"] > 0)
    losses = sum(1 for t in trades if t["pnl_net"] < 0)
    flats = n - wins - losses

    total_pnl = sum(t["pnl_net"] for t in trades)
    win_pnls = [t["pnl_net"] for t in trades if t["pnl_net"] > 0]
    loss_pnls = [t["pnl_net"] for t in trades if t["pnl_net"] < 0]
    avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
    avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0

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
    ann_ret = total_ret

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
    df = prepare_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
