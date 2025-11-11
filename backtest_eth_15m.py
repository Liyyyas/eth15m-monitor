#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

# ================= 基本参数 =================
CSV_PATH        = "okx_eth_15m.csv"
INITIAL_EQUITY  = 50.0
MARGIN_PER_TRADE = 25.0      # 固定每笔保证金
LEVERAGE        = 5.0
FEE_RATE        = 0.0007     # 单边手续费

ATR_LEN         = 21         # ATR长度
ADX_LEN         = 14
DONCHIAN_LEN    = 20         # ★ 从 55 改为 20，更早发现趋势
BB_LEN          = 20
BB_STD          = 2.0
RSI_LEN         = 2

# 趋势判定阈值（方向 4：放宽）
TREND_ADX_MIN       = 18.0   # ★ 原来 22 → 18
TREND_EMA_GAP_MIN   = 0.002  # ★ 原来 ~0.004 → 0.002 更宽

# 震荡系统的止盈/止损
RANGE_TP_MULT   = 1.0        # 回到布林中轨就止盈
RANGE_SL_ATR_M  = 1.5        # ATR 止损倍数

# 趋势系统止损：ATR * 3.5（你后面喜欢的设定）
TREND_SL_ATR_M  = 3.5


# ================ 工具函数 ================

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列
    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"], utc=True, errors="coerce")
    elif "iso" in df.columns:
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


def calc_rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0.0)).rolling(length).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(length).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    # EMA
    df["ema34"] = close.ewm(span=34, adjust=False).mean()
    df["ema144"] = close.ewm(span=144, adjust=False).mean()

    # ATR
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(ATR_LEN).mean()

    # ADX
    plus_dm = (high - high.shift()).clip(lower=0.0)
    minus_dm = (low.shift() - low).clip(lower=0.0)
    plus_dm[plus_dm < minus_dm] = 0.0
    minus_dm[minus_dm < plus_dm] = 0.0

    tr_w = tr.rolling(ADX_LEN).sum()
    plus_di = 100 * (plus_dm.rolling(ADX_LEN).sum() / tr_w.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(ADX_LEN).sum() / tr_w.replace(0, np.nan))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)) * 100
    df["adx"] = dx.rolling(ADX_LEN).mean()

    # Donchian 通道（趋势系统）
    df["donch_high"] = high.rolling(DONCHIAN_LEN).max()
    df["donch_low"] = low.rolling(DONCHIAN_LEN).min()

    # 布林带（震荡系统）
    df["bb_mid"] = close.rolling(BB_LEN).mean()
    df["bb_std"] = close.rolling(BB_LEN).std()
    df["bb_upper"] = df["bb_mid"] + BB_STD * df["bb_std"]
    df["bb_lower"] = df["bb_mid"] - BB_STD * df["bb_std"]

    # RSI(2)
    df["rsi2"] = calc_rsi(close, RSI_LEN)

    # EMA gap 比例
    df["ema_gap"] = (df["ema34"] - df["ema144"]).abs() / close

    # 丢掉指标不完整的前面几行
    df = df.dropna(subset=[
        "ema34", "ema144", "atr", "adx",
        "donch_high", "donch_low",
        "bb_mid", "bb_upper", "bb_lower",
        "rsi2", "ema_gap"
    ]).reset_index(drop=True)
    return df


# ================ 状态判定 ================

def is_trend_zone(row) -> bool:
    """
    趋势区判定（方向1+4合并版）：
    - ADX14 > 18（方向4放宽）
    - EMA34 / EMA144 张口比例 >= 0.002
    """
    return (row["adx"] > TREND_ADX_MIN) and (row["ema_gap"] >= TREND_EMA_GAP_MIN)


def trend_signal(row, prev_row):
    """
    趋势信号（方向1：更早介入）：
    - Donchian 周期从 55 → 20
    - 采用通道突破 + EMA 排列
    """
    c = row["close"]
    ema34 = row["ema34"]
    ema144 = row["ema144"]

    donch_high_prev = prev_row["donch_high"]
    donch_low_prev = prev_row["donch_low"]

    up = (c > ema34 > ema144) and (c > donch_high_prev)
    down = (c < ema34 < ema144) and (c < donch_low_prev)

    if up and not down:
        return 1
    elif down and not up:
        return -1
    else:
        return 0


def range_signal(row):
    """
    震荡信号：简单布林+RSI(2) 极值反转
    - RSI2 < 10 且 close < 下轨 → 做多
    - RSI2 > 90 且 close > 上轨 → 做空
    """
    c = row["close"]
    rsi2 = row["rsi2"]
    upper = row["bb_upper"]
    lower = row["bb_lower"]

    if (rsi2 < 10) and (c < lower):
        return 1
    elif (rsi2 > 90) and (c > upper):
        return -1
    return 0


# ================ 回测主逻辑 ================

def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0       # 1 多 -1 空
    entry_price = None
    entry_time = None
    trade_type = None   # "trend" or "range"
    atr_entry = None

    trades = []

    for i in range(1, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        atr = float(row["atr"])

        # ----- 先处理持仓止损 / 目标 -----
        if in_pos:
            exit_price = None
            exit_reason = None

            if trade_type == "trend":
                # 趋势仓位：固定 ATR*3.5 止损，多空对称
                sl_multiple = TREND_SL_ATR_M
                if direction == 1:
                    sl = entry_price - atr_entry * sl_multiple
                    if l <= sl:
                        exit_price = sl
                        exit_reason = "trend_atr_sl"
                else:
                    sl = entry_price + atr_entry * sl_multiple
                    if h >= sl:
                        exit_price = sl
                        exit_reason = "trend_atr_sl"

            elif trade_type == "range":
                # 震荡仓位：ATR1.5 止损 + 回归中轨止盈
                if direction == 1:
                    sl = entry_price - atr_entry * RANGE_SL_ATR_M
                    tp = row["bb_mid"]
                    if l <= sl:
                        exit_price = sl
                        exit_reason = "range_sl"
                    elif tp is not None and c >= tp:
                        exit_price = tp
                        exit_reason = "range_tp"
                else:
                    sl = entry_price + atr_entry * RANGE_SL_ATR_M
                    tp = row["bb_mid"]
                    if h >= sl:
                        exit_price = sl
                        exit_reason = "range_sl"
                    elif tp is not None and c <= tp:
                        exit_price = tp
                        exit_reason = "range_tp"

            # 执行平仓
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
                })

                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                trade_type = None
                atr_entry = None

        # ----- 没仓位 → 看能不能开新仓 -----
        if (not in_pos) and equity > MARGIN_PER_TRADE:
            if is_trend_zone(row):
                sig = trend_signal(row, prev)
                if sig != 0:
                    # 开趋势仓
                    in_pos = True
                    direction = sig
                    entry_price = c
                    entry_time = dt
                    trade_type = "trend"
                    atr_entry = atr
            else:
                sig = range_signal(row)
                if sig != 0:
                    # 开震荡仓
                    in_pos = True
                    direction = sig
                    entry_price = c
                    entry_time = dt
                    trade_type = "range"
                    atr_entry = atr

    return equity, trades


# ================ 统计输出 ================

def summarize(df, equity, trades):
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

    print("========== 回测结果（双状态·趋势/震荡 + 提前趋势信号版） ==========")
    print(f"总交易数: {n}")
    print(f"胜: {wins}  负: {losses}  和: {flats}")
    win_rate = wins / n * 100 if n > 0 else 0.0
    print(f"胜率: {win_rate:.2f}%")
    print(f"总盈亏: {total_pnl:.4f} U")
    print(f"期末资金: {equity:.4f} U (初始 {INITIAL_EQUITY} U)")
    print(f"平均盈利单: {avg_win:.4f} U")
    print(f"平均亏损单: {avg_loss:.4f} U")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"总收益率: {total_ret*100:.2f}%  | 年化收益率估计: {total_ret*100:.2f}%")
    print()
    print("前 5 笔已平仓交易示例:")
    for t in trades[:5]:
        print(t)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
