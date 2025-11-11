#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from math import isnan

# ================= 基本配置 =================

CSV_PATH        = "okx_eth_15m.csv"   # 你的 ETH 15m K线
INITIAL_EQUITY  = 50.0                # 初始资金
LEVERAGE        = 5.0                 # 杠杆
FEE_RATE        = 0.0007              # 单边手续费率（0.07%）
MARGIN_RATIO    = 0.5                 # 每笔用当前资金的 50% 做保证金

# EMA 趋势判断
EMA_FAST        = 34
EMA_SLOW        = 144

# ATR 止损 / 追踪配置（“右尾结构”版本）
ATR_PERIOD      = 34
ATR_STOP_MULT   = 2.0     # 固定止损：2 * ATR
TRIGGER_MULT    = 1.2     # 浮盈达到 1.2 * ATR 后
TRAIL_BACK_MULT = 0.6     # 止损抬到最高/最低回撤 0.6 * ATR，同时至少保本

# 回踩限价入场设置：EMA34 ± 0.5 * ATR 以内视为“回踩区”
PULLBACK_ATR_MULT   = 0.5
PULLBACK_MAX_BARS   = 6   # 信号出现后，最多等 6 根 15m K 线

# 交易时段过滤（UTC 时间）
SESSION_START_HOUR  = 14  # 14:00
SESSION_END_HOUR    = 21  # 21:00，含头含尾


# ================= 工具函数 =================

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列处理：优先 iso，其次 ts，否则用第一列兜底
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


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    # EMA34 & EMA144
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR(34)
    high = df["high"]
    low  = df["low"]

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Wilder 风格 ATR
    df["atr"] = tr.ewm(alpha=1/ATR_PERIOD, adjust=False).mean()

    df = df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)
    return df


def get_bias(ema_fast: float, ema_slow: float) -> int:
    """EMA 趋势方向：1=多头，-1=空头，0=中性"""
    if ema_fast > ema_slow:
        return 1
    elif ema_fast < ema_slow:
        return -1
    else:
        return 0


def in_session(dt) -> bool:
    """是否在允许开仓的UTC时间段内"""
    h = dt.hour
    return SESSION_START_HOUR <= h <= SESSION_END_HOUR


# ================= 回测主体 =================

def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos          = False
    direction       = 0   # 1=多，-1=空
    entry_price     = None
    entry_time      = None
    margin_used     = 0.0
    atr_entry       = None
    highest_price   = None
    lowest_price    = None
    trail_active    = False

    # EMA 趋势 & 回踩待入场信号
    last_bias           = 0
    pending_dir         = 0   # 待入场方向
    pending_bars_left   = 0   # 还剩几根K可以等回踩

    trades = []

    for idx, row in df.iterrows():
        dt   = row["dt"]
        o    = float(row["open"])
        h    = float(row["high"])
        l    = float(row["low"])
        c    = float(row["close"])
        emaf = float(row["ema_fast"])
        emas = float(row["ema_slow"])
        atr  = float(row["atr"])

        bias = get_bias(emaf, emas)

        # ===== 先管理已有仓位 =====
        if in_pos:
            exit_price  = None
            exit_reason = None

            # 更新极值
            if direction == 1:   # 多单：看最高价
                if highest_price is None:
                    highest_price = h
                else:
                    highest_price = max(highest_price, h)

                # 以入场时 ATR 为基准
                risk_unit = ATR_STOP_MULT * atr_entry
                base_stop = entry_price - risk_unit

                # 启动追踪：浮盈 >= 1.2 * ATR
                move_up = highest_price - entry_price
                if (not trail_active) and move_up >= TRIGGER_MULT * atr_entry:
                    trail_active = True

                if trail_active:
                    dyn_stop = highest_price - TRAIL_BACK_MULT * atr_entry
                    # 至少保本
                    dyn_stop = max(dyn_stop, entry_price)
                    stop_price = max(base_stop, dyn_stop)
                else:
                    stop_price = base_stop

                # 先看是否被止损/追踪打到
                if l <= stop_price:
                    exit_price  = stop_price
                    exit_reason = "atr_sl_or_trail"

                # 再看 EMA 反转出场（不立刻反向开仓）
                elif bias == -1:
                    exit_price  = c
                    exit_reason = "ema_flip_close"

                if exit_price is not None:
                    # 结算多单
                    notional = margin_used * LEVERAGE
                    size     = notional / entry_price
                    gross_pnl = (exit_price - entry_price) * size
                    fee_open  = notional * FEE_RATE
                    fee_close = abs(exit_price * size) * FEE_RATE
                    pnl_net   = gross_pnl - fee_open - fee_close
                    equity   += pnl_net

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

                    # 清空仓位状态
                    in_pos        = False
                    direction     = 0
                    entry_price   = None
                    entry_time    = None
                    margin_used   = 0.0
                    atr_entry     = None
                    highest_price = None
                    lowest_price  = None
                    trail_active  = False

            else:  # direction == -1，空单
                if lowest_price is None:
                    lowest_price = l
                else:
                    lowest_price = min(lowest_price, l)

                risk_unit = ATR_STOP_MULT * atr_entry
                base_stop = entry_price + risk_unit

                move_down = entry_price - lowest_price
                if (not trail_active) and move_down >= TRIGGER_MULT * atr_entry:
                    trail_active = True

                if trail_active:
                    dyn_stop = lowest_price + TRAIL_BACK_MULT * atr_entry
                    # 至少保本
                    dyn_stop = min(dyn_stop, entry_price)
                    stop_price = min(base_stop, dyn_stop)
                else:
                    stop_price = base_stop

                if h >= stop_price:
                    exit_price  = stop_price
                    exit_reason = "atr_sl_or_trail"
                elif bias == 1:
                    exit_price  = c
                    exit_reason = "ema_flip_close"

                if exit_price is not None:
                    # 结算空单
                    notional = margin_used * LEVERAGE
                    size     = notional / entry_price
                    gross_pnl = (entry_price - exit_price) * size
                    fee_open  = notional * FEE_RATE
                    fee_close = abs(exit_price * size) * FEE_RATE
                    pnl_net   = gross_pnl - fee_open - fee_close
                    equity   += pnl_net

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

                    in_pos        = False
                    direction     = 0
                    entry_price   = None
                    entry_time    = None
                    margin_used   = 0.0
                    atr_entry     = None
                    highest_price = None
                    lowest_price  = None
                    trail_active  = False

        # ===== 管理 EMA 趋势变化 & 回踩信号 =====
        # 只有无持仓时才考虑新信号
        if not in_pos:
            # 检查是否出现新的 EMA 反转
            if bias != 0 and bias != last_bias:
                pending_dir       = bias
                pending_bars_left = PULLBACK_MAX_BARS
            elif bias == 0:
                # 无明确趋势则清空待入场
                pending_dir       = 0
                pending_bars_left = 0

            # 有待入场方向、还有等待次数，并且当前趋势仍同向
            if pending_dir != 0 and pending_bars_left > 0 and bias == pending_dir:
                # 只在指定时段内允许入场
                if in_session(dt) and equity > 0:
                    zone_center = emaf
                    zone_low    = zone_center - PULLBACK_ATR_MULT * atr
                    zone_high   = zone_center + PULLBACK_ATR_MULT * atr

                    # 本根K线是否触及“回踩区”
                    touched = (l <= zone_high) and (h >= zone_low)
                    if touched:
                        # 入场价：用 EMA34，但限定在当根K线范围内，防止超现实
                        entry_px = max(min(zone_center, h), l)

                        margin = equity * MARGIN_RATIO
                        if margin > 0:
                            in_pos        = True
                            direction     = pending_dir
                            entry_price   = entry_px
                            entry_time    = dt
                            margin_used   = margin
                            atr_entry     = atr
                            highest_price = entry_px if direction == 1 else None
                            lowest_price  = entry_px if direction == -1 else None
                            trail_active  = False

                            # 入场后清空 pending
                            pending_dir       = 0
                            pending_bars_left = 0

                # 等待次数减一
                pending_bars_left -= 1
                if pending_bars_left <= 0:
                    pending_dir = 0
                    pending_bars_left = 0

        # 记录本根 bias，供下一根比较
        last_bias = bias

    return equity, trades


# ================= 结果统计 =================

def summarize(df: pd.DataFrame, equity, trades):
    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}")
    print()

    n = len(trades)
    wins   = sum(1 for t in trades if t["pnl_net"] > 0)
    losses = sum(1 for t in trades if t["pnl_net"] < 0)
    flats  = n - wins - losses

    total_pnl = sum(t["pnl_net"] for t in trades)

    win_pnls  = [t["pnl_net"] for t in trades if t["pnl_net"] > 0]
    loss_pnls = [t["pnl_net"] for t in trades if t["pnl_net"] < 0]

    avg_win  = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
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
    ann_ret   = total_ret  # 一年数据，近似等于总收益率

    print("========== 回测结果（新基础版·回踩+时段+右尾结构 合体版） ==========")
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


# ================= 主入口 =================

if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
