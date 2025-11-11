#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
新基础版 · 回踩 + 时段 + ATR(34)*3.5 止损 + 6%→3% 追踪
这里版本 = “回踩 + 时段 + ATR + 固定 50% 仓位”

- 数据: okx_eth_15m.csv
- 只做 ETH 单品种（多空双向）
- 15m 级别
"""

import pandas as pd
import numpy as np

CSV_PATH       = "okx_eth_15m.csv"
INITIAL_EQUITY = 50.0
LEVERAGE       = 5.0
FEE_RATE       = 0.0007      # 单边手续费 0.07%

# ATR 参数
ATR_LEN        = 34
ATR_MULT_SL    = 3.5         # ATR*3.5 止损

# 浮盈追踪参数（在 ATR 止损之外叠加）
TRAIL_TRIGGER  = 0.06        # 浮盈 ≥ 6% 启用追踪
TRAIL_BACK     = 0.03        # 回撤 3% 平仓

# 仓位：固定 50% 资金
MARGIN_FRACTION = 0.5

# 回踩带：要求 close 靠近 EMA34
RETRACE_ATR_MULT = 0.8       # |close - ema34| <= ATR * 0.8 视为“回踩靠近”

# 时段过滤：只做 UTC 8:00–21:00
SESSION_START_H = 8
SESSION_END_H   = 21         # 小于 21 点


# ========= 工具函数 =========

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列处理：优先 iso，其次 ts，再不行用第 1 列兜底
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    else:
        first = df.columns[0]
        med = pd.to_numeric(df[first], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    # EMA34 / EMA144
    df["ema34"]  = close.ewm(span=34, adjust=False).mean()
    df["ema144"] = close.ewm(span=144, adjust=False).mean()

    # ATR(34)
    high = df["high"]
    low  = df["low"]
    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low  - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df["atr"] = tr.ewm(span=ATR_LEN, adjust=False).mean()

    # 趋势方向：EMA34 vs EMA144
    raw_sign = np.sign(df["ema34"] - df["ema144"])

    # 连续 3 根同向才算确认
    trend_dir = []
    window = 3
    for i in range(len(df)):
        if i < window - 1:
            trend_dir.append(0)
        else:
            w = raw_sign.iloc[i - window + 1 : i + 1]
            if (w > 0).all():
                trend_dir.append(1)
            elif (w < 0).all():
                trend_dir.append(-1)
            else:
                trend_dir.append(0)
    df["trend_dir"] = trend_dir

    # 回踩带：要求靠近 ema34
    df["retrace_ok"] = (df["close"] - df["ema34"]).abs() <= df["atr"] * RETRACE_ATR_MULT

    # 丢掉指标没算完的前几行
    df = df.dropna(subset=["ema34", "ema144", "atr"]).reset_index(drop=True)
    return df


def in_session(dt) -> bool:
    """UTC 时段过滤：只做 8:00–21:00"""
    h = dt.hour
    return SESSION_START_H <= h < SESSION_END_H


# ========= 回测主逻辑 =========

def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0       # 1=多，-1=空
    entry_price = None
    entry_time = None
    best_price = None   # 多单记录最高价；空单记录最低价
    atr_at_entry = None
    margin_used = 0.0

    trades = []

    for i, row in df.iterrows():
        dt = row["dt"]
        o  = float(row["open"])
        h  = float(row["high"])
        l  = float(row["low"])
        c  = float(row["close"])
        atr = float(row["atr"])
        trend_dir = int(row["trend_dir"])
        retrace_ok = bool(row["retrace_ok"])

        # ===== 先管理已有仓位 =====
        if in_pos:
            # 更新 best_price
            if direction == 1:
                best_price = max(best_price, h)
            else:
                best_price = min(best_price, l)

            # ATR 固定止损
            if direction == 1:
                atr_stop = entry_price - atr_at_entry * ATR_MULT_SL
            else:
                atr_stop = entry_price + atr_at_entry * ATR_MULT_SL

            # 浮盈
            if direction == 1:
                gain_pct = (best_price - entry_price) / entry_price
            else:
                gain_pct = (entry_price - best_price) / entry_price

            # 6% 启动 3% 回撤追踪
            trail_stop = None
            if gain_pct >= TRAIL_TRIGGER:
                if direction == 1:
                    trail_stop = best_price * (1 - TRAIL_BACK)
                else:
                    trail_stop = best_price * (1 + TRAIL_BACK)

            # 合并止损价格
            stop_price = atr_stop
            if trail_stop is not None:
                if direction == 1:
                    stop_price = max(stop_price, trail_stop)
                else:
                    stop_price = min(stop_price, trail_stop)

            exit_price = None
            exit_reason = None

            # 价格触及止损/追踪
            if direction == 1 and l <= stop_price:
                exit_price = stop_price
                exit_reason = "atr_sl_or_trail"
            elif direction == -1 and h >= stop_price:
                exit_price = stop_price
                exit_reason = "atr_sl_or_trail"

            # EMA 反转：趋势方向与持仓方向相反，直接按收盘价出
            if exit_price is None and trend_dir != 0 and trend_dir != direction:
                exit_price = c
                exit_reason = "ema_flip_close"

            # 如果有任何退出信号，就平仓
            if exit_price is not None:
                # 计算 PnL
                notional = margin_used * LEVERAGE
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
                    "margin_used": margin_used,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin_used if margin_used > 0 else 0.0,
                    "equity_after": equity,
                })

                # 清空仓位
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                best_price = None
                atr_at_entry = None
                margin_used = 0.0

                # 如果归零或接近归零，直接结束
                if equity <= 0:
                    equity = 0.0
                    break

        # ===== 没有持仓 → 看是否可以开仓 =====
        if not in_pos and equity > 0:
            # 时段过滤
            if not in_session(dt):
                continue

            # 必须有趋势方向
            if trend_dir == 0:
                continue

            # 必须是回踩到 EMA34 附近
            if not retrace_ok:
                continue

            # 固定 50% 仓位
            margin = equity * MARGIN_FRACTION
            if margin <= 0:
                continue

            # 开仓
            in_pos = True
            direction = trend_dir
            entry_price = c
            entry_time = dt
            best_price = h if direction == 1 else l
            atr_at_entry = atr
            margin_used = margin

    return equity, trades, df


# ========= 结果汇总 =========

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
    ann_ret = total_ret  # 一年数据，近似等于总收益率

    print("========== 回测结果（新基础版·回踩 + 时段 + ATR + 固定50%仓位） ==========")
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
    df = add_indicators(df)
    equity, trades, df = backtest(df)
    summarize(df, equity, trades)
