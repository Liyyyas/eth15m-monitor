#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

CSV_PATH = "okx_eth_15m.csv"

INITIAL_EQUITY = 50.0
LEVERAGE = 5.0
RISK_FRACTION = 0.5    # 用总资金 50% 做保证金
FEE_RATE = 0.0007       # 单边手续费

EMA_FAST = 34
EMA_SLOW = 144

ATR_WINDOW = 14
ATR_MULT_SL = 2.5       # ATR 初始止损倍数
ATR_MULT_TRAIL = 3.5    # ATR 追踪启用倍数（可按你喜好调）

# === 新增：价格百分比移动止盈规则 ===
EXTRA_PROFIT_TRIGGER = 0.06   # 浮盈 >= 6% 启动
EXTRA_TRAIL_PCT = 0.03        # 3% 回撤（基于入场后最高/最低价）


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

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    c = df["close"]
    df["ema_fast"] = c.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = c.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR
    h = df["high"]
    l = df["low"]
    prev_close = c.shift(1)
    tr1 = h - l
    tr2 = (h - prev_close).abs()
    tr3 = (l - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(window=ATR_WINDOW, min_periods=ATR_WINDOW).mean()

    df = df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)
    return df


def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0      # 1 多，-1 空
    entry_price = None
    entry_time = None
    size = None
    margin_used = None

    atr_stop = None
    atr_trail_on = False
    extra_trail_on = False

    best_price = None  # 多单记录最高价，空单记录最低价

    trades = []

    for i, row in df.iterrows():
        dt = row["dt"]
        o, h, l, c = map(float, [row["open"], row["high"], row["low"], row["close"]])
        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])
        atr = float(row["atr"])

        # 先管理已有仓位
        if in_pos:
            # 更新入场以来的最佳价格（多=最高，空=最低）
            if best_price is None:
                best_price = h if direction == 1 else l
            else:
                if direction == 1:
                    best_price = max(best_price, h)
                else:
                    best_price = min(best_price, l)

            # 1）ATR 初始止损
            if direction == 1:
                atr_stop = entry_price - ATR_MULT_SL * atr
            else:
                atr_stop = entry_price + ATR_MULT_SL * atr

            # 2）ATR 浮盈足够时启用 ATR 追踪
            #   当价格偏离 entry >= ATR_MULT_TRAIL * ATR，就开始用 ATR 追踪止损
            if direction == 1:
                move_from_entry = best_price - entry_price
            else:
                move_from_entry = entry_price - best_price

            if move_from_entry >= ATR_MULT_TRAIL * atr:
                atr_trail_on = True

            if atr_trail_on:
                if direction == 1:
                    trail_stop_by_atr = best_price - ATR_MULT_SL * atr
                    atr_stop = max(atr_stop, trail_stop_by_atr)
                else:
                    trail_stop_by_atr = best_price + ATR_MULT_SL * atr
                    atr_stop = min(atr_stop, trail_stop_by_atr)

            # 3）新增：价格百分比移动止盈（浮盈 >= 6% → 3% 回撤）
            #   gain_pct = (best_price / entry_price - 1) * direction
            if direction == 1:
                gain_pct = (best_price / entry_price - 1.0)
            else:
                gain_pct = (entry_price / best_price - 1.0)

            if gain_pct >= EXTRA_PROFIT_TRIGGER:
                extra_trail_on = True

            if extra_trail_on:
                if direction == 1:
                    # 多单：最高价回撤 3% 为价格止损线
                    extra_stop = best_price * (1.0 - EXTRA_TRAIL_PCT)
                    atr_stop = max(atr_stop, extra_stop)
                else:
                    # 空单：最低价反弹 3% 为止损线
                    extra_stop = best_price * (1.0 + EXTRA_TRAIL_PCT)
                    atr_stop = min(atr_stop, extra_stop)

            # 决定这根K线是否触发止损
            exit_price = None
            exit_reason = None

            if direction == 1:
                if l <= atr_stop:   # 多单被打到止损
                    exit_price = atr_stop
                    exit_reason = "atr_sl_or_trail"
            else:
                if h >= atr_stop:   # 空单被打到止损
                    exit_price = atr_stop
                    exit_reason = "atr_sl_or_trail"

            # 如触发离场，结算
            if exit_price is not None:
                notional = margin_used * LEVERAGE
                gross_pnl = (exit_price - entry_price) * size * direction
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
                    "equity_after": equity
                })

                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                size = None
                margin_used = None
                atr_stop = None
                atr_trail_on = False
                extra_trail_on = False
                best_price = None

        # 如果没仓位并且还有钱 → 看是否开仓（原始策略逻辑）
        if (not in_pos) and equity > 0:
            if ema_fast > ema_slow:
                new_dir = 1
            elif ema_fast < ema_slow:
                new_dir = -1
            else:
                new_dir = 0

            if new_dir != 0:
                margin_used = equity * RISK_FRACTION
                notional = margin_used * LEVERAGE
                entry_price = c
                size = notional / entry_price * new_dir
                entry_time = dt
                direction = new_dir
                in_pos = True

                atr_trail_on = False
                extra_trail_on = False
                best_price = None
                atr_stop = None

    return equity, trades


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

    print("========== 回测结果（原始策略 + ATR 动态止损/追踪 + 6%浮盈→3%回撤） ==========")
    print(f"总交易数: {n}")
    print(f"胜: {wins}  负: {losses}  和: {flats}")
    win_rate = wins / n * 100 if n > 0 else 0.0
    print(f"胜率: {win_rate:.2f}%")
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
    df = add_indicators(df)
    final_equity, trades = backtest(df)
    summarize(df, final_equity, trades)
