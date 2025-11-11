#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

# ========= 基本参数 =========
CSV_PATH = "okx_eth_15m.csv"

INITIAL_EQUITY = 50.0          # 初始资金
LEVERAGE = 5.0                 # 杠杆
RISK_FRACTION = 0.5            # 每次用当前资金的 50% 做保证金
FEE_RATE = 0.0007              # 单边手续费 0.07%

# EMA 趋势参数
EMA_FAST = 34
EMA_SLOW = 144

# ATR 参数
ATR_PERIOD = 14
ATR_MULT_SL = 2.5              # 初始止损 ATR 倍数
ATR_MULT_TRAIL = 2.0           # ATR 跟踪止盈倍数

# 额外浮盈回撤止盈（你之前那条）
FLOAT_TRIGGER = 0.06           # 浮盈 ≥ 6%
FLOAT_RET = 0.03               # 回撤 3%

# EMA 反转确认参数（✅ 新增：延迟 1 根确认）
EMA_CONFIRM_BARS = 2           # 至少连续 2 根同方向才认定反转（也就是反转延迟 1 根）


# ========= 读数据 & 预处理 =========
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
            raise ValueError(f"缺少列: {col}")

    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # EMA
    df["ema_fast"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df["atr"] = tr.rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()

    # 丢掉指标没算完的前面部分
    df = df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)
    return df


# ========= 回测逻辑 =========
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0              # 1=多, -1=空
    entry_price = None
    entry_time = None
    margin_used = 0.0
    size = 0.0
    stop_price = None
    best_price = None          # 多单用最高价，空单用最低价
    entry_atr = None

    # EMA 反转延迟相关（✅ 新增）
    prev_raw_trend = None      # 上一根的「瞬时趋势」（ema_fast vs ema_slow）
    stable_trend = None        # 生效的「确认趋势」（需要连续 EMA_CONFIRM_BARS 根）
    trend_buffer = []          # 保存最近几根 raw_trend

    trades = []

    for i, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])
        atr = float(row["atr"])

        # ===== 1. 计算 EMA 趋势 & 延迟确认 =====
        if ema_fast > ema_slow:
            raw_trend = 1
        elif ema_fast < ema_slow:
            raw_trend = -1
        else:
            raw_trend = prev_raw_trend if prev_raw_trend is not None else 0

        trend_buffer.append(raw_trend)
        if len(trend_buffer) > EMA_CONFIRM_BARS:
            trend_buffer.pop(0)

        # 连续 EMA_CONFIRM_BARS 根同方向 → 才更新 stable_trend（✅ 关键）
        if len(trend_buffer) == EMA_CONFIRM_BARS and all(
            t == trend_buffer[0] for t in trend_buffer
        ):
            stable_trend = trend_buffer[0]

        prev_raw_trend = raw_trend

        # 如果还没确认趋势，先不交易
        if stable_trend is None:
            continue

        # ===== 2. 有仓位 → 管理止损 / 追踪 =====
        if in_pos:
            # 更新 best_price（有利方向的极值）
            if direction == 1:  # 多
                best_price = max(best_price, h if best_price is not None else h)
                move = (best_price - entry_price) / entry_price
            else:               # 空
                best_price = min(best_price, l if best_price is not None else l)
                move = (entry_price - best_price) / entry_price

            # ATR 初始止损
            if direction == 1:
                base_sl = entry_price - ATR_MULT_SL * entry_atr
            else:
                base_sl = entry_price + ATR_MULT_SL * entry_atr

            # ATR 跟踪止损
            if direction == 1:
                atr_trail = c - ATR_MULT_TRAIL * atr
            else:
                atr_trail = c + ATR_MULT_TRAIL * atr

            # 浮盈 ≥6% → 启动 3% 回撤止盈
            float_trail = None
            if move >= FLOAT_TRIGGER:
                if direction == 1:
                    float_trail = best_price * (1 - FLOAT_RET)
                else:
                    float_trail = best_price * (1 + FLOAT_RET)

            # 综合止损价（多头取最高，空头取最低）
            candidates = [base_sl, atr_trail]
            if float_trail is not None:
                candidates.append(float_trail)

            if direction == 1:
                new_stop = max(candidates)
            else:
                new_stop = min(candidates)

            # stop_price 只能往有利方向移动
            if stop_price is None:
                stop_price = new_stop
            else:
                if direction == 1:
                    stop_price = max(stop_price, new_stop)
                else:
                    stop_price = min(stop_price, new_stop)

            exit_price = None
            exit_reason = None

            # 看看是不是被打到止损/止盈
            if direction == 1 and l <= stop_price:
                exit_price = stop_price
                exit_reason = "atr_sl_or_trail"
            elif direction == -1 and h >= stop_price:
                exit_price = stop_price
                exit_reason = "atr_sl_or_trail"

            # 或者 EMA 趋势反转（✨ 这里已经是延迟确认后的 stable_trend）
            if exit_price is None and stable_trend is not None:
                if direction == 1 and stable_trend == -1:
                    exit_price = c
                    exit_reason = "ema_flip_close"
                elif direction == -1 and stable_trend == 1:
                    exit_price = c
                    exit_reason = "ema_flip_close"

            # ===== 2.1 执行平仓 =====
            if exit_price is not None:
                # 计算 PnL
                notional = margin_used * LEVERAGE
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
                margin_used = 0.0
                size = 0.0
                stop_price = None
                best_price = None
                entry_atr = None

        # ===== 3. 无仓位 → 根据 stable_trend 找机会开仓 =====
        if not in_pos and equity > 0 and stable_trend in (1, -1):
            # 原始策略是「永不休息，一直做到归零」，这里只要有钱就上
            margin = equity * RISK_FRACTION
            if margin <= 0:
                continue

            notional = margin * LEVERAGE
            size = notional / c

            in_pos = True
            direction = stable_trend
            entry_price = c
            entry_time = dt
            margin_used = margin
            stop_price = None
            best_price = c
            entry_atr = atr

    return equity, trades


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
    ann_ret = total_ret  # 数据大约一年

    print("========== 回测结果（原始策略 + ATR + 6%→3%回撤 + EMA反转延迟1根确认） ==========")
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
    print("前 5 笔交易示例:")
    for t in trades[:5]:
        print(t)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
