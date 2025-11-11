#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
原始策略 + ATR(34) 动态止损/追踪 + 6%浮盈→3%回撤 + 50%动态仓位

本版只相对“新基础版”改了两点：
1）ATR 周期：21 -> 34
2）止损触发：多单 close < entry - ATR*3.5，空单 close > entry + ATR*3.5
"""

import pandas as pd
import math

# ===== 基本参数 =====
CSV_PATH = "okx_eth_15m.csv"   # 你的 ETH 15m K线
INITIAL_EQUITY = 50.0          # 初始资金（U）
RISK_FRACTION = 0.5            # 每次用可用资金的 50% 做保证金
LEVERAGE = 5.0                 # 杠杆
FEE_RATE = 0.0007              # 单边手续费率 0.07%（按实际交易所改）

ATR_PERIOD = 34                # ATR 计算周期（更平滑）
ATR_STOP_MULT = 3.5            # 止损倍数：3.5 * ATR
FLOAT_TRIGGER = 0.06           # 浮盈 >= 6% 启动移动止盈
FLOAT_TRAIL_PCT = 0.03         # 启动后按 3% 回撤止盈


# ===== 读数据并预处理 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列：优先 iso，其次 ts，再兜底第一列
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

    # 必要列检查
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


# ===== 指标计算：EMA34 / EMA144 + ATR(34) =====
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high = df["high"]
    low = df["low"]

    # EMA
    df["ema34"] = close.ewm(span=34, adjust=False).mean()
    df["ema144"] = close.ewm(span=144, adjust=False).mean()

    # True Range
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # ATR(34) 使用 Wilder 风格：EMA(alpha=1/period)
    df["atr"] = tr.ewm(alpha=1/ATR_PERIOD, adjust=False).mean()

    # 去掉前面算不出指标的
    df = df.dropna(subset=["ema34", "ema144", "atr"]).reset_index(drop=True)
    return df


# ===== 主回测逻辑 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0         # 1 = 多单, -1 = 空单
    entry_price = None
    entry_time = None
    pos_size = 0.0        # 持仓张数（ETH 数量）
    margin_used = 0.0
    best_price = None     # 多单：最高价；空单：最低价

    trades = []

    for i in range(len(df)):
        row = df.iloc[i]
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        ema34 = float(row["ema34"])
        ema144 = float(row["ema144"])
        atr = float(row["atr"])

        if atr <= 0 or math.isnan(atr):
            continue

        # ===== 持仓管理：先看有没有需要平仓 =====
        if in_pos:
            stop_price = None
            exit_price = None
            exit_reason = None

            # 更新 best_price
            if direction == 1:  # 多单：记录最高价
                best_price = max(best_price, h)
                # ATR 止损（更宽的空间）
                base_stop = entry_price - ATR_STOP_MULT * atr

                # 浮盈百分比（按 best_price 计算）
                gain_pct = (best_price - entry_price) / entry_price

                if gain_pct >= FLOAT_TRIGGER:
                    # 启动 3% 回撤移动止盈
                    trail_stop = best_price * (1 - FLOAT_TRAIL_PCT)
                    stop_price = max(base_stop, trail_stop)
                else:
                    stop_price = base_stop

                # intrabar 检查：最低价打到止损
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            elif direction == -1:  # 空单：记录最低价
                best_price = min(best_price, l)
                base_stop = entry_price + ATR_STOP_MULT * atr

                gain_pct = (entry_price - best_price) / entry_price

                if gain_pct >= FLOAT_TRIGGER:
                    trail_stop = best_price * (1 + FLOAT_TRAIL_PCT)
                    stop_price = min(base_stop, trail_stop)
                else:
                    stop_price = base_stop

                # intrabar 检查：最高价打到止损
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            # 触发平仓
            if exit_price is not None:
                notional_entry = abs(entry_price * pos_size)
                notional_exit = abs(exit_price * pos_size)

                gross_pnl = (exit_price - entry_price) * pos_size  # 多空统一公式
                fee_open = notional_entry * FEE_RATE
                fee_close = notional_exit * FEE_RATE
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
                pos_size = 0.0
                margin_used = 0.0
                best_price = None

                # 如果资金基本归零，直接结束
                if equity <= 0:
                    equity = 0.0
                    break

        # ===== 进场逻辑：没持仓就按 EMA34/144 方向入场 =====
        if not in_pos and equity > 0:
            # 用当前 EMA 方向：上多下空
            if ema34 > ema144:
                new_dir = 1
            elif ema34 < ema144:
                new_dir = -1
            else:
                continue  # 完全重叠就跳过

            # 动态仓位：当前资金的 50%
            margin = equity * RISK_FRACTION
            if margin <= 0:
                continue

            notional = margin * LEVERAGE
            pos_size = notional / c  # ETH 数量

            in_pos = True
            direction = new_dir
            entry_price = c
            entry_time = dt
            margin_used = margin

            # 初始化 best_price
            if direction == 1:
                best_price = c
            else:
                best_price = c

    return equity, trades


# ===== 汇总输出 =====
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
    ann_ret = total_ret  # 一年数据，近似年化

    print("========== 回测结果（原始策略 + ATR(34)*3.5 止损 + 6%→3%回撤） ==========")
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
