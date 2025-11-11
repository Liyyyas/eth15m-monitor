#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

# ===== 基本配置 =====
CSV_15M_PATH = "okx_eth_15m.csv"   # 你的一年 15m 数据
INITIAL_EQUITY = 50.0              # 初始资金
LEVERAGE = 2.0                     # 杠杆（A路线用 2x）
FEE_RATE = 0.0007                  # 单边手续费率 0.07%

# 仓位规则：
# equity >= 40U → 50% 仓位； equity < 40U → 30% 仓位
MARGIN_HIGH_RATIO = 0.5
MARGIN_LOW_RATIO = 0.3
MARGIN_LOW_THRESHOLD = 40.0

# EMA 趋势参数
EMA_FAST = 34
EMA_SLOW = 144

# ATR 参数（4h 上）
ATR_PERIOD = 21
ATR_MULT = 2.5   # 止损宽度：ATR * 2.5

# 追踪止盈两档
TRAIL_T1_TRIGGER = 0.06   # 浮盈 ≥ 6% 启用第一档
TRAIL_T1_DROP    = 0.03   # 第一档 3% 回撤止盈
TRAIL_T2_TRIGGER = 0.08   # 浮盈 ≥ 8% 启用第二档
TRAIL_T2_DROP    = 0.01   # 第二档 1% 回撤止盈

# 需要连续几根 K 线趋势同向才允许入场（多 / 空）
TREND_CONFIRM_BARS = 2    # 连续 2 根 4h


# ===== 工具函数：加载 15m 数据并重采样为 4h =====
def load_15m_to_4h(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列：优先 iso，其次 ts，其次第一列兜底
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

    # 用 dt 做索引重采样到 4 小时
    df = df.set_index("dt")
    df_4h = df.resample("4H").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    }).dropna().reset_index()

    return df_4h


# ===== 指标计算：EMA & ATR & 趋势方向 =====
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR(21) on 4h
    high = df["high"]
    low = df["low"]

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df["atr"] = tr.rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()

    # 趋势方向：ema_fast - ema_slow 的符号
    diff = df["ema_fast"] - df["ema_slow"]
    df["trend_dir"] = np.sign(diff).replace(0, np.nan)  # 1 多头，-1 空头，NaN 无趋势

    df = df.dropna(subset=["ema_fast", "ema_slow", "atr", "trend_dir"]).reset_index(drop=True)
    return df


# ===== 仓位计算：动态仓位（50% / 30%） =====
def calc_margin(equity: float) -> float:
    if equity <= 0:
        return 0.0
    if equity < MARGIN_LOW_THRESHOLD:
        return equity * MARGIN_LOW_RATIO
    else:
        return equity * MARGIN_HIGH_RATIO


# ===== 回测主逻辑（4h A 路线进阶版） =====
def backtest_4h(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0  # 1 多、-1 空
    entry_price = None
    entry_time = None
    margin_used = 0.0
    size = 0.0
    stop_price = None
    high_since = None
    low_since = None

    t1_on = False
    t2_on = False

    trades = []

    for i, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        atr = float(row["atr"])
        trend_now = int(row["trend_dir"])

        # ========= 持仓管理：先处理止损 / 追踪 =========
        if in_pos:
            # 更新有利方向的极值
            if direction == 1:
                # 多单：最高价
                high_since = max(high_since, h)
                # 当前最大浮盈
                gain = (high_since - entry_price) / entry_price

                # 第一档：浮盈 ≥ 6% → 3% 回撤
                if gain >= TRAIL_T1_TRIGGER:
                    t1_on = True
                    candidate = high_since * (1 - TRAIL_T1_DROP)
                    # 多单止损只会“上移”
                    if stop_price is None:
                        stop_price = candidate
                    else:
                        stop_price = max(stop_price, candidate)

                # 第二档：浮盈 ≥ 8% → 1% 回撤（更紧）
                if gain >= TRAIL_T2_TRIGGER:
                    t2_on = True
                    candidate = high_since * (1 - TRAIL_T2_DROP)
                    stop_price = max(stop_price, candidate)

                # 触发：最低价跌破止损线
                exit_price = None
                exit_reason = None
                if stop_price is not None and l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"

            else:
                # 空单：最低价
                low_since = min(low_since, l)
                gain = (entry_price - low_since) / entry_price

                # 第一档：浮盈 ≥ 6% → 3% 回撤
                if gain >= TRAIL_T1_TRIGGER:
                    t1_on = True
                    candidate = low_since * (1 + TRAIL_T1_DROP)
                    # 空单止损只会“下移”（价格越低越紧）
                    if stop_price is None:
                        stop_price = candidate
                    else:
                        stop_price = min(stop_price, candidate)

                # 第二档：浮盈 ≥ 8% → 1% 回撤
                if gain >= TRAIL_T2_TRIGGER:
                    t2_on = True
                    candidate = low_since * (1 + TRAIL_T2_DROP)
                    stop_price = min(stop_price, candidate)

                exit_price = None
                exit_reason = None
                if stop_price is not None and h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"

            # ==== 如果这根K线触发了离场 ====
            if exit_price is not None:
                notional = margin_used * LEVERAGE
                # size 已包含方向
                fee_open = notional * FEE_RATE
                fee_close = abs(exit_price * size) * FEE_RATE
                gross_pnl = (exit_price - entry_price) * size
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
                    "bars_held": (dt - entry_time).total_seconds() / (4 * 3600.0),
                })

                # 清空持仓状态
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                margin_used = 0.0
                size = 0.0
                stop_price = None
                high_since = None
                low_since = None
                t1_on = False
                t2_on = False

        # ========= 空仓 → 考虑开仓 =========
        if not in_pos:
            if equity <= 0:
                break  # 爆仓了，直接停止

            # 需要足够数据：至少能看 TREND_CONFIRM_BARS 根趋势
            if i < TREND_CONFIRM_BARS - 1:
                continue

            # 连续 TREND_CONFIRM_BARS 根趋势方向一致，且非 0
            recent_dirs = df["trend_dir"].iloc[i - TREND_CONFIRM_BARS + 1 : i + 1].values
            if np.any(pd.isna(recent_dirs)):
                continue

            if not (np.all(recent_dirs > 0) or np.all(recent_dirs < 0)):
                continue

            trend_dir = int(np.sign(recent_dirs[-1]))  # 当前确定的趋势方向

            # 回踩条件：价格要“碰”到 ema_fast 附近
            ema_fast = row["ema_fast"]
            # 使用“高低包住” 或 “收盘离 EMA 在 1% 内”
            touch_fast = (l <= ema_fast <= h) or (abs(c - ema_fast) / c <= 0.01)

            if not touch_fast:
                continue

            # ATR 必须有效
            if np.isnan(atr) or atr <= 0:
                continue

            # 根据当前资金算仓位
            margin = calc_margin(equity)
            if margin < 1.0:  # 太小就算了
                continue

            # 决定方向：顺势交易
            direction = 1 if trend_dir > 0 else -1
            entry_price = c
            entry_time = dt
            margin_used = margin
            notional = margin_used * LEVERAGE
            size = notional / entry_price * direction

            # 入场同时先扣一次开仓手续费（体现在 PnL 里，用 fee_close 一起算更直观，这里不直接扣 equity）
            # 设置初始 ATR 止损（只用入场时的 ATR，不再放宽）
            if direction == 1:
                stop_price = entry_price - ATR_MULT * atr
                high_since = entry_price
                low_since = None
            else:
                stop_price = entry_price + ATR_MULT * atr
                low_since = entry_price
                high_since = None

            t1_on = False
            t2_on = False
            in_pos = True

    return equity, trades


# ===== 统计输出 =====
def summarize(df_4h: pd.DataFrame, equity: float, trades):
    print(f"4h 数据行数: {len(df_4h)}")
    print(f"时间范围: {df_4h['dt'].iloc[0]} -> {df_4h['dt'].iloc[-1]}")
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

    # 计算最大回撤
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
    ann_ret = total_ret  # 一年数据，近似认为年化 = 总收益率

    print("========== 回测结果（4 小时版·A 路线进阶版） ==========")
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


# ===== 主入口 =====
if __name__ == "__main__":
    df_4h = load_15m_to_4h(CSV_15M_PATH)
    df_4h = add_indicators(df_4h)
    equity, trades = backtest_4h(df_4h)
    summarize(df_4h, equity, trades)
