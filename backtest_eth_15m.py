#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

# ================== 参数区 ==================
CSV_PATH           = "okx_eth_15m.csv"   # ETH/USDT 15m K线CSV，需含 open/high/low/close
INITIAL_EQUITY     = 50.0                # 初始资金（U）
TARGET_MARGIN      = 25.0                # 目标保证金（不足则用全部余额）
LEVERAGE           = 5.0                 # 杠杆
FEE_RATE           = 0.0007              # 单边手续费率（按名义本金计）

# 入场过滤：你的五均线多头 + 回撤到ema144附近 + H1趋势确认
EMA_1 = 144
EMA_2 = 169
EMA_3 = 288
EMA_4 = 338
MA_MID = 120
NEAR_EMA144_PCT = 0.003                  # 与ema144距离 <= 0.3% 视作“回撤到均线附近”

# 止损/止盈：ATR止损 + R分级
ATR_LEN        = 14
ATR_MULT       = 2.0                     # 初始止损距离 = max( ATR_MULT*ATR, MIN_STOP_PCT * entry )
MIN_STOP_PCT   = 0.03                    # 初始止损最小百分比（3%）
MOVE_BE_BREAK  = 0.5                     # 浮盈 >= 0.5R 移动止损到入场价（保本）
TRAIL_START_R  = 1.5                     # 浮盈 >= 1.5R 启动追踪止盈
TRAIL_PCT      = 0.015                   # 启动后按 1.5% 回撤追踪

# 报表
TZ_NAME        = "Asia/Singapore"
REPORT_PATH    = "backtest_eth_15m_report.txt"


# ================== 工具函数 ==================
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 解析时间列（iso/ts/timestamp/第一列兜底）
    if "iso" in df.columns:
        dt = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med and med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit=unit, utc=True, errors="coerce")
    elif "timestamp" in df.columns:
        med = pd.to_numeric(df["timestamp"], errors="coerce").dropna().median()
        unit = "ms" if med and med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"), unit=unit, utc=True, errors="coerce")
    else:
        col0 = df.columns[0]
        med = pd.to_numeric(df[col0], errors="coerce").dropna().median()
        unit = "ms" if med and med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df[col0], errors="coerce"), unit=unit, utc=True, errors="coerce")

    df["dt"] = dt
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    # 必要列校验
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV缺少列: {col}")

    # 数值化
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

    # 本地日界（仅统计用，不触发强平）
    df["dt_sgt"] = df["dt"].dt.tz_convert(TZ_NAME)
    df["date_sgt"] = df["dt_sgt"].dt.date
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    c = df["close"]

    # 15m均线组
    df["ema144"] = c.ewm(span=EMA_1, adjust=False).mean()
    df["ema169"] = c.ewm(span=EMA_2, adjust=False).mean()
    df["ema288"] = c.ewm(span=EMA_3, adjust=False).mean()
    df["ema338"] = c.ewm(span=EMA_4, adjust=False).mean()
    df["ma120"]  = c.rolling(window=MA_MID, min_periods=MA_MID).mean()

    # ATR(14) 15m
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c_prev = c.shift(1)
    tr = pd.concat([
        (h - l),
        (h - c_prev).abs(),
        (l - c_prev).abs()
    ], axis=1).max(axis=1)
    # Wilder平滑
    df["atr"] = tr.ewm(alpha=1/ATR_LEN, adjust=False, min_periods=ATR_LEN).mean()

    # H1过滤：close > ema200 且 ema200上行
    h1 = (df[["dt", "close"]]
           .set_index("dt")
           .resample("1H")
           .last()
           .dropna())
    h1["ema200"] = h1["close"].ewm(span=200, adjust=False).mean()
    h1["ema200_slope"] = h1["ema200"].diff()
    h1["h1_ok"] = (h1["close"] > h1["ema200"]) & (h1["ema200_slope"] > 0)

    # 回填到15m（向后合并）
    h1_ = h1[["h1_ok"]].reset_index()
    df = pd.merge_asof(df.sort_values("dt"), h1_.sort_values("dt"),
                       on="dt", direction="backward")
    df["h1_ok"] = df["h1_ok"].fillna(False)

    # 丢掉指标未就绪的前段
    df = df.dropna(subset=["ema144", "ema169", "ema288", "ema338", "ma120", "atr"]).reset_index(drop=True)
    return df


def is_uptrend_15m(row) -> bool:
    return (row["close"] > row["ema144"] > row["ema169"] > row["ema288"] > row["ema338"]) and (row["ma120"] > row["ema288"])


def near_ema144(row) -> bool:
    # 接近ema144 或 当根K线触碰到ema144
    if row["ema144"] <= 0:
        return False
    near = abs(row["close"] - row["ema144"]) / row["close"] <= NEAR_EMA144_PCT
    touch = (row["low"] <= row["ema144"] <= row["high"])
    return bool(near or touch)


# ================== 回测主体 ==================
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    entry_price = None
    entry_time = None
    entry_date_sgt = None
    margin_used = 0.0

    # ATR基止损
    init_stop = None
    R = None

    # 追踪止盈
    high_since_entry = None
    stop_price = None
    be_done = False          # 是否已移到保本
    trail_on = False         # 是否已启用追踪

    trades = []
    last_close = None

    for _, row in df.iterrows():
        dt = row["dt"]
        date_sgt = row["date_sgt"]
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])

        # ===== 仓位管理 =====
        if in_pos:
            # 最高价与浮盈
            high_since_entry = h if high_since_entry is None else max(high_since_entry, h)
            # 初始化R（一次）
            if R is None and init_stop is not None:
                R = entry_price - init_stop

            # 0.5R 移保本
            if (not be_done) and R is not None and (high_since_entry - entry_price) >= MOVE_BE_BREAK * R:
                stop_price = max(stop_price, entry_price) if stop_price is not None else entry_price
                be_done = True

            # 1.5R 开启追踪
            if (not trail_on) and R is not None and (high_since_entry - entry_price) >= TRAIL_START_R * R:
                trail_on = True

            # 追踪止盈（1.5%回撤）
            if trail_on:
                trail_level = high_since_entry * (1 - TRAIL_PCT)
                stop_price = trail_level if (stop_price is None) else max(stop_price, trail_level)

            # 固定底线：初始ATR止损
            stop_price = max(stop_price, init_stop) if stop_price is not None else init_stop

            # 触发离场：触发价按最低价触碰
            exit_price = None
            exit_reason = None
            if l <= stop_price:
                exit_price = stop_price
                exit_reason = "stop/ATR/trail"

            if exit_price is not None:
                # 结算
                notional = margin_used * LEVERAGE
                size = notional / entry_price if entry_price > 0 else 0.0
                gross_pnl = (exit_price - entry_price) * size
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
                    "margin_used": margin_used,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": (pnl_net / margin_used) if margin_used > 0 else 0.0,
                    "equity_after": equity,
                    "date_sgt": str(entry_date_sgt),
                })

                # 清仓
                in_pos = False
                entry_price = None
                entry_time = None
                entry_date_sgt = None
                margin_used = 0.0
                init_stop = None
                R = None
                high_since_entry = None
                stop_price = None
                be_done = False
                trail_on = False

        # ===== 开仓逻辑（直到归零，不设25U下限） =====
        if (not in_pos) and (equity > 0):
            # 多头趋势 + 回撤到ema144 + H1 OK
            if row["h1_ok"] and is_uptrend_15m(row) and near_ema144(row):
                margin_used = min(TARGET_MARGIN, equity)  # 余额不足25U也照样上
                if margin_used > 0:
                    in_pos = True
                    entry_price = c
                    entry_time = dt
                    entry_date_sgt = date_sgt

                    # 初始ATR止损
                    atr = float(row["atr"])
                    atr_stop_dist = max(ATR_MULT * atr, MIN_STOP_PCT * entry_price)
                    init_stop = entry_price - atr_stop_dist
                    stop_price = init_stop
                    high_since_entry = c
                    be_done = False
                    trail_on = False

        last_close = c

    # 数据结束时如仍持仓：为统计起见，按最后收盘价做“数据边界标记离场”
    # （不是“手动强平”，只是为了把未实现盈亏记入报表）
    if in_pos and entry_price is not None:
        exit_price = last_close
        notional = margin_used * LEVERAGE
        size = notional / entry_price if entry_price > 0 else 0.0
        gross_pnl = (exit_price - entry_price) * size
        fee_open = notional * FEE_RATE
        fee_close = abs(exit_price * size) * FEE_RATE
        pnl_net = gross_pnl - fee_open - fee_close
        equity += pnl_net
        trades.append({
            "entry_time": entry_time,
            "exit_time": df["dt"].iloc[-1],
            "entry_price": entry_price,
            "exit_price": exit_price,
            "exit_reason": "data_end_mark",
            "margin_used": margin_used,
            "pnl_net": pnl_net,
            "pnl_pct_on_margin": (pnl_net / margin_used) if margin_used > 0 else 0.0,
            "equity_after": equity,
            "date_sgt": str(entry_date_sgt),
        })

    return equity, trades


def summarize(df: pd.DataFrame, equity, trades):
    lines = []
    lines.append(f"数据行数: {len(df)}")
    lines.append(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}")
    lines.append("")

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
    peak, max_dd = eq_curve[0], 0.0
    for x in eq_curve:
        if x > peak: peak = x
        dd = (x - peak) / peak
        if dd < max_dd: max_dd = dd

    unique_days = df["date_sgt"].nunique()
    trade_days = len(set(t["date_sgt"] for t in trades))
    coverage = trade_days / unique_days * 100 if unique_days else 0.0

    total_ret = (equity - INITIAL_EQUITY) / INITIAL_EQUITY
    win_rate = (wins / n * 100) if n > 0 else 0.0

    lines.append("========== 回测结果（多头过滤 + 回撤入场 + H1确认 + ATR止损/追踪｜直到归零） ==========")
    lines.append(f"总交易数: {n}  | 交易天数/总天数: {trade_days}/{unique_days} 覆盖率: {coverage:.2f}%")
    lines.append(f"胜: {wins}  负: {losses}  和: {flats}")
    lines.append(f"胜率: {win_rate:.2f}%")
    lines.append(f"总盈亏: {total_pnl:.4f} U")
    lines.append(f"期末资金: {equity:.4f} U (初始 {INITIAL_EQUITY} U)")
    lines.append(f"平均盈利单: {avg_win:.4f} U")
    lines.append(f"平均亏损单: {avg_loss:.4f} U")
    lines.append(f"最大回撤: {max_dd*100:.2f}%")
    lines.append(f"总收益率: {total_ret*100:.2f}%")
    lines.append("")
    lines.append("前 5 笔交易示例:")
    for t in trades[:5]:
        lines.append(str(t))

    text = "\n".join(lines)
    print(text)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(text)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
