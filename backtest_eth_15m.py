#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ===== 基本配置 =====
CSV_PATH         = "okx_eth_15m.csv"   # 你的 ETH 15m CSV
INITIAL_EQUITY   = 50.0                # 初始资金
MARGIN_PER_TRADE = 25.0                # 每笔用多少保证金（想要更多笔数可以改小一点，比如 10）
LEVERAGE         = 5.0                 # 杠杆
FEE_RATE         = 0.0007              # 单边手续费率（0.07% 自己按交易所改）

STOP_LOSS_PCT    = 0.05                # 固定止损 5%
TRAIL_1_TRIGGER  = 0.05                # 浮盈 >= 5% 启动第一档跟踪
TRAIL_1_PCT      = 0.05                # 第一档：5% 回撤止盈
TRAIL_2_TRIGGER  = 0.10               # 浮盈 >= 10% 启动第二档
TRAIL_2_PCT      = 0.02               # 第二档：2% 回撤止盈

# ===== 关键：最多持有多少根 15mK（一根 15m，96 根 ≈ 1 天）=====
MAX_HOLD_BARS    = 96                 # 最多持仓 1 天，超时强制按收盘价平仓

# 趋势线参数：简单用 144 / 288 做多头过滤
EMA_SHORT = 144
EMA_LONG  = 288


# ===== 数据读取 & 预处理 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列：优先 iso，其次 ts/timestamp，最后用第一列兜底
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    elif "timestamp" in df.columns:
        med = pd.to_numeric(df["timestamp"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    else:
        first_col = df.columns[0]
        med = pd.to_numeric(df[first_col], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first_col], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")

    # 按时间排序，丢掉时间缺失
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    # 必要字段检查
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


# ===== 计算 EMA，用来判断多头环境 =====
def add_trend_columns(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    df["ema144"] = close.ewm(span=EMA_SHORT, adjust=False).mean()
    df["ema288"] = close.ewm(span=EMA_LONG,  adjust=False).mean()
    df = df.dropna(subset=["ema144", "ema288"]).reset_index(drop=True)
    return df


def is_uptrend(row) -> bool:
    """
    简单多头环境：
    - ema144 > ema288
    不再要求一堆均线完美排队，只要中短期在线上就算多头。
    """
    return row["ema144"] > row["ema288"]


# ===== 回测主体：单边做多 + 多头过滤 + 时间止盈 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    entry_price = None
    entry_time = None
    high_since_entry = None
    stop_price = None
    trail_mode = 0  # 0: 只有固定止损；1: 5% 回撤；2: 2% 回撤
    bars_in_trade = 0  # 已经持有了多少根K线

    trades = []

    for _, row in df.iterrows():
        dt = row["dt"]
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        # ===== 有持仓：管理止损 / 追踪止盈 / 时间平仓 =====
        if in_pos:
            bars_in_trade += 1

            # 更新入场以来最高价
            if high_since_entry is None:
                high_since_entry = h
            else:
                high_since_entry = max(high_since_entry, h)

            # 浮盈百分比（用 high_since_entry）
            gain_pct = (high_since_entry - entry_price) / entry_price

            # 1档：浮盈>=5% → 5% 回撤
            if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_1_PCT)
                trail_mode = 1

            # 2档：浮盈>=10% → 2% 回撤
            if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_2_PCT)
                trail_mode = 2

            # 跟随最高价上移止损
            if trail_mode == 1:
                new_stop = high_since_entry * (1 - TRAIL_1_PCT)
                if stop_price is None or new_stop > stop_price:
                    stop_price = new_stop
            elif trail_mode == 2:
                new_stop = high_since_entry * (1 - TRAIL_2_PCT)
                if stop_price is None or new_stop > stop_price:
                    stop_price = new_stop

            # 固定止损保护（5%）
            fixed_stop = entry_price * (1 - STOP_LOSS_PCT)
            if stop_price is None:
                stop_price = fixed_stop
            else:
                stop_price = max(stop_price, fixed_stop)

            exit_price = None
            exit_reason = None

            # ① 价格触发止损 / 跟踪止盈
            if l <= stop_price:
                exit_price = stop_price
                exit_reason = "stop_or_trail"

            # ② 时间止盈（最多持仓 MAX_HOLD_BARS 根K）
            if exit_price is None and bars_in_trade >= MAX_HOLD_BARS:
                exit_price = c
                exit_reason = "time_exit"

            # 真正平仓结算
            if exit_price is not None:
                margin = MARGIN_PER_TRADE
                notional = margin * LEVERAGE
                size = notional / entry_price

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
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin,
                    "equity_after": equity,
                    "bars_held": bars_in_trade,
                })

                # 清空仓位状态
                in_pos = False
                entry_price = None
                entry_time = None
                high_since_entry = None
                stop_price = None
                trail_mode = 0
                bars_in_trade = 0

        # ===== 没有持仓：看是否可以开多 =====
        if (not in_pos) and equity >= MARGIN_PER_TRADE:
            # 只要在多头环境里，就可以开多（大幅提高信号密度）
            if is_uptrend(row):
                in_pos = True
                entry_price = c  # 用收盘价开仓
                entry_time = dt
                high_since_entry = c
                stop_price = entry_price * (1 - STOP_LOSS_PCT)
                trail_mode = 0
                bars_in_trade = 0

    return equity, trades


# ===== 结果统计 =====
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
    ann_ret = total_ret  # 一年数据，年化≈总收益率

    print("========== 回测结果（单边做多 + 多头过滤 + 时间止盈） ==========")
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
    df = add_trend_columns(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
