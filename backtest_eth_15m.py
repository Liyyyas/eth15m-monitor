#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ===== 基本配置 =====
CSV_PATH           = "okx_eth_15m.csv"        # 你的 ETH 15m CSV
INITIAL_EQUITY     = 50.0                     # 初始资金
TARGET_MARGIN      = 25.0                     # 目标保证金（会动态下调，不再卡 25U 门槛）
LEVERAGE           = 5.0                      # 杠杆
FEE_RATE           = 0.0007                   # 单边手续费率（0.07%）

STOP_LOSS_PCT      = 0.05                     # 固定止损 5%
TRAIL_1_TRIGGER    = 0.05                     # 浮盈 >= 5% 启动第一档跟踪
TRAIL_1_PCT        = 0.05                     # 第一档：5% 回撤止盈
TRAIL_2_TRIGGER    = 0.10                     # 浮盈 >= 10% 启动第二档
TRAIL_2_PCT        = 0.02                     # 第二档：2% 回撤止盈

MAX_HOLD_BARS      = 96                       # 保险：最多持仓 1 天（15m*96）
TZ_NAME            = "Asia/Singapore"         # 以新加坡时区计算“跨日平仓”
REPORT_PATH        = "backtest_eth_15m_report.txt"

# 趋势线参数（跟你图上的五条线对应）
EMA_1 = 144
EMA_2 = 169
EMA_3 = 288
EMA_4 = 338
MA_MID = 120


# ===== 数据读取 & 预处理 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列：iso / ts / timestamp / 第一列兜底
    if "iso" in df.columns:
        dt = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                            unit=unit, utc=True, errors="coerce")
    elif "timestamp" in df.columns:
        med = pd.to_numeric(df["timestamp"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"),
                            unit=unit, utc=True, errors="coerce")
    else:
        first_col = df.columns[0]
        med = pd.to_numeric(df[first_col], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df[first_col]), unit=unit,
                            utc=True, errors="coerce")

    df["dt"] = dt
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    # 新加坡本地日界
    df["dt_sgt"] = df["dt"].dt.tz_convert(TZ_NAME)
    df["date_sgt"] = df["dt_sgt"].dt.date
    return df


# ===== 计算均线 / EMA，并做趋势过滤用 =====
def add_trend_columns(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)

    df["ema144"] = close.ewm(span=EMA_1, adjust=False).mean()
    df["ema169"] = close.ewm(span=EMA_2, adjust=False).mean()
    df["ema288"] = close.ewm(span=EMA_3, adjust=False).mean()
    df["ema338"] = close.ewm(span=EMA_4, adjust=False).mean()
    df["ma120"]  = close.rolling(window=MA_MID, min_periods=MA_MID).mean()

    # 丢掉前面均线没算完的部分
    df = df.dropna(subset=["ema144", "ema169", "ema288", "ema338", "ma120"]).reset_index(drop=True)
    return df


def is_uptrend(row) -> bool:
    """
    多头趋势定义：
    - 收盘价在均线组之上： close > ema144 > ema169 > ema288 > ema338
    - ma120 也在 ema288 之上，代表中期偏多
    """
    return (
        row["close"] > row["ema144"] > row["ema169"] > row["ema288"] > row["ema338"]
        and row["ma120"] > row["ema288"]
    )


# ===== 回测主体：只在多头趋势时开多，直到归零 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    entry_price = None
    entry_time = None
    entry_date_sgt = None
    high_since_entry = None
    stop_price = None
    trail_mode = 0
    bars_in_trade = 0
    margin_used = 0.0

    trades = []

    last_close = None  # 跨日强平用上一根收盘价

    for _, row in df.iterrows():
        dt = row["dt"]
        dt_sgt = row["dt_sgt"]
        date_sgt = row["date_sgt"]

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        # ===== 有持仓：先管仓位 =====
        if in_pos:
            bars_in_trade += 1

            high_since_entry = h if high_since_entry is None else max(high_since_entry, h)
            gain_pct = (high_since_entry - entry_price) / entry_price

            # 一档追踪：5% 浮盈 → 5% 回撤
            if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_1_PCT)
                trail_mode = 1
            # 二档追踪：10% 浮盈 → 2% 回撤
            if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                stop_price = high_since_entry * (1 - TRAIL_2_PCT)
                trail_mode = 2

            # 跟随抬高止损
            if trail_mode == 1:
                new_stop = high_since_entry * (1 - TRAIL_1_PCT)
                if stop_price is None or new_stop > stop_price:
                    stop_price = new_stop
            elif trail_mode == 2:
                new_stop = high_since_entry * (1 - TRAIL_2_PCT)
                if stop_price is None or new_stop > stop_price:
                    stop_price = new_stop

            # 固定止损 5%
            fixed_stop = entry_price * (1 - STOP_LOSS_PCT)
            stop_price = fixed_stop if stop_price is None else max(stop_price, fixed_stop)

            exit_price = None
            exit_reason = None

            # ① 日内触发止损 / 追踪止盈
            if l <= stop_price:
                exit_price = stop_price
                exit_reason = "stop_or_trail"

            # ② SGT 跨日强平：下一天第一根开始，就用上一根收盘价强平
            if exit_price is None and date_sgt != entry_date_sgt and last_close is not None:
                exit_price = last_close
                exit_reason = "day_end"

            # ③ 保险：持仓超过阈值，直接按当前收盘价平
            if exit_price is None and bars_in_trade >= MAX_HOLD_BARS:
                exit_price = c
                exit_reason = "max_hold"

            if exit_price is not None:
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
                    "bars_held": bars_in_trade,
                    "date_sgt": str(entry_date_sgt),
                })

                in_pos = False
                entry_price = None
                entry_time = None
                entry_date_sgt = None
                high_since_entry = None
                stop_price = None
                trail_mode = 0
                bars_in_trade = 0
                margin_used = 0.0

        # ===== 没持仓：只在多头趋势时开仓；直到 equity <= 0 自动断粮 =====
        if (not in_pos) and equity > 0 and is_uptrend(row):
            # 动态保证金：上限 TARGET_MARGIN，下限是当前 equity
            margin_used = min(TARGET_MARGIN, equity)
            if margin_used > 0:
                in_pos = True
                entry_price = c
                entry_time = dt
                entry_date_sgt = date_sgt
                high_since_entry = c
                stop_price = entry_price * (1 - STOP_LOSS_PCT)
                trail_mode = 0
                bars_in_trade = 0

        last_close = c

    return equity, trades


# ===== 结果统计 + 写入 report.txt =====
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
    peak = eq_curve[0]
    max_dd = 0.0
    for x in eq_curve:
        if x > peak:
            peak = x
        dd = (x - peak) / peak
        if dd < max_dd:
            max_dd = dd

    unique_days = df["date_sgt"].nunique()
    trade_days = len(set(t["date_sgt"] for t in trades))
    coverage = trade_days / unique_days * 100 if unique_days > 0 else 0.0

    total_ret = (equity - INITIAL_EQUITY) / INITIAL_EQUITY
    win_rate = (wins / n * 100) if n > 0 else 0.0

    lines.append("========== 回测结果（只在多头趋势开多 + 动态仓位 + 日内止损/追踪） ==========")
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

    # 写入 report.txt
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(text)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_trend_columns(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
