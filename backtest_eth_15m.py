#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ========= 基本参数 =========
CSV_PATH       = "okx_eth_15m.csv"   # 你的 ETH 15m K线CSV
INITIAL_EQUITY = 50.0                # 初始资金
TARGET_MARGIN  = 25.0                # 目标保证金，不足则全仓
LEVERAGE       = 5.0                 # 杠杆
FEE_RATE       = 0.0007              # 单边手续费率（按名义本金）

# 原始策略：5% 止损 + 两档移动止盈（5% / 2% 回撤）
STOP_LOSS_PCT   = 0.05               # 固定止损 5%
TRAIL_1_TRIGGER = 0.05               # 浮盈 >=5% 启动第一档
TRAIL_1_PCT     = 0.05               # 第一档：最大价回撤 5%
TRAIL_2_TRIGGER = 0.10               # 浮盈 >=10% 启动第二档
TRAIL_2_PCT     = 0.02               # 第二档：最大价回撤 2%

EMA_FAST = 34
EMA_SLOW = 144

REPORT_PATH = "backtest_ema_flip_eth_15m_report.txt"


# ========= 读数据 =========
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 解析时间：iso / ts / timestamp / 第1列兜底
    if "iso" in df.columns:
        dt = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med and med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                            unit=unit, utc=True, errors="coerce")
    elif "timestamp" in df.columns:
        med = pd.to_numeric(df["timestamp"], errors="coerce").dropna().median()
        unit = "ms" if med and med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"),
                            unit=unit, utc=True, errors="coerce")
    else:
        col0 = df.columns[0]
        med = pd.to_numeric(df[col0], errors="coerce").dropna().median()
        unit = "ms" if med and med > 1e11 else "s"
        dt = pd.to_datetime(pd.to_numeric(df[col0], errors="coerce"),
                            unit=unit, utc=True, errors="coerce")

    df["dt"] = dt
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    # 必要列
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV缺少列: {col}")

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

    # 计算 EMA
    close = df["close"]
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # 去掉EMA没算完的前面一段
    df = df.dropna(subset=["ema_fast", "ema_slow"]).reset_index(drop=True)

    return df


# ========= 回测：原始策略 + EMA双向反转 =========
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0       # +1 多，-1 空
    entry_price = None
    entry_time = None
    margin_used = 0.0

    high_since_entry = None  # 多单用
    low_since_entry  = None  # 空单用
    stop_price = None
    trail_mode = 0           # 0: 仅固定止损；1: 第一档；2: 第二档

    trades = []
    last_close = None

    for _, row in df.iterrows():
        dt = row["dt"]
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        ema_fast = row["ema_fast"]
        ema_slow = row["ema_slow"]

        # 当前趋势方向
        if ema_fast > ema_slow:
            trend_sign = 1
        elif ema_fast < ema_slow:
            trend_sign = -1
        else:
            trend_sign = 0

        # ===== 先处理止损 / 移动止盈 =====
        if in_pos:
            exit_price = None
            exit_reason = None

            # 固定止损 & 移动止盈，分多空
            if direction == 1:  # 多
                # 更新最高价
                if high_since_entry is None:
                    high_since_entry = h
                else:
                    high_since_entry = max(high_since_entry, h)

                # 浮盈（按最高价）
                gain_pct = (high_since_entry - entry_price) / entry_price

                # 档位切换
                if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                    stop_price = high_since_entry * (1 - TRAIL_1_PCT)
                    trail_mode = 1
                if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                    stop_price = high_since_entry * (1 - TRAIL_2_PCT)
                    trail_mode = 2

                # 跟随最高价抬升止损
                if trail_mode == 1:
                    new_stop = high_since_entry * (1 - TRAIL_1_PCT)
                    if stop_price is None or new_stop > stop_price:
                        stop_price = new_stop
                elif trail_mode == 2:
                    new_stop = high_since_entry * (1 - TRAIL_2_PCT)
                    if stop_price is None or new_stop > stop_price:
                        stop_price = new_stop

                # 固定 5% 止损底线
                fixed_stop = entry_price * (1 - STOP_LOSS_PCT)
                if stop_price is None:
                    stop_price = fixed_stop
                else:
                    stop_price = max(stop_price, fixed_stop)

                # 本K是否触发
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"

            elif direction == -1:  # 空
                # 更新最低价
                if low_since_entry is None:
                    low_since_entry = l
                else:
                    low_since_entry = min(low_since_entry, l)

                # 浮盈（按最低价）
                gain_pct = (entry_price - low_since_entry) / entry_price

                # 档位切换
                if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                    stop_price = low_since_entry * (1 + TRAIL_1_PCT)
                    trail_mode = 1
                if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                    stop_price = low_since_entry * (1 + TRAIL_2_PCT)
                    trail_mode = 2

                # 跟随最低价下移止损
                if trail_mode == 1:
                    new_stop = low_since_entry * (1 + TRAIL_1_PCT)
                    if stop_price is None or new_stop < stop_price:
                        stop_price = new_stop
                elif trail_mode == 2:
                    new_stop = low_since_entry * (1 + TRAIL_2_PCT)
                    if stop_price is None or new_stop < stop_price:
                        stop_price = new_stop

                # 固定 5% 止损顶线（空单亏损是向上）
                fixed_stop = entry_price * (1 + STOP_LOSS_PCT)
                if stop_price is None:
                    stop_price = fixed_stop
                else:
                    stop_price = min(stop_price, fixed_stop)

                # 本K是否触发
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"

            # 如有被止损/止盈，先结算
            if exit_price is not None:
                notional = margin_used * LEVERAGE
                size = notional / entry_price if entry_price > 0 else 0.0

                if direction == 1:
                    gross_pnl = (exit_price - entry_price) * size
                else:  # 空
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
                    "pnl_pct_on_margin": (pnl_net / margin_used) if margin_used > 0 else 0.0,
                    "equity_after": equity,
                })

                # 清仓
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                margin_used = 0.0
                high_since_entry = None
                low_since_entry = None
                stop_price = None
                trail_mode = 0

        # ===== 止损处理完，再看 EMA 反转换边 =====
        if in_pos and trend_sign != 0 and trend_sign != direction:
            # 用当前收盘价强平，并立刻反向开仓
            exit_price = c
            exit_reason = "ema_flip"

            notional = margin_used * LEVERAGE
            size = notional / entry_price if entry_price > 0 else 0.0

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
                "pnl_pct_on_margin": (pnl_net / margin_used) if margin_used > 0 else 0.0,
                "equity_after": equity,
            })

            # 清空旧仓
            in_pos = False
            direction = 0
            entry_price = None
            entry_time = None
            margin_used = 0.0
            high_since_entry = None
            low_since_entry = None
            stop_price = None
            trail_mode = 0

            # 反向开新仓（如果还有钱）
            if equity > 0:
                margin_used = min(TARGET_MARGIN, equity)
                if margin_used > 0:
                    in_pos = True
                    direction = trend_sign
                    entry_price = c
                    entry_time = dt
                    if direction == 1:
                        high_since_entry = c
                        low_since_entry = None
                    else:
                        low_since_entry = c
                        high_since_entry = None
                    stop_price = None
                    trail_mode = 0

        # ===== 如果当前没仓位，看是否按趋势开新仓 =====
        if (not in_pos) and equity > 0:
            if trend_sign != 0:
                margin_used = min(TARGET_MARGIN, equity)
                if margin_used > 0:
                    in_pos = True
                    direction = trend_sign
                    entry_price = c
                    entry_time = dt
                    if direction == 1:
                        high_since_entry = c
                        low_since_entry = None
                    else:
                        low_since_entry = c
                        high_since_entry = None
                    stop_price = None
                    trail_mode = 0

        last_close = c

    # 数据结束，如果还在持仓，为了报表记录，用最后收盘价平掉
    if in_pos and entry_price is not None:
        exit_price = last_close
        notional = margin_used * LEVERAGE
        size = notional / entry_price if entry_price > 0 else 0.0
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
            "exit_time": df["dt"].iloc[-1],
            "entry_price": entry_price,
            "exit_price": exit_price,
            "exit_reason": "data_end_mark",
            "direction": direction,
            "margin_used": margin_used,
            "pnl_net": pnl_net,
            "pnl_pct_on_margin": (pnl_net / margin_used) if margin_used > 0 else 0.0,
            "equity_after": equity,
        })

    return equity, trades


# ========= 统计 & 输出 =========
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

    total_ret = (equity - INITIAL_EQUITY) / INITIAL_EQUITY
    win_rate = (wins / n * 100) if n > 0 else 0.0

    lines.append("========== 回测结果（保命版：原始策略 + EMA34/144 反转立刻换边） ==========")
    lines.append(f"总交易数: {n}")
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
    equity, trades = backtest(df)
    summarize(df, equity, trades)
