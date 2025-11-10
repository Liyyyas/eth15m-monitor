#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ========= 基本参数 =========
CSV_PATH         = "okx_eth_15m.csv"  # 15m ETH 数据
INITIAL_EQUITY   = 50.0               # 初始资金
BASE_MARGIN      = 25.0               # 每笔目标保证金（不够就用剩下的全上）
LEVERAGE         = 5.0
FEE_RATE         = 0.0007             # 单边手续费率（按你交易所自己改）

# 原始策略止损 / 追踪止盈
STOP_LOSS_PCT    = 0.05               # 固定止损 5%
TRAIL_1_TRIGGER  = 0.05               # 浮盈 >= 5% 启动第一档
TRAIL_1_PCT      = 0.05               # 第一档：5% 回撤
TRAIL_2_TRIGGER  = 0.10               # 浮盈 >= 10% 启动第二档
TRAIL_2_PCT      = 0.02               # 第二档：2% 回撤

# EMA 反转 & 延迟确认
EMA_FAST         = 34
EMA_SLOW         = 144
MIN_WAIT_BARS    = 2                  # 反转后至少等待的 K 线根数
USE_MOMENTUM_SLOPE = True             # 是否要求 EMA34 斜率配合


# ========= 读取 & 预处理 =========
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列处理：优先 iso，其次 ts，否则尝试第一列
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

    # 列名统一：有 o/h/l/c 就重命名为 open/high/low/close
    if "open" not in df.columns and "o" in df.columns:
        df = df.rename(columns={
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close"
        })

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df


def add_ema(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()
    df = df.dropna(subset=["ema_fast", "ema_slow"]).reset_index(drop=True)
    return df


def sign_trend(ema_fast: float, ema_slow: float) -> int:
    """返回趋势方向：1=多头，-1=空头，0=无效/持平"""
    if pd.isna(ema_fast) or pd.isna(ema_slow):
        return 0
    if ema_fast > ema_slow:
        return 1
    if ema_fast < ema_slow:
        return -1
    return 0


# ========= 回测主逻辑 =========
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0            # 1 多 -1 空
    entry_price = None
    entry_time = None
    high_since_entry = None  # 多头用
    low_since_entry = None   # 空头用
    stop_price = None
    trail_mode = 0           # 0=只固定止损, 1=5%回撤, 2=2%回撤

    pending_dir = 0          # EMA 反转后等待的方向（1 多 -1 空）
    bars_since_flip = 0      # 反转后已等待多少根 K

    trades = []

    prev_ema_fast = None
    prev_ema_slow = None
    prev_trend = 0

    for i, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])
        curr_trend = sign_trend(ema_fast, ema_slow)

        # ===== 1. 先管理已有持仓（止损 / 追踪止盈） =====
        if in_pos:
            # 更新浮盈极值
            if direction == 1:
                # 多头：记录最高价
                if high_since_entry is None:
                    high_since_entry = h
                else:
                    high_since_entry = max(high_since_entry, h)

                gain_pct = (high_since_entry - entry_price) / entry_price

                # 追踪止盈档位
                if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                    stop_price = high_since_entry * (1 - TRAIL_1_PCT)
                    trail_mode = 1
                if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                    stop_price = high_since_entry * (1 - TRAIL_2_PCT)
                    trail_mode = 2

                # 跟随最高价抬止损
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
                if stop_price is None:
                    stop_price = fixed_stop
                else:
                    stop_price = max(stop_price, fixed_stop)

                # 检查是否打到止损/追踪线（先按止损逻辑出场）
                exit_price = None
                exit_reason = None
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "sl_or_trail"

            elif direction == -1:
                # 空头：记录最低价
                if low_since_entry is None:
                    low_since_entry = l
                else:
                    low_since_entry = min(low_since_entry, l)

                gain_pct = (entry_price - low_since_entry) / entry_price

                # 追踪止盈档位（对称处理）
                if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                    stop_price = low_since_entry * (1 + TRAIL_1_PCT)
                    trail_mode = 1
                if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                    stop_price = low_since_entry * (1 + TRAIL_2_PCT)
                    trail_mode = 2

                if trail_mode == 1:
                    new_stop = low_since_entry * (1 + TRAIL_1_PCT)
                    if stop_price is None or new_stop < stop_price:
                        stop_price = new_stop
                elif trail_mode == 2:
                    new_stop = low_since_entry * (1 + TRAIL_2_PCT)
                    if stop_price is None or new_stop < stop_price:
                        stop_price = new_stop

                fixed_stop = entry_price * (1 + STOP_LOSS_PCT)
                if stop_price is None:
                    stop_price = fixed_stop
                else:
                    stop_price = min(stop_price, fixed_stop)

                exit_price = None
                exit_reason = None
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "sl_or_trail"

            # 如果被止损/止盈打出
            if exit_price is not None:
                margin = min(BASE_MARGIN, equity)
                notional = margin * LEVERAGE
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
                    "margin_used": margin,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin if margin > 0 else 0.0,
                    "equity_after": equity,
                })

                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                high_since_entry = None
                low_since_entry = None
                stop_price = None
                trail_mode = 0

        # ===== 2. EMA 反转：先平仓，然后只记录“等待方向” =====
        if prev_trend != 0 and curr_trend != 0 and curr_trend != prev_trend:
            # 趋势翻转：如果有持仓，先按收盘价平掉
            if in_pos:
                exit_price = c
                margin = min(BASE_MARGIN, equity)
                notional = margin * LEVERAGE
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
                    "exit_reason": "ema_flip_close",
                    "direction": direction,
                    "margin_used": margin,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin if margin > 0 else 0.0,
                    "equity_after": equity,
                })

                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                high_since_entry = None
                low_since_entry = None
                stop_price = None
                trail_mode = 0

            # 不立即反向开仓：只记录新的目标方向，等待确认
            pending_dir = curr_trend
            bars_since_flip = 0

        # ===== 3. 反转后延迟入场确认 =====
        if pending_dir != 0:
            bars_since_flip += 1

        can_open = (not in_pos) and (equity > 0) and (pending_dir != 0)

        if can_open and bars_since_flip >= MIN_WAIT_BARS:
            # 价格证据：空头 / 多头
            price_ok = False
            mom_ok = True  # 默认不用斜率过滤；下面按需打开

            if pending_dir == 1:  # 准备开多
                price_ok = (c > ema_fast) and (c > ema_slow)
                if USE_MOMENTUM_SLOPE and prev_ema_fast is not None:
                    mom_ok = ema_fast > prev_ema_fast
            elif pending_dir == -1:  # 准备开空
                price_ok = (c < ema_fast) and (c < ema_slow)
                if USE_MOMENTUM_SLOPE and prev_ema_fast is not None:
                    mom_ok = ema_fast < prev_ema_fast

            if price_ok and mom_ok:
                # 开仓
                direction = pending_dir
                in_pos = True
                entry_price = c
                entry_time = dt
                high_since_entry = c
                low_since_entry = c
                stop_price = None
                trail_mode = 0

                # 固定止损初始
                if direction == 1:
                    stop_price = entry_price * (1 - STOP_LOSS_PCT)
                else:
                    stop_price = entry_price * (1 + STOP_LOSS_PCT)

                # 一旦真正开仓，这一轮等待结束
                pending_dir = 0
                bars_since_flip = 0

        # 更新“上一根”的 EMA / 趋势
        prev_ema_fast = ema_fast
        prev_ema_slow = ema_slow
        prev_trend = curr_trend

        # 资金归零直接终止
        if equity <= 0:
            equity = 0.0
            break

    return equity, trades, df


# ========= 结果统计 =========
def summarize(equity, trades, df):
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
    ann_ret = total_ret  # 一年样本，近似年化

    print("========== 回测结果（原始策略 + EMA反转延迟确认） ==========")
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
    df = add_ema(df)
    equity, trades, df_used = backtest(df)
    summarize(equity, trades, df_used)
