#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ===== 配置区 =====
CSV_PATH        = "okx_eth_15m.csv"   # 你的 ETH 15m 数据
INITIAL_EQUITY  = 50.0                # 初始资金
LEVERAGE        = 5.0                 # 杠杆
FEE_RATE        = 0.0007              # 单边手续费（0.07%，你可以改）

MARGIN_FRACTION = 0.5                 # 每次用当前资金的 50% 做保证金
MIN_MARGIN      = 1.0                 # 资金太小就用 1U 作为最小保证金（防止0除）

STOP_LOSS_PCT   = 0.08                # 固定止损 8%
TRAIL_TRIGGER   = 0.12                # 浮盈 ≥ 12% 启用移动止损
TRAIL_PCT       = 0.02                # 启动后回撤 2% 平仓

EMA_FAST        = 34                  # 快线
EMA_SLOW        = 144                 # 慢线
CONFIRM_BARS    = 5                   # 反转连续确认根数

# 震荡过滤：EMA34/144 距离太近就视为震荡（不新开仓）
MIN_SPREAD_RATIO = 0.0015             # |ema34-ema144| / close < 0.15% 视为震荡


# ===== 读取数据 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列处理：优先 iso，其次 ts，再不行用第一列兜底
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

    return df


# ===== 指标计算 =====
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)

    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # EMA 差值和比例，用来做震荡过滤
    df["ema_diff"] = df["ema_fast"] - df["ema_slow"]
    df["ema_spread_ratio"] = (df["ema_diff"].abs() / close)

    # 丢掉前面算不出 EMA 的部分
    df = df.dropna(subset=["ema_fast", "ema_slow"]).reset_index(drop=True)
    return df


def sign(x: float) -> int:
    if x > 0:
        return 1
    elif x < 0:
        return -1
    else:
        return 0


# ===== 回测主体 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0         # 1 = 多，-1 = 空
    entry_price = None
    entry_time = None
    high_since_entry = None
    low_since_entry = None
    stop_price = None
    trail_on = False

    trades = []

    # 趋势确认状态
    confirmed_trend = 0           # 当前确认的趋势方向：1 多，-1 空，0 无
    trend_candidate = 0           # 正在候选的方向
    trend_candidate_bars = 0      # 连续确认根数

    for i, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])
        ema_diff = ema_fast - ema_slow
        ema_sign = sign(ema_diff)

        # ===== 1. 维护“5根确认反转”的趋势方向 =====
        if ema_sign == 0:
            # 快慢线完全重合时，不更新趋势候选，但也不 reset
            pass
        else:
            if ema_sign == trend_candidate:
                trend_candidate_bars += 1
            else:
                trend_candidate = ema_sign
                trend_candidate_bars = 1

            if trend_candidate_bars >= CONFIRM_BARS:
                confirmed_trend = trend_candidate

        # 震荡过滤：EMA34/144 距离太近，视为震荡区——不新开仓
        is_choppy = (row["ema_spread_ratio"] < MIN_SPREAD_RATIO)

        # ===== 2. 有持仓时，先管止损 / 移动止损 =====
        if in_pos:
            if direction == 1:
                # 多单：更新最高价
                if high_since_entry is None:
                    high_since_entry = h
                else:
                    high_since_entry = max(high_since_entry, h)

                # 固定 8% 止损
                fixed_stop = entry_price * (1 - STOP_LOSS_PCT)

                # 浮盈 >= 12% 启动 2% 回撤移动止损
                gain_from_high = (high_since_entry - entry_price) / entry_price
                if gain_from_high >= TRAIL_TRIGGER:
                    trail_on = True

                trail_stop = None
                if trail_on:
                    trail_stop = high_since_entry * (1 - TRAIL_PCT)

                # 综合止损价：取“保护性更高”的那个
                if trail_stop is not None:
                    stop_price = max(fixed_stop, trail_stop)
                else:
                    stop_price = fixed_stop

                exit_price = None
                exit_reason = None

                # 低价触及止损价
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"

            elif direction == -1:
                # 空单：更新最低价
                if low_since_entry is None:
                    low_since_entry = l
                else:
                    low_since_entry = min(low_since_entry, l)

                fixed_stop = entry_price * (1 + STOP_LOSS_PCT)

                gain_from_low = (entry_price - low_since_entry) / entry_price
                if gain_from_low >= TRAIL_TRIGGER:
                    trail_on = True

                trail_stop = None
                if trail_on:
                    trail_stop = low_since_entry * (1 + TRAIL_PCT)

                if trail_stop is not None:
                    stop_price = min(fixed_stop, trail_stop)
                else:
                    stop_price = fixed_stop

                exit_price = None
                exit_reason = None

                # 高价触及止损价（对空单不利方向）
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"
            else:
                exit_price = None
                exit_reason = None

            # ===== 2.1 如果持仓方向和确认趋势方向反了，也要平仓 =====
            if in_pos and confirmed_trend != 0 and confirmed_trend != direction:
                # 反转平仓优先级低于止损：如果刚才已经触发止损，不重复
                if exit_price is None:
                    exit_price = c
                    exit_reason = "ema_flip_close"

            # ===== 2.2 执行平仓 =====
            if exit_price is not None:
                # 动态保证金：记录当时开仓时的 margin_used
                # 这里我们在 trade 里单独记录 margin_used，无需回溯
                # 所以在持仓时就必须记住 margin_used —— 放到状态里
                margin_used = current_margin

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

                # 清空持仓状态
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                high_since_entry = None
                low_since_entry = None
                stop_price = None
                trail_on = False

                # 如果已经爆到接近 0，直接结束
                if equity <= 0.01:
                    break

        # ===== 3. 无持仓 → 考虑开新仓 =====
        if (not in_pos) and equity > 0.01:
            # 只在有确认趋势、且非震荡区时开仓
            if confirmed_trend != 0 and (not is_choppy):
                # 动态保证金 = 资金的 50%
                margin_used = max(MIN_MARGIN, equity * MARGIN_FRACTION)
                if margin_used > equity:
                    margin_used = equity  # 理论上不会超过，但保险

                current_margin = margin_used  # 存到状态里用于平仓结算

                in_pos = True
                direction = confirmed_trend
                entry_price = c
                entry_time = dt
                high_since_entry = c
                low_since_entry = c
                stop_price = None
                trail_on = False

    return equity, trades, df


# ===== 结果汇总 =====
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
    ann_ret = total_ret  # 一年数据，年化≈总收益率

    print("========== 回测结果（原始策略 + EMA5根确认 + 8%止损 + 12%启用2%移动止损 + 50%动态仓位） ==========")
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
    equity, trades, df_used = backtest(df)
    summarize(equity, trades, df_used)
