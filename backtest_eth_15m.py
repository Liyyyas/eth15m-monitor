#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
回测：原始策略 + EMA34/EMA144 反转延迟确认 + 震荡行情休息(不新开仓)

- 数据：okx_eth_15m.csv，一年 ETH 15m
- 策略：
  * 方向来自 EMA34 / EMA144（金叉做多，死叉做空）
  * 反转需要 “连续 N 根 K 方向一致” 才确认（延迟确认）
  * 确认反转时，平掉旧方向仓位（理由: ema_flip_close）
  * 固定止损约 5%（按价格），有两档追踪止盈（5% 和 10%）
  * 杠杆 5x，手续费单边 0.07%
  * 震荡判断：EMA34/144 距离很近且近10根基本走平 → 不开新仓
"""

import pandas as pd

# ===== 基本参数 =====
CSV_PATH         = "okx_eth_15m.csv"
INITIAL_EQUITY   = 50.0
MARGIN_PER_TRADE = 25.0        # 每笔最多用这么多保证金
LEVERAGE         = 5.0
FEE_RATE         = 0.0007      # 单边手续费率

STOP_LOSS_PCT    = 0.05        # 固定止损 5%

TRAIL_1_TRIGGER  = 0.05        # 浮盈 >= 5% 启动第一档跟踪
TRAIL_1_PCT      = 0.05        # 回撤 5% 止盈
TRAIL_2_TRIGGER  = 0.10        # 浮盈 >= 10% 启动第二档跟踪
TRAIL_2_PCT      = 0.02        # 回撤 2% 止盈

EMA_FAST         = 34
EMA_SLOW         = 144
CONFIRM_BARS     = 2           # 反转延迟确认需要连续 N 根

# 震荡过滤参数
FLAT_DIST_THRESH   = 0.003     # EMA34 / EMA144 相对距离 < 0.3%
FLAT_SLOPE_FAST    = 0.003     # EMA34 10根内变化 < 0.3%
FLAT_SLOPE_SLOW    = 0.002     # EMA144 10根内变化 < 0.2%
FLAT_LOOKBACK      = 10        # 看回10根K线


# ===== 读取 & 预处理 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列：优先 iso，其次 ts，最后第一列兜底
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        s = pd.to_numeric(df["ts"], errors="coerce")
        med = s.dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(s, unit=unit, utc=True, errors="coerce")
    else:
        first_col = df.columns[0]
        s = pd.to_numeric(df[first_col], errors="coerce")
        med = s.dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(s, unit=unit, utc=True, errors="coerce")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)

    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # 为震荡过滤准备历史 EMA
    df["ema_fast_prev"] = df["ema_fast"].shift(FLAT_LOOKBACK)
    df["ema_slow_prev"] = df["ema_slow"].shift(FLAT_LOOKBACK)

    df = df.dropna(subset=["ema_fast", "ema_slow", "ema_fast_prev", "ema_slow_prev"]).reset_index(drop=True)
    return df


def detect_flat(row) -> bool:
    """
    判断当前是否“震荡行情”，在震荡时禁止新开仓。
    条件：
    - EMA34 与 EMA144 距离很近（< FLAT_DIST_THRESH）
    - 且两条 EMA 在最近 FLAT_LOOKBACK 根内几乎没动
    """
    close = float(row["close"])
    ef = float(row["ema_fast"])
    es = float(row["ema_slow"])
    ef_prev = float(row["ema_fast_prev"])
    es_prev = float(row["ema_slow_prev"])

    dist = abs(ef - es) / close
    slope_fast = abs(ef - ef_prev) / ef if ef != 0 else 0.0
    slope_slow = abs(es - es_prev) / es if es != 0 else 0.0

    return (
        dist < FLAT_DIST_THRESH
        and slope_fast < FLAT_SLOPE_FAST
        and slope_slow < FLAT_SLOPE_SLOW
    )


# ===== 回测逻辑 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0      # 1 = 多, -1 = 空
    entry_price = None
    entry_time = None
    high_since = None
    low_since = None
    stop_price = None
    trail_mode = 0     # 0:固定止损; 1:5%回撤; 2:2%回撤
    margin_used = 0.0

    trades = []

    # EMA 趋势延迟确认状态
    last_raw_dir = 0       # 上一根的 “即时方向”
    pending_dir = 0        # 候选方向
    confirm_count = 0      # 已连续的根数
    confirmed_dir = 0      # 真正生效的方向（策略用它）

    rows = df.to_dict("records")

    for i, row in enumerate(rows):
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        ef = float(row["ema_fast"])
        es = float(row["ema_slow"])

        # ===== 1. 计算“即时方向”和延迟确认方向 =====
        if ef > es:
            raw_dir = 1
        elif ef < es:
            raw_dir = -1
        else:
            raw_dir = 0

        ema_flip_close = False
        ema_flip_dir = confirmed_dir

        # 延迟确认逻辑
        if raw_dir == 0:
            # EMA 重叠时，不改变 confirmed_dir，只重置 pending
            pending_dir = 0
            confirm_count = 0
        else:
            if raw_dir != last_raw_dir:
                # 方向刚刚发生变化 → 开始计数
                pending_dir = raw_dir
                confirm_count = 1
            else:
                # 连续同一方向，且有候选方向
                if pending_dir == raw_dir and pending_dir != confirmed_dir:
                    confirm_count += 1
                    if confirm_count >= CONFIRM_BARS:
                        # 反转确认生效
                        ema_flip_close = True  # 本根要按 close 价平仓
                        ema_flip_dir = pending_dir
                        confirmed_dir = pending_dir
                        # 重置 pending 状态
                        pending_dir = confirmed_dir
                        confirm_count = 0

        last_raw_dir = raw_dir

        # ===== 2. 管理持仓（先看平仓，再考虑开仓） =====
        if in_pos:
            # 更新最高/最低价
            if direction == 1:
                # 多头，关心最高价
                high_since = h if high_since is None else max(high_since, h)
                # 浮盈百分比（使用最高价）
                gain_pct = (high_since - entry_price) / entry_price
            else:
                # 空头，关心最低价
                low_since = l if low_since is None else min(low_since, l)
                gain_pct = (entry_price - low_since) / entry_price

            # 追踪止盈逻辑
            if trail_mode == 0 and gain_pct >= TRAIL_1_TRIGGER:
                trail_mode = 1
            if trail_mode == 1 and gain_pct >= TRAIL_2_TRIGGER:
                trail_mode = 2

            # 根据模式计算 “价格回撤止盈线”
            trail_stop = None
            if direction == 1:
                # 多头：回撤 = 从 high_since 往下
                if trail_mode == 1:
                    trail_stop = high_since * (1 - TRAIL_1_PCT)
                elif trail_mode == 2:
                    trail_stop = high_since * (1 - TRAIL_2_PCT)
            else:
                # 空头：回撤 = 从 low_since 往上
                if trail_mode == 1:
                    trail_stop = low_since * (1 + TRAIL_1_PCT)
                elif trail_mode == 2:
                    trail_stop = low_since * (1 + TRAIL_2_PCT)

            # 固定止损价
            if direction == 1:
                fixed_stop = entry_price * (1 - STOP_LOSS_PCT)
            else:
                fixed_stop = entry_price * (1 + STOP_LOSS_PCT)

            # 综合 stop_price（谁更保守用谁）
            if trail_stop is None:
                stop_price = fixed_stop
            else:
                if direction == 1:
                    # 多头：止损价不能比固定止损更低
                    stop_price = max(fixed_stop, trail_stop)
                else:
                    # 空头：止损价不能比固定止损更高
                    stop_price = min(fixed_stop, trail_stop)

            exit_price = None
            exit_reason = None

            # 先看 SL / 追踪
            if direction == 1:
                # 多头：低价打到或跌破止损线
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"
            else:
                # 空头：高价打到或穿过止损线
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"

            # 再看 EMA 延迟反转平仓（如果还没被止损）
            if exit_price is None and ema_flip_close and confirmed_dir != direction and confirmed_dir != 0:
                exit_price = c
                exit_reason = "ema_flip_close"

            # 平仓结算
            if exit_price is not None:
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
                    "pnl_pct_on_margin": pnl_net / margin_used if margin_used > 0 else 0,
                    "equity_after": equity,
                })

                # 清空持仓状态
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                high_since = None
                low_since = None
                stop_price = None
                trail_mode = 0
                margin_used = 0.0

        # ===== 3. 开仓（只在空仓 & 不震荡 & 有确认方向 时） =====
        # 震荡过滤：只影响“开新仓”，不影响已有仓位的管理
        flat = detect_flat(row)

        if (not in_pos) and (equity > 0):
            if (not flat) and (confirmed_dir != 0):
                # 用“尽量接近原始策略”的设定：
                # 保留 MARGIN_PER_TRADE 上限，但允许资金不足时用剩余资金继续玩到归零
                margin_used = min(MARGIN_PER_TRADE, equity)
                if margin_used < 1e-6:
                    # 彻底玩完
                    break

                in_pos = True
                direction = confirmed_dir
                entry_price = c
                entry_time = dt
                high_since = c
                low_since = c
                trail_mode = 0
                stop_price = None  # 进场后下一根再计算

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

    print("========== 回测结果（原始策略 + EMA反转延迟确认 + 震荡过滤） ==========")
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
