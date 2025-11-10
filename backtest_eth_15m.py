#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from math import copysign

# ========= 配置部分 =========

CSV_PATH        = "okx_eth_15m.csv"  # 你的 ETH 15m 数据
INITIAL_EQUITY  = 50.0               # 初始资金
LEVERAGE        = 5.0                # 杠杆
FEE_RATE        = 0.0007             # 单边手续费率

POSITION_PCT    = 0.5                # 使用当前资金的 50% 做保证金
MIN_MARGIN      = 5.0                # 资金很少时，至少这点保证金（防止过早停摆）

EMA_FAST        = 34
EMA_SLOW        = 144
CONFIRM_BARS    = 3                  # 原来 5 根 → 现在 3 根确认
VOL_FILTER      = 0.0005             # 震荡过滤：|ema34-ema144|/close < 0.0005 = 0.05% 视为震荡

STOP_LOSS_PCT   = 0.08               # 固定止损 8%
TRAIL_TRIGGER   = 0.12               # 浮盈 ≥12% 启动移动止盈
TRAIL_PCT       = 0.02               # 移动止盈回撤 2%


# ========= 数据处理 =========

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列：优先 iso，其次 ts，其次第一列
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


def add_ema(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()
    df = df.dropna(subset=["ema_fast", "ema_slow"]).reset_index(drop=True)
    return df


# ========= 趋势判定 & 震荡过滤 =========

def get_trend_dir(row) -> int:
    """
    返回:
      +1 多头趋势
      -1 空头趋势
       0 震荡 / 无趋势（EMA 太接近）
    """
    ef = float(row["ema_fast"])
    es = float(row["ema_slow"])
    c  = float(row["close"])

    gap_ratio = abs(ef - es) / c

    # 震荡过滤：差距 < VOL_FILTER 视为无趋势
    if gap_ratio < VOL_FILTER:
        return 0

    if ef > es:
        return 1
    elif ef < es:
        return -1
    else:
        return 0


# ========= 回测主逻辑 =========

def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0  # +1 多，-1 空
    entry_price = None
    entry_time = None
    margin_used = 0.0

    best_price = None        # 多：最高价 / 空：最低价（用于移动止盈）
    trail_active = False     # 是否已触发移动止盈

    last_trend_dirs = []     # 最近 N 根 K 的趋势方向（非 0）

    trades = []

    for _, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        trend_dir = get_trend_dir(row)

        # 维护最近的非零趋势方向，用于 N 根确认
        if trend_dir != 0:
            last_trend_dirs.append(trend_dir)
            if len(last_trend_dirs) > CONFIRM_BARS:
                last_trend_dirs.pop(0)
        else:
            # 震荡区间：趋势视为不连续，中断确认
            last_trend_dirs.clear()

        confirmed_trend = 0
        if len(last_trend_dirs) == CONFIRM_BARS and len(set(last_trend_dirs)) == 1:
            confirmed_trend = last_trend_dirs[0]

        # ===== 管理已有仓位：止损 / 移动止盈 =====
        if in_pos:
            if direction == 1:
                # 多单：更新最高价
                best_price = max(best_price, h) if best_price is not None else h

                # 固定止损价
                stop_fix = entry_price * (1 - STOP_LOSS_PCT)

                # 浮盈比例（基于 best_price）
                gain_pct = (best_price - entry_price) / entry_price

                # 启动移动止盈
                if (not trail_active) and gain_pct >= TRAIL_TRIGGER:
                    trail_active = True

                stop_trail = None
                if trail_active:
                    stop_trail = best_price * (1 - TRAIL_PCT)

                # 最终止损价 = max(固定止损, 移动止损)
                stop_price = stop_fix
                if stop_trail is not None:
                    stop_price = max(stop_price, stop_trail)

                exit_price = None
                exit_reason = None

                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"

            else:
                # 空单：更新最低价
                best_price = min(best_price, l) if best_price is not None else l

                stop_fix = entry_price * (1 + STOP_LOSS_PCT)
                gain_pct = (entry_price - best_price) / entry_price

                if (not trail_active) and gain_pct >= TRAIL_TRIGGER:
                    trail_active = True

                stop_trail = None
                if trail_active:
                    stop_trail = best_price * (1 + TRAIL_PCT)

                stop_price = stop_fix
                if stop_trail is not None:
                    # 对空单来说：价格越低越有利，止损应该是“往下移动上界”
                    stop_price = min(stop_price, stop_trail)

                exit_price = None
                exit_reason = None

                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "stop_or_trail"

            # ===== EMA 反转：如果出现相反趋势确认，平仓（是否反向开仓另外处理） =====
            # 这里不管有没有 trail/stop 打到，只要出现 confirmed_trend 与当前方向相反，就按收盘价强制平仓
            if confirmed_trend != 0 and confirmed_trend != direction:
                exit_price = c
                exit_reason = "ema_flip_close"

            if "exit_price" in locals() and exit_price is not None:
                # 平仓结算
                size = margin_used * LEVERAGE / entry_price
                if direction == 1:
                    gross_pnl = (exit_price - entry_price) * size
                else:
                    gross_pnl = (entry_price - exit_price) * size

                fee_open = margin_used * LEVERAGE * FEE_RATE
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
                    "equity_after": equity
                })

                # 清空仓位状态
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                margin_used = 0.0
                best_price = None
                trail_active = False

                # 确保 exit_price 局部变量不干扰下一根
                exit_price = None
                exit_reason = None

        # ===== 无仓位 / 刚平仓 → 看是否根据趋势开新仓 =====
        if (not in_pos) and confirmed_trend != 0 and equity > 0:
            # 震荡过滤已经在 get_trend_dir 里做了，这里 confirmed_trend != 0 就说明：
            # - EMA 有足够斜率（> VOL_FILTER）
            # - 且最近 CONFIRM_BARS 根都同方向
            direction = confirmed_trend

            # 动态保证金 = 当前资金的 50%，但至少 MIN_MARGIN，最多不超过 equity
            margin_used = max(MIN_MARGIN, equity * POSITION_PCT)
            margin_used = min(margin_used, equity)

            if margin_used <= 0:
                continue

            entry_price = c
            entry_time = dt
            in_pos = True

            # 初始化最有利价
            best_price = entry_price
            trail_active = False

    return equity, trades


# ========= 结果统计 =========

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
        drawdown = (x - peak) / peak
        if drawdown < max_dd:
            max_dd = drawdown

    total_ret = (equity - INITIAL_EQUITY) / INITIAL_EQUITY
    ann_ret = total_ret  # 一年数据，年化≈总收益率

    print("========== 回测结果（原始策略 + EMA3根确认 + 8%止损 + 12%启用2%移动止损 + 50%动态仓位 + 0.05%震荡过滤） ==========")
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
    final_equity, trades = backtest(df)
    summarize(df, final_equity, trades)
