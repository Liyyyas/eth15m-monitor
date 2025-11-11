#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

# ========== 基本配置 ==========
CSV_PATH = "okx_eth_15m.csv"   # 你的 15m ETH K线

INITIAL_EQUITY = 50.0          # 初始资金
POSITION_PCT   = 0.5           # 每次用总资金的 50% 做保证金
LEVERAGE       = 5.0           # 杠杆
FEE_RATE       = 0.0007        # 单边手续费率

# EMA & ATR 参数
EMA_FAST = 34
EMA_SLOW = 144

ATR_PERIOD      = 21
ATR_STOP_MULT   = 3.0          # ATR 止损倍数

# 浮盈启用移动止盈：6% → 3% 回撤
TRAIL_TRIGGER_PCT = 0.06       # 浮盈达到 6% 启动
TRAIL_BACK_PCT    = 0.03       # 从最高价/最低价回撤 3% 止盈

# ========== 读取 K 线数据 ==========
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列：优先 iso，其次 ts，最后第一列兜底
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

# ========== 计算 15m EMA & ATR ==========
def add_intraday_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    high  = df["high"].astype(float)
    low   = df["low"].astype(float)

    # 15m EMA
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # True Range & ATR
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df["atr"] = tr.ewm(span=ATR_PERIOD, adjust=False).mean()

    return df

# ========== 生成 1H 级别 EMA，并合并回 15m ==========
def add_htf_trend(df: pd.DataFrame) -> pd.DataFrame:
    """
    用 15m 数据重采样出 1H 收盘价，算 1H 级别的 EMA34/144，
    然后 merge_asof 回 15m K 线，得到 htf_trend 列：
      +1 = 1H 多头；-1 = 1H 空头；0 = 不确定
    """
    # 用 dt 做 index 方便 resample
    df_15m = df.copy()
    df_15m = df_15m.sort_values("dt").reset_index(drop=True)

    htf = (
        df_15m[["dt", "close"]]
        .set_index("dt")
        .resample("1H")
        .last()
        .dropna()
        .reset_index()
    )

    # 1H EMA
    htf["ema34_1h"]  = htf["close"].ewm(span=EMA_FAST, adjust=False).mean()
    htf["ema144_1h"] = htf["close"].ewm(span=EMA_SLOW, adjust=False).mean()

    # 合并回 15m：用“向后匹配”(backward)拿到当前 bar 所在小时的 EMA
    merged = pd.merge_asof(
        df_15m.sort_values("dt"),
        htf[["dt", "ema34_1h", "ema144_1h"]].sort_values("dt"),
        on="dt",
        direction="backward"
    )

    # 计算 15m 与 1h 的趋势方向
    merged["trend_15m"] = 0
    merged.loc[merged["ema_fast"] > merged["ema_slow"], "trend_15m"] = 1
    merged.loc[merged["ema_fast"] < merged["ema_slow"], "trend_15m"] = -1

    merged["htf_trend"] = 0
    merged.loc[merged["ema34_1h"] > merged["ema144_1h"], "htf_trend"] = 1
    merged.loc[merged["ema34_1h"] < merged["ema144_1h"], "htf_trend"] = -1

    return merged

# ========== 回测主体：原始策略 + ATR + 6%→3% + 多周期确认 ==========
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0     # +1 多，-1 空
    entry_price = None
    entry_time  = None
    margin_used = 0.0

    best_price  = None            # 多单记录最高价；空单记录最低价
    trail_active = False          # 是否已经触发 6%→3% 移动止盈

    trades = []

    for _, row in df.iterrows():
        dt = row["dt"]
        o  = float(row["open"])
        h  = float(row["high"])
        l  = float(row["low"])
        c  = float(row["close"])
        atr = float(row["atr"]) if not np.isnan(row["atr"]) else None

        trend_15m = int(row["trend_15m"])
        htf_trend = int(row["htf_trend"])

        # ========== 先管理已有持仓 ==========
        if in_pos:
            # 更新 best_price（对我们有利的极值）
            if best_price is None:
                best_price = c
            else:
                if direction == 1:   # 多单，记最高价
                    best_price = max(best_price, h)
                elif direction == -1:  # 空单，记最低价
                    best_price = min(best_price, l)

            # 计算当前浮盈百分比（基于 best_price）
            gain_pct = direction * (best_price - entry_price) / entry_price

            # 满足 6% 浮盈 → 启用 3% 回撤
            if (not trail_active) and gain_pct >= TRAIL_TRIGGER_PCT:
                trail_active = True

            # ATR 止损价（原始策略里的动态止损）
            atr_stop_price = None
            if atr is not None and atr > 0:
                if direction == 1:      # 多
                    atr_stop_price = entry_price - ATR_STOP_MULT * atr
                elif direction == -1:   # 空
                    atr_stop_price = entry_price + ATR_STOP_MULT * atr

            # 6%→3% 移动止盈的止损价
            trail_stop_price = None
            if trail_active and best_price is not None:
                if direction == 1:      # 多：从最高价回撤 3%
                    trail_stop_price = best_price * (1 - TRAIL_BACK_PCT)
                elif direction == -1:   # 空：从最低价向上回撤 3%
                    trail_stop_price = best_price * (1 + TRAIL_BACK_PCT)

            # 综合止损价：取“更紧”的那个
            stop_price = None
            if direction == 1:
                candidates = []
                if atr_stop_price is not None:
                    candidates.append(atr_stop_price)
                if trail_stop_price is not None:
                    candidates.append(trail_stop_price)
                if candidates:
                    # 多单，保护方向往下 → 取价格更高的那个（离当前价更近）
                    stop_price = max(candidates)
            elif direction == -1:
                candidates = []
                if atr_stop_price is not None:
                    candidates.append(atr_stop_price)
                if trail_stop_price is not None:
                    candidates.append(trail_stop_price)
                if candidates:
                    # 空单，保护方向往上 → 取价格更低的那个（离当前价更近）
                    stop_price = min(candidates)

            exit_price = None
            exit_reason = None

            # 1) 止损 / 移动止盈触发
            if stop_price is not None:
                if direction == 1 and l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"
                elif direction == -1 and h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            # 2) EMA 反转（15m 级别）→ 直接平仓，不再强制立即反向开仓
            if exit_price is None:
                if trend_15m != 0 and trend_15m * direction < 0:
                    # 趋势真正反转才平仓
                    exit_price = c
                    exit_reason = "ema_flip_close"

            # 如果本 bar 确定平仓
            if exit_price is not None:
                notional = margin_used * LEVERAGE
                size = notional / entry_price

                # 多单 PnL = (exit - entry)*size
                # 空单 PnL = (entry - exit)*size
                gross_pnl = direction * (exit_price - entry_price) * size

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

                # 清空仓位状态
                in_pos = False
                direction = 0
                entry_price = None
                entry_time  = None
                margin_used = 0.0
                best_price  = None
                trail_active = False

        # ========== 没有持仓 → 看是否可以新开仓 ==========
        if (not in_pos) and equity > 0:
            # 只有当 15m 与 1h 趋势同向时才允许开仓
            if trend_15m != 0 and trend_15m == htf_trend:
                new_direction = trend_15m  # +1 多，-1 空

                # 动态仓位：用当前资金的指定比例
                margin_used = equity * POSITION_PCT
                margin_used = min(margin_used, equity)  # 不要超过总资金
                if margin_used <= 0:
                    continue

                in_pos = True
                direction = new_direction
                entry_price = c
                entry_time  = dt
                best_price  = c
                trail_active = False

                # 开仓时扣手续费（按开仓名义价值）
                notional = margin_used * LEVERAGE
                open_fee = notional * FEE_RATE
                equity -= open_fee
                # 注意：这里我们不改变 margin_used，手续费视为额外成本

    return equity, trades

# ========== 结果统计 ==========
def summarize(df: pd.DataFrame, equity: float, trades):
    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}")
    print()

    n = len(trades)
    wins = sum(1 for t in trades if t["pnl_net"] > 0)
    losses = sum(1 for t in trades if t["pnl_net"] < 0)
    flats = n - wins - losses

    total_pnl = sum(t["pnl_net"] for t in trades)
    avg_win = (sum(t["pnl_net"] for t in trades if t["pnl_net"] > 0) / wins) if wins > 0 else 0.0
    avg_loss = (sum(t["pnl_net"] for t in trades if t["pnl_net"] < 0) / losses) if losses > 0 else 0.0

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
    ann_ret = total_ret  # 一年数据 → 年化近似总收益率

    print("========== 回测结果（新基础版 + 多周期趋势确认） ==========")
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
    df = add_intraday_indicators(df)
    df = add_htf_trend(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
