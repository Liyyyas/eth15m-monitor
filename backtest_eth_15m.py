#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ===== 基本配置 =====
CSV_PATH          = "okx_eth_15m.csv"   # 你的 ETH 15m 数据
INITIAL_EQUITY    = 50.0                # 初始资金
MARGIN_PER_TRADE  = 25.0                # 每笔保证金（资金不足就不开）
LEVERAGE          = 2.0                 # 杠杆（从 5x 降到 2x）
FEE_RATE          = 0.0007              # 单边手续费率（按交易所自己改）

EMA_FAST          = 34
EMA_SLOW          = 144

ATR_PERIOD        = 34                  # ATR 周期（更平滑）
ATR_MULT          = 2.5                 # 止损宽度：ATR * 2.5

TRAIL_TRIGGER_PCT = 0.03                # 浮盈 ≥ 3% 启动追踪
TRAIL_GAP_PCT     = 0.015               # 追踪间距 1.5%


# ===== 工具函数：加载 15m 数据并转为 4h =====
def load_15m_to_4h(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列识别：优先 iso，其次 ts，最后第一列兜底
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

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    df = df.set_index("dt")
    # 15m → 4H 聚合
    df_4h = df.resample("4H").agg({
        "open":  "first",
        "high":  "max",
        "low":   "min",
        "close": "last",
    }).dropna().reset_index()

    return df_4h


# ===== 指标计算：EMA + ATR =====
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR(34)
    high = df["high"]
    low = df["low"]
    prev_close = close.shift(1)

    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df["atr"] = tr.rolling(ATR_PERIOD).mean()

    # 丢掉指标未就绪的起始部分
    df = df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)
    return df


# ===== 回测主体（4 小时版） =====
def backtest_4h(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0      # 1 多，-1 空
    entry_price = None
    entry_time = None
    best_price = None  # 多单最高价 / 空单最低价
    stop_price = None
    entry_idx = None

    trades = []

    for idx, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        ema_f = float(row["ema_fast"])
        ema_s = float(row["ema_slow"])
        atr = float(row["atr"])

        # 指标不正常就跳过
        if not pd.notna(atr) or not pd.notna(ema_f) or not pd.notna(ema_s):
            continue

        # ========= 有持仓：先管理止损 / 追踪 =========
        if in_pos:
            # 更新最佳价格：多单看高，空单看低
            if direction == 1:
                best_price = max(best_price, h)
            else:
                best_price = min(best_price, l)

            # 计算浮盈百分比
            if direction == 1:
                profit_pct = (best_price - entry_price) / entry_price
                base_sl = entry_price - ATR_MULT * atr
            else:
                profit_pct = (entry_price - best_price) / entry_price
                base_sl = entry_price + ATR_MULT * atr

            # 追踪止损逻辑
            if profit_pct >= TRAIL_TRIGGER_PCT:
                if direction == 1:
                    trail_stop = best_price * (1 - TRAIL_GAP_PCT)
                    stop_price = max(base_sl, trail_stop)
                else:
                    trail_stop = best_price * (1 + TRAIL_GAP_PCT)
                    stop_price = min(base_sl, trail_stop)
            else:
                stop_price = base_sl

            exit_price = None
            exit_reason = None

            # 先看价格是否打到止损/追踪
            if direction == 1 and l <= stop_price:
                exit_price = stop_price
                exit_reason = "stop_or_trail"
            elif direction == -1 and h >= stop_price:
                exit_price = stop_price
                exit_reason = "stop_or_trail"

            # 再看 EMA 反转，方向完全反了也离场
            if exit_price is None:
                if direction == 1 and ema_f < ema_s:
                    exit_price = c
                    exit_reason = "ema_flip_close"
                elif direction == -1 and ema_f > ema_s:
                    exit_price = c
                    exit_reason = "ema_flip_close"

            if exit_price is not None:
                # 结算
                margin = min(MARGIN_PER_TRADE, equity)
                if margin <= 0:
                    # 理论上不该出现，但保险
                    in_pos = False
                    direction = 0
                    entry_price = None
                    entry_time = None
                    best_price = None
                    stop_price = None
                    entry_idx = None
                    continue

                notional = margin * LEVERAGE
                size = notional / entry_price * direction

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
                    "direction": direction,
                    "margin_used": margin,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / margin,
                    "equity_after": equity,
                    "bars_held": idx - entry_idx,
                })

                # 清空仓位
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                best_price = None
                stop_price = None
                entry_idx = None

        # ========= 无持仓：看是否可以开仓 =========
        if (not in_pos) and equity > MARGIN_PER_TRADE:
            # 方向判定：简单 EMA 趋势
            new_direction = 0
            if c > ema_f > ema_s:
                new_direction = 1
            elif c < ema_f < ema_s:
                new_direction = -1

            if new_direction != 0:
                in_pos = True
                direction = new_direction
                entry_price = c
                entry_time = dt
                best_price = c
                # 初始止损
                if direction == 1:
                    stop_price = entry_price - ATR_MULT * atr
                else:
                    stop_price = entry_price + ATR_MULT * atr
                entry_idx = idx

    return equity, trades


# ===== 结果汇总 =====
def summarize(df_4h: pd.DataFrame, equity, trades):
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
    ann_ret = total_ret  # 一年数据近似当年化

    print("========== 回测结果（4 小时版·A 路线参数） ==========")
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


if __name__ == "__main__":
    df_4h = load_15m_to_4h(CSV_PATH)
    df_4h = add_indicators(df_4h)
    equity, trades = backtest_4h(df_4h)
    summarize(df_4h, equity, trades)
