#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import timedelta

# ========== 基本参数 ==========
CSV_PATH          = "okx_eth_15m.csv"   # K线数据
INITIAL_EQUITY    = 50.0                # 初始资金
LEVERAGE          = 5.0                 # 杠杆
FEE_RATE          = 0.0007              # 单边手续费率（0.07%）

# 仓位 = 资金比例
POSITION_EQUITY_RATIO = 0.5            # 每次用总资金的 50% 做保证金

# EMA / 趋势相关
EMA_FAST_PERIOD   = 34
EMA_SLOW_PERIOD   = 144
EMA_CONFIRM_BARS  = 3                  # 方向确认需要连续 3 根

# 震荡过滤（越小越严格）
CHOP_THRESHOLD_PCT = 0.0015            # 0.15% （|close-EMA|/close）

# ATR 动态止损参数
ATR_PERIOD           = 14
ATR_MULT_SL          = 2.5             # 止损 = 2.5 * ATR
ATR_MULT_TP_TRIGGER  = 3.0             # 浮盈 >= 3 * ATR → 启动追踪止盈
ATR_MULT_TRAIL_BACK  = 1.0             # 启动后回撤 1 * ATR 止盈

# 为防止浮点误差搞出负资金
MIN_EQUITY_TO_TRADE  = 0.5


# ========== 数据读取 ==========
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列：优先 iso，其次 ts，再其次第一列兜底
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


# ========== 指标计算：EMA & ATR & 震荡过滤 ==========
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low  = df["low"].astype(float)

    # EMA
    df["ema_fast"] = close.ewm(span=EMA_FAST_PERIOD, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW_PERIOD, adjust=False).mean()

    # ATR
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()

    # 震荡过滤：价格离两条 EMA 都非常近就认为是“震荡”
    dist_fast = (close - df["ema_fast"]).abs() / close
    dist_slow = (close - df["ema_slow"]).abs() / close
    df["is_chop"] = (dist_fast <= CHOP_THRESHOLD_PCT) & (dist_slow <= CHOP_THRESHOLD_PCT)

    df = df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)
    return df


def get_ema_trend_sign(row) -> int:
    """
    ema_fast > ema_slow → +1（多头）
    ema_fast < ema_slow → -1（空头）
    否则 0（纠缠 / 无方向）
    """
    if row["ema_fast"] > row["ema_slow"]:
        return 1
    elif row["ema_fast"] < row["ema_slow"]:
        return -1
    else:
        return 0


# ========== 回测主体 ==========
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0         # +1 多单，-1 空单
    entry_price = None
    entry_time = None
    margin_used = None
    size = None
    atr_at_entry = None
    best_price = None     # 多单：最高价；空单：最低价
    trail_active = False
    stop_price = None

    trades = []

    # 用来做 EMA 方向的连续确认
    ema_sign_hist = []

    for _, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])
        atr = float(row["atr"])
        is_chop = bool(row["is_chop"])

        # --- 更新 EMA 方向队列 ---
        ema_sign = get_ema_trend_sign(row)
        ema_sign_hist.append(ema_sign)
        if len(ema_sign_hist) > EMA_CONFIRM_BARS:
            ema_sign_hist.pop(0)

        confirmed_dir = 0
        if len(ema_sign_hist) == EMA_CONFIRM_BARS:
            if all(s == 1 for s in ema_sign_hist):
                confirmed_dir = 1
            elif all(s == -1 for s in ema_sign_hist):
                confirmed_dir = -1

        # ========== 先管理已有持仓 ==========
        if in_pos:
            # 更新 best_price（多：最高，空：最低）
            if direction == 1:
                best_price = h if best_price is None else max(best_price, h)
            else:  # direction == -1
                best_price = l if best_price is None else min(best_price, l)

            # 初始固定 ATR 止损
            if direction == 1:
                base_stop = entry_price - ATR_MULT_SL * atr_at_entry
            else:
                base_stop = entry_price + ATR_MULT_SL * atr_at_entry

            # 启动追踪止盈？
            if not trail_active:
                if direction == 1:
                    # 浮盈（价格） = best_price - entry_price
                    if best_price - entry_price >= ATR_MULT_TP_TRIGGER * atr_at_entry:
                        trail_active = True
                else:
                    if entry_price - best_price >= ATR_MULT_TP_TRIGGER * atr_at_entry:
                        trail_active = True

            # 计算当前追踪止损价
            if trail_active:
                if direction == 1:
                    trail_stop = best_price - ATR_MULT_TRAIL_BACK * atr_at_entry
                    stop_price = max(base_stop, trail_stop) if stop_price is None else max(stop_price, base_stop, trail_stop)
                else:
                    trail_stop = best_price + ATR_MULT_TRAIL_BACK * atr_at_entry
                    stop_price = min(base_stop, trail_stop) if stop_price is None else min(stop_price, base_stop, trail_stop)
            else:
                # 还没启用追踪，只用固定 ATR
                if stop_price is None:
                    stop_price = base_stop
                else:
                    # 多单不能把止损抬得比 base_stop 更宽
                    if direction == 1:
                        stop_price = max(stop_price, base_stop)
                    else:
                        stop_price = min(stop_price, base_stop)

            exit_price = None
            exit_reason = None

            # === 止损/追踪出场：用当根的 low/high 模拟打到价格 ===
            if direction == 1:
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"
            else:
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            # === EMA 反转确认强平（方向反了，且已确认） ===
            if exit_price is None and confirmed_dir != 0 and confirmed_dir != direction:
                exit_price = c
                exit_reason = "ema_flip_close"

            if exit_price is not None:
                # 结算
                notional = margin_used * LEVERAGE
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

                # 清空仓位
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                margin_used = None
                size = None
                atr_at_entry = None
                best_price = None
                trail_active = False
                stop_price = None

        # ========== 再考虑是否开新仓 ==========
        if equity <= MIN_EQUITY_TO_TRADE:
            continue  # 资金基本归零，停止开仓

        if (not in_pos) and confirmed_dir != 0 and (not is_chop):
            # 开仓方向 = EMA 确认方向
            direction = confirmed_dir

            # 仓位 = 当前资金的 50%
            margin_used = equity * POSITION_EQUITY_RATIO
            if margin_used <= 0:
                continue
            notional = margin_used * LEVERAGE
            size = notional / c

            entry_price = c
            entry_time = dt
            atr_at_entry = atr
            best_price = c
            trail_active = False
            stop_price = None

            in_pos = True

    return equity, trades


# ========== 结果统计输出 ==========
def summarize(df: pd.DataFrame, equity, trades):
    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}")
    print()
    print("========== 回测结果（原始策略 + ATR 动态止损/追踪） ==========")

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
    ann_ret = total_ret  # 一年数据约等于总收益率

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
