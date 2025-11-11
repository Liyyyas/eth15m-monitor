#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

# ========== 参数配置 ==========
CSV_PATH = "okx_eth_15m.csv"   # 你的 15m ETH CSV

INITIAL_EQUITY = 50.0
LEVERAGE       = 5.0
FEE_RATE       = 0.0007        # 单边手续费 0.07%
RISK_FRACTION  = 0.5           # 每次用总资金的 50% 做保证金

ATR_LEN        = 34            # ATR 周期
ATR_MEAN_LEN   = 34            # ATR 均值用同一个周期
ATR_BASE_MULT  = 3.5           # 原来固定 3.5 的基础倍数（现在改为动态）

# 动态 ATR 倍数阈值
ATR_HIGH_TH    = 1.2           # ATR > mean * 1.2 → 高波动
ATR_LOW_TH     = 0.8           # ATR < mean * 0.8 → 低波动
ATR_MULT_HIGH  = 4.0           # 高波动 → 倍数 4.0 （止损更宽）
ATR_MULT_LOW   = 3.0           # 低波动 → 倍数 3.0 （止损更紧）

# 6% 启动 3% 回撤移动止盈
TRAIL_TRIGGER  = 0.06          # 浮盈 >= 6% 启动
TRAIL_BACK     = 0.03          # 从最高/最低价回撤 3% 止盈

EMA_FAST       = 34
EMA_SLOW       = 144


# ========== 工具函数 ==========
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 尝试识别时间列
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


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    high  = df["high"].astype(float)
    low   = df["low"].astype(float)

    # EMA34 / EMA144
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR(34)
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df["atr"] = tr.rolling(ATR_LEN, min_periods=ATR_LEN).mean()

    # ATR 均值（同周期）
    df["atr_mean"] = df["atr"].rolling(ATR_MEAN_LEN, min_periods=ATR_MEAN_LEN).mean()

    # 动态 ATR 倍数
    def _get_mult(row):
        a = row["atr"]
        m = row["atr_mean"]
        if np.isnan(a) or np.isnan(m):
            return np.nan
        if a > m * ATR_HIGH_TH:
            return ATR_MULT_HIGH
        elif a < m * ATR_LOW_TH:
            return ATR_MULT_LOW
        else:
            return ATR_BASE_MULT

    df["atr_mult"] = df.apply(_get_mult, axis=1)

    # 丢掉指标还没算好的部分
    df = df.dropna(subset=["ema_fast", "ema_slow", "atr", "atr_mean", "atr_mult"]).reset_index(drop=True)
    return df


# ========== 回测主体：新基础版 + 动态 ATR 倍数 ==========
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0        # 1 = 多，-1 = 空
    entry_price = None
    entry_time  = None
    margin_used = None
    size        = None   # 带方向的仓位（数量）
    best_price  = None   # 多单记录最高价，空单记录最低价
    trail_active = False

    trades = []

    for i, row in df.iterrows():
        dt = row["dt"]
        o  = float(row["open"])
        h  = float(row["high"])
        l  = float(row["low"])
        c  = float(row["close"])
        ema_f = row["ema_fast"]
        ema_s = row["ema_slow"]
        atr   = row["atr"]
        atr_mean = row["atr_mean"]
        atr_mult = row["atr_mult"]

        # 还没 ATR 或 EMA 就跳过
        if np.isnan(ema_f) or np.isnan(ema_s) or np.isnan(atr) or np.isnan(atr_mean) or np.isnan(atr_mult):
            continue

        # ========= 有持仓：先管理止损 / 移动止盈 =========
        if in_pos:
            # 更新 best_price
            if direction == 1:
                # 多单：看最高价
                best_price = max(best_price, h)
                # 浮盈百分比（相对 entry_price）
                gain_pct = (best_price - entry_price) / entry_price
            else:
                # 空单：看最低价
                best_price = min(best_price, l)
                gain_pct = (entry_price - best_price) / entry_price

            # 6% 浮盈 → 启动 3% 回撤移动止盈
            if (not trail_active) and gain_pct >= TRAIL_TRIGGER:
                trail_active = True

            # 计算 ATR 止损价（动态倍数）
            if direction == 1:
                atr_stop = entry_price - atr * atr_mult
            else:
                atr_stop = entry_price + atr * atr_mult

            # 计算移动止盈（如果已激活）
            trail_stop = None
            if trail_active:
                if direction == 1:
                    # 多单：从最高价回撤 3%
                    trail_stop = best_price * (1 - TRAIL_BACK)
                else:
                    # 空单：从最低价向上回撤 3%
                    trail_stop = best_price * (1 + TRAIL_BACK)

            # 合成最终止损价：取对自己更“紧”的那个
            if direction == 1:
                # 多单：止损价越高越紧
                if trail_stop is not None:
                    stop_price = max(atr_stop, trail_stop)
                else:
                    stop_price = atr_stop

                exit_price = None
                exit_reason = None

                # 低点碰到 stop 就视为触发
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            else:
                # 空单：止损价越低越紧（因为价格往上是亏）
                if trail_stop is not None:
                    stop_price = min(atr_stop, trail_stop)
                else:
                    stop_price = atr_stop

                exit_price = None
                exit_reason = None

                # 高点碰到 stop 就视为触发
                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            # 如果触发了离场
            if exit_price is not None:
                notional = margin_used * LEVERAGE
                # size 已经包含方向
                gross_pnl = (exit_price - entry_price) * size

                fee_open  = notional * FEE_RATE
                fee_close = abs(exit_price * size) * FEE_RATE
                pnl_net   = gross_pnl - fee_open - fee_close

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
                    "pnl_pct_on_margin": pnl_net / margin_used,
                    "equity_after": equity,
                })

                # 清空仓位状态
                in_pos = False
                direction = 0
                entry_price = None
                entry_time  = None
                margin_used = None
                size        = None
                best_price  = None
                trail_active = False

        # ========= 空仓：按“新基础版”规则开仓 =========
        if (not in_pos) and equity > 0:
            # 每次用 50% 资金做保证金
            margin = equity * RISK_FRACTION
            if margin <= 0:
                continue

            # 根据 EMA34 / EMA144 决定方向
            if ema_f > ema_s:
                new_direction = 1   # 多
            elif ema_f < ema_s:
                new_direction = -1  # 空
            else:
                new_direction = 0

            if new_direction != 0:
                entry_price = c
                entry_time  = dt
                direction   = new_direction
                margin_used = margin

                notional = margin_used * LEVERAGE
                size = notional / entry_price * direction

                # 初始化 best_price
                best_price = entry_price
                trail_active = False
                in_pos = True

    return equity, trades


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

    avg_win  = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
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
    ann_ret   = total_ret  # 一年期，就不再折算

    print("========== 回测结果（新基础版 + 动态 ATR 倍数 + 6%→3%追踪） ==========")
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
