#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETH 15m 回测脚本（新基础版 + 五项进阶强化）

基础框架 = 你之前胜率 ~51.04% 的版本：
- 15m ETH K线
- EMA34 / EMA144 判断方向（多 / 空）
- ATR(34) 止损
- 浮盈触发移动止盈
- 5x 杠杆

本版本在此基础上叠加 5 个增强：
1）趋势扩展：浮盈 >= 8% 启动 2% 回撤移动止盈（替代原 6%→3%）
2）自适应 ATR 止损：上笔赢 → ATR*3.8；上笔亏 → ATR*2.5；首笔 ATR*3.5
3）动量过滤：RSI(14) 多单要求 RSI>55，空单要求 RSI<45，中间视为震荡不入场
4）分级止盈：浮盈 >=10% 时先平掉 50% 仓位，其余继续跑移动止盈
5）分段加仓：初始用 50% 仓；浮盈 >=5% 时加到 75% 仓；若从峰值回撤 >=3%，全平
"""

import pandas as pd
import numpy as np

# ===== 参数区 =====
CSV_PATH = "okx_eth_15m.csv"     # 你的 ETH 15m 数据
INITIAL_EQUITY = 50.0
LEVERAGE = 5.0
FEE_RATE = 0.0007                # 单边手续费 0.07%，你可按交易所修改

EMA_FAST = 34
EMA_SLOW = 144
ATR_PERIOD = 34
RSI_PERIOD = 14

# ATR 倍数（自适应）
ATR_MULT_BASE = 3.5
ATR_MULT_WIN = 3.8
ATR_MULT_LOSS = 2.5

# 浮盈触发移动止盈
TRAIL_TRIGGER = 0.08     # 浮盈 >=8%
TRAIL_BACK = 0.02        # 回撤 2%

# 分级止盈
PARTIAL_TP_TRIGGER = 0.10   # 浮盈 >=10% 先平一半
PARTIAL_TP_FRACTION = 0.5   # 平掉 50%

# 分段加仓
SCALE_IN_TRIGGER = 0.05     # 浮盈 >=5% 时加仓
SCALE_IN_EQUITY_RATIO = 0.75  # 加仓后总仓位 = 75% equity（初始为 50%）

# 回撤强制平仓（只对加仓后的仓位）
SCALE_IN_DRAWDOWN_EXIT = 0.03  # 从峰值回撤 3% 就全平


# ===== 技术指标计算 =====

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 处理时间列（优先 iso，其次 ts，否则默认第一列作时间戳）
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    else:
        first = df.columns[0]
        med = pd.to_numeric(df[first], errors="coerce").dropna().median()
        unit = "ms" if med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    # 必须有 OHLC
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    # EMA
    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR(34)
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(ATR_PERIOD, min_periods=ATR_PERIOD).mean()

    # RSI(14)（Wilder）
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    roll_up = gain.ewm(alpha=1.0 / RSI_PERIOD, adjust=False).mean()
    roll_down = loss.ewm(alpha=1.0 / RSI_PERIOD, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    df["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    # 去掉指标没准备好的前面一截
    df = df.dropna(subset=["ema_fast", "ema_slow", "atr", "rsi"]).reset_index(drop=True)
    return df


# ===== 辅助函数 =====

def get_trend_direction(row) -> int:
    """根据 EMA34/144 判定方向：1=多头；-1=空头；0=无趋势"""
    if row["ema_fast"] > row["ema_slow"]:
        return 1
    elif row["ema_fast"] < row["ema_slow"]:
        return -1
    else:
        return 0


def pass_rsi_filter(direction: int, rsi_val: float) -> bool:
    """RSI 动量过滤：多单 RSI>55；空单 RSI<45；否则不做单"""
    if direction == 1:
        return rsi_val > 55.0
    elif direction == -1:
        return rsi_val < 45.0
    else:
        return False


# ===== 主回测逻辑 =====

def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0          # 1=多，-1=空
    entry_price = None
    size = 0.0             # 持仓币数
    margin_used = 0.0      # 当前保证金（不从 equity 扣，只作风险量化）

    peak_price = None      # 多单最高价 / 空单最低价
    scaled_in = False      # 是否加仓过
    partial_taken = False  # 是否已经部分止盈

    last_trade_win = None  # 上一笔是否盈利，用于自适应 ATR 倍数

    trades = []

    for idx, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        atr = float(row["atr"])
        rsi = float(row["rsi"])

        trend = get_trend_direction(row)

        # 选 ATR 倍数（自适应）
        if last_trade_win is None:
            atr_mult = ATR_MULT_BASE
        elif last_trade_win:
            atr_mult = ATR_MULT_WIN
        else:
            atr_mult = ATR_MULT_LOSS

        # ===== 有持仓，先管理仓位 =====
        if in_pos and size > 0:
            # 更新浮盈峰值
            if direction == 1:
                peak_price = h if peak_price is None else max(peak_price, h)
            else:
                peak_price = l if peak_price is None else min(peak_price, l)

            # 当前仓位的“理论入场成本”
            notional = margin_used * LEVERAGE
            # 这里 entry_price 是加权平均价，size = notional / entry_price -> 一致
            # 未实现盈亏
            current_pnl = (c - entry_price) * direction * size
            current_pnl_pct_on_margin = current_pnl / (margin_used + 1e-12)

            # ======================
            # ① 分段加仓：浮盈 >=5% 时，总仓位提高到 75% equity
            # ======================
            if (not scaled_in) and current_pnl_pct_on_margin >= SCALE_IN_TRIGGER and equity > 0:
                target_margin = equity * SCALE_IN_EQUITY_RATIO
                if target_margin > margin_used * 1.01:  # 至少增加一点点
                    add_margin = target_margin - margin_used
                    add_notional = add_margin * LEVERAGE
                    add_size = add_notional / c

                    # 更新加权平均价和总 size / margin
                    new_notional_total = notional + add_notional
                    new_size_total = size + add_size
                    new_entry_price = new_notional_total / new_size_total

                    # 不收多一次开仓手续费（若想更严苛，可以在此扣 fee_open）
                    margin_used = target_margin
                    size = new_size_total
                    entry_price = new_entry_price

                    scaled_in = True
                    # 加仓后，高点 / 低点重新从当前价格计也可以，这里保留原 peak_price

            # 浮盈比例（基于峰值）
            if direction == 1:
                if peak_price is None:
                    gain_from_entry = 0.0
                    dd_from_peak = 0.0
                else:
                    gain_from_entry = (peak_price - entry_price) / entry_price
                    dd_from_peak = (peak_price - c) / peak_price if peak_price > 0 else 0.0
            else:
                if peak_price is None:
                    gain_from_entry = 0.0
                    dd_from_peak = 0.0
                else:
                    gain_from_entry = (entry_price - peak_price) / entry_price
                    dd_from_peak = (c - peak_price) / peak_price if peak_price > 0 else 0.0

            # ======================
            # ② 分级止盈：浮盈 >=10% 先平一半
            # ======================
            if (not partial_taken) and gain_from_entry >= PARTIAL_TP_TRIGGER:
                # 按“达标价”平一半仓位
                if direction == 1:
                    tp_price = entry_price * (1 + PARTIAL_TP_TRIGGER)
                else:
                    tp_price = entry_price * (1 - PARTIAL_TP_TRIGGER)

                close_fraction = PARTIAL_TP_FRACTION
                close_size = size * close_fraction
                close_notional = tp_price * close_size

                gross_pnl = (tp_price - entry_price) * direction * close_size
                fee_close = abs(close_notional) * FEE_RATE
                # 注：开仓手续费已经在开仓时扣过，不反复扣

                pnl_net = gross_pnl - fee_close
                equity += pnl_net

                # 剩余仓位
                remain_size = size - close_size
                if remain_size <= 0:
                    # 理论上不会 <=0，但防御一下
                    in_pos = False
                    size = 0.0
                    margin_used = 0.0
                    entry_price = None
                    peak_price = None
                else:
                    # margin 按比例缩小
                    margin_used *= (remain_size / size)
                    size = remain_size
                    # entry_price 保持不变（或者按加权也可以，这里直接沿用）

                trades.append({
                    "entry_time": dt,                 # 为简单，这里不区分开仓时间
                    "exit_time": dt,
                    "entry_price": entry_price,
                    "exit_price": tp_price,
                    "exit_reason": "partial_tp_10pct",
                    "direction": direction,
                    "margin_used": margin_used,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / (margin_used + 1e-12),
                    "equity_after": equity,
                })

                partial_taken = True

            # ======================
            # ③ 计算 ATR 止损 & 8%→2% 移动止盈
            # ======================
            if direction == 1:
                atr_stop = entry_price - atr * atr_mult
            else:
                atr_stop = entry_price + atr * atr_mult

            trail_stop = None
            if gain_from_entry >= TRAIL_TRIGGER and peak_price is not None:
                if direction == 1:
                    trail_stop = peak_price * (1 - TRAIL_BACK)
                else:
                    trail_stop = peak_price * (1 + TRAIL_BACK)

            # 最终止损价格：离现价更近的那个
            final_stop = None
            if direction == 1:
                candidates = [x for x in [atr_stop, trail_stop] if x is not None]
                if candidates:
                    final_stop = max(candidates)  # 多单：止损价格越高越保守
            else:
                candidates = [x for x in [atr_stop, trail_stop] if x is not None]
                if candidates:
                    final_stop = min(candidates)  # 空单：止损价格越低越保守

            exit_price = None
            exit_reason = None

            # ======================
            # ④ 加仓后的 3% 回撤强平
            # ======================
            if scaled_in and dd_from_peak >= SCALE_IN_DRAWDOWN_EXIT:
                exit_price = c
                exit_reason = "scalein_dd_exit"

            # ======================
            # ⑤ 普通 ATR/追踪 止损
            # ======================
            if exit_price is None and final_stop is not None:
                if direction == 1 and l <= final_stop:
                    exit_price = final_stop
                    exit_reason = "atr_sl_or_trail"
                elif direction == -1 and h >= final_stop:
                    exit_price = final_stop
                    exit_reason = "atr_sl_or_trail"

            # 执行平仓
            if exit_price is not None:
                notional = margin_used * LEVERAGE
                # 理论上：size ≈ notional / entry_price
                gross_pnl = (exit_price - entry_price) * direction * size
                fee_close = abs(exit_price * size) * FEE_RATE

                pnl_net = gross_pnl - fee_close
                equity += pnl_net

                last_trade_win = pnl_net > 0

                trades.append({
                    "entry_time": dt,  # 为简化未记录真正的入场时间
                    "exit_time": dt,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "exit_reason": exit_reason,
                    "direction": direction,
                    "margin_used": margin_used,
                    "pnl_net": pnl_net,
                    "pnl_pct_on_margin": pnl_net / (margin_used + 1e-12),
                    "equity_after": equity,
                })

                # 清空仓位
                in_pos = False
                direction = 0
                entry_price = None
                size = 0.0
                margin_used = 0.0
                peak_price = None
                scaled_in = False
                partial_taken = False

        # ===== 空仓时，考虑开新仓 =====
        if (not in_pos) and equity > 0:
            # 趋势 + RSI 动量过滤
            if trend != 0 and pass_rsi_filter(trend, rsi):
                # 用 50% 仓位入场
                margin_used = equity * 0.5
                if margin_used <= 0:
                    continue
                notional = margin_used * LEVERAGE
                size = notional / c

                entry_price = c
                direction = trend
                peak_price = c
                scaled_in = False
                partial_taken = False

                # 开仓手续费
                fee_open = notional * FEE_RATE
                equity -= fee_open

                trades.append({
                    "entry_time": dt,
                    "exit_time": None,
                    "entry_price": entry_price,
                    "exit_price": None,
                    "exit_reason": "open",
                    "direction": direction,
                    "margin_used": margin_used,
                    "pnl_net": -fee_open,
                    "pnl_pct_on_margin": -fee_open / (margin_used + 1e-12),
                    "equity_after": equity,
                })

                in_pos = True

    return equity, trades, df


# ===== 结果统计 =====

def summarize(equity, trades, df: pd.DataFrame):
    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}")
    print()

    # 只统计真正平仓的交易
    real_trades = [t for t in trades if t["exit_reason"] not in (None, "open")]
    n = len(real_trades)
    wins = sum(1 for t in real_trades if t["pnl_net"] > 0)
    losses = sum(1 for t in real_trades if t["pnl_net"] < 0)
    flats = n - wins - losses

    total_pnl = sum(t["pnl_net"] for t in real_trades)
    win_pnls = [t["pnl_net"] for t in real_trades if t["pnl_net"] > 0]
    loss_pnls = [t["pnl_net"] for t in real_trades if t["pnl_net"] < 0]

    avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
    avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0

    # 资金曲线 & 最大回撤
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
    ann_ret = total_ret  # 一年数据，粗略当作年化

    print("========== 回测结果（新基础版·综合进阶版） ==========")
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
    for t in real_trades[:5]:
        print(t)


if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_indicators(df)
    equity, trades, df_used = backtest(df)
    summarize(equity, trades, df_used)
