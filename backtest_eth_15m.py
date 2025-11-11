#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
新基础版 + 轻量震荡过滤（BB宽度 < 全局均值的 0.8 时，不开新仓）

基础规则回顾（你确认保留的部分）：
- 标的：ETHUSDT 15m K线（okx_eth_15m.csv）
- 方向：按 EMA34 / EMA144 判断
    - ema34 > ema144 → 看多，只开多
    - ema34 < ema144 → 看空，只开空
- 仓位：每次用当前资金的 50% 作为保证金，5x 杠杆
- 手续费：每边 0.07%（可改 FEE_RATE）
- 止损：ATR(34) * 3.5 作为动态止损带：
    - 多单：如果 close <= entry - ATR*3.5 → 止损
    - 空单：如果 close >= entry + ATR*3.5 → 止损
- 浮盈追踪：
    - 浮盈 ≥ 6% 时，启用 3% 回撤移动止损
    - 回撤 3% 触发平仓（多空对称）
- 始终「有交易机会就开仓」，不做资金下限保护（可以归零）

新增的「轻量震荡过滤」：
- 先算 Bollinger Band（BB）：
    - 用 34 根 close：mid = MA34，std = 标准差
    - 上轨 upper = mid + 2*std，下轨 lower = mid - 2*std
    - 宽度 bb_width = (upper - lower) / mid
- 计算全局均值 bb_width_mean（忽略 NaN）
- 开新仓时，若当前 bb_width < bb_width_mean * 0.8 → 认为波动过窄，跳过，不开仓
"""

import pandas as pd

# ===== 基本配置 =====
CSV_PATH        = "okx_eth_15m.csv"   # 你的 CSV 文件名
INITIAL_EQUITY  = 50.0                # 初始资金
LEVERAGE        = 5.0                 # 杠杆
FEE_RATE        = 0.0007              # 单边手续费率（0.07%）

ATR_PERIOD      = 34                  # ATR 计算周期
ATR_STOP_K      = 3.5                 # ATR 止损倍数

TRAIL_TRIGGER   = 0.06                # 浮盈 ≥ 6% 启动移动止损
TRAIL_PCT       = 0.03                # 移动止损回撤 3%

BB_PERIOD       = 34                  # BB 用的周期
BB_WIDTH_MULT   = 0.8                 # 当前 bb_width < 全局均值 * 0.8 时不入场

MIN_EQUITY_TO_TRADE = 1.0             # 资金太少就不再开新仓，防止浮点乱跑


# ===== 工具函数 =====
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 时间列处理：优先 iso，其次 ts，其次第一列
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

    # 必要列检查
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算：
    - EMA34 / EMA144
    - ATR(34)
    - BB 宽度（34，2σ）和全局均值
    """
    close = df["close"]
    high = df["high"]
    low  = df["low"]

    # EMA
    df["ema34"]  = close.ewm(span=34, adjust=False).mean()
    df["ema144"] = close.ewm(span=144, adjust=False).mean()

    # ATR(34)
    # true range: max(h-l, |h-prev_close|, |l-prev_close|)
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()

    # Bollinger Band 宽度（34, 2σ）
    bb_mid = close.rolling(window=BB_PERIOD, min_periods=BB_PERIOD).mean()
    bb_std = close.rolling(window=BB_PERIOD, min_periods=BB_PERIOD).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    df["bb_width"] = (bb_upper - bb_lower) / bb_mid

    # 全局均值，用于轻量震荡过滤
    bb_mean = df["bb_width"].mean(skipna=True)
    df["bb_ok"] = df["bb_width"] >= bb_mean * BB_WIDTH_MULT

    # 删掉指标未就绪的部分
    df = df.dropna(subset=["ema34", "ema144", "atr", "bb_width"]).reset_index(drop=True)
    return df


# ===== 回测主体 =====
def backtest(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0        # 1 = 多, -1 = 空
    entry_price = None
    entry_time = None
    margin_used = None
    best_price = None    # 对多单记录最高价，对空单记录最低价
    trail_active = False

    trades = []

    for _, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        ema34 = float(row["ema34"])
        ema144 = float(row["ema144"])
        atr = float(row["atr"])
        bb_ok = bool(row["bb_ok"])

        # ===== 有持仓时：管理止损 / 追踪止盈 =====
        if in_pos and margin_used is not None and atr > 0:
            # 更新 best_price（多单用最高价，空单用最低价）
            if direction == 1:
                if best_price is None:
                    best_price = h
                else:
                    best_price = max(best_price, h)
                move_pct = (best_price - entry_price) / entry_price
            else:  # 空单
                if best_price is None:
                    best_price = l
                else:
                    best_price = min(best_price, l)
                move_pct = (entry_price - best_price) / entry_price  # 空单浮盈百分比

            # 是否启动追踪止盈（浮盈 ≥ 6%）
            if (not trail_active) and move_pct >= TRAIL_TRIGGER:
                trail_active = True

            # 计算 ATR 固定止损价格
            if direction == 1:
                atr_stop = entry_price - ATR_STOP_K * atr   # 多单：向下 3.5 ATR
            else:
                atr_stop = entry_price + ATR_STOP_K * atr   # 空单：向上 3.5 ATR

            exit_price = None
            exit_reason = None

            # 追踪止盈价格（如果已经启动）
            if trail_active and best_price is not None:
                if direction == 1:
                    trail_stop = best_price * (1 - TRAIL_PCT)
                    # 优先看是不是触发 trail 止盈
                    if l <= trail_stop:
                        exit_price = trail_stop
                        exit_reason = "trail_6_to_3"
                else:
                    trail_stop = best_price * (1 + TRAIL_PCT)
                    if h >= trail_stop:
                        exit_price = trail_stop
                        exit_reason = "trail_6_to_3"

            # 若尚未退出，再检查 ATR 止损
            if exit_price is None:
                if direction == 1 and c <= atr_stop:
                    exit_price = atr_stop
                    exit_reason = "atr_sl_or_trail"
                elif direction == -1 and c >= atr_stop:
                    exit_price = atr_stop
                    exit_reason = "atr_sl_or_trail"

            # 真的退出一笔
            if exit_price is not None:
                # 计算仓位规模
                notional = margin_used * LEVERAGE
                size = notional / entry_price
                gross_pnl = (exit_price - entry_price) * size * direction
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
                entry_time = None
                margin_used = None
                best_price = None
                trail_active = False

        # ===== 没有持仓 → 看是否开新仓 =====
        if (not in_pos) and equity > MIN_EQUITY_TO_TRADE:
            # EMA 决定方向
            if ema34 > ema144:
                new_dir = 1
            elif ema34 < ema144:
                new_dir = -1
            else:
                new_dir = 0

            # 轻量震荡过滤：波动过窄就休息
            if (new_dir != 0) and bb_ok:
                # 用 50% 资金开仓
                margin = equity * 0.5
                if margin <= 0:
                    continue

                in_pos = True
                direction = new_dir
                entry_price = c
                entry_time = dt
                margin_used = margin
                best_price = h if direction == 1 else l
                trail_active = False

    return equity, trades


# ===== 结果统计 =====
def summarize(df: pd.DataFrame, equity: float, trades):
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
    ann_ret = total_ret  # 一年数据，直接当年化

    print("========== 回测结果（新基础版 + 轻量BB震荡过滤） ==========")
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


# ===== 主入口 =====
if __name__ == "__main__":
    df = load_data(CSV_PATH)
    df = add_indicators(df)
    final_equity, trades = backtest(df)
    summarize(df, final_equity, trades)
