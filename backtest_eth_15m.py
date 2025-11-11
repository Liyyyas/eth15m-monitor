#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

# ================= 基本参数 =================
CSV_PATH         = "okx_eth_15m.csv"   # 你的一年 15m 数据
INITIAL_EQUITY   = 50.0                # 初始资金
MARGIN_PER_TRADE = 25.0                # 每次用 25U 保证金（≈50% 仓）
LEVERAGE         = 5.0                 # 杠杆
FEE_RATE         = 0.0007              # 单边手续费率（0.07%）

EMA_FAST         = 34
EMA_SLOW         = 144

ATR_LEN          = 34                  # ATR 周期（新基础版 v2.0 用的 34）
ATR_MULT         = 3.5                 # ATR 倍数止损

TRAIL_TRIGGER    = 0.06                # 浮盈 ≥ 6% 启动移动止盈
TRAIL_DRAWBACK   = 0.03                # 允许 3% 回撤（6%→3%）


# =============== 读 15m 数据 & 转成 4h K 线 ===============
def load_15m(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # 尝试解析时间列：优先 iso，其次 ts，否则第一列当时间戳
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

    # 保证有 open/high/low/close
    for col in ["open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"CSV 缺少列: {col}")

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df


def to_4h(df_15m: pd.DataFrame) -> pd.DataFrame:
    """
    把 15m 聚合成 4h K线：
    - open: 第一根 open
    - high: 4 小时内最高价
    - low : 4 小时内最低价
    - close: 最后一根 close
    """
    df = df_15m.set_index("dt")

    ohlc = df[["open", "high", "low", "close"]].resample(
        "4H", label="right", closed="right"
    ).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    })

    ohlc = ohlc.dropna().reset_index()
    return ohlc


# =============== 技术指标：EMA & ATR ===============
def add_ema_and_atr(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    df["ema_fast"] = close.ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = close.ewm(span=EMA_SLOW, adjust=False).mean()

    # ATR(34)
    df["prev_close"] = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["prev_close"]).abs()
    tr3 = (df["low"] - df["prev_close"]).abs()
    df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = df["tr"].rolling(ATR_LEN, min_periods=ATR_LEN).mean()

    df = df.dropna(subset=["ema_fast", "ema_slow", "atr"]).reset_index(drop=True)
    return df


# =============== 回测：4 小时版本新基础版 ===============
def backtest_4h(df: pd.DataFrame):
    equity = INITIAL_EQUITY

    in_pos = False
    direction = 0       # 1 多，-1 空
    entry_price = None
    entry_time = None
    margin_used = None

    # 用高/低跟踪浮盈
    high_since_entry = None
    low_since_entry = None
    trail_active = False
    stop_price = None

    trades = []

    for _, row in df.iterrows():
        dt = row["dt"]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        atr = float(row["atr"])
        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])

        # 先管理已有持仓
        if in_pos:
            # 更新极值
            if direction == 1:  # 多单
                if high_since_entry is None:
                    high_since_entry = h
                else:
                    high_since_entry = max(high_since_entry, h)

                # 浮盈比例
                gain_pct = (high_since_entry - entry_price) / entry_price

                # ATR 固定止损
                atr_sl = entry_price - ATR_MULT * atr

                # 如果浮盈 >= 6% 启动追踪
                if gain_pct >= TRAIL_TRIGGER:
                    trail_active = True
                    trail_stop = high_since_entry * (1 - TRAIL_DRAWBACK)
                else:
                    trail_stop = None

                # 综合止损：取“最保守”的那个（价格最高的止损）
                if trail_active and trail_stop is not None:
                    stop_price = max(atr_sl, trail_stop)
                else:
                    stop_price = atr_sl

                exit_price = None
                exit_reason = None

                # 低价触及止损
                if l <= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"
                # 没打止损，就用收盘价检查是否要提前止盈（这里我们就交给 ATR+追踪，额外不做强平）

            else:  # direction == -1 空单
                if low_since_entry is None:
                    low_since_entry = l
                else:
                    low_since_entry = min(low_since_entry, l)

                gain_pct = (entry_price - low_since_entry) / entry_price

                atr_sl = entry_price + ATR_MULT * atr

                if gain_pct >= TRAIL_TRIGGER:
                    trail_active = True
                    trail_stop = low_since_entry * (1 + TRAIL_DRAWBACK)
                else:
                    trail_stop = None

                if trail_active and trail_stop is not None:
                    stop_price = min(atr_sl, trail_stop)
                else:
                    stop_price = atr_sl

                exit_price = None
                exit_reason = None

                if h >= stop_price:
                    exit_price = stop_price
                    exit_reason = "atr_sl_or_trail"

            if exit_price is not None:
                # 结算一笔
                notional = margin_used * LEVERAGE
                size = notional / entry_price  # 多空都一样，用 entry_price 计算张数

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
                    "pnl_pct_on_margin": pnl_net / margin_used,
                    "equity_after": equity,
                })

                # 清空仓位状态
                in_pos = False
                direction = 0
                entry_price = None
                entry_time = None
                margin_used = None
                high_since_entry = None
                low_since_entry = None
                trail_active = False
                stop_price = None

        # 再看是否可以开新仓
        if (not in_pos) and (equity >= MARGIN_PER_TRADE):
            # 按 EMA34/144 决定方向
            if ema_fast > ema_slow:
                new_dir = 1
            elif ema_fast < ema_slow:
                new_dir = -1
            else:
                new_dir = 0

            if new_dir != 0:
                in_pos = True
                direction = new_dir
                entry_price = c  # 用 4h 收盘价入场
                entry_time = dt
                margin_used = MARGIN_PER_TRADE

                if direction == 1:
                    high_since_entry = c
                    low_since_entry = None
                else:
                    low_since_entry = c
                    high_since_entry = None

                trail_active = False
                stop_price = None

    return equity, trades


# =============== 结果汇总 ===============
def summarize(df_4h: pd.DataFrame, equity: float, trades: list):
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
    ann_ret = total_ret  # 一年数据，近似等于总收益率

    print("========== 回测结果（新基础版·4 小时版） ==========")
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


# =============== 主函数入口 ===============
if __name__ == "__main__":
    df_15m = load_15m(CSV_PATH)
    df_4h = to_4h(df_15m)
    df_4h = add_ema_and_atr(df_4h)
    equity, trades = backtest_4h(df_4h)
    summarize(df_4h, equity, trades)
