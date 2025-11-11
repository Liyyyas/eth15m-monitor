#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

# ===== 配置 =====
CSV_PATH = "okx_eth_15m.csv"
INITIAL_EQUITY = 50.0
LEVERAGE = 5
FEE_RATE = 0.0007
POSITION_PCT = 0.5  # 仓位比例：资金的 50%

ATR_PERIOD = 34
ATR_MULT = 3.5
RSI_PERIOD = 14
RSI_LONG = 55
RSI_SHORT = 45

FLOAT_PROFIT_TRIGGER = 0.06
FLOAT_PROFIT_FALLBACK = 0.03

# ===== 指标计算 =====
def calc_indicators(df):
    df["ema34"] = df["close"].ewm(span=34, adjust=False).mean()
    df["ema144"] = df["close"].ewm(span=144, adjust=False).mean()

    # ATR(34)
    df["tr"] = np.maximum(df["high"] - df["low"],
                          np.maximum(abs(df["high"] - df["close"].shift()),
                                     abs(df["low"] - df["close"].shift())))
    df["atr"] = df["tr"].rolling(ATR_PERIOD).mean()

    # RSI
    delta = df["close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(RSI_PERIOD).mean()
    avg_loss = pd.Series(loss).rolling(RSI_PERIOD).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    df["rsi"] = 100 - (100 / (1 + rs))

    df.dropna(inplace=True)
    return df

# ===== 策略逻辑 =====
def backtest(df):
    equity = INITIAL_EQUITY
    position = 0  # 1=多，-1=空
    entry_price = 0
    entry_time = None
    peak = 0

    trades = []

    for i in range(1, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        # EMA方向判断
        if row["ema34"] > row["ema144"]:
            ema_dir = 1
        elif row["ema34"] < row["ema144"]:
            ema_dir = -1
        else:
            ema_dir = 0

        # RSI动量过滤
        if ema_dir == 1 and row["rsi"] > RSI_LONG:
            signal = 1
        elif ema_dir == -1 and row["rsi"] < RSI_SHORT:
            signal = -1
        else:
            signal = 0

        if position == 0 and signal != 0:
            # 开仓
            margin = equity * POSITION_PCT
            entry_price = row["close"]
            entry_time = row["dt"]
            position = signal
            atr = row["atr"]
            peak = entry_price
            stop_loss = entry_price - position * atr * ATR_MULT

        elif position != 0:
            price = row["close"]
            atr = row["atr"]

            # 动态止损价
            stop_loss = entry_price - position * atr * ATR_MULT

            # 更新浮盈高点/低点
            if position == 1:
                peak = max(peak, price)
                float_gain = (peak - entry_price) / entry_price
                if float_gain >= FLOAT_PROFIT_TRIGGER:
                    stop_loss = max(stop_loss, peak * (1 - FLOAT_PROFIT_FALLBACK))
            else:
                peak = min(peak, price)
                float_gain = (entry_price - peak) / entry_price
                if float_gain >= FLOAT_PROFIT_TRIGGER:
                    stop_loss = min(stop_loss, peak * (1 + FLOAT_PROFIT_FALLBACK))

            # 止损触发
            close_pos = False
            if position == 1 and price < stop_loss:
                close_pos = True
            elif position == -1 and price > stop_loss:
                close_pos = True
            elif (position == 1 and ema_dir == -1) or (position == -1 and ema_dir == 1):
                close_pos = True

            if close_pos:
                exit_price = price
                pnl = (exit_price - entry_price) * position * LEVERAGE * (equity * POSITION_PCT) / entry_price
                fee = equity * POSITION_PCT * LEVERAGE * FEE_RATE * 2
                pnl -= fee
                equity += pnl
                trades.append({
                    "entry_time": entry_time,
                    "exit_time": row["dt"],
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl_net": pnl,
                    "equity_after": equity,
                    "direction": position
                })
                position = 0
                entry_price = 0
                peak = 0

        if equity <= 0:
            break

    return equity, trades


# ===== 主函数 =====
def summarize(df, equity, trades):
    print(f"数据行数: {len(df)}")
    print(f"时间范围: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}")
    print()
    print("========== 回测结果（新基础版 + 动量确认RSI） ==========")
    print(f"总交易数: {len(trades)}")
    wins = [t for t in trades if t["pnl_net"] > 0]
    losses = [t for t in trades if t["pnl_net"] <= 0]
    print(f"胜: {len(wins)}  负: {len(losses)}  胜率: {len(wins)/len(trades)*100 if trades else 0:.2f}%")
    total_pnl = sum(t["pnl_net"] for t in trades)
    print(f"总盈亏: {total_pnl:.4f} U")
    print(f"期末资金: {equity:.4f} U (初始 {INITIAL_EQUITY} U)")
    print(f"最大回撤: {min(t['equity_after'] for t in trades)/INITIAL_EQUITY - 1:.2%}" if trades else "")
    print()
    print("前5笔交易:")
    for t in trades[:5]:
        print(t)

if __name__ == "__main__":
    df = pd.read_csv(CSV_PATH)
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = calc_indicators(df)
    equity, trades = backtest(df)
    summarize(df, equity, trades)
