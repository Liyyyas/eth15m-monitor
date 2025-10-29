import json
import pandas as pd
import numpy as np

CSV_PATH = "okx_eth_15m.csv"
REPORT_JSON = "backtest_report.json"
EQUITY_CSV = "equity_curve.csv"
TRADES_CSV = "trades.csv"

df = pd.read_csv(CSV_PATH)
df.columns = [c.strip().lower() for c in df.columns]
df = df.sort_values("ts").reset_index(drop=True)
for c in ["open","high","low","close","vol"]:
  df[c] = pd.to_numeric(df[c], errors="coerce")

# === 指标 ===
df["ema34"]  = df["close"].ewm(span=34,  adjust=False).mean()
df["ema144"] = df["close"].ewm(span=144, adjust=False).mean()
df["slope10"] = (df["close"] - df["close"].shift(10)) / (10*np.maximum(1e-9, df["close"].shift(10)))

# === 参数 ===
SL = -0.06
TP = +0.10

equity = 1.0
risk_per_trade = 1
in_pos = False
entry_px = None
entry_i = None
trades = []
eq_curve = []

# === 出场函数 ===
def exit_trade(exit_px, exit_idx, reason):
    global equity, in_pos, entry_px, entry_i
    ret = (exit_px/entry_px) - 1.0
    equity *= (1.0 + ret*risk_per_trade)
    trades.append({
        "entry_time": df.loc[entry_i,"iso"],
        "entry_px": float(entry_px),
        "exit_time": df.loc[exit_idx,"iso"],
        "exit_px": float(exit_px),
        "ret": float(ret),
        "reason": reason
    })
    in_pos = False
    entry_px = None
    entry_i = None

# === 主循环 ===
for i in range(len(df)):
    px_o = df.loc[i,"open"]
    px_h = df.loc[i,"high"]
    px_l = df.loc[i,"low"]
    px_c = df.loc[i,"close"]
    ema34  = df.loc[i,"ema34"]
    ema144 = df.loc[i,"ema144"]
    slope  = df.loc[i,"slope10"]

    eq_curve.append({"iso": df.loc[i,"iso"], "equity": float(equity)})

    if not in_pos:
        if (px_c>ema34) and (ema34>ema144) and (slope>0):
            if i+1 < len(df):
                entry_px = float(df.loc[i+1,"open"])
                entry_i = i+1
                in_pos = True
    else:
        sl_px = entry_px*(1.0+SL)
        tp_px = entry_px*(1.0+TP)
        hit_sl = px_l <= sl_px
        hit_tp = px_h >= tp_px
        if hit_sl and hit_tp:
            exit_trade(sl_px, i, "SL&TP_samebar->SL")
        elif hit_sl:
            exit_trade(sl_px, i, "SL")
        elif hit_tp:
            exit_trade(tp_px, i, "TP")
        elif px_c < ema34:
            exit_trade(px_c, i, "Reverse")

if in_pos:
    exit_trade(float(df.iloc[-1]["close"]), len(df)-1, "EOD")

# === 汇总 ===
rets = np.array([t["ret"] for t in trades]) if trades else np.array([])
wins = (rets>0).sum() if trades else 0
losses = (rets<=0).sum() if trades else 0
win_rate = float(wins/len(trades)) if len(trades)>0 else 0.0
avg_ret = float(rets.mean()) if len(trades)>0 else 0.0
gross_profit = float(rets[rets>0].sum()) if len(trades)>0 else 0.0
gross_loss = float(-rets[rets<=0].sum()) if len(trades)>0 else 0.0
profit_factor = float(gross_profit/gross_loss) if gross_loss>0 else float("inf")

ec = pd.Series([x["equity"] for x in eq_curve])
peak = ec.cummax()
dd = (ec/peak - 1.0).min() if len(ec)>0 else 0.0
max_dd = float(dd)

report = {
    "trades": len(trades),
    "win_rate": win_rate,
    "avg_trade_return": avg_ret,
    "profit_factor": profit_factor,
    "final_equity": float(ec.iloc[-1] if len(ec)>0 else 1.0),
    "max_drawdown": max_dd,
    "params": {"ema_fast":34,"ema_slow":144,"slope_len":10,"SL":SL,"TP":TP}
}

pd.DataFrame(eq_curve).to_csv(EQUITY_CSV, index=False)
pd.DataFrame(trades).to_csv(TRADES_CSV, index=False)
with open(REPORT_JSON,"w",encoding="utf-8") as f:
    json.dump(report,f,ensure_ascii=False,indent=2)

print(json.dumps(report,ensure_ascii=False,indent=2))
