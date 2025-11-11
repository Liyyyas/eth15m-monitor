#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import timezone, timedelta

# ===== 路径与基础参数 =====
CSV_PATH      = "okx_eth_15m.csv"
INITIAL_EQ    = 50.0
MARGIN_FIXED  = 25.0          # 固定保证金（胜率优化不依赖仓位大小，但保留费用计算）
LEVERAGE      = 5.0
FEE_RATE      = 0.0007        # 单边 0.07%

# ====== 工具函数 ======
def read_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # 解析时间
    if "iso" in df.columns:
        df["dt"] = pd.to_datetime(df["iso"], utc=True, errors="coerce")
    elif "ts" in df.columns:
        med = pd.to_numeric(df["ts"], errors="coerce").dropna().median()
        unit = "ms" if med and med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    else:
        first_col = df.columns[0]
        med = pd.to_numeric(df[first_col], errors="coerce").dropna().median()
        unit = "ms" if med and med > 1e11 else "s"
        df["dt"] = pd.to_datetime(pd.to_numeric(df[first_col], errors="coerce"),
                                  unit=unit, utc=True, errors="coerce")
    # 必要列
    for c in ["open","high","low","close"]:
        if c not in df.columns:
            raise ValueError(f"CSV缺少列: {c}")
    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df[["dt","open","high","low","close"]]

def ema(x: pd.Series, n: int):
    return x.ewm(span=n, adjust=False).mean()

def true_range(h,l,c_prev):
    return np.maximum(h-l, np.maximum(np.abs(h-c_prev), np.abs(l-c_prev)))

def atr(df: pd.DataFrame, n: int):
    c_prev = df["close"].shift(1)
    tr = true_range(df["high"].values, df["low"].values, c_prev.values)
    tr = pd.Series(tr, index=df.index)
    return tr.ewm(alpha=1/n, adjust=False).mean()

def boll_bw(close: pd.Series, n: int=20):
    ma = close.rolling(n).mean()
    std = close.rolling(n).std()
    upper = ma + 2*std
    lower = ma - 2*std
    width = (upper - lower) / ma
    return width

def resample_h1(df):
    g = df.set_index("dt").resample("1H", label="right", closed="right").agg(
        {"open":"first","high":"max","low":"min","close":"last"}
    ).dropna().reset_index()
    g["ema34_h1"]  = ema(g["close"], 34)
    g["ema144_h1"] = ema(g["close"], 144)
    g["h1_up"] = (g["ema34_h1"] > g["ema144_h1"]).astype(int)
    return g

# ====== 指标与状态 ======
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df["ema34"]  = ema(df["close"], 34)
    df["ema144"] = ema(df["close"], 144)
    df["ema_gap"] = (df["ema34"] - df["ema144"]) / df["close"]

    df["atr21"] = atr(df, 21)
    df["atr34"] = atr(df, 34)

    df["bb_width20"] = boll_bw(df["close"], 20)
    # Donchian 高低
    for n in [10, 20, 30]:
        df[f"donhi{n}"] = df["high"].rolling(n).max()
        df[f"donlo{n}"] = df["low"].rolling(n).min()

    # H1 趋势对齐
    h1 = resample_h1(df)
    df = df.merge(h1[["dt","ema34_h1","ema144_h1","h1_up"]]
                  .rename(columns={"dt":"dt_h1"}),
                  left_on=df["dt"].dt.floor("H"),
                  right_on="dt_h1", how="left").drop(columns=["key_0","dt_h1"])
    df["h1_up"].fillna(method="ffill", inplace=True)

    return df.dropna().reset_index(drop=True)

# ====== 策略组件（胜率优先） ======
def in_session(dt_utc, session: str|None):
    # 以 UTC 过滤时段，简洁：Asia(00-08), EU(07-15), US(13-21)
    h = dt_utc.hour
    if session is None: return True
    if session == "ASIA": return 0 <= h < 8
    if session == "EU":   return 7 <= h < 15
    if session == "US":   return 13 <= h < 21
    return True

def regime_trend(row, ema_gap_thr: float, need_h1_align: bool, squeeze_k: float|None, bw_ma: float|None):
    if need_h1_align and row["h1_up"] != (1 if row["ema34"]>row["ema144"] else 0):
        return False
    if row["ema34"] <= row["ema144"]:
        return False
    if row["ema_gap"] < ema_gap_thr:
        return False
    if squeeze_k is not None and bw_ma is not None:
        # 压缩时不做（避免假突破）
        if row["bb_width20"] < bw_ma * squeeze_k:
            return False
    return True

def regime_range(row, squeeze_k: float|None, bw_ma: float|None, ema_gap_soft: float):
    # 震荡定义：EMA差很小 + BB收窄
    cond1 = abs(row["ema_gap"]) <= ema_gap_soft
    cond2 = True
    if squeeze_k is not None and bw_ma is not None:
        cond2 = row["bb_width20"] < bw_ma * squeeze_k
    return cond1 and cond2

def entry_trend_pullback(idx, df, pull_k: int, use_retest: bool):
    """ 回踩EMA34后再收回，上破上一根高点入场（多头） """
    if idx < pull_k+2: return None
    rows = df.iloc[idx-pull_k-2:idx+1]
    now  = rows.iloc[-1]
    prev = rows.iloc[-2]
    # 最近 pull_k 根至少有一次触碰/下穿 ema34
    touched = (rows["low"] <= rows["ema34"]).any()
    # 当前K线收上 ema34 且突破上一根高点
    cond_up = (now["close"] > now["ema34"]) and (now["high"] > prev["high"])
    if touched and cond_up:
        return +1
    return None

def entry_breakout_retest(idx, df, don_len: int, rt_atr: float):
    """ 突破 Donchian 高点后，回踩不跌破 突破位-rt_atr*ATR34，随后再向上 """
    if idx < don_len+2: return None
    now = df.iloc[idx]
    prev = df.iloc[idx-1]
    donhi = df.iloc[idx-1][f"donhi{don_len}"]
    if np.isnan(donhi): return None
    # 前一根已突破
    if prev["high"] <= donhi: return None
    # 回踩不低于突破位 - 容忍 * atr34
    floor = donhi - rt_atr * prev["atr34"]
    # 当前K线站回突破位之上（确认继续）
    if now["low"] >= floor and now["close"] > donhi:
        return +1
    return None

def entry_range_fade(idx, df, n_bb: int, tol_atr: float):
    """ 震荡反向：接近上轨做空/下轨做多，要求回落/回升确认 """
    if idx < n_bb+2: return None
    seg = df.iloc[idx-n_bb-2:idx+1]
    ma = seg["close"].rolling(n_bb).mean().iloc[-1]
    std = seg["close"].rolling(n_bb).std().iloc[-1]
    if np.isnan(ma) or np.isnan(std): return None
    upper = ma + 2*std
    lower = ma - 2*std
    now, prev = seg.iloc[-1], seg.iloc[-2]
    # 上轨转弱 → 做空
    if prev["high"] >= upper and now["close"] < prev["close"] and (prev["high"]-upper) <= tol_atr*prev["atr34"]:
        return -1
    # 下轨转强 → 做多
    if prev["low"] <= lower and now["close"] > prev["close"] and (lower - prev["low"]) <= tol_atr*prev["atr34"]:
        return +1
    return None

def fill_exit_prices(o,h,l,c, sl,tp, direction):
    """ 简单保守撮合：同根内先触发止损，再触发止盈 """
    if direction==+1:
        # long: 先看是否触发SL
        if l <= sl: return sl, "sl"
        if h >= tp: return tp, "tp"
        return c, "time"
    else:
        # short
        if h >= sl: return sl, "sl"
        if l <= tp: return tp, "tp"
        return c, "time"

# ====== 回测引擎（单组合） ======
def run_one(df, params):
    """
    params:
      ema_gap_thr: 趋势上限口阈值（如0.002=0.2%）
      don_len:     Donchian长度（趋势突破）
      pull_k:      回踩窗口（趋势回踩入场）
      use_retest:  是否启用突破回踩再上（更严）
      rt_atr:      回踩容忍ATR倍数
      range_bb:    震荡BB计算窗口
      squeeze_k:   BB低波动过滤倍率（相对均值）
      ema_gap_soft:判定震荡的EMA差阈值
      atr_src:     使用atr21或atr34
      stop_mult:   ATR止损倍数（胜率模式：较大）
      tp_mult:     ATR止盈倍数（胜率模式：较小）
      session:     ASIA/EU/US/None
      need_h1:     是否要求H1对齐
      activate_trend/activate_range: 是否启用两个子系统
    """
    equity = INITIAL_EQ
    in_pos = False
    direction = 0
    entry = None
    atr_name = params["atr_src"]

    trades=[]
    # 预计算压缩均值
    bw_ma = df["bb_width20"].rolling(200).mean()

    for i in range(max(200, params["don_len"]+5), len(df)-1):
        row  = df.iloc[i]
        nxt  = df.iloc[i+1]
        if not in_session(row["dt"], params["session"]):
            # 平仓（按收盘）或不操作 -> 胜率模式不强平，等规则
            pass

        # 管理持仓
        if in_pos:
            atr_now = row[atr_name]
            if np.isnan(atr_now): atr_now = row["atr21"]
            if direction==+1:
                sl = entry - params["stop_mult"]*atr_now
                tp = entry + params["tp_mult"]*atr_now
            else:
                sl = entry + params["stop_mult"]*atr_now
                tp = entry - params["tp_mult"]*atr_now

            # 用下一根K撮合（避免同bar作弊）
            o,h,l,c = nxt["open"], nxt["high"], nxt["low"], nxt["close"]
            exit_px, why = fill_exit_prices(o,h,l,c, sl,tp, direction)

            if (why in ("sl","tp")):
                # 结算
                margin = min(MARGIN_FIXED, equity)  # 防越界
                if margin <= 0: break
                notional = margin * LEVERAGE
                size = notional / entry
                gross = (exit_px - entry) * size * (1 if direction==+1 else -1)
                fee_open  = notional * FEE_RATE
                fee_close = abs(exit_px * size) * FEE_RATE
                pnl = gross - fee_open - fee_close
                equity += pnl
                trades.append({
                    "entry_time": row["dt"],  # 以当前bar时间标注出场前一刻
                    "exit_time":  nxt["dt"],
                    "entry": entry, "exit": exit_px,
                    "direction": direction,
                    "reason": why,
                    "pnl": pnl, "eq": equity
                })
                in_pos=False
                direction=0
                entry=None

        # 开仓（只在空仓 & 有钱）
        if (not in_pos) and equity > 0 and margin_available(equity):
            atr_now = row[atr_name]
            if np.isnan(atr_now): continue

            trend_ok = regime_trend(row, params["ema_gap_thr"], params["need_h1"],
                                    params["squeeze_k"], bw_ma.iloc[i])
            range_ok = regime_range(row, params["squeeze_k"], bw_ma.iloc[i], params["ema_gap_soft"])

            sig = None
            # 趋势系统优先
            if params["activate_trend"] and trend_ok:
                # 两种入口并存：回踩 & 突破回踩再上
                a = entry_trend_pullback(i, df, params["pull_k"], params["use_retest"])
                if a is None:
                    a = entry_breakout_retest(i, df, params["don_len"], params["rt_atr"])
                sig = a
            # 震荡系统
            if sig is None and params["activate_range"] and range_ok:
                sig = entry_range_fade(i, df, params["range_bb"], tol_atr=0.6)

            if sig is not None and sig != 0 and in_session(row["dt"], params["session"]):
                in_pos=True
                direction = int(sig)
                entry = row["close"]

    # 统计
    wins = sum(1 for t in trades if t["pnl"]>0)
    losses = sum(1 for t in trades if t["pnl"]<0)
    winrate = (wins/len(trades)*100) if trades else 0.0
    eq_curve = [INITIAL_EQ] + [t["eq"] for t in trades]
    peak = eq_curve[0]
    maxdd = 0.0
    for x in eq_curve:
        if x>peak: peak=x
        dd = (x-peak)/peak
        if dd<maxdd: maxdd=dd

    return {
        "trades": trades,
        "n": len(trades),
        "wins": wins,
        "losses": losses,
        "winrate": winrate,
        "equity": equity,
        "pnl": equity-INITIAL_EQ,
        "maxdd_pct": maxdd*100
    }

def margin_available(eq):
    return eq > 0.0

# ====== 网格搜索（胜率第一） ======
def grid_search(df: pd.DataFrame):
    # 基于经验，范围收敛：小TP + 大SL + 严格趋势 & 回踩/回测
    grid = []
    ema_gap_thrs = [0.0020, 0.0025, 0.0030]      # 0.20%~0.30%
    don_lens     = [10, 20]
    pull_ks      = [2, 3]
    use_retests  = [False, True]
    rt_atrs      = [0.5, 1.0]                     # 回踩容忍
    squeeze_ks   = [0.85, 0.90, None]             # None=不做压缩过滤
    ema_gap_softs= [0.0005, 0.0008]               # 震荡判定阈值
    atr_srcs     = ["atr34", "atr21"]
    stop_mults   = [2.5, 3.0, 3.5, 4.0]           # 胜率模式：更宽
    tp_mults     = [0.4, 0.6, 0.8, 1.0]           # 胜率模式：更紧
    sessions     = [None, "EU", "US"]             # 过滤时段（减少噪音）
    need_h1s     = [False, True]
    activate_trend = [True]
    activate_range = [False, True]                # 可叠加，但以趋势优先
    # 最少交易笔数：防止“只做几单100%”骗胜率
    MIN_TRADES   = 40

    results=[]
    total = (len(ema_gap_thrs)*len(don_lens)*len(pull_ks)*len(use_retests)*
             len(rt_atrs)*len(squeeze_ks)*len(ema_gap_softs)*len(atr_srcs)*
             len(stop_mults)*len(tp_mults)*len(sessions)*len(need_h1s)*
             len(activate_trend)*len(activate_range))

    k=0
    for a in ema_gap_thrs:
      for b in don_lens:
        for c in pull_ks:
          for d in use_retests:
            for e in rt_atrs:
              for f in squeeze_ks:
                for g in ema_gap_softs:
                  for h in atr_srcs:
                    for i in stop_mults:
                      for j in tp_mults:
                        for s in sessions:
                          for nh in need_h1s:
                            for t_act in activate_trend:
                              for r_act in activate_range:
                                k+=1
                                params = dict(
                                  ema_gap_thr=a, don_len=b, pull_k=c,
                                  use_retest=d, rt_atr=e,
                                  squeeze_k=f, ema_gap_soft=g,
                                  atr_src=h, stop_mult=i, tp_mult=j,
                                  session=s, need_h1=nh,
                                  activate_trend=t_act, activate_range=r_act
                                )
                                out = run_one(df, params)
                                if out["n"] < MIN_TRADES:
                                    continue
                                results.append({
                                  "winrate": out["winrate"],
                                  "n": out["n"],
                                  "maxdd%": out["maxdd_pct"],
                                  "pnl": out["pnl"],
                                  **params
                                })
    if not results:
        return None, None, None
    res = pd.DataFrame(results).sort_values(
        ["winrate","n", "maxdd%"], ascending=[False, False, True]
    )
    best = res.iloc[0].to_dict()
    return res, best, best_params_to_result(df, best)

def best_params_to_result(df, best_params):
    out = run_one(df, best_params)
    trades = pd.DataFrame(out["trades"])
    return out, trades

def main():
    df = read_data(CSV_PATH)
    df = enrich(df)

    # 预计算 BB均值（供压缩过滤参考）
    # 已在 run_one 内部 rolling 200 计算

    res, best, best_out = grid_search(df)
    if res is None:
        with open("extreme_winrate_report.txt","w",encoding="utf-8") as f:
            f.write("没有满足最小交易笔数的组合，尝试放宽过滤或增大参数网格。\n")
        print("NO RESULT"); return

    out, trades = best_out
    # 保存报告
    lines=[]
    lines.append("========== 胜率极限搜索·结果 ==========\n")
    lines.append(f"样本K数: {len(df)}  时间: {df['dt'].iloc[0]} -> {df['dt'].iloc[-1]}\n")
    lines.append(f"最优组合（按胜率、并以交易数/回撤作次序筛选）:\n")
    lines.append(str(best)+"\n\n")
    lines.append("—— 最优组合回测统计 ——\n")
    lines.append(f"总交易数: {out['n']}\n")
    lines.append(f"胜: {out['wins']} 负: {out['losses']}\n")
    lines.append(f"胜率: {out['winrate']:.2f}%\n")
    lines.append(f"期末资金: {out['equity']:.4f} U (初始 {INITIAL_EQ} U)\n")
    lines.append(f"总盈亏: {out['pnl']:.4f} U\n")
    lines.append(f"最大回撤: {out['maxdd_pct']:.2f}%\n")

    with open("extreme_winrate_report.txt","w",encoding="utf-8") as f:
        f.write("\n".join(lines))

    res.head(20).to_csv("extreme_winrate_top.csv", index=False)
    trades.to_csv("trades_best.csv", index=False)
    print("DONE. 已生成 extreme_winrate_report.txt / extreme_winrate_top.csv / trades_best.csv")

if __name__ == "__main__":
    main()
