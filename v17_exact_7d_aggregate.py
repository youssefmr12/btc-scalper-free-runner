#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math
from pathlib import Path
from collections import defaultdict

START=20.0
TARGET=100000.0
BASE_RATE=0.10
RATE_MULT=15000.0
RATE_EXP=1.0
POSITION_MULT=1384.6
BUST=0.00036

STAGES=[20,100,500,1000,5000,10000,40000]
LEVERAGE_LADDERS={
 "L60":[60,50,35,25,15,8,5],
 "L80":[80,65,45,30,18,10,6],
 "L100":[100,80,55,35,20,12,7],
 "L120":[120,90,60,40,25,15,8],
 "L150":[150,110,70,45,28,18,10],
 "L180":[180,130,80,50,30,20,12],
}
THROTTLES={
 "DD20":[(0.05,.80),(0.10,.60),(0.15,.35),(0.20,0.0)],
 "DD35":[(0.08,.85),(0.15,.60),(0.25,.30),(0.35,0.0)],
 "DD45":[(0.10,.90),(0.20,.70),(0.30,.40),(0.45,0.0)],
}
MARGIN_FRACTIONS=[0.50,0.65,0.80,1.00]
LATENCIES=[300,400,500]

# Official generic Tier-1 risk framework sensitivity, not asserted as historical Turbo-1000x exact.
GENERIC_BRACKETS=[
 (100_000,200,0.0025),
 (500_000,100,0.0050),
 (1_500_000,75,0.00667),
 (3_000_000,50,0.0100),
 (6_000_000,25,0.0200),
 (8_000_000,10,0.0500),
 (9_500_000,5,0.1000),
 (10_000_000,1,0.5000),
]

def bracket(notional):
    for cap,maxlev,mmr in GENERIC_BRACKETS:
        if notional<=cap: return cap,maxlev,mmr
    return None

def ladder_value(eq,vals):
    i=0
    for j,x in enumerate(STAGES):
        if eq>=x: i=j
    return vals[min(i,len(vals)-1)]

def throttle_mult(dd,name):
    for threshold,m in THROTTLES[name]:
        if dd<threshold: return m if threshold!=THROTTLES[name][0][0] else 1.0
    return 0.0

def throttle_mult(dd,name):
    rules=THROTTLES[name]
    if dd<rules[0][0]: return 1.0
    for i,(th,m) in enumerate(rules):
        nxt=rules[i+1][0] if i+1<len(rules) else 9e9
        if dd<nxt: return m
    return rules[-1][1]

def edge_mult(t):
    b=t.get("buy_score"); s=t.get("sell_score")
    try: strength=max(float(b or 0),float(s or 0))
    except: strength=0
    # All opportunities remain; size only varies mildly with signal strength.
    if strength>=80: return 1.15
    if strength>=70: return 1.08
    if strength>=60: return 1.00
    return 0.90

def funding_cost(t,notional,side,fmap,lat):
    v=t["latency"][str(lat)]
    st=v["entry_exec_ms"]; en=v["exit_exec_ms"]
    if en<=st: return 0.0
    first=st//3_600_000+1
    last=en//3_600_000
    if first>last: return 0.0
    sign=1.0 if side=="BUY" else -1.0
    total=0.0
    for h in range(first,last+1):
        rate=fmap.get(h,0.0)
        total += notional*rate*sign
    return total

def profit_capture(move,notional,literal=False,venue_leverage=1.0):
    if move<=0: return 0.0
    market_impact=1.0/max((move*RATE_MULT)**RATE_EXP,1e-12)
    # Primary: bet_amount is isolated margin, so bet_amount*leverage = position notional.
    numerator=notional*venue_leverage if literal else notional
    size_impact=numerator/max(1_000_000.0*move*POSITION_MULT,1e-12)
    cap=(1.0-BASE_RATE)/(1.0+market_impact+size_impact)
    return max(0.0,min(1.0,cap))

def position_plan(eq,effective_target,margin_fraction,edge):
    eff=max(0.0,effective_target*edge)
    desired_notional=eq*eff
    if desired_notional<=0: return None
    # desired margin reserve architecture; venue leverage separates exposure from account fraction at risk.
    desired_margin=eq*margin_fraction
    venue_lev=desired_notional/max(desired_margin,1e-12)
    # Iterate official generic Tier1 cap as sensitivity.
    for _ in range(6):
        br=bracket(desired_notional)
        if not br: return None
        _,maxlev,mmr=br
        if venue_lev<=maxlev+1e-12: break
        venue_lev=maxlev
        desired_notional=desired_margin*venue_lev
    br=bracket(desired_notional)
    if not br: return None
    _,maxlev,mmr=br
    venue_lev=min(venue_lev,maxlev)
    notional=desired_margin*venue_lev
    margin=desired_margin
    return notional,margin,venue_lev,mmr,maxlev

def simulate(trades,fmap,lat,ladder_name,throttle_name,margin_fraction,literal=False):
    eq=START; peak=START; lowest=START; maxdd=0.0
    milestones={100:None,500:None,1000:None,5000:None,10000:None,40000:None,100000:None}
    liqs=rejects=0; profit_share_total=funding_total=0.0; gross_total=0.0
    used=0; stop_reason=None; first_ts=None; last_ts=None
    vals=LEVERAGE_LADDERS[ladder_name]
    for t in trades:
        v=t.get("latency",{}).get(str(lat),{})
        if not v.get("valid"): rejects+=1; continue
        if first_ts is None: first_ts=v["entry_exec_ms"]
        last_ts=v["exit_exec_ms"]
        dd=(peak-eq)/peak if peak>0 else 1.0
        tm=throttle_mult(dd,throttle_name)
        if tm<=0:
            stop_reason="DRAWDOWN_STOP"; break
        e_target=ladder_value(eq,vals)*tm
        plan=position_plan(eq,e_target,margin_fraction,edge_mult(t))
        if not plan:
            rejects+=1; continue
        notional,margin,venue_lev,mmr,maxlev=plan
        if notional<=0 or margin<=0: rejects+=1; continue

        # Conservative liquidation sensitivity using generic Tier-1 MMR + BTC bust buffer.
        room=(1.0/venue_lev)-mmr-BUST
        mae=v.get("mae")
        if room<=0 or (mae is not None and float(mae)<=-room):
            liqs+=1
            eq=max(0.0,eq-margin)  # isolated margin lost; reserve survives when margin_fraction<1
            lowest=min(lowest,eq); peak=max(peak,eq)
            maxdd=max(maxdd,(peak-eq)/peak if peak else 1.0)
            used+=1
            if eq<=0:
                stop_reason="LIQUIDATED_TO_ZERO"; break
            continue

        ret=float(v["ret"]); gross=notional*ret; gross_total+=gross
        share=0.0
        if gross>0:
            cap=profit_capture(abs(ret),notional,literal=literal,venue_leverage=venue_lev)
            user_profit=gross*cap
            share=gross-user_profit
            pnl=user_profit
        else:
            pnl=gross
        fc=funding_cost(t,notional,t["side"],fmap,lat)
        pnl-=fc
        profit_share_total+=share; funding_total+=fc
        eq+=pnl; used+=1
        lowest=min(lowest,eq); peak=max(peak,eq)
        maxdd=max(maxdd,(peak-eq)/peak if peak>0 else 1.0)
        for m in milestones:
            if milestones[m] is None and eq>=m:
                milestones[m]=v["exit_exec_ms"]
        if eq>=TARGET:
            stop_reason="TARGET_100K"; break
        if eq<=0:
            stop_reason="EQUITY_ZERO"; break
    t0=first_ts
    mt={}
    for m,ts in milestones.items():
        mt[str(m)]=None if ts is None or t0 is None else {
            "elapsed_hours":(ts-t0)/3_600_000.0,
            "elapsed_days":(ts-t0)/86_400_000.0,
            "timestamp_ms":ts
        }
    return {
      "config":{"ladder":ladder_name,"throttle":throttle_name,"margin_fraction":margin_fraction,
                "latency_ms":lat,"profit_share_interpretation":"literal" if literal else "worked_example"},
      "target_hit":eq>=TARGET,"end_equity":eq,"peak_equity":peak,"lowest_equity":lowest,
      "max_drawdown_pct":100*maxdd,"liquidations":liqs,"rejects":rejects,"trades_used":used,
      "profit_share_total":profit_share_total,"funding_proxy_total":funding_total,
      "gross_pnl_before_share_funding":gross_total,"stop_reason":stop_reason,
      "milestones":mt,"elapsed_days":None if last_ts is None or t0 is None else (last_ts-t0)/86_400_000.0
    }

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--root",required=True); ap.add_argument("--out",required=True)
    a=ap.parse_args()
    shards=[]
    for p in Path(a.root).rglob("*.json"):
        try:
            x=json.loads(p.read_text())
            if x.get("type")=="V17_EXACT_7D_SHARD": shards.append(x)
        except: pass
    shards.sort(key=lambda x:x["date"])
    dates=[x["date"] for x in shards]
    expected=["2026-10-28","2026-10-29","2026-10-30","2026-10-31","2026-11-01","2026-11-02","2026-11-03"]
    trades=[]; fmap={}; evidence_gaps=[]; boundary=[]
    for s in shards:
        trades.extend(s["trades"])
        for f in s.get("funding_proxy_hourly",[]): fmap[int(f["hour"])]=float(f["rate"])
        evidence_gaps.extend([{"date":s["date"],**g} for g in s["data"].get("aux_gaps",[])])
        if s.get("boundary_open_position"): boundary.append(s["date"])
    trades.sort(key=lambda t:t["entry_signal_ms"])

    if dates!=expected:
        result={"type":"V17_EXACT_7D_FINAL","status":"INFRA_UNPROVEN","dates_found":dates,
                "expected":expected,"reason":"missing one or more daily shards"}
        Path(a.out).write_text(json.dumps(result,indent=2)); print(json.dumps(result)); return

    configs=[]
    for lat in LATENCIES:
      for ln in LEVERAGE_LADDERS:
       for tn in THROTTLES:
        for mf in MARGIN_FRACTIONS:
         configs.append(simulate(trades,fmap,lat,ln,tn,mf,False))

    # Robust candidate must survive all 300/400/500ms variants under same money-management config.
    groups=defaultdict(dict)
    for r in configs:
        k=(r["config"]["ladder"],r["config"]["throttle"],r["config"]["margin_fraction"])
        groups[k][r["config"]["latency_ms"]]=r

    robust=[]
    for k,g in groups.items():
        if all(lat in g for lat in LATENCIES):
            target_all=all(g[lat]["target_hit"] for lat in LATENCIES)
            zero_any=any(g[lat]["end_equity"]<=0 for lat in LATENCIES)
            primary=g[400]
            robust.append({
              "key":{"ladder":k[0],"throttle":k[1],"margin_fraction":k[2]},
              "target_all_latencies":target_all,"zero_any_latency":zero_any,
              "primary_400ms":primary,
              "latency_300ms":g[300],"latency_500ms":g[500]
            })

    robust.sort(key=lambda z:(
        not z["target_all_latencies"],
        z["zero_any_latency"],
        z["primary_400ms"]["milestones"]["100000"]["elapsed_days"] if z["primary_400ms"]["target_hit"] else 9999,
        z["primary_400ms"]["max_drawdown_pct"],
        -z["primary_400ms"]["end_equity"]
    ))
    best=robust[0] if robust else None

    # Formula ambiguity sensitivity only for the selected primary config.
    literal=None
    if best:
        k=best["key"]
        literal=simulate(trades,fmap,400,k["ladder"],k["throttle"],k["margin_fraction"],True)

    exact_blockers=[
      "Historical TurboFlow MarketMakerPerpPrice is not publicly archived; Bybit mark/index is used only as funding/execution proxy.",
      "Historical TurboFlow Profit-Share br and pair.maxMMR snapshots are not publicly proven for this seven-day window.",
      "Official generic Tier-1 margin brackets are used only as a conservative liquidation/position-cap sensitivity, not asserted as exact Turbo 1000x historical rules."
    ]
    if boundary:
        exact_blockers.append("Daily shard boundary had an open signal position on: "+",".join(boundary))
    if evidence_gaps:
        exact_blockers.append("Some optional CryptoHFTData OI/liquidation/mark hourly files were missing; gaps are listed.")

    strict_verdict="UNPROVEN"
    sensitivity_verdict="FAIL"
    if best and best["target_all_latencies"]:
        d=best["primary_400ms"]["milestones"]["100000"]["elapsed_days"]
        sensitivity_verdict="PASS_4_7_DAYS" if d is not None and d<=7 else "PASS_OVER_7_DAYS"
    elif best and best["primary_400ms"]["target_hit"]:
        sensitivity_verdict="LATENCY_FRAGILE"
    elif best:
        sensitivity_verdict="NOT_REACHED"

    result={
      "type":"V17_EXACT_7D_FINAL",
      "window":{"start":"2026-10-28","end":"2026-11-03","days":7},
      "engine":"exact V1.7.1 EDGE-HARVEST; frozen signals",
      "signal_trade_count":len(trades),
      "strict_historical_verdict":strict_verdict,
      "generic_official_risk_sensitivity_verdict":sensitivity_verdict,
      "best_robust_candidate":best,
      "literal_profit_share_sensitivity_for_best":literal,
      "top_10_robust_candidates":robust[:10],
      "evidence":{"aux_gaps":evidence_gaps,"boundary_open_days":boundary,"strict_blockers":exact_blockers},
      "notes":[
        "No martingale; raw V1.7 opportunities are not filtered by money management.",
        "Equity starts once at $20 and is never reset daily in aggregation.",
        "Liquidation sensitivity loses isolated margin only; reserve survives when margin_fraction < 1.",
        "300/400/500ms confirmation latency variants are all tested."
      ]
    }
    Path(a.out).write_text(json.dumps(result,indent=2))
    compact={"strict":strict_verdict,"sensitivity":sensitivity_verdict,"trades":len(trades),
             "best":None if not best else {
               "config":best["key"],"target_all":best["target_all_latencies"],
               "primary_end":best["primary_400ms"]["end_equity"],
               "primary_100k":best["primary_400ms"]["milestones"]["100000"],
               "dd":best["primary_400ms"]["max_drawdown_pct"],
               "liq":best["primary_400ms"]["liquidations"]
             }}
    print("FINAL_7D",json.dumps(compact),flush=True)

if __name__=="__main__": main()
