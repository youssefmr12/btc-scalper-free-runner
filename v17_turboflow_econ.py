#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,gzip,heapq,json,math,shutil,sys,time,urllib.request
from collections import defaultdict
from pathlib import Path
import yaml

TARDIS="https://datasets.tardis.dev/v1/bybit"
BASE_RATE=0.10
RATE_MULT=15000.0
RATE_EXP=1.0
POS_MULT=1384.6
BUST_BUFFER=0.00036
BENCHMARK_DATE="2026-09-01"
BENCHMARK_START=20.0
BENCHMARK_ZERO_END=239.40

def download(url,dest):
    dest=Path(dest);dest.parent.mkdir(parents=True,exist_ok=True)
    if dest.exists() and dest.stat().st_size>1024:return
    tmp=Path(str(dest)+".part")
    if tmp.exists():tmp.unlink()
    print("DOWNLOAD",url,flush=True)
    with urllib.request.urlopen(url,timeout=240) as r,open(tmp,"wb") as w:
        shutil.copyfileobj(r,w,1024*1024)
    tmp.replace(dest);print("SAVED",dest,dest.stat().st_size,flush=True)

class Stream:
    def __init__(self,path,kind):
        self.kind=kind;self.f=gzip.open(path,"rt",encoding="utf-8",newline="");self.r=csv.DictReader(self.f);self.seq=0
    def __iter__(self):return self
    def __next__(self):
        while True:
            row=next(self.r);self.seq+=1
            try:ts=int(row["timestamp"])
            except Exception:continue
            return ts,self.seq,row
    def close(self):self.f.close()

def nx(it):
    try:return next(it)
    except StopIteration:return None

def book(row,levels=25):
    bids={};asks={}
    for i in range(levels):
        try:
            bp=float(row.get(f"bids[{i}].price") or 0);bs=float(row.get(f"bids[{i}].amount") or 0)
            ap=float(row.get(f"asks[{i}].price") or 0);az=float(row.get(f"asks[{i}].amount") or 0)
            if bp>0 and bs>0:bids[bp]=bs
            if ap>0 and az>0:asks[ap]=az
        except Exception:pass
    return bids,asks

def mid(bids,asks):
    return (max(bids)+min(asks))*0.5 if bids and asks else None

def load_engine(private_root):
    src=Path(private_root)/"btc_scalper_v190"/"recovered_sources"
    sys.path.insert(0,str(src))
    from signal_engine import SignalEngine
    cfg=yaml.safe_load((src/"config_v183.yaml").read_text())
    cfg["signal"]["cost_aware"]["enabled"]=False
    cfg["signal"]["cost_aware"]["minimum_harvest_profit_bps"]=0.0
    return SignalEngine,cfg

def replay(date,private_root,data_dir):
    y,m,d=date.split("-");root=Path(data_dir)/date
    specs={
      "book":("book_snapshot_25","BTCUSDT"),
      "trades":("trades","BTCUSDT"),
      "ticker":("derivative_ticker","BTCUSDT"),
      "liq":("liquidations","PERPETUALS"),
    }
    paths={}
    for k,(dtype,sym) in specs.items():
        p=root/f"{k}.csv.gz";u=f"{TARDIS}/{dtype}/{y}/{m}/{d}/{sym}.csv.gz"
        try:download(u,p);paths[k]=p
        except Exception as e:
            if k in ("book","trades"):raise
            print("OPTIONAL_MISSING",k,repr(e),flush=True)
    SignalEngine,cfg=load_engine(private_root);eng=SignalEngine(cfg)
    streams={k:Stream(p,k) for k,p in paths.items()};order={"book":0,"trades":1,"ticker":2,"liq":3}
    heap=[]
    for k,s in streams.items():
        x=nx(s)
        if x:heapq.heappush(heap,(x[0],order[k],x[1],k,x[2]))
    if not heap:raise RuntimeError("no events")
    first_ms=heap[0][0]//1000;interval=int(cfg["signal"]["evaluation_interval_ms"])
    next_eval=((first_ms+interval-1)//interval)*interval;eng.started_ms=next_eval
    pos=None;rows=[];counts=defaultdict(int);last_exit_side="NEUTRAL"
    cur_bids={};cur_asks={};events=evals=0;t0=time.time();last_mid=None

    def update_path(px):
        nonlocal pos
        if not pos or px is None:return
        raw=(px/pos["entry"]-1.0)*(1 if pos["side"]=="BUY" else -1)
        pos["mfe"]=max(pos["mfe"],raw);pos["mae"]=min(pos["mae"],raw)

    def close(ts_ms,px,reason):
        nonlocal pos,last_exit_side
        if not pos or px is None:return
        r=(px/pos["entry"]-1.0)*(1 if pos["side"]=="BUY" else -1)
        rows.append({"side":pos["side"],"entry":pos["entry"],"exit":px,"open_ms":pos["open_ms"],"close_ms":ts_ms,
                     "ret":r,"mfe":pos["mfe"],"mae":pos["mae"],"reason":reason})
        last_exit_side=pos["side"];pos=None

    def handle(dec,px):
        nonlocal pos,last_exit_side
        a=dec.action
        if "->" not in a:return
        old,new=a.split("->",1);reason=dec.reason
        counts["transitions"]+=1
        if old in ("BUY","SELL") and new=="NEUTRAL":
            close(dec.ts_ms,px,reason)
            if "EDGE_HARVEST" in reason.upper():counts["harvest_exits"]+=1
            if "FAILED_ENTRY_EXIT" in reason.upper():counts["failed_entry_exits"]+=1
        elif old in ("BUY","SELL") and new in ("BUY","SELL") and old!=new:
            close(dec.ts_ms,px,reason);counts["flips"]+=1
            if px is not None:pos={"side":new,"entry":px,"open_ms":dec.ts_ms,"mfe":0.0,"mae":0.0}
        elif old=="NEUTRAL" and new in ("BUY","SELL"):
            counts["entries"]+=1
            if last_exit_side==new:counts["same_side_reentries"]+=1
            if px is not None:pos={"side":new,"entry":px,"open_ms":dec.ts_ms,"mfe":0.0,"mae":0.0}

    while heap:
        ts_us,_,_,kind,row=heapq.heappop(heap);ts_ms=ts_us//1000
        while next_eval<ts_ms:
            if cur_bids and cur_asks:
                eng.touch_bybit(next_eval);px=mid(cur_bids,cur_asks);update_path(px);dec=eng.evaluate(next_eval);evals+=1;handle(dec,px)
            next_eval+=interval
        if kind=="book":
            b,a=book(row)
            if b and a:
                cur_bids=b;cur_asks=a;eng.bids=dict(b);eng.asks=dict(a);eng.last_bookmap_ms=ts_ms
                last_mid=mid(b,a);update_path(last_mid)
        elif kind=="trades":
            try:
                px=float(row["price"]);amt=float(row["amount"]);side=row["side"].lower()
                eng.on_trade({"price":px,"size":amt,"is_buy":side=="buy"},ts_ms)
            except Exception:pass
        elif kind=="ticker":
            try:
                oi=row.get("open_interest")
                if oi not in (None,""):eng.on_open_interest(float(oi),ts_ms)
            except Exception:pass
        elif kind=="liq":
            try:
                if row.get("symbol")=="BTCUSDT":
                    px=float(row["price"]);amt=float(row["amount"]);side=row["side"].lower()
                    eng.on_liquidation(px*amt,side,ts_ms)
            except Exception:pass
        eng.touch_bybit(ts_ms)
        if next_eval==ts_ms and cur_bids and cur_asks:
            px=mid(cur_bids,cur_asks);update_path(px);dec=eng.evaluate(ts_ms);evals+=1;handle(dec,px);next_eval+=interval
        z=nx(streams[kind])
        if z:heapq.heappush(heap,(z[0],order[kind],z[1],kind,z[2]))
        events+=1
        if events%1000000==0:print("PROGRESS",date,events,"trades",len(rows),"elapsed",round(time.time()-t0,1),flush=True)
    if pos and last_mid is not None:close(next_eval,last_mid,"END_OF_DAY")
    for s in streams.values():s.close()
    return rows,dict(counts),{"events":events,"evals":evals,"aux":sorted(paths.keys()),"elapsed_s":time.time()-t0}

def capture(move,notional,literal=False,leverage=1.0):
    if move<=0:return 0.0
    market=1.0/((move*RATE_MULT)**RATE_EXP)
    numerator=notional*(leverage if literal else 1.0)
    size=numerator/(1_000_000.0*move*POS_MULT)
    return (1.0-BASE_RATE)/(1.0+market+size)

def econ_fixed_qty(trades,qty,start=20.0,literal=False):
    eq=start;peak=start;maxdd=0.0;gp=gl=0.0;fund_h=0.0;hour_cross=0;liq_hits=0;max_req_lev=0.0
    for t in trades:
        entry=t["entry"];exit=t["exit"];direction=1 if t["side"]=="BUY" else -1
        notional=qty*entry;req_lev=notional/max(eq,1e-12);max_req_lev=max(max_req_lev,req_lev)
        # Approx Turbo low-size liquidation buffer from docs: MMR ~= 0.5*IMR, minus 0.036% trigger buffer.
        adverse=max(0.0,-float(t["mae"]));liq_threshold=max(0.0,0.5/max(req_lev,1e-12)-BUST_BUFFER)
        if req_lev>1 and adverse>=liq_threshold and liq_threshold>0:
            liq_hits+=1;eq=0.0;break
        gross=qty*direction*(exit-entry)
        move=abs(exit/entry-1.0)
        cap=capture(move,notional,literal=literal,leverage=req_lev) if gross>0 else 1.0
        pnl=gross*cap if gross>0 else gross
        eq+=pnl
        if pnl>0:gp+=pnl
        elif pnl<0:gl-=pnl
        hold=max(0,t["close_ms"]-t["open_ms"])/1000.0
        fund_h+=notional*hold/3600.0
        if t["open_ms"]//3600000 != t["close_ms"]//3600000:hour_cross+=1
        peak=max(peak,eq)
        if peak>0:maxdd=max(maxdd,(peak-eq)/peak*100)
        if eq<=0:break
    return {"qty_btc":qty,"end_equity_pre_funding":eq,"net_pre_funding":eq-start,"return_pct":(eq/start-1)*100,
            "profit_factor":gp/gl if gl>0 else None,"max_dd_pct":maxdd,"liquidation_hits":liq_hits,
            "max_required_leverage_full_margin":max_req_lev,"funding_notional_hours":fund_h,"hour_boundary_crossings":hour_cross,
            "formula":"literal" if literal else "official_worked_example"}

def econ_full_margin(trades,L,start=20.0,literal=False):
    eq=start;peak=start;maxdd=0;gp=gl=0;liq_hits=0;fund_h=0;cross=0
    for t in trades:
        entry=t["entry"];direction=1 if t["side"]=="BUY" else -1;notional=eq*L
        adverse=max(0,-float(t["mae"]));liq_threshold=max(0,0.5/L-BUST_BUFFER)
        if adverse>=liq_threshold:
            liq_hits+=1;eq=0;break
        r=direction*(t["exit"]-entry)/entry;gross=notional*r;move=abs(t["exit"]/entry-1)
        cap=capture(move,notional,literal=literal,leverage=L) if gross>0 else 1
        pnl=gross*cap if gross>0 else gross;eq+=pnl
        if pnl>0:gp+=pnl
        elif pnl<0:gl-=pnl
        hold=max(0,t["close_ms"]-t["open_ms"])/1000;fund_h+=notional*hold/3600
        if t["open_ms"]//3600000 != t["close_ms"]//3600000:cross+=1
        peak=max(peak,eq);maxdd=max(maxdd,(peak-eq)/peak*100 if peak>0 else 0)
        if eq<=0:break
    return {"leverage":L,"end_equity_pre_funding":eq,"net_pre_funding":eq-start,"return_pct":(eq/start-1)*100,
            "profit_factor":gp/gl if gl>0 else None,"max_dd_pct":maxdd,"liquidation_hits":liq_hits,
            "funding_notional_hours":fund_h,"hour_boundary_crossings":cross,
            "formula":"literal" if literal else "official_worked_example"}

def summarize(trades,date):
    wins=[t for t in trades if t["ret"]>0];loss=[t for t in trades if t["ret"]<0]
    return {"date":date,"trades":len(trades),"win_rate_pct":100*len(wins)/len(trades) if trades else 0,
            "sum_raw_bps":sum(t["ret"] for t in trades)*1e4,
            "gross_positive_bps":sum(max(0,t["ret"]) for t in trades)*1e4,
            "gross_negative_bps":sum(min(0,t["ret"]) for t in trades)*1e4,
            "avg_trade_bps":sum(t["ret"] for t in trades)*1e4/len(trades) if trades else 0,
            "avg_hold_s":sum((t["close_ms"]-t["open_ms"])/1000 for t in trades)/len(trades) if trades else 0,
            "max_hold_s":max(((t["close_ms"]-t["open_ms"])/1000 for t in trades),default=0),
            "sum_signed_px_delta_per_1btc":sum((1 if t["side"]=="BUY" else -1)*(t["exit"]-t["entry"]) for t in trades)}

def aggregate(root,out):
    root=Path(root);files=list(root.rglob("*.json"));ds={}
    for p in files:
        try:
            x=json.loads(p.read_text())
            if x.get("type")=="V17_TURBOFLOW_ECON_DAY":ds[x["date"]]=x
        except Exception:pass
    cal=ds.get(BENCHMARK_DATE);target=ds.get("2026-10-21")
    if not cal or not target:raise RuntimeError(f"missing dates have={list(ds)}")
    delta=cal["summary"]["sum_signed_px_delta_per_1btc"]
    qcal=(BENCHMARK_ZERO_END-BENCHMARK_START)/delta if delta>0 else None
    result={"type":"V17_TURBOFLOW_ECON_FINAL","calibration_date":BENCHMARK_DATE,"target_date":"2026-10-21",
            "benchmark":{"start":BENCHMARK_START,"zero_cost_end":BENCHMARK_ZERO_END,"trades":4212},
            "calibration_proxy":{"summary":cal["summary"],"fidelity":cal["fidelity"],"implied_fixed_qty_btc":qcal},
            "target_summary":target["summary"],"target_fixed_0_01":target["fixed_0_01"],
            "target_full_margin_scenarios":target["full_margin_scenarios"],
            "warnings":[
              "Replay is recovered V1.7 proxy (V1.8 core with V1.8 cost gate disabled), not byte-identical original V1.7 ZIP.",
              "TurboFlow historical BTC funding rate for 2026-10-21 was not available; outputs are pre-funding and report funding exposure separately.",
              "TurboFlow official API access currently requires approved beta access; >$100k cumulative volume is only eligibility to apply.",
              "Official TurboFlow docs have notation tension in size-impact formula; primary uses their worked-example interpretation and literal formula is also reported."
            ]}
    if qcal and qcal>0:
        tr=target["trades"]
        result["target_benchmark_calibrated"]={
          "qty_btc":qcal,
          "worked_example":econ_fixed_qty(tr,qcal,literal=False),
          "literal_formula":econ_fixed_qty(tr,qcal,literal=True)
        }
    Path(out).write_text(json.dumps(result,indent=2))
    print("FINAL",json.dumps(result),flush=True)

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--date");ap.add_argument("--private-root",default="private");ap.add_argument("--data-dir",default="data_v17_econ");ap.add_argument("--out",default="result.json");ap.add_argument("--aggregate")
    a=ap.parse_args()
    if a.aggregate:return aggregate(a.aggregate,a.out)
    if not a.date:raise SystemExit("--date required")
    trades,counts,meta=replay(a.date,a.private_root,a.data_dir);s=summarize(trades,a.date)
    fidelity={}
    if a.date==BENCHMARK_DATE:
        known={"trades":4212,"harvest_exits":3582,"failed_entry_exits":594,"flips":33,"same_side_reentries":1929}
        got={"trades":len(trades),**{k:counts.get(k,0) for k in known if k!="trades"}}
        fidelity={k:{"known":known[k],"got":got[k],"rel_err_pct":100*abs(got[k]-known[k])/max(known[k],1)} for k in known}
    fixed={"worked_example":econ_fixed_qty(trades,0.01,literal=False),"literal_formula":econ_fixed_qty(trades,0.01,literal=True)}
    full={}
    for L in (10,20,50,100):
        full[str(L)]={"worked_example":econ_full_margin(trades,L,literal=False),"literal_formula":econ_full_margin(trades,L,literal=True)}
    result={"type":"V17_TURBOFLOW_ECON_DAY","date":a.date,"source":"Tardis Bybit L2+trades+OI+liquidations where available",
            "strategy":"recovered V1.7 proxy: V1.8 core with cost gate disabled","summary":s,"counts":counts,"fidelity":fidelity,
            "fixed_0_01":fixed,"full_margin_scenarios":full,"meta":meta,"trades":trades}
    Path(a.out).write_text(json.dumps(result))
    print("DAY_RESULT",json.dumps({k:v for k,v in result.items() if k!="trades"}),flush=True)

if __name__=="__main__":main()
