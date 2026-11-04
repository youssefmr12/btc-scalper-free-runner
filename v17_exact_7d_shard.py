#!/usr/bin/env python3
from __future__ import annotations
import argparse, bisect, csv, gzip, json, math, os, sys, time, urllib.request, zipfile, tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests
import pyarrow.parquet as pq
import yaml

BOOK_BASE="https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT"
TRADES_BASE="https://public.bybit.com/trading/BTCUSDT"
CHD_BASE="https://api.cryptohftdata.com/v1/download?file=bybit"

def download(url,dest,optional=False,timeout=300):
    dest=Path(dest); dest.parent.mkdir(parents=True,exist_ok=True)
    if dest.exists() and dest.stat().st_size>64: return True
    tmp=Path(str(dest)+".part")
    if tmp.exists(): tmp.unlink()
    for attempt in range(6):
        try:
            req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req,timeout=timeout) as r, open(tmp,"wb") as w:
                while True:
                    b=r.read(1024*1024)
                    if not b: break
                    w.write(b)
            tmp.replace(dest)
            return True
        except Exception as e:
            code=getattr(e,"code",None)
            if optional and code==404: return False
            if attempt==5:
                if optional:
                    print("OPTIONAL_DOWNLOAD_FAIL",url,repr(e),flush=True); return False
                raise
            time.sleep(min(20,2**attempt))
    return False

def to_ms(v):
    if v is None: return None
    if isinstance(v,datetime):
        if v.tzinfo is None: v=v.replace(tzinfo=timezone.utc)
        return int(v.timestamp()*1000)
    if hasattr(v,"timestamp") and not isinstance(v,(int,float,str)):
        try: return int(v.timestamp()*1000)
        except: pass
    if isinstance(v,str):
        try:
            z=v.replace("Z","+00:00")
            return int(datetime.fromisoformat(z).timestamp()*1000)
        except: pass
    x=float(v)
    ax=abs(x)
    if ax>1e17: return int(x/1e6)      # ns
    if ax>1e14: return int(x/1e3)      # us
    if ax>1e11: return int(x)           # ms
    return int(x*1000)                  # sec

def pick(d,*names):
    for n in names:
        if n in d and d[n] is not None: return d[n]
    return None

def load_parquet_rows(path):
    t=pq.read_table(path)
    return t.to_pylist()

def load_aux(date,root):
    events=[]
    gaps=[]
    for hour in range(24):
        hh=f"{hour:02d}"
        for typ in ("open_interest","liquidations","mark_price"):
            p=Path(root)/date/hh/f"{typ}.parquet"
            u=f"{CHD_BASE}/{date}/{hh}/BTCUSDT_{typ}.parquet"
            ok=download(u,p,optional=True,timeout=120)
            time.sleep(1.25)
            if not ok:
                gaps.append({"hour":hh,"type":typ,"status":"missing"})
                continue
            try:
                rows=load_parquet_rows(p)
            except Exception as e:
                gaps.append({"hour":hh,"type":typ,"status":"parse_error","error":repr(e)})
                continue
            for r in rows:
                if typ=="liquidations":
                    ts=to_ms(pick(r,"trade_time","event_time","received_time","timestamp"))
                    side=str(pick(r,"side","liquidation_side") or "").upper()
                    px=float(pick(r,"price","bankruptcy_price","mark_price") or 0)
                    qty=float(pick(r,"quantity","size","qty") or 0)
                    if ts is not None and side in ("BUY","SELL") and px>0 and qty>0:
                        events.append((ts,1,{"side":side,"notional":px*qty,"price":px,"qty":qty}))
                elif typ=="open_interest":
                    ts=to_ms(pick(r,"event_time","received_time","timestamp"))
                    oi=pick(r,"sum_open_interest","open_interest","openInterest")
                    if ts is not None and oi is not None:
                        try: events.append((ts,2,{"oi":float(oi)}))
                        except: pass
                else:
                    ts=to_ms(pick(r,"event_time","received_time","timestamp"))
                    mp=pick(r,"mark_price","markPrice","price")
                    ip=pick(r,"index_price","indexPrice")
                    fr=pick(r,"funding_rate","fundingRate")
                    if ts is not None and mp is not None:
                        try:
                            events.append((ts,0,{
                                "mark":float(mp),
                                "index":None if ip is None else float(ip),
                                "funding_rate":None if fr is None else float(fr),
                            }))
                        except: pass
    events.sort(key=lambda x:(x[0],x[1]))
    return events,gaps

def decimal_seconds_to_us(s):
    s=str(s).strip()
    if "." in s: whole,frac=s.split(".",1)
    else: whole,frac=s,""
    frac=(frac+"000000")[:6]
    return int(whole)*1_000_000+int(frac)

class TradeStream:
    def __init__(self,path):
        self.f=gzip.open(path,"rt",encoding="utf-8",newline="")
        self.r=csv.DictReader(self.f); self.peek=None; self.rows=0; self.bad=0
        self._adv()
    def _adv(self):
        self.peek=None
        while True:
            try: r=next(self.r)
            except StopIteration: return
            self.rows+=1
            try:
                us=decimal_seconds_to_us(r["timestamp"]); side=r["side"]
                qty=float(r["size"]); px=float(r["price"])
                if side not in ("Buy","Sell"): raise ValueError
                self.peek=(us,side,qty,px); return
            except Exception: self.bad+=1
    def consume_before(self,cutoff_us,eng):
        while self.peek is not None and self.peek[0]<cutoff_us:
            us,side,qty,px=self.peek
            eng.on_trade({"price":px,"size":qty,"is_buy":side=="Buy"},us//1000)
            self._adv()
    def close(self): self.f.close()

class Side:
    def __init__(self,bid): self.bid=bid; self.q={}
    def clear(self): self.q.clear()
    def set(self,px,sz):
        if sz==0: self.q.pop(px,None)
        else: self.q[px]=sz
    def best(self):
        if not self.q: return None
        return max(self.q) if self.bid else min(self.q)

class Book:
    def __init__(self): self.b=Side(True); self.a=Side(False); self.init=False
    def apply(self,obj):
        data=obj.get("data") or {}; typ=obj.get("type")
        if typ=="snapshot":
            self.b.clear(); self.a.clear()
            for p,s in data.get("b",[]): self.b.set(float(p),float(s))
            for p,s in data.get("a",[]): self.a.set(float(p),float(s))
            self.init=True
        elif typ=="delta":
            if not self.init: return False
            for p,s in data.get("b",[]): self.b.set(float(p),float(s))
            for p,s in data.get("a",[]): self.a.set(float(p),float(s))
        else: return False
        return bool(self.b.q and self.a.q)
    def mid(self):
        b=self.b.best(); a=self.a.best()
        return (b+a)*.5 if b and a else None

def load_engine(core_dir):
    core=Path(core_dir)
    sys.path.insert(0,str(core))
    from signal_engine import SignalEngine
    cfg=yaml.safe_load((core/"config.yaml").read_text())
    return SignalEngine,cfg

def asof_after(times,vals,ts):
    i=bisect.bisect_left(times,ts)
    if i>=len(times): return None,None
    return times[i],vals[i]

def path_extremes(times,refs,start,end,side,entry):
    i=bisect.bisect_left(times,start); j=bisect.bisect_right(times,end)
    if i>=j: return None
    seg=refs[i:j]; tseg=times[i:j]
    if not seg: return None
    sign=1 if side=="BUY" else -1
    rets=[(p/entry-1)*sign for p in seg]
    mi=min(range(len(rets)),key=rets.__getitem__)
    ma=max(range(len(rets)),key=rets.__getitem__)
    return {
        "mae":rets[mi],"mae_ts_ms":tseg[mi],"mae_ref":seg[mi],
        "mfe":rets[ma],"mfe_ts_ms":tseg[ma],"mfe_ref":seg[ma]
    }

def replay(date,core_dir,data_dir):
    root=Path(data_dir)/date
    bz=root/f"{date}_BTCUSDT_ob200.data.zip"
    tgz=root/f"BTCUSDT{date}.csv.gz"
    download(f"{BOOK_BASE}/{date}_BTCUSDT_ob200.data.zip",bz,timeout=600)
    download(f"{TRADES_BASE}/BTCUSDT{date}.csv.gz",tgz,timeout=300)
    aux,gaps=load_aux(date,Path(data_dir)/"aux")
    SignalEngine,cfg=load_engine(core_dir)
    eng=SignalEngine(cfg); book=Book(); tr=TradeStream(tgz)
    interval=int(cfg["signal"]["evaluation_interval_ms"])
    next_eval=None; pos=None; raw=[]; counts=defaultdict(int)
    sample_t=[]; sample_ref=[]; sample_mid=[]; sample_index=[]
    aux_i=0; current_mark=None; current_index=None; current_funding=None
    last_mid=None; first_cts=None; last_cts=None
    day_start=int(datetime.fromisoformat(date+"T00:00:00+00:00").timestamp()*1000)
    warm_until=day_start+30_000

    def consume_aux(cut):
        nonlocal aux_i,current_mark,current_index,current_funding
        while aux_i<len(aux) and aux[aux_i][0]<=cut:
            ts,k,d=aux[aux_i]
            if k==0:
                current_mark=d["mark"]; current_index=d.get("index"); current_funding=d.get("funding_rate")
            elif k==1:
                eng.on_liquidation(d["notional"],d["side"].lower(),ts)
            elif k==2:
                eng.on_open_interest(d["oi"],ts)
            aux_i+=1

    def close_trade(ts,mid,reason):
        nonlocal pos
        if not pos: return
        pos["exit_signal_ms"]=ts; pos["exit_signal_mid"]=mid; pos["exit_reason"]=reason
        raw.append(pos); pos=None

    def transition(dec,mid):
        nonlocal pos
        if "->" not in dec.action: return
        old,new=dec.action.split("->",1); counts["transitions"]+=1
        if old in ("BUY","SELL") and new=="NEUTRAL":
            close_trade(dec.ts_ms,mid,dec.reason)
        elif old in ("BUY","SELL") and new in ("BUY","SELL") and old!=new:
            close_trade(dec.ts_ms,mid,dec.reason)
            pos={"side":new,"entry_signal_ms":dec.ts_ms,"entry_signal_mid":mid,
                 "entry_reason":dec.reason,"buy_score":getattr(dec,"buy_score",None),
                 "sell_score":getattr(dec,"sell_score",None)}
        elif old=="NEUTRAL" and new in ("BUY","SELL"):
            counts["entries"]+=1
            pos={"side":new,"entry_signal_ms":dec.ts_ms,"entry_signal_mid":mid,
                 "entry_reason":dec.reason,"buy_score":getattr(dec,"buy_score",None),
                 "sell_score":getattr(dec,"sell_score",None)}

    with zipfile.ZipFile(bz,"r") as z:
        members=[n for n in z.namelist() if not n.endswith("/")]
        if not members: raise RuntimeError("empty orderbook zip")
        with z.open(members[0],"r") as f:
            for rawline in f:
                try: obj=json.loads(rawline.decode("utf-8"))
                except: counts["json_bad"]+=1; continue
                try: cts=int(obj.get("cts"))
                except: continue
                if first_cts is None: first_cts=cts
                last_cts=cts
                tr.consume_before(cts*1000,eng)
                consume_aux(cts)
                if not book.apply(obj): continue
                eng.bids=dict(book.b.q); eng.asks=dict(book.a.q)
                eng.last_bookmap_ms=cts; eng.touch_bybit(cts)
                mid=book.mid(); last_mid=mid
                if next_eval is None:
                    next_eval=((cts+interval-1)//interval)*interval
                    eng.started_ms=next_eval
                if cts>=next_eval:
                    ref=current_mark if current_mark and current_mark>0 else mid
                    if ref:
                        sample_t.append(cts); sample_ref.append(ref); sample_mid.append(mid or ref)
                        sample_index.append(current_index if current_index and current_index>0 else ref)
                    dec=eng.evaluate(cts); counts["evals"]+=1
                    transition(dec,mid)
                    next_eval=cts+interval
    tr.close()

    boundary_open=None
    if pos:
        boundary_open=dict(pos)
        counts["open_at_end"]=1

    outtr=[]
    for t in raw:
        if t["entry_signal_ms"]<warm_until:
            counts["warmup_trades_excluded"]+=1
            continue
        x={k:v for k,v in t.items()}
        x["latency"]={}
        for lat in (300,400,500):
            ets,ep=asof_after(sample_t,sample_ref,t["entry_signal_ms"]+lat)
            xts,xp=asof_after(sample_t,sample_ref,t["exit_signal_ms"]+lat)
            if ep is None or xp is None or xts is None or ets is None or xts<=ets:
                x["latency"][str(lat)]={"valid":False}
                continue
            ext=path_extremes(sample_t,sample_ref,ets,xts,t["side"],ep)
            ret=(xp/ep-1)*(1 if t["side"]=="BUY" else -1)
            x["latency"][str(lat)]={"valid":True,"entry_exec_ms":ets,"exit_exec_ms":xts,
                                   "entry_ref":ep,"exit_ref":xp,"hold_ms":xts-ets,"ret":ret,**(ext or {})}
        outtr.append(x)

    # funding proxy samples: hourly average premium from Bybit mark/index proxy
    buckets=defaultdict(list)
    for ts,mk,ix in zip(sample_t,sample_ref,sample_index):
        if ix and ix>0:
            buckets[ts//3_600_000].append(mk/ix-1)
    funding_proxy=[]
    for h,arr in sorted(buckets.items()):
        avg=sum(arr)/len(arr)
        clamp=max(-0.0005,min(0.0005,0.0000125-avg))
        fr=max(-0.04,min(0.04,avg+clamp))
        funding_proxy.append({"hour":h,"avg_premium":avg,"rate":fr,"samples":len(arr)})

    result={
        "type":"V17_EXACT_7D_SHARD","date":date,
        "engine":"exact V1.7.1 EDGE-HARVEST package; signal logic frozen",
        "execution_reference":"CryptoHFTData Bybit mark/index proxy when available; Bybit L2 midpoint fallback",
        "warmup_ms":30000,
        "counts":dict(counts),
        "data":{"first_book_ms":first_cts,"last_book_ms":last_cts,"trade_rows":tr.rows,
                "trade_bad":tr.bad,"aux_events":len(aux),"aux_gaps":gaps},
        "boundary_open_position":boundary_open,
        "funding_proxy_hourly":funding_proxy,
        "trades":outtr
    }
    return result

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--date",required=True); ap.add_argument("--core-dir",required=True)
    ap.add_argument("--data-dir",default="data_exact"); ap.add_argument("--out",required=True)
    a=ap.parse_args()
    r=replay(a.date,a.core_dir,a.data_dir)
    Path(a.out).parent.mkdir(parents=True,exist_ok=True)
    Path(a.out).write_text(json.dumps(r,separators=(",",":")))
    print("SHARD_RESULT",json.dumps({"date":a.date,"trades":len(r["trades"]),"counts":r["counts"],
          "aux_gaps":len(r["data"]["aux_gaps"]),"boundary_open":bool(r["boundary_open_position"])}),flush=True)

if __name__=="__main__": main()
