#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,gzip,json,sys,time,urllib.request,zipfile,shutil
from collections import defaultdict
from pathlib import Path
import yaml
import v17_turboflow_econ as econ

BOOK_BASE="https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT"
TRADES_BASE="https://public.bybit.com/trading/BTCUSDT"

def download(url,dest):
    dest=Path(dest);dest.parent.mkdir(parents=True,exist_ok=True)
    if dest.exists() and dest.stat().st_size>1024:return
    tmp=Path(str(dest)+".part")
    if tmp.exists():tmp.unlink()
    print("DOWNLOAD",url,flush=True)
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=240) as r,open(tmp,"wb") as w:
        total=int(r.headers.get("Content-Length") or 0);got=0;nextp=32*1024*1024
        while True:
            b=r.read(1024*1024)
            if not b:break
            w.write(b);got+=len(b)
            if got>=nextp:
                print("  MB",round(got/1048576,1),"/",round(total/1048576,1) if total else "?",flush=True);nextp+=32*1024*1024
    tmp.replace(dest);print("SAVED",dest,round(dest.stat().st_size/1048576,2),"MiB",flush=True)

def decimal_seconds_to_us(s):
    s=str(s).strip()
    if "." in s:whole,frac=s.split(".",1)
    else:whole,frac=s,""
    frac=(frac+"000000")[:6]
    return int(whole)*1_000_000+int(frac)

class TradeStream:
    def __init__(self,path):
        self.f=gzip.open(path,"rt",encoding="utf-8",newline="");self.r=csv.DictReader(self.f);self.peek=None;self.rows=0;self.bad=0;self._adv()
    def _adv(self):
        self.peek=None
        while True:
            try:r=next(self.r)
            except StopIteration:return
            self.rows+=1
            try:
                us=decimal_seconds_to_us(r["timestamp"]);side=r["side"];qty=float(r["size"]);px=float(r["price"])
                if side not in ("Buy","Sell"):raise ValueError
                self.peek=(us,side,qty,px);return
            except Exception:self.bad+=1
    def consume_before(self,cutoff_us,eng):
        n=0
        while self.peek is not None and self.peek[0]<cutoff_us:
            us,side,qty,px=self.peek
            eng.on_trade({"price":px,"size":qty,"is_buy":side=="Buy"},us//1000);n+=1;self._adv()
        return n
    def close(self):self.f.close()

class Side:
    def __init__(self,bid):self.bid=bid;self.q={}
    def clear(self):self.q.clear()
    def set(self,px,sz):
        if sz==0:self.q.pop(px,None)
        else:self.q[px]=sz
    def best(self):
        if not self.q:return None
        return max(self.q) if self.bid else min(self.q)

class Book:
    def __init__(self):self.b=Side(True);self.a=Side(False);self.init=False
    def apply(self,obj):
        data=obj.get("data") or {};typ=obj.get("type")
        if typ=="snapshot":
            self.b.clear();self.a.clear()
            for p,s in data.get("b",[]):self.b.set(float(p),float(s))
            for p,s in data.get("a",[]):self.a.set(float(p),float(s))
            self.init=True
        elif typ=="delta":
            if not self.init:return False
            for p,s in data.get("b",[]):self.b.set(float(p),float(s))
            for p,s in data.get("a",[]):self.a.set(float(p),float(s))
        else:return False
        return bool(self.b.q and self.a.q)
    def mid(self):
        x=self.b.best();y=self.a.best()
        return (x+y)*.5 if x and y else None

def load_engine(root):
    src=Path(root)/"btc_scalper_v190"/"recovered_sources";sys.path.insert(0,str(src))
    from signal_engine import SignalEngine
    cfg=yaml.safe_load((src/"config_v183.yaml").read_text())
    cfg["signal"]["cost_aware"]["enabled"]=False;cfg["signal"]["cost_aware"]["minimum_harvest_profit_bps"]=0.0
    return SignalEngine,cfg

def replay(date,private_root,data_dir):
    root=Path(data_dir)/date
    bz=root/f"{date}_BTCUSDT_ob200.data.zip";tgz=root/f"BTCUSDT{date}.csv.gz"
    download(f"{BOOK_BASE}/{date}_BTCUSDT_ob200.data.zip",bz)
    download(f"{TRADES_BASE}/BTCUSDT{date}.csv.gz",tgz)
    SignalEngine,cfg=load_engine(private_root);eng=SignalEngine(cfg);book=Book();tr=TradeStream(tgz)
    interval=int(cfg["signal"]["evaluation_interval_ms"]);next_eval=None;pos=None;rows=[];counts=defaultdict(int);last_exit_side="NEUTRAL"
    stats={"book_messages":0,"json_bad":0,"trade_rows":0,"trade_bad":0};t0=time.time();last_mid=None

    def update_path(px):
        nonlocal pos
        if not pos or px is None:return
        r=(px/pos["entry"]-1)*(1 if pos["side"]=="BUY" else -1)
        pos["mfe"]=max(pos["mfe"],r);pos["mae"]=min(pos["mae"],r)

    def close(ts,px,reason):
        nonlocal pos,last_exit_side
        if not pos or px is None:return
        r=(px/pos["entry"]-1)*(1 if pos["side"]=="BUY" else -1)
        rows.append({"side":pos["side"],"entry":pos["entry"],"exit":px,"open_ms":pos["open_ms"],"close_ms":ts,
                     "ret":r,"mfe":pos["mfe"],"mae":pos["mae"],"reason":reason})
        last_exit_side=pos["side"];pos=None

    def transition(dec,px):
        nonlocal pos,last_exit_side
        if "->" not in dec.action:return
        old,new=dec.action.split("->",1);reason=dec.reason;counts["transitions"]+=1
        if old in ("BUY","SELL") and new=="NEUTRAL":
            close(dec.ts_ms,px,reason)
            if "EDGE_HARVEST" in reason.upper():counts["harvest_exits"]+=1
            if "FAILED_ENTRY_EXIT" in reason.upper():counts["failed_entry_exits"]+=1
        elif old in ("BUY","SELL") and new in ("BUY","SELL") and old!=new:
            close(dec.ts_ms,px,reason);counts["flips"]+=1
            pos={"side":new,"entry":px,"open_ms":dec.ts_ms,"mfe":0.0,"mae":0.0}
        elif old=="NEUTRAL" and new in ("BUY","SELL"):
            counts["entries"]+=1
            if last_exit_side==new:counts["same_side_reentries"]+=1
            pos={"side":new,"entry":px,"open_ms":dec.ts_ms,"mfe":0.0,"mae":0.0}

    with zipfile.ZipFile(bz,"r") as z:
        members=[n for n in z.namelist() if not n.endswith("/")]
        if not members:raise RuntimeError("empty orderbook zip")
        with z.open(members[0],"r") as f:
            for raw in f:
                stats["book_messages"]+=1
                try:obj=json.loads(raw.decode("utf-8"))
                except Exception:stats["json_bad"]+=1;continue
                try:cts=int(obj.get("cts"))
                except Exception:continue
                tr.consume_before(cts*1000,eng)
                if not book.apply(obj):continue
                eng.bids=dict(book.b.q);eng.asks=dict(book.a.q);eng.last_bookmap_ms=cts;eng.touch_bybit(cts)
                px=book.mid();last_mid=px;update_path(px)
                if next_eval is None:
                    next_eval=((cts+interval-1)//interval)*interval;eng.started_ms=next_eval
                if cts>=next_eval:
                    # One causal evaluation on the first real book state at/after each 100ms grid point.
                    dec=eng.evaluate(cts);transition(dec,px);counts["evals"]+=1
                    next_eval=cts+interval
                if stats["book_messages"]%500000==0:
                    print("PROGRESS",date,stats["book_messages"],"trades",len(rows),"trade_rows",tr.rows,"elapsed",round(time.time()-t0,1),flush=True)
    if pos and last_mid is not None:close(next_eval or 0,last_mid,"END_OF_DAY")
    stats["trade_rows"]=tr.rows;stats["trade_bad"]=tr.bad;tr.close();stats["elapsed_s"]=time.time()-t0
    return rows,dict(counts),stats

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--date",required=True);ap.add_argument("--private-root",default="private");ap.add_argument("--data-dir",default="data_direct");ap.add_argument("--out",required=True)
    a=ap.parse_args();trades,counts,meta=replay(a.date,a.private_root,a.data_dir);s=econ.summarize(trades,a.date)
    fidelity={}
    if a.date==econ.BENCHMARK_DATE:
        known={"trades":4212,"harvest_exits":3582,"failed_entry_exits":594,"flips":33,"same_side_reentries":1929}
        got={"trades":len(trades),**{k:counts.get(k,0) for k in known if k!="trades"}}
        fidelity={k:{"known":known[k],"got":got[k],"rel_err_pct":100*abs(got[k]-known[k])/max(known[k],1)} for k in known}
    fixed={"worked_example":econ.econ_fixed_qty(trades,0.01,literal=False),"literal_formula":econ.econ_fixed_qty(trades,0.01,literal=True)}
    full={str(L):{"worked_example":econ.econ_full_margin(trades,L,literal=False),"literal_formula":econ.econ_full_margin(trades,L,literal=True)} for L in (10,20,50,100)}
    result={"type":"V17_TURBOFLOW_ECON_DAY","date":a.date,"source":"Bybit public direct L2 orderbook.200 + public trades",
            "strategy":"recovered V1.7 proxy: V1.8 core with cost gate disabled; no historical OI/liquidation auxiliary feed",
            "summary":s,"counts":counts,"fidelity":fidelity,"fixed_0_01":fixed,"full_margin_scenarios":full,"meta":meta,"trades":trades}
    Path(a.out).write_text(json.dumps(result))
    print("DAY_RESULT",json.dumps({k:v for k,v in result.items() if k!="trades"}),flush=True)
if __name__=="__main__":main()
