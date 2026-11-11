#!/usr/bin/env python3
import os, time, math, json, collections, datetime as dt
from pathlib import Path
import requests
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sortedcontainers import SortedDict
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline

DATE=os.getenv("TEST_DATE","2026-11-10"); START_EQUITY=float(os.getenv("START_EQUITY","5"))
LEVERAGE=50.; TAKER_DELAY_MS=300; MIN_NOTIONAL=10.; MAINT_MARGIN=.012; LIQ_FEE_P=.01
TICK_MS=100; WARMUP_HOURS=2; TARGET_BPS=10.; STOP_BPS=2.5; HORIZON_MS=10000
RETRAIN_MS=30*60*1000; COOLDOWN_MS=700
DATA=Path("rs11_24h_data"); OUT=Path("rs11_24h_output"); DATA.mkdir(exist_ok=True); OUT.mkdir(exist_ok=True)
BASE="https://api.cryptohftdata.com/v1/download?file="

def dts(s): return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)
def ns(x): return int(x.timestamp()*1e9)
def bps(a,b): return (b/a-1)*10000 if a else 0.
def download(path,local):
    if local.exists() and local.stat().st_size>500:return
    for k in range(8):
        try:
            r=requests.get(BASE+path,timeout=180,allow_redirects=True)
            if r.status_code==200 and len(r.content)>500: local.write_bytes(r.content); return
            print("DOWNLOAD_RETRY",path,r.status_code,len(r.content),flush=True)
        except Exception as e: print("DOWNLOAD_ERR",path,repr(e),flush=True)
        time.sleep(min(20,2+k*2))
    raise RuntimeError("download failed "+path)
def fetch_all():
    h=dts(DATE+"T00:00:00")-dt.timedelta(hours=WARMUP_HOURS); end=dts(DATE+"T23:00:00"); req=0
    while h<=end:
        ds=h.strftime("%Y-%m-%d"); hh=h.strftime("%H")
        for ex,sym,typ in [("lighter","BTC","orderbook"),("lighter","BTC","trades"),("lighter","BTC","mark_price"),
                           ("binance_futures","BTCUSDT","trades"),("bybit","BTCUSDT","trades")]:
            p=f"{ex}/{ds}/{hh}/{sym}_{typ}.parquet"; local=DATA/f"{ex}_{ds}_{hh}_{sym}_{typ}.parquet"
            if not local.exists():
                if req: time.sleep(1.05)
                download(p,local); req+=1
        h+=dt.timedelta(hours=1)

class TradeStream:
    def __init__(self,path):
        t=pq.read_table(path,columns=["received_time","price","quantity","is_buyer_maker"])
        self.ts=t["received_time"].to_numpy(zero_copy_only=False).astype(np.int64)
        self.p=np.array(t["price"].to_pylist(),dtype=float); self.q=np.array(t["quantity"].to_pylist(),dtype=float)
        self.m=np.array(t["is_buyer_maker"].to_pylist(),dtype=bool); self.i=0; self.n=len(self.ts)
    def until(self,ts_ns,dq,ph=None):
        i=self.i
        while i<self.n and self.ts[i]<=ts_ns:
            if self.p[i]>0 and self.q[i]>0:
                s=-1. if self.m[i] else 1.; n=self.p[i]*self.q[i]
                dq.append((int(self.ts[i]),s*n,n,float(self.p[i])))
                if ph is not None: ph.append((int(self.ts[i]),float(self.p[i])))
            i+=1
        self.i=i

class BookStream:
    def __init__(self,path):
        t=pq.read_table(path,columns=["received_time","event_type","final_update_id","prev_final_update_id","side","price","quantity"])
        self.ts=t["received_time"].to_numpy(zero_copy_only=False).astype(np.int64)
        self.et=np.array(t["event_type"].to_pylist(),dtype=object)
        self.fid=np.array([-1 if x is None else int(x) for x in t["final_update_id"].to_pylist()],dtype=np.int64)
        self.prev=np.array([-1 if x is None else int(x) for x in t["prev_final_update_id"].to_pylist()],dtype=np.int64)
        self.side=np.array(t["side"].to_pylist(),dtype=object)
        self.p=np.array(t["price"].to_pylist(),dtype=float); self.q=np.array(t["quantity"].to_pylist(),dtype=float)
        self.i=0; self.n=len(self.ts)

class Book:
    def __init__(self):
        self.bids=SortedDict(); self.asks=SortedDict(); self.valid=False; self.group=None
        self.group_type=None; self.group_fid=None; self.last_fid=None; self.gaps=0; self.crosses=0; self.snapshots=0
    def finish(self):
        if self.group is None:return
        if self.group_type=="snapshot": self.valid=True; self.snapshots+=1
        if self.group_fid is not None and self.group_fid>=0:self.last_fid=self.group_fid
    def apply_until(self,bs,ts_ns):
        i=bs.i
        while i<bs.n and bs.ts[i]<=ts_ns:
            key=(int(bs.ts[i]),str(bs.et[i]),int(bs.fid[i]))
            if key!=self.group:
                self.finish(); typ=str(bs.et[i]); fid=int(bs.fid[i]); prev=int(bs.prev[i])
                if typ=="snapshot": self.bids.clear(); self.asks.clear(); self.valid=False
                elif self.valid and prev>=0 and self.last_fid is not None and prev!=self.last_fid:
                    self.valid=False; self.gaps+=1
                self.group=key; self.group_type=typ; self.group_fid=fid
            p=float(bs.p[i]); q=float(bs.q[i]); d=self.bids if str(bs.side[i]).lower().startswith("bid") else self.asks
            if q<=0: d.pop(p,None)
            else: d[p]=q
            i+=1
        bs.i=i
        if self.bids and self.asks and self.bids.peekitem(-1)[0]>=self.asks.peekitem(0)[0]:
            self.valid=False; self.crosses+=1
        return self.valid
    def top(self,n=5):
        if not self.valid or not self.bids or not self.asks:return None
        return list(reversed(self.bids.items()))[:n],list(self.asks.items())[:n]
    def mid(self):
        if not self.valid:return None
        return (self.bids.peekitem(-1)[0]+self.asks.peekitem(0)[0])/2
    def spread_bps(self):
        bb=self.bids.peekitem(-1)[0]; ba=self.asks.peekitem(0)[0]; return (ba-bb)/((ba+bb)/2)*10000
    def fill(self,side,notional=None,qty=None):
        if not self.valid:return None
        lv=list(self.asks.items()) if side=="BUY" else list(reversed(self.bids.items()))
        got=cash=0.; rq=qty; rn=notional
        for p,q in lv:
            if qty is not None:
                take=min(q,rq); got+=take; cash+=take*p; rq-=take
                if rq<=1e-12:break
            else:
                takecash=min(q*p,rn); got+=takecash/p; cash+=takecash; rn-=takecash
                if rn<=1e-9:break
        ok=(qty is not None and rq<=1e-9) or (qty is None and rn<=1e-6)
        return (cash/got,got) if ok and got>0 else None

class MarkStream:
    def __init__(self,path):
        t=pq.read_table(path); names=t.column_names
        self.ts=t["received_time"].to_numpy(zero_copy_only=False).astype(np.int64)
        self.rate=np.array([float(x) if x is not None else np.nan for x in t["funding_rate"].to_pylist()],dtype=float)
        self.i=0; self.n=len(self.ts)
    def until(self,ts_ns,cur):
        while self.i<self.n and self.ts[self.i]<=ts_ns:
            if np.isfinite(self.rate[self.i]):cur=float(self.rate[self.i])
            self.i+=1
        return cur

def trim(dq,cut):
    while dq and dq[0][0]<cut:dq.popleft()
def flow(dq,now,ms):
    cut=now-ms*1_000_000; s=tot=0.
    for t,sv,n,p in reversed(dq):
        if t<cut:break
        s+=sv; tot+=n
    return s/tot if tot else 0.
def old_price(ph,now,ms):
    if not ph:return None,None
    cur=ph[-1][1]; cut=now-ms*1_000_000; old=ph[0][1]
    for t,p in reversed(ph):
        if t<=cut: old=p; break
    return cur,old

class Candidate:
    def __init__(self,t,side,mid,x):self.t=t;self.side=side;self.mid=mid;self.x=np.array(x,float);self.resolved=False;self.y=None
class Position:
    def __init__(self,side,sig,fill,entry,qty,notional,bal,smid):
        self.side=side;self.signal_t=sig;self.fill_t=fill;self.entry=entry;self.qty=qty;self.notional=notional
        self.balance_before=bal;self.signal_mid=smid;self.mfe=-1e9;self.mae=1e9;self.exhaust=0;self.funding=0.

class Replay:
    def __init__(self):
        self.balance=START_EQUITY;self.peak=START_EQUITY;self.maxdd=0.;self.trades=[];self.pos=None
        self.pe=None;self.px=None;self.cands=[];self.model=None;self.thr=.58;self.next_train=None;self.last_cand=0
        self.prev_score=0.;self.scores=collections.deque(maxlen=1800);self.last_depth=None;self.invalid=0;self.missed=0
        self.liq=0;self.funding=0.;self.fund_events=0;self.slip_sig=[];self.slip_mid=[];self.retrains=[];self.accepted=0;self.ncand=0
    def train(self,now):
        z=[c for c in self.cands if c.resolved and c.t>=now-2*3600*1e9 and c.t<now-10*1e9]
        if len(z)<80 or len({c.y for c in z})<2:return
        X=np.stack([c.x for c in z]);y=np.array([c.y for c in z])
        m=make_pipeline(StandardScaler(),LogisticRegression(max_iter=300,class_weight="balanced",C=.5,random_state=7));m.fit(X,y)
        probs=m.predict_proba(X)[:,1]; self.thr=float(np.clip(np.quantile(probs,.66),.53,.78)); self.model=m
        self.retrains.append({"t":int(now),"n":len(y),"pos":float(y.mean()),"thr":self.thr})
    def resolve(self,now,mid):
        for c in self.cands[-5000:]:
            if c.resolved:continue
            mv=bps(c.mid,mid)*c.side
            if mv>=TARGET_BPS:c.y=1;c.resolved=True
            elif mv<=-STOP_BPS or now-c.t>=HORIZON_MS*1e6:c.y=0;c.resolved=True
    def candidate(self,now,mid,x,score):
        self.scores.append(abs(score))
        if len(self.scores)<150:return None
        q=float(np.quantile(np.fromiter(self.scores,float),.985)); d=1 if score>0 else -1
        crossed=(abs(score)>=q and abs(self.prev_score)<q) or (score*self.prev_score<0 and abs(score)>=q);self.prev_score=score
        if not crossed or now-self.last_cand<COOLDOWN_MS*1e6:return None
        if d*(x[1]+x[2]+.5*x[9]+.5*x[10])<=-.05:return None
        self.last_cand=now;self.ncand+=1;c=Candidate(now,d,mid,x);self.cands.append(c)
        if self.model is None:return None
        p=float(self.model.predict_proba(c.x.reshape(1,-1))[0,1]);return (d,p) if p>=self.thr else None
    def dd(self,mid):
        eq=self.balance+(self.pos.qty*(mid-self.pos.entry)*self.pos.side if self.pos else 0);self.peak=max(self.peak,eq)
        self.maxdd=max(self.maxdd,(self.peak-eq)/self.peak*100 if self.peak>0 else 100)
    def apply_funding(self,r):
        if self.pos and np.isfinite(r):
            a=-self.pos.side*self.pos.notional*r;self.balance+=a;self.pos.funding+=a;self.funding+=a;self.fund_events+=1
    def finish(self,now,px,reason,fee=0):
        p=self.pos;pricep=p.qty*(px-p.entry)*p.side
        if reason!="LIQ":self.balance+=pricep
        self.trades.append({"signal_t":p.signal_t,"fill_t":p.fill_t,"exit_t":now,"side":p.side,"entry":p.entry,"exit":px,"qty":p.qty,
          "notional":p.notional,"gross_bps":bps(p.entry,px)*p.side,"pnl_price":pricep,"funding":p.funding,"reason":reason,
          "hold_s":(now-p.fill_t)/1e9,"balance_before":p.balance_before,"balance_after":self.balance,"mfe_bps":p.mfe,"mae_bps":p.mae})
        self.pos=None;self.px=None
    def exec_entry(self,now,book,mid):
        if not self.pe or now<self.pe["at"]:return
        pe=self.pe;self.pe=None;n=self.balance*LEVERAGE
        if n<MIN_NOTIONAL:self.missed+=1;return
        f=book.fill("BUY" if pe["d"]==1 else "SELL",notional=n)
        if not f:self.missed+=1;return
        px,q=f;self.pos=Position(pe["d"],pe["sig"],now,px,q,n,self.balance,pe["mid"]);self.accepted+=1
        self.slip_sig.append(bps(pe["mid"],px)*pe["d"]);self.slip_mid.append(bps(mid,px)*pe["d"])
    def exec_exit(self,now,book):
        if not self.px or now<self.px["at"] or not self.pos:return
        f=book.fill("SELL" if self.pos.side==1 else "BUY",qty=self.pos.qty)
        if f:self.finish(now,f[0],self.px["why"])
        else:self.missed+=1
    def liquidate(self,now,mid,book):
        if not self.pos:return False
        u=self.pos.qty*(mid-self.pos.entry)*self.pos.side
        if self.balance+u>self.pos.notional*MAINT_MARGIN:return False
        f=book.fill("SELL" if self.pos.side==1 else "BUY",qty=self.pos.qty)
        if not f:return False
        pp=self.pos.qty*(f[0]-self.pos.entry)*self.pos.side;fee=self.pos.notional*LIQ_FEE_P;self.balance+=pp-fee;self.liq+=1
        self.finish(now,f[0],"LIQ",fee);return True
    def tick(self,now,book,mid,x,score,t0,t1):
        self.resolve(now,mid)
        if self.next_train is None:self.next_train=((now//int(RETRAIN_MS*1e6))+1)*int(RETRAIN_MS*1e6)
        if now>=self.next_train:self.train(now);self.next_train+=int(RETRAIN_MS*1e6)
        self.exec_entry(now,book,mid);self.exec_exit(now,book)
        if self.pos:
            mv=bps(self.pos.entry,mid)*self.pos.side;self.pos.mfe=max(self.pos.mfe,mv);self.pos.mae=min(self.pos.mae,mv)
            if self.liquidate(now,mid,book):return
            if self.px is None:
                why=None
                if mv<=-STOP_BPS:why="STOP"
                elif mv>=TARGET_BPS:why="TARGET"
                elif now-self.pos.fill_t>=HORIZON_MS*1e6:why="TIME"
                elif now-self.pos.fill_t>=1.5e9 and self.pos.mfe>=1:
                    self.pos.exhaust=self.pos.exhaust+1 if score*self.pos.side<0 else max(0,self.pos.exhaust-1)
                    if self.pos.exhaust>=3:why="EXHAUST"
                if why:self.px={"at":now+TAKER_DELAY_MS*1_000_000,"why":why}
            self.dd(mid);return
        if self.pe:return
        c=self.candidate(now,mid,x,score)
        if c and t0<=now<t1:self.pe={"at":now+TAKER_DELAY_MS*1_000_000,"d":c[0],"sig":now,"mid":mid}
        self.dd(mid)

def depthfeat(book,prev):
    z=book.top(5)
    if not z:return None
    bi,as_=z;bq=sum(q for p,q in bi);aq=sum(q for p,q in as_);den=bq+aq;qimb=(bq-aq)/den if den else 0
    bb,bs=bi[0];ba,aa=as_[0];micro=((ba*bs+bb*aa)/(bs+aa)-(bb+ba)/2)/max((ba-bb)/2,1e-9)
    ofi=depl=0
    if prev:
        pb,pa=prev;sc=max(pb+pa,1e-9);ofi=((bq-pb)-(aq-pa))/sc;depl=((pa-aq)-(pb-bq))/sc
    return qimb,float(np.clip(micro,-3,3)),float(np.clip(ofi,-3,3)),float(np.clip(depl,-3,3)),(bq,aq)

def read_hour(h):
    ds=h.strftime("%Y-%m-%d");hh=h.strftime("%H")
    p=lambda e,s,t:DATA/f"{e}_{ds}_{hh}_{s}_{t}.parquet"
    return BookStream(p("lighter","BTC","orderbook")),TradeStream(p("lighter","BTC","trades")),MarkStream(p("lighter","BTC","mark_price")),TradeStream(p("binance_futures","BTCUSDT","trades")),TradeStream(p("bybit","BTCUSDT","trades"))

def run():
    fetch_all();ts0=dts(DATE+"T00:00:00");ts1=ts0+dt.timedelta(days=1);h=ts0-dt.timedelta(hours=WARMUP_HOURS)
    rep=Replay();book=Book();lq=collections.deque();bq=collections.deque();yq=collections.deque();lp=collections.deque();bp=collections.deque();yp=collections.deque();mids=collections.deque()
    curfund=0.;nextfund=None;hourly=[]
    while h<ts1:
        print("LOAD_HOUR",h.isoformat(),flush=True);bs,lt,ms,bt,yt=read_hour(h);a=ns(h);b=ns(h+dt.timedelta(hours=1));tick=a
        if nextfund is None:nextfund=((a//int(3600e9))+1)*int(3600e9)
        n0=len(rep.trades);bal0=rep.balance
        while tick<b:
            ok=book.apply_until(bs,tick);lt.until(tick,lq,lp);bt.until(tick,bq,bp);yt.until(tick,yq,yp);curfund=ms.until(tick,curfund)
            for q in (lq,bq,yq):trim(q,tick-5_000_000_000)
            for q in (lp,bp,yp):
                while q and q[0][0]<tick-5_000_000_000:q.popleft()
            if tick>=nextfund:rep.apply_funding(curfund);nextfund+=int(3600e9)
            if not ok:rep.invalid+=1;tick+=TICK_MS*1_000_000;continue
            mid=book.mid();df=depthfeat(book,rep.last_depth)
            if mid is None or df is None:tick+=TICK_MS*1_000_000;continue
            qimb,micro,ofi,depl,rep.last_depth=df;mids.append((tick,mid))
            while mids and mids[0][0]<tick-5_000_000_000:mids.popleft()
            def mom(msw):
                cut=tick-msw*1_000_000;old=mids[0][1]
                for tt,mm in mids:
                    if tt>=cut:old=mm;break
                return bps(old,mid)
            def xm(ph,w):
                c,o=old_price(ph,tick,w);return bps(o,c) if c and o else 0.
            lf=flow(lq,tick,1000);bf=flow(bq,tick,1000);yf=flow(yq,tick,1000);m05=mom(500);m2=mom(2000);bm=xm(bp,500);ym=xm(yp,500)
            ret=[bps(mids[i-1][1],mids[i][1]) for i in range(1,len(mids))];vol=float(np.std(ret)) if ret else 0;sp=book.spread_bps()
            x=[lf,bf,yf,qimb,micro,ofi,depl,m05,m2,bm,ym,sp,vol]
            score=1.15*lf+.85*bf+.70*yf+.70*qimb+.45*micro+.75*ofi+.55*depl+.16*np.clip(m05,-8,8)+.08*np.clip(m2,-12,12)+.09*np.clip(bm,-8,8)+.07*np.clip(ym,-8,8)
            rep.tick(tick,book,mid,x,float(score),ns(ts0),ns(ts1));tick+=TICK_MS*1_000_000
        hourly.append({"hour":h.isoformat(),"trades":len(rep.trades)-n0,"bal_start":bal0,"bal_end":rep.balance,"book_rows":bs.n,"lighter_trades":lt.n,"binance_trades":bt.n,"bybit_trades":yt.n})
        print("HOUR_DONE",hourly[-1],flush=True);h+=dt.timedelta(hours=1)
    if rep.pos:
        f=book.fill("SELL" if rep.pos.side==1 else "BUY",qty=rep.pos.qty)
        if f:rep.finish(ns(ts1),f[0],"END")
    tr=rep.trades;wins=[t for t in tr if t["pnl_price"]+t["funding"]>0];losses=[t for t in tr if t["pnl_price"]+t["funding"]<0]
    gp=sum(t["pnl_price"]+t["funding"] for t in wins);gl=-sum(t["pnl_price"]+t["funding"] for t in losses)
    R={"type":"RS11_BASELINE_RECONSTRUCTED_FIDELITY_LIGHTER_24H_CAUSAL","exact_binary_rs11":False,
       "reason":"Original RS11 executable was not preserved; architecture and frozen quantitative rules reconstructed from project checkpoint.",
       "date_utc":DATE,"start_equity":START_EQUITY,"final_equity":rep.balance,"return_pct":(rep.balance/START_EQUITY-1)*100,
       "trades":len(tr),"trades_per_min":len(tr)/1440,"win_rate_pct":len(wins)/len(tr)*100 if tr else 0,"profit_factor":gp/gl if gl else None,
       "max_drawdown_pct":rep.maxdd,"liquidations":rep.liq,"funding_total_usd":rep.funding,"funding_events":rep.fund_events,
       "avg_trade_bps":float(np.mean([t["gross_bps"] for t in tr])) if tr else 0,"avg_winner_bps":float(np.mean([t["gross_bps"] for t in wins])) if wins else 0,
       "avg_loser_bps":float(np.mean([t["gross_bps"] for t in losses])) if losses else 0,"avg_hold_s":float(np.mean([t["hold_s"] for t in tr])) if tr else 0,
       "avg_signal_to_fill_ms":300,"avg_entry_cost_vs_signal_bps":float(np.mean(rep.slip_sig)) if rep.slip_sig else None,
       "avg_entry_cost_vs_exec_mid_bps":float(np.mean(rep.slip_mid)) if rep.slip_mid else None,"platform_trading_fees_usd":0.,
       "leverage":50,"min_notional":10,"maintenance_margin_rate":.012,"candidate_count":rep.ncand,"accepted_entries":rep.accepted,"missed_exec":rep.missed,
       "invalid_book_ticks":rep.invalid,"book_gaps":book.gaps,"book_crosses":book.crosses,"book_snapshots":book.snapshots,"model_retrains":rep.retrains,
       "data_source":"CryptoHFTData Lighter L2/trades/mark+funding; Binance Futures trades; Bybit trades","hourly":hourly}
    (OUT/"result.json").write_text(json.dumps(R,indent=2));pd.DataFrame(tr).to_csv(OUT/"trades.csv",index=False)
    (OUT/"summary.txt").write_text("\n".join(f"{k}: {v}" for k,v in R.items() if k not in ("hourly","model_retrains")))
    print("FINAL_RESULT",json.dumps(R),flush=True)
if __name__=="__main__":run()
