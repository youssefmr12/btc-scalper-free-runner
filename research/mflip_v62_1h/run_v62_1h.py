import csv,gzip,hashlib,json,time
from collections import deque
from pathlib import Path
import requests,numpy as np,pandas as pd,pyarrow.parquet as pq
from sortedcontainers import SortedDict
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss
B=1764547200000;LAT=300;CUT=250;H=4000;TP=4.75;SL=.40;COST=.44
D=Path('v62data');O=Path('v62out');D.mkdir(exist_ok=True);O.mkdir(exist_ok=True)
OB='https://api.cryptohftdata.com/v1/download?file=binance_futures/2025-12-01/00/BTCUSDT_orderbook.parquet'
TR='https://datasets.tardis.dev/v1/binance-futures/trades/2025/12/01/BTCUSDT.csv.gz'; TRSHA='70c587a603b739fec775beada5e943ec8d4409b49bbc607bf3cabec5aa35d26f'
PXY=['leader','f250','f1','f3','ci','mx']; L2=['p250','p1s','a250','r250','a1s','r1s','d1','d5','d10','micro','vamp','q1','m250','m1s','tf250','tf1s','abs250','abs1s','spread']
def dl(u,p):
 if p.exists() and p.stat().st_size>1000:return
 for k in range(7):
  try:
   with requests.get(u,stream=True,timeout=180,headers={'User-Agent':'Mozilla/5.0'}) as r:
    print('GET',r.status_code,u,flush=True)
    if r.status_code==200:
     with open(p,'wb') as f:
      for z in r.iter_content(1<<20):
       if z:f.write(z)
     if p.stat().st_size>1000:return
  except Exception as e:print('DLERR',repr(e),flush=True)
  time.sleep(2+k)
 raise RuntimeError('download failed')
def sha(p):
 h=hashlib.sha256()
 with open(p,'rb') as f:
  for z in iter(lambda:f.read(1<<20),b''):h.update(z)
 return h.hexdigest()
def ms(x):
 x=int(x)
 return x//1000000 if x>10**17 else (x//1000 if x>10**14 else x)
def trades(p):
 t=[];x=[];s=[];n=[]
 with gzip.open(p,'rt',newline='') as f:
  for r in csv.DictReader(f):
   q=int(r['timestamp'])//1000
   if q<B-2000:continue
   if q>B+3610000:break
   px=float(r['price']);am=float(r['amount']);t.append(q);x.append(px);s.append(1 if r['side'].lower().startswith('buy') else -1);n.append(px*am)
 return np.array(t,np.int64),np.array(x),np.array(s,np.int8),np.array(n)
def out(t,x,dec,side,lat=LAT):
 e=dec+lat;i=np.searchsorted(t,e);ep=x[i];j=np.searchsorted(t,e+H,'right');k=max(i,j-1)
 for z in range(i,j):
  g=side*(x[z]/ep-1)*1e4
  if g>=TP or g<=-SL:k=z;break
 return float(side*(x[k]/ep-1)*1e4-COST),int(t[k])
def st(x):
 x=np.array(x,float)
 if not len(x):return {'n':0,'wr':None,'pf':None,'avg':None}
 gp=x[x>0].sum();gl=-x[x<0].sum();return {'n':len(x),'wr':float((x>0).mean()),'pf':float(gp/gl) if gl else None,'avg':float(x.mean())}
class Book:
 def __init__(q):q.b=SortedDict();q.a=SortedDict();q.v=False;q.f=deque();q.m=deque();q.snap=q.gap=q.cross=0;q.last=None;q.rows=0
 def apply(q,t,typ,fid,prev,rows):
  sn=str(typ).lower().startswith('snapshot')
  if sn:q.b.clear();q.a.clear();q.v=False;q.snap+=1
  elif q.v and prev>=0 and q.last is not None and prev!=q.last:q.v=False;q.gap+=1
  for side,p,z in rows:
   d=q.b if str(side).lower().startswith('bid') else q.a;old=float(d.get(p,0));dd=z-old
   if z<=0:d.pop(p,None)
   else:d[p]=z
   if not sn and dd:
    bi=str(side).lower().startswith('bid');k=(0 if bi else 1) if dd>0 else (2 if bi else 3);q.f.append((t,k,abs(dd)*p))
  if fid>=0:q.last=fid
  if q.b and q.a:
   bb=q.b.peekitem(-1)[0];aa=q.a.peekitem(0)[0]
   if bb>=aa:q.v=False;q.cross+=1
   elif sn or q.last is not None:q.v=True
   if q.v:q.m.append((t,(bb+aa)/2))
  while q.f and q.f[0][0]<t-1500:q.f.popleft()
  while q.m and q.m[0][0]<t-1500:q.m.popleft()
 def fg(q,t,w):
  z=[0.]*4
  for tt,k,v in reversed(q.f):
   if tt>t:continue
   if tt<t-w:break
   z[k]+=v
  return z
 def old(q,t,w):
  for tt,m in reversed(q.m):
   if tt<=t-w:return m
  return q.m[0][1] if q.m else np.nan
def groups(p):
 cur=None;rows=[];meta=None
 with gzip.open(p,'rt',newline='') as f:
  rd=csv.DictReader(f)
  need={'timestamp','is_snapshot','side','price','amount'}
  if not need.issubset(set(rd.fieldnames or [])):raise RuntimeError('L2 schema '+str(rd.fieldnames))
  for r in rd:
   t=int(r['timestamp'])//1000
   if t<B-1000:continue
   if t>B+3600_000+1000:break
   sn=str(r['is_snapshot']).lower() in ('true','1','yes')
   typ='snapshot' if sn else 'update';key=(t,typ)
   if cur is not None and key!=cur:
    yield meta,rows;rows=[]
   cur=key;meta=(t,typ,-1,-1);rows.append((r['side'],float(r['price']),float(r['amount'])))
 if cur is not None:yield meta,rows
def tflow(tt,ss,nn,t,w,side):
 l=np.searchsorted(tt,t-w);r=np.searchsorted(tt,t,'right');den=nn[l:r].sum();return float(side*np.sum(ss[l:r]*nn[l:r])/den) if den else 0.
def feat(q,t,side,tt,ss,nn):
 if not q.v or not q.b or not q.a:return None
 def fl(w):
  ba,aa,br,ar=q.fg(t,w)
  if side>0:sup,opp=ba+ar,aa+br;ad=(ba-aa)/(ba+aa+1e-12);rm=(ar-br)/(ar+br+1e-12)
  else:sup,opp=aa+br,ba+ar;ad=(aa-ba)/(ba+aa+1e-12);rm=(br-ar)/(ar+br+1e-12)
  return (sup-opp)/(sup+opp+1e-12),ad,rm
 p2,a2,r2=fl(250);p1,a1,r1=fl(1000);bb,bq=q.b.peekitem(-1);aa,aq=q.a.peekitem(0);mid=(bb+aa)/2;micro=side*((aa*bq+bb*aq)/(bq+aq+1e-12)/mid-1)*1e4
 def dep(n):
  B=list(reversed(q.b.items()))[:n];A=list(q.a.items())[:n];bn=sum(p*z for p,z in B);an=sum(p*z for p,z in A);bqs=sum(z for p,z in B);aqs=sum(z for p,z in A);return side*(bn-an)/(bn+an+1e-12),sum(p*z for p,z in B)/(bqs+1e-12),sum(p*z for p,z in A)/(aqs+1e-12),bqs,aqs
 d1,bv,av,b5,a5=dep(1);d5,bv5,av5,b5,a5=dep(5);d10,bv10,av10,b10,a10=dep(10);vamp=side*((av10*b10+bv10*a10)/(b10+a10+1e-12)/mid-1)*1e4;q1=side*(bq-aq)/(bq+aq+1e-12)
 m2=side*(mid/q.old(t,250)-1)*1e4 if q.m else 0;m1=side*(mid/q.old(t,1000)-1)*1e4 if q.m else 0;tf2=tflow(tt,ss,nn,t,250,side);tf1=tflow(tt,ss,nn,t,1000,side)
 return [p2,p1,a2,r2,a1,r1,d1,d5,d10,micro,vamp,q1,m2,m1,tf2,tf1,tf2-np.tanh(m2/.5),tf1-np.tanh(m1/.8),(aa-bb)/mid*1e4]
def capture(ob,e,tt,ss,nn):
 e=e.sort_values('cutoff').reset_index(drop=True);q=Book();g=groups(ob);pen=next(g,None);R=[]
 for _,r in e.iterrows():
  while pen is not None and pen[0][0]<=r.cutoff:
   q.apply(*pen[0],pen[1]);pen=next(g,None)
  z=feat(q,int(r.cutoff),int(r.side),tt,ss,nn);d=r.to_dict();d['ready']=int(z is not None);d.update(dict(zip(L2,z if z is not None else [0.]*len(L2))));R.append(d)
 return pd.DataFrame(R),q
def model(tr,cols,C):
 m=make_pipeline(StandardScaler(),LogisticRegression(C=C,class_weight='balanced',max_iter=2000,random_state=7));m.fit(tr[cols],tr.label);return m
def pr(m,d,cols):
 p=m.predict_proba(d[cols]);o={c:np.zeros(len(d)) for c in ['CONTINUE','REJECT','FLIP']}
 for j,c in enumerate(m.classes_):o[c]=p[:,j]
 return o
def route(p):
 return ['FLIP' if pf>=.5 and pf-pc>=.1 else ('REJECT' if pj>=.8 else 'CONTINUE') for pc,pj,pf in zip(p['CONTINUE'],p['REJECT'],p['FLIP'])]
def select(tr):
 best=None
 for mode in ['l2','combined']:
  cols=L2 if mode=='l2' else PXY+L2
  for C in [.03,.1,.3,1.]:
   L=[]
   for a,b in [(18,26),(26,34)]:
    m=model(tr.iloc[:a],cols,C);pp=m.predict_proba(tr.iloc[a:b][cols]);L.append(log_loss(tr.iloc[a:b].label,pp,labels=list(m.classes_)))
   x=(float(np.mean(L)),mode,C)
   if best is None or x<best:best=x
 return {'cv_logloss':best[0],'mode':best[1],'C':best[2]}
def execute(df,rm,tt,px,lat=300):
 a=-1;P=[];rows=[]
 for _,r in df.sort_values('decision_ms').iterrows():
  if r.decision_ms<=a:continue
  z=rm.get(int(r.rel_s),'CONTINUE')
  if z=='REJECT':rows.append((r.rel_s,z,None));continue
  n,ex=out(tt,px,int(r.decision_ms),int(r.side)*(-1 if z=='FLIP' else 1),lat);P.append(n);a=ex;rows.append((r.rel_s,z,n))
 return st(P),rows
def main():
 ev=pd.read_csv('events_1h.csv');ev['cutoff']=ev.decision_ms+CUT
 for a,b in [('leader','leader_score_x1e4'),('f250','flow250_x1e4'),('f1','flow1s_x1e4'),('f3','flow3s_x1e4'),('ci','count_imb1s_x1e4'),('mx','max_trade_share1s_x1e4')]:ev[a]=ev[b]/1e4
 ob=D/'ob.parquet';tr=D/'tr.csv.gz';dl(OB,ob);dl(TR,tr)
 if sha(tr)!=TRSHA:raise RuntimeError('trade sha mismatch')
 tt,px,ss,nn=trades(tr);F,q=capture(ob,ev,tt,ss,nn)
 C=[];X=[];Y=[]
 for _,r in F.iterrows():
  c,_=out(tt,px,int(r.decision_ms),int(r.side));f,_=out(tt,px,int(r.decision_ms),-int(r.side));C.append(c);X.append(f);Y.append('FLIP' if f>0 and f>c+.15 else ('CONTINUE' if c>0 and c>=f-.05 else 'REJECT'))
 F['cont']=C;F['flip']=X;F['label']=Y;trn=F[(F.rel_s<1800)&(F.ready>0)].reset_index(drop=True);oos=F[F.rel_s>=1800].reset_index(drop=True)
 if len(trn)<30:raise RuntimeError('valid L2 train '+str(len(trn)))
 sel=select(trn);cols=L2 if sel['mode']=='l2' else PXY+L2;m=model(trn,cols,sel['C']);R=['CONTINUE']*len(oos);v=oos.ready.values>0
 if v.any():
  z=route(pr(m,oos.loc[v],cols));j=0
  for i,x in enumerate(v):
   if x:R[i]=z[j];j+=1
 rm={int(r.rel_s):z for (_,r),z in zip(oos.iterrows(),R)};bm={int(r.rel_s):'CONTINUE' for _,r in oos.iterrows()};base,_=execute(oos,bm,tt,px);v62,rows=execute(oos,rm,tt,px);full,_=execute(F,{int(r.rel_s):'CONTINUE' for _,r in F.iterrows()},tt,px)
 if full['n']!=70 or abs(full['pf']-1.609540990289726)>.00001:raise RuntimeError('V6 baseline mismatch '+str(full))
 stress={str(x):execute(oos,rm,tt,px,x)[0] for x in [300,400,500,750]}
 rp=np.array([0 if z=='REJECT' else (r.flip if z=='FLIP' else r.cont) for (_,r),z in zip(oos.iterrows(),R)]);delta=rp-oos.cont.values;rng=np.random.default_rng(7);M=[rng.choice(delta,len(delta),replace=True).mean() for _ in range(5000)];lo,hi=np.quantile(M,[.025,.975])
 RES={'name':'MFLIP_V6.2_FULL_L2_1H','window':'2025-12-01 00:00-01:00 UTC','split':'first30m train / last30m untouched OOS','data':{'l2':'Tardis Binance Futures incremental_book_L2 exact daily file; replay limited to first hour','execution':'Tardis raw trades','l2_rows_processed':q.rows,'l2_bytes':ob.stat().st_size,'l2_sha':sha(ob),'trade_sha':sha(tr)},'integrity':{'snapshots':q.snap,'gaps':q.gap,'crosses':q.cross,'valid_events':int(F.ready.sum()),'events':len(F)},'selection':sel,'train_labels':trn.label.value_counts().to_dict(),'v6_full_hour':full,'v6_oos':base,'v62_oos':v62,'freq_retention':v62['n']/base['n'],'routes':{'continue':R.count('CONTINUE'),'flip':R.count('FLIP'),'reject':R.count('REJECT'),'invalid_default_continue':int((~v).sum())},'latency_stress':stress,'bootstrap':{'mean_delta_per_signal':float(delta.mean()),'ci95':[float(lo),float(hi)],'p_gt_0':float(np.mean(np.array(M)>0))},'note':'one-hour diagnostic; no OOS tuning'}
 O.joinpath('result.json').write_text(json.dumps(RES,indent=2));F.to_csv(O/'features.csv',index=False);pd.DataFrame(rows,columns=['rel_s','route','net']).to_csv(O/'routes.csv',index=False);print('FINAL',json.dumps(RES),flush=True)
if __name__=='__main__':main()
