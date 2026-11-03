import requests, json, re, math, sys, time
BASES=["https://api.turboflow.xyz","https://apis.turboflow.xyz"]
sess=requests.Session()
sess.headers.update({"User-Agent":"Mozilla/5.0"})
out={"bases":{}}
for base in BASES:
    rec={}
    try:
        r=sess.get(base+"/public/home/pairs",timeout=20)
        rec["home_status"]=r.status_code
        rec["home_text"]=r.text[:200000]
        try: rec["home_json"]=r.json()
        except: rec["home_json"]=None
    except Exception as e:
        rec["home_error"]=repr(e)
    out["bases"][base]=rec

def walk(obj,path=""):
    if isinstance(obj,dict):
        for k,v in obj.items():
            yield from walk(v,path+("/"+str(k)))
    elif isinstance(obj,list):
        for i,v in enumerate(obj):
            yield from walk(v,path+f"/{i}")
    else:
        yield path,obj

btc_candidates=[]
for base,rec in out["bases"].items():
    j=rec.get("home_json")
    if j is None: continue
    # recursively find dicts mentioning BTC
    stack=[j]
    while stack:
        x=stack.pop()
        if isinstance(x,dict):
            txt=json.dumps(x,ensure_ascii=False)
            if "BTC" in txt.upper() and any(k in x for k in ["pair_id","pairId","id","symbol","pair_name","name"]):
                btc_candidates.append((base,x))
            stack.extend(x.values())
        elif isinstance(x,list):
            stack.extend(x)

# de-duplicate short records and extract likely pair ids + price
seen=set(); tests=[]
for base,x in btc_candidates:
    txt=json.dumps(x,sort_keys=True,default=str)
    if txt in seen: continue
    seen.add(txt)
    pair_id=x.get("pair_id",x.get("pairId",x.get("id")))
    symbol=x.get("symbol",x.get("pair_name",x.get("name","")))
    if pair_id is None: continue
    price=None
    for k in ["price","mark_price","index_price","last_price","close","oracle_price"]:
        if x.get(k) not in [None,""]:
            try: price=float(x[k]); break
            except: pass
    tests.append({"base":base,"pair_id":str(pair_id),"symbol":symbol,"price":price,"raw":x})

out["btc_candidates"]=tests[:20]

# Try share-fee endpoint on each likely BTC candidate with several plausible vols/leverage
share_results=[]
for t in tests[:10]:
    if "BTC" not in str(t.get("symbol","")).upper() and "BTC" not in json.dumps(t["raw"]).upper():
        continue
    base=t["base"]; pid=t["pair_id"]; price=t["price"] or 77500
    for lev in [1,10,39,75,100,500,1000]:
      for order_way in [1,3]: # open long/open short
        for vol in [0.00025,0.001,0.01,2,20]:
          params={"pair_id":pid,"price":price,"vol":vol,"leverage":lev,"order_way":order_way}
          try:
            r=sess.get(base+"/public/pair/share/fees",params=params,timeout=15)
            try: body=r.json()
            except: body=r.text[:5000]
            share_results.append({"base":base,"params":params,"status":r.status_code,"body":body})
            if r.status_code==200 and isinstance(body,dict) and body:
                # enough variants; keep going but cap overall
                pass
          except Exception as e:
            share_results.append({"base":base,"params":params,"error":repr(e)})
          if len(share_results)>=140: break
        if len(share_results)>=140: break
      if len(share_results)>=140: break
    if len(share_results)>=140: break
out["share_results"]=share_results

# Also try public profit stats, trading configs, pair price with minimal/no params
for path in ["/public/pair/profit/stats","/public/trading/configs","/market/pair/price"]:
    arr=[]
    for base in BASES:
      for params in [{},{"type":"1"},{"type":"All"}]:
        try:
          r=sess.get(base+path,params=params,timeout=15)
          try:b=r.json()
          except:b=r.text[:20000]
          arr.append({"base":base,"params":params,"status":r.status_code,"body":b})
        except Exception as e: arr.append({"base":base,"params":params,"error":repr(e)})
    out[path]=arr

open("turboflow_public_probe.json","w").write(json.dumps(out,indent=2,ensure_ascii=False))
# compact summary
print("TF_PUBLIC_PROBE_BEGIN")
print("BTC_CANDIDATES",json.dumps(out["btc_candidates"][:8],ensure_ascii=False))
ok=[x for x in share_results if x.get("status")==200]
print("SHARE_OK_COUNT",len(ok))
for x in ok[:25]:
    print("SHARE",json.dumps(x,ensure_ascii=False))
for p in ["/public/pair/profit/stats","/public/trading/configs","/market/pair/price"]:
    print("ENDPOINT",p,json.dumps(out[p][:4],ensure_ascii=False)[:20000])
print("TF_PUBLIC_PROBE_END")
