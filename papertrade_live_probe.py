import requests,json,sys
BASE="https://exchange.papertrade.xyz"
paths=[
 "/state/trading",
 "/query/protocol/summary",
 "/relayer/health"
]
out={}
for p in paths:
 try:
  r=requests.get(BASE+p,timeout=30)
  out[p]={"status":r.status_code,"headers":dict(r.headers),"body":r.json() if "json" in r.headers.get("content-type","") else r.text[:100000]}
  print("PATH",p,"STATUS",r.status_code)
  print(json.dumps(out[p]["body"],ensure_ascii=False)[:100000])
 except Exception as e:
  out[p]={"error":repr(e)};print("ERR",p,repr(e))
open("papertrade_live_probe.json","w").write(json.dumps(out,indent=2,ensure_ascii=False))
