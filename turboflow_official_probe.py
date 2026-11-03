import requests,re,json,os
S=requests.Session(); S.headers.update({"User-Agent":"Mozilla/5.0"})
urls=[
 "https://turboflow.gitbook.io/whitepaper/llms.txt",
 "https://turboflow.gitbook.io/whitepaper/turboflow/products/turbo-perps.md",
 "https://turboflow.gitbook.io/whitepaper/turboflow/resources/fee-schedule.md",
 "https://turboflow.gitbook.io/whitepaper/turboflow/resources/faq.md",
 "https://turboflow.gitbook.io/whitepaper/turboflow/resources/deposits-and-withdrawals.md",
 "https://app.tf.xyz/",
 "https://tf.xyz/"
]
os.makedirs("tf_docs",exist_ok=True)
out={}
for u in urls:
  try:
    r=S.get(u,timeout=30,allow_redirects=True)
    txt=r.text
    out[u]={"status":r.status_code,"final_url":r.url,"content_type":r.headers.get("content-type"),"len":len(txt),"text":txt[:500000]}
    fn=re.sub(r'[^A-Za-z0-9._-]+','_',u.split('://',1)[1]).strip('_')
    open("tf_docs/"+fn+".txt","w",encoding="utf-8").write(txt)
    print("\nURL",u,"STATUS",r.status_code,"FINAL",r.url,"LEN",len(txt))
    for line in txt.splitlines():
      if re.search(r'profit.?share|funding|zero spread|slippage|BTC|1000x|minimum|API|oracle|fee',line,re.I):
        print(line[:1200])
  except Exception as e:
    out[u]={"error":repr(e)}; print("ERR",u,repr(e))
# find JS assets from app
html=out.get("https://app.tf.xyz/",{}).get("text","")
srcs=re.findall(r'<script[^>]+src=["\']([^"\']+)',html,re.I)
print("APP_SCRIPTS",srcs)
assets=[]
from urllib.parse import urljoin
for src in srcs[:80]:
  u=urljoin("https://app.tf.xyz/",src)
  try:
    r=S.get(u,timeout=30); t=r.text
    hit=bool(re.search(r'profit.?share|share.?pnl|1000x|turbo.?perp|max_share|pnl_base|api\.turboflow|apis\.turboflow',t,re.I))
    if hit:
      assets.append({"url":u,"status":r.status_code,"len":len(t),"text":t[:1000000]})
      print("ASSET_HIT",u,len(t))
      # print around important tokens
      for pat in ["profitShare","profit_share","sharePnl","share_pnl","1000x","max_share","pnl_base","api.turboflow","apis.turboflow"]:
        for m in list(re.finditer(pat,t,re.I))[:5]:
          print("CTX",pat,t[max(0,m.start()-400):m.start()+900].replace("\n"," ")[:1400])
  except Exception as e: print("ASSET_ERR",u,repr(e))
out["asset_hits"]=assets
open("tf_docs/full_probe.json","w",encoding="utf-8").write(json.dumps(out,ensure_ascii=False,indent=2))
