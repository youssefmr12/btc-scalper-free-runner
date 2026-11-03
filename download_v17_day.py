#!/usr/bin/env python3
import os, requests, sys, json
base="https://datasets.tardis.dev/v1/bybit"
date=("2026","09","01")
items=[
 ("trades","BTCUSDT"),
 ("book_snapshot_25","BTCUSDT"),
 ("derivative_ticker","BTCUSDT"),
 ("liquidations","PERPETUALS"),
]
os.makedirs("v17_day",exist_ok=True)
out=[]
for dtype,sym in items:
    url=f"{base}/{dtype}/{date[0]}/{date[1]}/{date[2]}/{sym}.csv.gz"
    fn=f"v17_day/{dtype}_{sym}.csv.gz"
    print("DOWNLOAD",url,flush=True)
    with requests.get(url,stream=True,timeout=120) as r:
        print("STATUS",r.status_code,r.headers.get("content-length"),r.headers.get("content-type"),flush=True)
        r.raise_for_status()
        n=0
        with open(fn,"wb") as f:
            for chunk in r.iter_content(1024*1024):
                if chunk:
                    f.write(chunk); n+=len(chunk)
        out.append({"type":dtype,"symbol":sym,"bytes":n,"file":fn,"url":url})
        print("SAVED",fn,n,flush=True)
open("v17_day/manifest.json","w").write(json.dumps(out,indent=2))
print(json.dumps(out,indent=2))
