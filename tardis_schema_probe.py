import gzip, io, json, requests

urls = {
  "book":"https://datasets.tardis.dev/v1/bybit/book_snapshot_25/2026/09/01/BTCUSDT.csv.gz",
  "trades":"https://datasets.tardis.dev/v1/bybit/trades/2026/09/01/BTCUSDT.csv.gz",
  "ticker":"https://datasets.tardis.dev/v1/bybit/derivative_ticker/2026/09/01/BTCUSDT.csv.gz",
  "liq":"https://datasets.tardis.dev/v1/bybit/liquidations/2026/09/01/PERPETUALS.csv.gz",
}
out={}
for k,u in urls.items():
    r=requests.get(u,stream=True,timeout=30)
    out[k]={"status":r.status_code,"headers":dict(r.headers)}
    if r.status_code==200:
        raw=r.raw
        raw.decode_content=False
        gz=gzip.GzipFile(fileobj=raw)
        lines=[]
        for _ in range(3):
            b=gz.readline()
            if not b: break
            lines.append(b.decode("utf-8","replace").rstrip())
        out[k]["lines"]=lines
    r.close()
print("TARDIS_SCHEMA "+json.dumps(out))
