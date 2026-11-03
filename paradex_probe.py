import requests, time, json, statistics
BASE='https://api.prod.paradex.trade/v1'
rows=[]
for i in range(60):
    try:
        r=requests.get(f'{BASE}/orderbook/BTC-USD-PERP',params={'depth':20},timeout=10)
        d=r.json()
        def px(v):
            if not v: return None
            if isinstance(v,list):
                if len(v)>=2 and not isinstance(v[0],list): return float(v[0])
                if v and isinstance(v[0],list): return float(v[0][0])
            return float(v)
        bids=d.get('bids') or []; asks=d.get('asks') or []
        bid=float(bids[0][0]) if bids else None
        ask=float(asks[0][0]) if asks else None
        bai=px(d.get('best_ask_interactive')); bbi=px(d.get('best_bid_interactive'))
        baa=px(d.get('best_ask_api')); bba=px(d.get('best_bid_api'))
        mid=(bid+ask)/2 if bid and ask else None
        spread=(ask-bid)/mid*1e4 if mid else None
        imid=(bbi+bai)/2 if bbi and bai else None
        ispread=(bai-bbi)/imid*1e4 if imid else None
        rows.append({'t':time.time(),'bid':bid,'ask':ask,'spread_bps':spread,'api_bid':bba,'api_ask':baa,'interactive_bid':bbi,'interactive_ask':bai,'interactive_spread_bps':ispread})
    except Exception as e:
        rows.append({'t':time.time(),'error':repr(e)})
    time.sleep(1)

ok=[x for x in rows if x.get('spread_bps') is not None]
iok=[x for x in rows if x.get('interactive_spread_bps') is not None]
out={'samples':len(rows),'ok':len(ok),'interactive_ok':len(iok),'first':rows[:3],'last':rows[-3:]}
if ok:
    vals=[x['spread_bps'] for x in ok]
    out['spread_bps']={'min':min(vals),'median':statistics.median(vals),'mean':statistics.mean(vals),'max':max(vals)}
if iok:
    vals=[x['interactive_spread_bps'] for x in iok]
    out['interactive_spread_bps']={'min':min(vals),'median':statistics.median(vals),'mean':statistics.mean(vals),'max':max(vals)}
    imp=[]
    for x in iok:
        if x.get('spread_bps') is not None: imp.append(x['spread_bps']-x['interactive_spread_bps'])
    if imp: out['rpi_improvement_bps']={'min':min(imp),'median':statistics.median(imp),'mean':statistics.mean(imp),'max':max(imp)}
print('PARADEX_PROBE_RESULT '+json.dumps(out))
open('paradex_probe.json','w').write(json.dumps({'summary':out,'rows':rows},indent=2))
