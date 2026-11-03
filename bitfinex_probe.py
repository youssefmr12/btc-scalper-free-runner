import requests,time,json,statistics
BOOK='https://api-pub.bitfinex.com/v2/book/tBTCF0:USTF0/P0'
rows=[]
for i in range(60):
    t0=time.time()
    try:
        d=requests.get(BOOK,params={'len':25},timeout=10).json()
        bids=[]; asks=[]
        for row in d:
            if not isinstance(row,list) or len(row)<3: continue
            px=float(row[0]); amt=float(row[2])
            if amt>0: bids.append((px,amt))
            elif amt<0: asks.append((px,-amt))
        if bids and asks:
            bids.sort(reverse=True); asks.sort()
            bid=bids[0][0]; ask=asks[0][0]; mid=(bid+ask)/2
            spread=(ask-bid)/mid*1e4
            def vwap(levels,usd):
                rem=usd; cost=0.0; qty=0.0
                for px,amt in levels:
                    cap=px*amt
                    take=min(rem,cap)
                    if take<=0: break
                    q=take/px; cost+=q*px; qty+=q; rem-=take
                    if rem<=1e-9: break
                return (cost/qty if qty else None, usd-rem, rem)
            buy,filled_b,rem_b=vwap(asks,20.0)
            sell,filled_s,rem_s=vwap([(p,a) for p,a in bids],20.0)
            impact_buy=((buy-ask)/mid*1e4) if buy else None
            impact_sell=((bid-sell)/mid*1e4) if sell else None
            rows.append({'bid':bid,'ask':ask,'spread_bps':spread,'buy_vwap20':buy,'sell_vwap20':sell,
                         'buy_impact_bps':impact_buy,'sell_impact_bps':impact_sell,
                         'fill20_buy':filled_b,'fill20_sell':filled_s,
                         'http_ms':(time.time()-t0)*1000})
        else: rows.append({'error':'missing_book'})
    except Exception as e: rows.append({'error':repr(e)})
    time.sleep(1)
ok=[x for x in rows if 'spread_bps' in x]
out={'samples':len(rows),'ok':len(ok),'first':rows[:3],'last':rows[-3:]}
for k in ['spread_bps','http_ms','buy_impact_bps','sell_impact_bps']:
    vals=[x[k] for x in ok if x.get(k) is not None]
    if vals: out[k]={'min':min(vals),'median':statistics.median(vals),'mean':statistics.mean(vals),'max':max(vals)}
try:
    conf=requests.get('https://api-pub.bitfinex.com/v2/conf/pub:info:pair:futures',timeout=10).json()
    matches=[]
    def walk(x):
        if isinstance(x,list):
            if x and isinstance(x[0],str) and 'BTCF0' in x[0]: matches.append(x)
            for y in x: walk(y)
    walk(conf)
    out['future_conf_matches']=matches[:10]
except Exception as e: out['conf_error']=repr(e)
print('BITFINEX_PROBE_RESULT '+json.dumps(out))
open('bitfinex_probe.json','w').write(json.dumps({'summary':out,'rows':rows},indent=2))
