import requests,time,json,statistics
SYMS=['tBTCUSD','tBTCUST']
allout={}
for sym in SYMS:
    rows=[]
    for i in range(60):
        t0=time.time()
        try:
            d=requests.get(f'https://api-pub.bitfinex.com/v2/book/{sym}/P0',params={'len':25},timeout=10).json()
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
                        cap=px*amt; take=min(rem,cap)
                        if take<=0: break
                        q=take/px; cost+=q*px; qty+=q; rem-=take
                        if rem<=1e-9: break
                    return (cost/qty if qty else None, usd-rem, rem)
                buy,fb,rb=vwap(asks,20.0); sell,fs,rs=vwap(bids,20.0)
                rows.append({'bid':bid,'ask':ask,'spread_bps':spread,'buy_vwap20':buy,'sell_vwap20':sell,
                             'buy_impact_bps':((buy-ask)/mid*1e4 if buy else None),
                             'sell_impact_bps':((bid-sell)/mid*1e4 if sell else None),
                             'fill20_buy':fb,'fill20_sell':fs,'http_ms':(time.time()-t0)*1000})
            else: rows.append({'error':'missing_book'})
        except Exception as e: rows.append({'error':repr(e)})
        time.sleep(0.5)
    ok=[x for x in rows if 'spread_bps' in x]
    out={'samples':len(rows),'ok':len(ok),'first':rows[:3],'last':rows[-3:]}
    for k in ['spread_bps','http_ms','buy_impact_bps','sell_impact_bps']:
        vals=[x[k] for x in ok if x.get(k) is not None]
        if vals: out[k]={'min':min(vals),'median':statistics.median(vals),'mean':statistics.mean(vals),'max':max(vals)}
    allout[sym]=out
print('BITFINEX_SPOT_PROBE_RESULT '+json.dumps(allout))
open('bitfinex_spot_probe.json','w').write(json.dumps(allout,indent=2))
