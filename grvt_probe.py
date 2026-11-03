import requests,time,json,statistics
URL='https://market-data.grvt.io/full/v1/book'
rows=[]
for i in range(90):
    t0=time.time()
    try:
        d=requests.post(URL,json={'instrument':'BTC_USDT_Perp','depth':50},timeout=10).json()
        r=d.get('result') or {}
        bids=[(float(x['price']),float(x['size'])) for x in r.get('bids',[]) if float(x['size'])>0]
        asks=[(float(x['price']),float(x['size'])) for x in r.get('asks',[]) if float(x['size'])>0]
        if bids and asks:
            bid=max(bids)[0]; ask=min(asks)[0]; mid=(bid+ask)/2
            spread=(ask-bid)/mid*1e4
            def vwap(levels,usd,side):
                rem=usd; notion=0.0; qty=0.0
                lv=sorted(levels, key=lambda z:z[0], reverse=(side=='sell'))
                for px,sz in lv:
                    cap=px*sz
                    take=min(rem,cap)
                    if take<=0: break
                    q=take/px; notion+=q*px; qty+=q; rem-=take
                    if rem<=1e-9: break
                return (notion/qty if qty else None, usd-rem)
            buy,fb=vwap(asks,20,'buy'); sell,fs=vwap(bids,20,'sell')
            rows.append({
              'bid':bid,'ask':ask,'spread_bps':spread,
              'buy_vwap20':buy,'sell_vwap20':sell,
              'buy_impact_bps':((buy-ask)/mid*1e4 if buy else None),
              'sell_impact_bps':((bid-sell)/mid*1e4 if sell else None),
              'fill20_buy':fb,'fill20_sell':fs,
              'http_ms':(time.time()-t0)*1000
            })
        else:
            rows.append({'error':'missing_book','raw':d})
    except Exception as e:
        rows.append({'error':repr(e)})
    time.sleep(0.5)
ok=[x for x in rows if 'spread_bps' in x]
out={'samples':len(rows),'ok':len(ok),'first':rows[:3],'last':rows[-3:]}
for k in ['spread_bps','http_ms','buy_impact_bps','sell_impact_bps']:
    vals=[x[k] for x in ok if x.get(k) is not None]
    if vals:
        out[k]={'min':min(vals),'median':statistics.median(vals),'mean':statistics.mean(vals),'max':max(vals)}
# maker theoretical explicit cost if filled passively both entry+exit: rebate twice
out['maker_rebate_bps_per_fill']=-0.01
out['maker_rebate_bps_roundtrip']=-0.02
print('GRVT_PROBE_RESULT '+json.dumps(out))
open('grvt_probe.json','w').write(json.dumps({'summary':out,'rows':rows},indent=2))
