import requests, time, json, statistics
BASE='https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders'
rows=[]
for i in range(60):
    try:
        d=requests.get(BASE,params={'market_id':1,'limit':250},timeout=10).json()
        asks=[(float(x['price']), float(x['remaining_base_amount'])) for x in d.get('asks',[]) if float(x['remaining_base_amount'])>0]
        bids=[(float(x['price']), float(x['remaining_base_amount'])) for x in d.get('bids',[]) if float(x['remaining_base_amount'])>0]
        ask=min(asks)[0] if asks else None
        bid=max(bids)[0] if bids else None
        if ask and bid:
            mid=(ask+bid)/2
            spread=(ask-bid)/mid*1e4
            rows.append({'t':time.time(),'bid':bid,'ask':ask,'spread_bps':spread})
        else:
            rows.append({'t':time.time(),'error':'missing_bbo'})
    except Exception as e:
        rows.append({'t':time.time(),'error':repr(e)})
    time.sleep(1)
ok=[x for x in rows if 'spread_bps' in x]
out={'samples':len(rows),'ok':len(ok),'first':rows[:3],'last':rows[-3:]}
if ok:
    vals=[x['spread_bps'] for x in ok]
    out['spread_bps']={'min':min(vals),'median':statistics.median(vals),'mean':statistics.mean(vals),'max':max(vals)}
print('LIGHTER_PROBE_RESULT '+json.dumps(out))
open('lighter_probe.json','w').write(json.dumps({'summary':out,'rows':rows},indent=2))
