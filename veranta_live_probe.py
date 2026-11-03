import asyncio, json, math
from veranta_sdk import AsyncVeranta

async def main():
    async with AsyncVeranta() as client:
        snap = await client.markets.snapshot(force=True)
        p = await client.markets.upside_pair_for("BTC/USD")
        px = await client.markets.price(p.index)
        qty_btc = 0.01
        notional = qty_btc * px
        # minimum leverage that makes 0.01 BTC fit inside $20 collateral
        need_lev = math.ceil(notional / 20.0)
        lev = max(float(p.leverages.pnl_min_leverage or 1), float(need_lev))
        lev = min(lev, float(p.leverages.pnl_max_leverage or lev))
        collateral = notional / lev
        def d(x):
            try: return x.model_dump(by_alias=True)
            except Exception: return x
        spreads = {}
        for side,is_long in [("long",True),("short",False)]:
            for leg,is_open in [("open",True),("close",False)]:
                try:
                    s = await client.markets.spread(
                        p.index, is_long=is_long, coin_size=qty_btc,
                        leverage=lev, is_open=is_open, order_type="market"
                    )
                    spreads[f"{side}_{leg}"] = s
                except Exception as e:
                    spreads[f"{side}_{leg}"] = {"error":repr(e)}
        out = {
            "pair_index":p.index,
            "symbol":p.symbol,
            "base_symbol":p.base_symbol,
            "price":px,
            "qty_btc":qty_btc,
            "notional_usd":notional,
            "chosen_leverage":lev,
            "collateral_usd":collateral,
            "snapshot_max_trades_per_pair":snap.max_trades_per_pair,
            "leverages":d(p.leverages),
            "spreadP":p.spread_p,
            "pnlSpreadP":p.pnl_spread_p,
            "openFeeP":p.open_fee_p,
            "closeFeeP":p.close_fee_p,
            "minLevPosUSDC":p.min_lev_pos_usdc,
            "fundingRate":d(p.funding_rate),
            "fundingFeePerHourP":p.funding_fee_per_hour_p,
            "marginFee":d(p.margin_fee),
            "pnlFees":d(p.pnl_fees),
            "values":d(p.values),
            "openInterest":d(p.open_interest),
            "pairOI":p.pair_oi,
            "pairMaxOI":p.pair_max_oi,
            "maxWalletOI":p.max_wallet_oi,
            "liquidity":p.liquidity,
            "storagePairParams":d(p.storage_pair_params),
            "additionalPairParams2":d(p.additional_params),
            "is_upside":p.is_upside,
            "is_market_open":p.is_market_open,
            "spreads":spreads,
        }
        print("VERANTA_LIVE_ECONOMICS "+json.dumps(out,separators=(',',':')))
        open("veranta_live_economics.json","w").write(json.dumps(out,indent=2))

asyncio.run(main())
