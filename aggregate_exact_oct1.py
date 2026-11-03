#!/usr/bin/env python3
import json, pathlib
import v17_turboflow_econ as econ

ROOT=pathlib.Path("exact_artifacts")
days={}
for p in ROOT.rglob("*.json"):
    try:
        x=json.loads(p.read_text())
        if x.get("type")=="V17_TURBOFLOW_ECON_DAY":
            days[x["date"]]=x
    except Exception:
        pass

cal=days["2026-09-01"]
target=days["2026-10-01"]
q=(239.40-20.0)/cal["summary"]["sum_signed_px_delta_per_1btc"]

out={
  "type":"V17_EXACT_TURBOFLOW_OCT1_FINAL",
  "calibration":{
    "known_zero_cost_start":20.0,
    "known_zero_cost_end":239.40,
    "known_trades":4212,
    "replay_summary":cal["summary"],
    "fidelity":cal["fidelity"],
    "implied_fixed_qty_btc":q
  },
  "target_summary":target["summary"],
  "target_counts":target["counts"],
  "fixed_0_01":target["fixed_0_01"],
  "benchmark_calibrated_qty_btc":q,
  "benchmark_calibrated":{
    "worked_example":econ.econ_fixed_qty(target["trades"],q,start=20.0,literal=False),
    "literal_formula":econ.econ_fixed_qty(target["trades"],q,start=20.0,literal=True)
  },
  "notes":[
    "Signal engine is exact V1.7/V1.7.1 Edge-Harvest logic extracted from the original package.",
    "Historical input: Tardis Bybit L2+trades+OI+liquidations.",
    "TurboFlow historical BTC funding rate for 2026-10-01 is not included; economics are pre-funding.",
    "TurboFlow docs contain a size-impact notation tension; both worked-example and literal interpretations are reported."
  ]
}
pathlib.Path("FINAL_V17_EXACT_TURBOFLOW_2026-10-01.json").write_text(json.dumps(out,indent=2))
print(json.dumps(out,indent=2))
