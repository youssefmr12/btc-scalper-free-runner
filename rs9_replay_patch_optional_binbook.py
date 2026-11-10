from pathlib import Path
p=Path("rs9_replay_run/bundle/replay_6h.py")
s=p.read_text()
s=s.replace(" 'bin_book':'https://data.binance.vision/data/futures/um/daily/bookTicker/BTCUSDT/BTCUSDT-bookTicker-2026-11-09.zip',\n","")
old="    bin_ok=True\n    try:its += [zip_csv_iter(paths['bin_agg'],'binagg'),zip_csv_iter(paths['bin_book'],'binbook')]\n    except Exception:bin_ok=False\n"
new="    bin_ok=True\n    try:its.append(zip_csv_iter(paths['bin_agg'],'binagg'))\n    except Exception:bin_ok=False\n"
if old not in s:
    raise SystemExit("expected replay block not found")
p.write_text(s.replace(old,new))
print("PATCHED_BINANCE_BOOK_OPTIONAL")
