# update_etf_daily.py
# Runs in GitHub Actions (Azure IPs — Yahoo usually not blocked there).
# Pulls true daily close for 21 ETFs via yfinance with impersonated curl_cffi
# session, writes data/etf_daily.parquet + data/etf_daily_meta.json which are
# then committed back to the repo by the workflow.

import json
import os
import time
import pandas as pd
import yfinance as yf
from curl_cffi import requests as curl_requests

TICKERS = ["SPY","IWM","QQQ","XLF","XLE","XLP","XLU","XLK","XLY","XLI","XLB","XLV","XLRE",
           "TLT","IEF","SHY","HYG","LQD","GLD","UUP","DBC"]

session = curl_requests.Session(impersonate="chrome")

def fetch_one(tkr: str, retries: int = 4) -> pd.Series | None:
    for attempt in range(retries):
        try:
            t = yf.Ticker(tkr, session=session)
            h = t.history(period="max", auto_adjust=True, actions=False)
            if h is None or h.empty:
                raise RuntimeError("empty frame")
            s = h["Close"]
            s.index = s.index.tz_localize(None)
            return s
        except Exception as e:
            wait = 10 * (2 ** attempt)
            print(f"  {tkr} attempt {attempt+1}/{retries}: {str(e)[:100]}; sleeping {wait}s", flush=True)
            time.sleep(wait)
    print(f"  !! {tkr} FAILED after {retries} retries", flush=True)
    return None

print(f"Fetching {len(TICKERS)} tickers via yfinance (serial, 3s spacing)...", flush=True)
cols: dict[str, pd.Series] = {}
for tkr in TICKERS:
    s = fetch_one(tkr)
    if s is not None and len(s) > 0:
        cols[f"px_{tkr}"] = s
        print(f"  {tkr}: {len(s)} rows ({s.index.min().date()} -> {s.index.max().date()})", flush=True)
    time.sleep(3)

if not cols:
    raise SystemExit("No tickers fetched — aborting to avoid committing empty data.")

df = pd.DataFrame(cols).sort_index()
os.makedirs("data", exist_ok=True)
df.to_parquet("data/etf_daily.parquet")

meta = {
    "updated_utc": pd.Timestamp.utcnow().isoformat(),
    "tickers_ok": sorted(cols.keys()),
    "tickers_failed": sorted([f"px_{t}" for t in TICKERS if f"px_{t}" not in cols]),
    "row_count": int(len(df)),
    "date_range": [str(df.index.min().date()), str(df.index.max().date())],
    "source": "yfinance via GitHub Actions",
    "adjustment": "auto_adjust=True (split+dividend adjusted close)",
}
with open("data/etf_daily_meta.json", "w") as f:
    json.dump(meta, f, indent=2)

print(f"\nSaved: data/etf_daily.parquet  {df.shape}")
print(f"Meta:  data/etf_daily_meta.json")
print(f"OK: {len(cols)}/{len(TICKERS)} tickers")
if meta["tickers_failed"]:
    print(f"FAILED: {meta['tickers_failed']}")
