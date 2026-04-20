# fix_equity_data_daily.py
# Overwrites the px_* columns in regime_data.parquet with true daily closes
# pulled from the macro-regime-atlas repo (kept fresh by the Actions
# workflow, which runs yfinance from Azure IPs where Yahoo is not blocked).
#
# Dependencies: pip install pandas pyarrow curl_cffi

import io
import pandas as pd
from curl_cffi import requests as curl_requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOCAL_PARQUET  = "./regime_data.parquet"
REMOTE_PARQUET = ("https://raw.githubusercontent.com/smallfishmacro-Git/"
                  "macro-regime-atlas/main/data/etf_daily.parquet")
REMOTE_META    = ("https://raw.githubusercontent.com/smallfishmacro-Git/"
                  "macro-regime-atlas/main/data/etf_daily_meta.json")

http = curl_requests.Session(impersonate="chrome", verify=False)

print("Fetching ETF meta...")
m = http.get(REMOTE_META, timeout=30).json()
print(f"  Actions last ran: {m['updated_utc']}")
print(f"  Range: {m['date_range'][0]} -> {m['date_range'][1]} ({m['row_count']} rows)")
print(f"  Tickers: {len(m['tickers_ok'])} ok, {len(m['tickers_failed'])} failed")
if m["tickers_failed"]:
    print(f"  WARNING failed: {m['tickers_failed']}")

print("Fetching ETF daily parquet...")
r = http.get(REMOTE_PARQUET, timeout=60)
r.raise_for_status()
etf_daily = pd.read_parquet(io.BytesIO(r.content))
etf_daily.index = pd.to_datetime(etf_daily.index)
etf_daily.index.name = "date"
print(f"  {etf_daily.shape}, {etf_daily.index.min().date()} -> {etf_daily.index.max().date()}")

print(f"Loading {LOCAL_PARQUET}...")
df = pd.read_parquet(LOCAL_PARQUET)
df.index = pd.to_datetime(df.index)
px_cols_before = [c for c in df.columns if c.startswith("px_")]

# Union the indexes so older ETF dates (back to 1993 for SPY) are preserved.
full_index = df.index.union(etf_daily.index).sort_values()
df = df.reindex(full_index)

# Drop the old px_* columns (weekly-ffilled from Alpha Vantage) and replace
# with the fresh daily series from the Actions-maintained repo.
df = df.drop(columns=px_cols_before)
for col in etf_daily.columns:
    df[col] = etf_daily[col].reindex(full_index)

print(f"  before: {len(px_cols_before)} px_ cols, weekly-ffill")
print(f"  after:  {sum(c.startswith('px_') for c in df.columns)} px_ cols, true daily")
print(f"  shape:  {df.shape}")

df.to_parquet(LOCAL_PARQUET)
print(f"Saved: {LOCAL_PARQUET}")
