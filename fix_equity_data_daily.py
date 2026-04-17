# fix_equity_data_daily.py
# Replaces the weekly-ffilled px_* columns in regime_data.parquet with TRUE
# daily closes for 2016+ via Nasdaq's public historical API. Pre-2016 history
# keeps the Alpha Vantage weekly-ffill (which is all that was available
# free-tier; Yahoo's IP block is still active, so yfinance is not usable here).
#
# Dependencies: pip install pandas pyarrow curl_cffi

import time
import pandas as pd
from curl_cffi import requests as curl_requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PARQUET = "./regime_data.parquet"
TICKERS = ["SPY","IWM","QQQ","XLF","XLE","XLP","XLU","XLK","XLY","XLI","XLB","XLV","XLRE",
           "TLT","IEF","SHY","HYG","LQD","GLD","UUP","DBC"]

http = curl_requests.Session(impersonate="chrome", verify=False)

def fetch_nasdaq_daily(ticker: str, retries: int = 3) -> pd.Series | None:
    url = (f"https://api.nasdaq.com/api/quote/{ticker}/historical"
           f"?assetclass=etf&fromdate=2000-01-01&todate=2030-12-31"
           f"&limit=9999&timeframe=max")
    for attempt in range(retries):
        try:
            r = http.get(url, timeout=30)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            rows = r.json().get("data", {}).get("tradesTable", {}).get("rows") or []
            if not rows:
                raise RuntimeError("empty rows")
            df_t = pd.DataFrame(rows)
            df_t["date"] = pd.to_datetime(df_t["date"])
            df_t["close"] = pd.to_numeric(
                df_t["close"].astype(str).str.replace(",", "").str.replace("$", ""),
                errors="coerce",
            )
            df_t = df_t.dropna(subset=["close"]).set_index("date").sort_index()
            return df_t["close"]
        except Exception as e:
            wait = 3 * (2 ** attempt)
            print(f"  {ticker} attempt {attempt+1} failed ({str(e)[:80]}); sleeping {wait}s")
            time.sleep(wait)
    print(f"  !! {ticker} FAILED after {retries} retries")
    return None

print(f"Loading {PARQUET}...")
df = pd.read_parquet(PARQUET)
print(f"  before: {df.shape}, px_SPY non-null = {df['px_SPY'].notna().sum()}")

print("Fetching Nasdaq daily closes (no key, ~0.5s/ticker)...")
nasdaq_daily: dict[str, pd.Series] = {}
for tkr in TICKERS:
    s = fetch_nasdaq_daily(tkr)
    if s is not None:
        nasdaq_daily[tkr] = s
        print(f"  {tkr}: {len(s)} daily rows ({s.index.min().date()} -> {s.index.max().date()})")
    time.sleep(0.5)

# Build a daily-indexed frame covering min->max of both datasets, using
# Nasdaq where we have it and falling back to the AV weekly (ffilled) elsewhere.
existing_index = df.index
nd_frame = pd.DataFrame(nasdaq_daily)
nd_frame.index.name = "date"
full_index = existing_index.union(nd_frame.index).sort_values()
df = df.reindex(full_index)

for tkr in TICKERS:
    col = f"px_{tkr}"
    if tkr not in nasdaq_daily:
        continue
    nd = nasdaq_daily[tkr].reindex(full_index)
    # Nasdaq daily takes priority in its range; older dates keep AV ffill.
    df[col] = nd.combine_first(df[col])

print(f"  after:  {df.shape}, px_SPY non-null = {df['px_SPY'].notna().sum()}")

df.to_parquet(PARQUET)
print(f"Saved: {PARQUET}")
