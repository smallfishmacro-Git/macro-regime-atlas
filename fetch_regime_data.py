# fetch_regime_data.py
# Saves ./regime_data.parquet with macro + vol + ETF data for Session A.
# Sources: FRED (macro), user GitHub barchart (vol/breadth/SPX), Nasdaq API (ETFs).
# Dependencies: pip install pandas numpy fredapi pyarrow curl_cffi

import io
import os
import time
import pandas as pd
from fredapi import Fred
from curl_cffi import requests as curl_requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load keys from .env if present (keeps them out of git).
_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_path):
    for line in open(_env_path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

FRED_API_KEY = os.environ["FRED_API_KEY"]
AV_API_KEY   = os.environ["AV_API_KEY"]
START = "2000-01-01"

# Corporate network injects a self-signed cert; all outbound HTTPS must go
# through a session with verify disabled for curl_cffi to negotiate.
http = curl_requests.Session(impersonate="chrome", verify=False)

# --- 1. FRED: yield curve, real yields, inflation expectations, credit, Fed ---
fred = Fred(api_key=FRED_API_KEY)
fred_series = {
    "Y_3M":   "DGS3MO",  "Y_2Y":   "DGS2",   "Y_5Y":   "DGS5",
    "Y_7Y":   "DGS7",    "Y_10Y":  "DGS10",  "Y_30Y":  "DGS30",
    "RY_5Y":  "DFII5",   "RY_10Y": "DFII10", "RY_30Y": "DFII30",
    "BE_5Y":  "T5YIE",   "BE_10Y": "T10YIE", "BE_5Y5Y":"T5YIFR",
    "HY_OAS": "BAMLH0A0HYM2",
    "IG_OAS": "BAMLC0A0CM",
    "BBB_OAS":"BAMLC0A4CBBB",
    "FED_FUNDS": "DFF",
    "IORB":      "IORB",
}
print("Pulling FRED data...")
def fetch_fred(sid: str, retries: int = 4):
    for attempt in range(retries):
        try:
            return fred.get_series(sid, observation_start=START)
        except Exception as e:
            wait = 3 * (2 ** attempt)
            print(f"  {sid} attempt {attempt+1} failed ({str(e)[:80]}); sleeping {wait}s")
            time.sleep(wait)
    print(f"  !! {sid} FAILED after {retries} retries; dropping series")
    return pd.Series(dtype="float64")

fred_cols = {}
for name, sid in fred_series.items():
    s = fetch_fred(sid)
    if len(s) > 0:
        fred_cols[name] = s
        print(f"  {name} ({sid}): {len(s)} rows")
    time.sleep(0.2)
fred_df = pd.DataFrame(fred_cols)
fred_df.index.name = "date"
fred_df.index = pd.to_datetime(fred_df.index)
print(f"  FRED: {len(fred_df)} rows, {len(fred_df.columns)} series")

# --- 2. Barchart CSVs from user's market-dashboard repo ---
# Files are indexed daily; schema: Time,Open,High,Low,Last,Change,%Chg,Volume
GH_OWNER = "smallfishmacro-Git"
GH_REPO  = "market-dashboard"
GH_DIR   = "data/barchart"

print("Listing GitHub barchart files...")
r = http.get(
    f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_DIR}",
    timeout=30, headers={"Accept": "application/vnd.github.v3+json"},
)
r.raise_for_status()
gh_files = [i["name"] for i in r.json() if i["name"].endswith(".csv")]
print(f"  {len(gh_files)} CSVs found")

def slug_from_filename(fn: str) -> str:
    # "Move_Index_$MOVE.csv" -> "MOVE"; "S&P_500_Index_$SPX.csv" -> "SPX"
    stem = fn[:-4]
    if "$" in stem:
        return stem.split("$", 1)[1]
    # fallback: use stem lightly cleaned
    return stem.replace(" ", "_").replace("&", "and")

def fetch_barchart(fn: str) -> pd.Series | None:
    url = f"https://raw.githubusercontent.com/{GH_OWNER}/{GH_REPO}/main/{GH_DIR}/{fn}"
    # URL-encode just the $ (which GitHub serves at %24); other chars like & are fine raw in the API contents call but raw endpoint needs encoding
    safe = url.replace("$", "%24").replace(" ", "%20").replace("&", "%26")
    try:
        r = http.get(safe, timeout=30)
        if r.status_code != 200 or len(r.text) < 50:
            print(f"  !! {fn}: HTTP {r.status_code}")
            return None
        df_t = pd.read_csv(io.StringIO(r.text))
        if "Time" not in df_t.columns or "Last" not in df_t.columns:
            print(f"  !! {fn}: unexpected schema {list(df_t.columns)[:5]}")
            return None
        df_t["Time"] = pd.to_datetime(df_t["Time"], errors="coerce")
        df_t = df_t.dropna(subset=["Time"]).set_index("Time").sort_index()
        s = pd.to_numeric(df_t["Last"], errors="coerce").dropna()
        return s
    except Exception as e:
        print(f"  !! {fn}: {str(e)[:80]}")
        return None

print("Pulling barchart CSVs...")
bc_cols = {}
for fn in gh_files:
    slug = slug_from_filename(fn)
    s = fetch_barchart(fn)
    if s is not None and len(s) > 0:
        col = f"bc_{slug}"
        bc_cols[col] = s
        print(f"  {slug}: {len(s)} rows ({s.index.min().date()} -> {s.index.max().date()})")
    time.sleep(0.3)
bc_df = pd.DataFrame(bc_cols)
bc_df.index.name = "date"

# --- 3. Alpha Vantage: ETF prices (weekly-adjusted = free + full history) ---
# Free tier: 25 calls/day, 5 calls/min. Daily full-history is premium-only.
# Weekly adjusted gives inception-to-today with dividend/split adjustments,
# forward-filled to daily during the final join.
av_tickers = ["SPY","IWM","QQQ","XLF","XLE","XLP","XLU","XLK","XLY","XLI","XLB","XLV","XLRE",
              "TLT","IEF","SHY","HYG","LQD","GLD","UUP","DBC"]
print("Pulling Alpha Vantage ETF data (weekly adjusted, 5/min rate limit)...")

def fetch_av(ticker: str) -> pd.Series | None:
    url = ("https://www.alphavantage.co/query"
           f"?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={ticker}"
           f"&datatype=csv&apikey={AV_API_KEY}")
    try:
        r = http.get(url, timeout=60)
        if r.status_code != 200 or len(r.text) < 200:
            print(f"  !! {ticker}: HTTP {r.status_code}, bytes={len(r.text)}, body={r.text[:120]}")
            return None
        if r.text.lstrip().startswith("{"):
            print(f"  !! {ticker}: JSON response (likely rate-limit/error): {r.text[:200]}")
            return None
        df_t = pd.read_csv(io.StringIO(r.text))
        price_col = "adjusted close" if "adjusted close" in df_t.columns else "close"
        if "timestamp" not in df_t.columns or price_col not in df_t.columns:
            print(f"  !! {ticker}: unexpected schema {list(df_t.columns)}")
            return None
        df_t["timestamp"] = pd.to_datetime(df_t["timestamp"])
        df_t = df_t.set_index("timestamp").sort_index()
        s = pd.to_numeric(df_t[price_col], errors="coerce").dropna()
        s = s[s.index >= pd.Timestamp(START)]
        return s
    except Exception as e:
        print(f"  !! {ticker}: {str(e)[:120]}")
        return None

nd_cols = {}
for i, tkr in enumerate(av_tickers):
    s = fetch_av(tkr)
    if s is not None:
        nd_cols[f"px_{tkr}"] = s
        print(f"  {tkr}: {len(s)} rows ({s.index.min().date()} -> {s.index.max().date()})")
    # 5 calls/min on free tier = 12s spacing with a small safety margin
    if i < len(av_tickers) - 1:
        time.sleep(13)
nd_df = pd.DataFrame(nd_cols)
nd_df.index.name = "date"

# --- 4. Join and save ---
print("Joining...")
df = fred_df.join(bc_df, how="outer").join(nd_df, how="outer")
df = df.sort_index().ffill()
df = df.dropna(how="all")
print(f"Final dataset: {df.index[0].date()} -> {df.index[-1].date()}, "
      f"{len(df)} rows, {len(df.columns)} series")

df.to_parquet("./regime_data.parquet")
print("Saved: regime_data.parquet")
print(f"Columns: {list(df.columns)}")
