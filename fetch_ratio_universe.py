"""
fetch_ratio_universe.py
=======================
Pulls all tickers needed for the 47-ratio cross-asset composite.

Run this via your GitHub Actions setup (same pattern you used for fix_equity_data_daily.py).
Output: merges all new tickers into your existing regime_data.parquet

What's new vs what's already in your parquet:
  Already have: SPY IWM QQQ XLF XLE XLP XLU XLK XLY XLI XLB XLV XLRE TLT IEF SHY
                HYG LQD GLD UUP DBC  (21 ETFs)
  NEW ETFs needed: MDY RSP XHB EMB PFF SOXX SPHB SPLV IWD IWF IYT COPX DJP XME
                   KRE BKLN JNK ITB  (18 ETFs)
  NEW FX needed:  DXY AUDJPY ZARJPY USDZAR ZARCHF  (5)
  NEW CRYPTO:     BTC-USD ETH-USD  (2)
  NEW VOL:        ^VIX ^VXV ^SKEW  (3)

Total new pulls: 28 tickers.

Dependencies: pip install yfinance pandas pyarrow
"""
import pandas as pd
import yfinance as yf

INPUT_PARQUET = "regime_data.parquet"       # your current file
OUTPUT_PARQUET = "regime_data_ratios.parquet"

# =============================================================================
# NEW EQUITY ETFs (18)
# =============================================================================
NEW_ETFS = [
    "MDY",     # S&P 400 mid-cap
    "RSP",     # equal-weight S&P 500
    "XHB",     # homebuilders
    "EMB",     # EM USD sovereign
    "PFF",     # preferred stock
    "SOXX",    # semiconductors
    "SPHB",    # high-beta S&P 500
    "SPLV",    # low-vol S&P 500
    "IWD",     # Russell 1000 Value
    "IWF",     # Russell 1000 Growth
    "IYT",     # transportation
    "COPX",    # copper miners
    "DJP",     # broad commodities
    "XME",     # metals & mining
    "KRE",     # regional banks
    "BKLN",    # leveraged loans
    "JNK",     # high-yield bond (alternative to HYG)
    "ITB",     # home construction
]

# =============================================================================
# FX — yfinance uses =X suffix for FX pairs
# =============================================================================
FX_TICKERS = {
    "DXY":     "DX-Y.NYB",    # US Dollar Index
    "AUDJPY":  "AUDJPY=X",
    "ZARJPY":  "ZARJPY=X",
    "USDZAR":  "USDZAR=X",
    "ZARCHF":  "ZARCHF=X",
}

# =============================================================================
# CRYPTO — yfinance uses -USD suffix
# =============================================================================
CRYPTO_TICKERS = {
    "BTC":  "BTC-USD",
    "ETH":  "ETH-USD",
}

# =============================================================================
# VOL indexes — use ^ prefix for Yahoo indexes
# =============================================================================
VOL_TICKERS = {
    "VIX":   "^VIX",
    "VXV":   "^VXV",     # 3-month VIX
    "SKEW":  "^SKEW",
}

# =============================================================================
# LOAD EXISTING PARQUET
# =============================================================================
print("Loading existing parquet...")
df = pd.read_parquet(INPUT_PARQUET)
print(f"  Existing: {df.index[0].date()} -> {df.index[-1].date()}, {len(df.columns)} cols")

df.index = pd.to_datetime(df.index).normalize()

# =============================================================================
# FETCH NEW EQUITY ETFs
# =============================================================================
print(f"\nFetching {len(NEW_ETFS)} new equity ETFs...")
etf_data = yf.download(NEW_ETFS, start="2000-01-01", interval="1d",
                       auto_adjust=False, progress=True)
etf_px = etf_data["Adj Close"].copy()
etf_px.columns = [f"px_{c}" for c in etf_px.columns]
etf_px.index = pd.to_datetime(etf_px.index).normalize()
print(f"  ETF range: {etf_px.index[0].date()} -> {etf_px.index[-1].date()}")
for c in etf_px.columns:
    non_null = etf_px[c].notna().sum()
    first = etf_px[c].dropna().index[0].date() if non_null > 0 else "N/A"
    print(f"    {c}: {non_null} obs, starts {first}")

# =============================================================================
# FETCH FX
# =============================================================================
print(f"\nFetching {len(FX_TICKERS)} FX pairs...")
fx_frames = []
for name, yf_ticker in FX_TICKERS.items():
    try:
        d = yf.download(yf_ticker, start="2000-01-01", interval="1d",
                        auto_adjust=False, progress=False)
        if not d.empty:
            px = d["Close"].copy() if "Close" in d.columns else d.iloc[:, 0]
            px.name = f"fx_{name}"
            px.index = pd.to_datetime(px.index).normalize()
            fx_frames.append(px)
            print(f"  fx_{name}: {px.notna().sum()} obs, starts {px.dropna().index[0].date()}")
        else:
            print(f"  fx_{name}: NO DATA (will need to source elsewhere)")
    except Exception as e:
        print(f"  fx_{name}: FAILED - {e}")

fx_df = pd.concat(fx_frames, axis=1) if fx_frames else pd.DataFrame()

# =============================================================================
# FETCH CRYPTO
# =============================================================================
print(f"\nFetching {len(CRYPTO_TICKERS)} crypto tickers...")
crypto_frames = []
for name, yf_ticker in CRYPTO_TICKERS.items():
    try:
        d = yf.download(yf_ticker, start="2014-01-01", interval="1d",
                        auto_adjust=False, progress=False)
        if not d.empty:
            px = d["Close"].copy() if "Close" in d.columns else d.iloc[:, 0]
            px.name = f"crypto_{name}"
            px.index = pd.to_datetime(px.index).normalize()
            crypto_frames.append(px)
            print(f"  crypto_{name}: {px.notna().sum()} obs, starts {px.dropna().index[0].date()}")
    except Exception as e:
        print(f"  crypto_{name}: FAILED - {e}")

crypto_df = pd.concat(crypto_frames, axis=1) if crypto_frames else pd.DataFrame()

# =============================================================================
# FETCH VOL INDEXES
# =============================================================================
print(f"\nFetching {len(VOL_TICKERS)} vol indexes...")
vol_frames = []
for name, yf_ticker in VOL_TICKERS.items():
    try:
        d = yf.download(yf_ticker, start="1990-01-01", interval="1d",
                        auto_adjust=False, progress=False)
        if not d.empty:
            px = d["Close"].copy() if "Close" in d.columns else d.iloc[:, 0]
            px.name = f"vol_{name}"
            px.index = pd.to_datetime(px.index).normalize()
            vol_frames.append(px)
            print(f"  vol_{name}: {px.notna().sum()} obs, starts {px.dropna().index[0].date()}")
    except Exception as e:
        print(f"  vol_{name}: FAILED - {e}")

vol_df = pd.concat(vol_frames, axis=1) if vol_frames else pd.DataFrame()

# =============================================================================
# MERGE ALL
# =============================================================================
print("\nMerging into parquet...")
merged = df.copy()
for name, new_df in [("ETFs", etf_px), ("FX", fx_df),
                     ("Crypto", crypto_df), ("Vol", vol_df)]:
    if not new_df.empty:
        merged = merged.join(new_df, how="outer")
        print(f"  + {name}: {new_df.shape[1]} columns")

print(f"\nMerged shape: {merged.shape}")
print(f"Range: {merged.index[0].date()} -> {merged.index[-1].date()}")

merged.to_parquet(OUTPUT_PARQUET)
print(f"\n✓ Saved {OUTPUT_PARQUET}")

# Verify by listing all new columns
new_cols = [c for c in merged.columns if c not in df.columns]
print(f"\n{len(new_cols)} new columns added:")
for c in sorted(new_cols):
    last_val = merged[c].dropna().iloc[-1] if merged[c].notna().any() else "N/A"
    print(f"  {c}: last value = {last_val}")

print("\nUpload regime_data_ratios.parquet to Claude for the next session.")
