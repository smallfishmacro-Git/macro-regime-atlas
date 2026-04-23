"""
fetch_fred_data.py
==================
Pulls FRED macro series and merges them into regime_data_ratios.parquet.

Replaces the FRED portion of the legacy fetch_regime_data.py. Runs in GitHub
Actions: requires FRED_API_KEY as a repo secret (read via env var).

WHAT THIS ADDS TO THE PARQUET:
  Yield curve:       Y_3M, Y_2Y, Y_5Y, Y_7Y, Y_10Y, Y_30Y
  Real yields:       RY_5Y, RY_10Y, RY_30Y
  Breakevens:        BE_5Y, BE_10Y, BE_5Y5Y
  Credit spreads:    HY_OAS, IG_OAS, BBB_OAS
  Fed policy:        FED_FUNDS, IORB

DEPENDENCIES:
  pip install pandas pyarrow fredapi

USAGE:
  FRED_API_KEY=... python scripts/fetch_fred_data.py
"""
import os
import sys
import time
from pathlib import Path

import pandas as pd
from fredapi import Fred

# Load keys from .env if present (keeps them out of git).
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for line in open(_env_path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

PARQUET_PATH = Path("regime_data_ratios.parquet")
START = "2000-01-01"

FRED_SERIES = {
    # Yield curve
    "Y_3M":      "DGS3MO",
    "Y_2Y":      "DGS2",
    "Y_5Y":      "DGS5",
    "Y_7Y":      "DGS7",
    "Y_10Y":     "DGS10",
    "Y_30Y":     "DGS30",
    # Real yields
    "RY_5Y":     "DFII5",
    "RY_10Y":    "DFII10",
    "RY_30Y":    "DFII30",
    # Inflation breakevens
    "BE_5Y":     "T5YIE",
    "BE_10Y":    "T10YIE",
    "BE_5Y5Y":   "T5YIFR",
    # Credit spreads
    "HY_OAS":    "BAMLH0A0HYM2",
    "IG_OAS":    "BAMLC0A0CM",
    "BBB_OAS":   "BAMLC0A4CBBB",
    # Fed policy rates
    "FED_FUNDS": "DFF",
    "IORB":      "IORB",
}


def fetch_fred_series(fred, sid, retries=4):
    """Fetch a single FRED series with exponential-backoff retries."""
    for attempt in range(retries):
        try:
            return fred.get_series(sid, observation_start=START)
        except Exception as e:
            wait = 3 * (2 ** attempt)
            msg = str(e)[:80]
            print(f"    {sid} attempt {attempt+1} failed ({msg}); sleeping {wait}s")
            time.sleep(wait)
    print(f"    !! {sid} FAILED after {retries} retries; dropping series")
    return pd.Series(dtype="float64")


def main():
    print("=" * 70)
    print("FRED -> parquet merge")
    print("=" * 70)

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print("\n[FATAL] FRED_API_KEY not set in environment.", file=sys.stderr)
        print("  Local: create .env with FRED_API_KEY=... in repo root", file=sys.stderr)
        print("  Actions: add FRED_API_KEY as a repo secret", file=sys.stderr)
        sys.exit(1)

    if not PARQUET_PATH.exists():
        print(f"\n[FATAL] {PARQUET_PATH} not found.", file=sys.stderr)
        print("  Run fetch_ratio_universe.py first to create the base parquet.",
              file=sys.stderr)
        sys.exit(1)

    # --- Pull FRED ---
    print(f"\n[1/3] Fetching {len(FRED_SERIES)} FRED series...")
    fred = Fred(api_key=api_key)
    fred_cols = {}
    for name, sid in FRED_SERIES.items():
        s = fetch_fred_series(fred, sid)
        if len(s) > 0:
            fred_cols[name] = s
            print(f"    {name:<10} ({sid}): {len(s)} rows, "
                  f"last={s.dropna().iloc[-1] if s.notna().any() else 'NA'}")
        time.sleep(0.2)  # gentle pace, FRED has no hard rate limit but be polite

    if not fred_cols:
        print("\n[FATAL] No FRED series fetched. Aborting merge.", file=sys.stderr)
        sys.exit(1)

    fred_df = pd.DataFrame(fred_cols)
    fred_df.index = pd.to_datetime(fred_df.index).normalize()
    fred_df.index.name = "date"
    print(f"  FRED combined: {fred_df.shape}")

    # --- Load existing parquet ---
    print(f"\n[2/3] Loading existing {PARQUET_PATH}...")
    df = pd.read_parquet(PARQUET_PATH)
    df.index = pd.to_datetime(df.index).normalize()
    print(f"  Before merge: {df.shape}")

    # Drop any pre-existing FRED columns so we do a clean overwrite
    existing = [c for c in fred_df.columns if c in df.columns]
    if existing:
        print(f"  Removing pre-existing FRED cols for clean overwrite: {existing}")
        df = df.drop(columns=existing)

    # --- Merge and save ---
    print(f"\n[3/3] Merging and saving...")
    merged = df.join(fred_df, how="outer").sort_index()
    print(f"  After merge: {merged.shape}")

    merged.to_parquet(PARQUET_PATH)
    print(f"  Saved: {PARQUET_PATH}")

    # --- Quick validation ---
    print("\nLatest FRED values (most recent non-null per column):")
    for col in FRED_SERIES.keys():
        if col in merged.columns:
            s = merged[col].dropna()
            if len(s) > 0:
                print(f"  {col:<10}: {s.iloc[-1]:.3f}  (as of {s.index[-1].date()})")
            else:
                print(f"  {col:<10}: (empty)")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[FATAL] {e}", file=sys.stderr)
        sys.exit(1)
