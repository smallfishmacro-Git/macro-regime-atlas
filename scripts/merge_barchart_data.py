"""
merge_barchart_data.py
=======================
Pulls CSV files from the smallfishmacro-Git/market-dashboard repo (data/barchart/)
and merges them into regime_data_ratios.parquet as bc_* columns.

The market-dashboard repo has its own GitHub Actions cron that refreshes these
CSVs twice daily from Barchart, so this script just consumes the already-updated
public files and doesn't need any credentials.

WHAT THIS ADDS TO THE PARQUET:
  bc_VIX, bc_MOVE, bc_VXV, bc_VVIX, bc_OVX, bc_SKEW, bc_VXN, ...
  (whatever CSVs exist in data/barchart/ at the time of run — no hardcoded list)

File naming convention in market-dashboard:
  "Move_Index_$MOVE.csv"       -> bc_MOVE
  "S&P_500_Index_$SPX.csv"     -> bc_SPX
  "ISM Manufacturing Index.csv"-> bc_ISM_Manufacturing_Index (fallback)

CSV schema (from Barchart):
  Time, Open, High, Low, Last, Change, %Chg, Volume
  We take 'Last' as the daily value.

DEPENDENCIES:
  pip install pandas pyarrow requests

USAGE:
  python scripts/merge_barchart_data.py
"""
import io
import sys
import time
from pathlib import Path

import pandas as pd
import requests

PARQUET_PATH = Path("regime_data_ratios.parquet")

GH_OWNER = "smallfishmacro-Git"
GH_REPO = "market-dashboard"
GH_DIR = "data/barchart"
GH_API = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_DIR}"
GH_RAW = f"https://raw.githubusercontent.com/{GH_OWNER}/{GH_REPO}/main/{GH_DIR}"


def slug_from_filename(fn):
    """
    Convert a barchart filename to a clean bc_* column name.

    Examples:
      Move_Index_$MOVE.csv                 -> MOVE
      S&P_500_Index_$SPX.csv               -> SPX
      ISM Manufacturing Index.csv          -> ISM_Manufacturing_Index
    """
    stem = fn[:-4]  # strip .csv
    if "$" in stem:
        return stem.split("$", 1)[1]
    return stem.replace(" ", "_").replace("&", "and")


def list_barchart_csvs(session):
    """Query GitHub API for the list of CSV files in data/barchart/."""
    r = session.get(GH_API, timeout=30,
                    headers={"Accept": "application/vnd.github.v3+json"})
    r.raise_for_status()
    return [item["name"] for item in r.json() if item["name"].endswith(".csv")]


def fetch_barchart_csv(session, filename):
    """Download one CSV and return a time series indexed by date."""
    url = f"{GH_RAW}/{filename}"
    safe_url = url.replace("$", "%24").replace(" ", "%20").replace("&", "%26")
    try:
        r = session.get(safe_url, timeout=30)
        if r.status_code != 200 or len(r.text) < 50:
            print(f"    !! {filename}: HTTP {r.status_code}")
            return None
        df = pd.read_csv(io.StringIO(r.text))
        if "Time" not in df.columns or "Last" not in df.columns:
            print(f"    !! {filename}: unexpected schema {list(df.columns)[:5]}")
            return None
        df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
        df = df.dropna(subset=["Time"]).set_index("Time").sort_index()
        s = pd.to_numeric(df["Last"], errors="coerce").dropna()
        return s
    except Exception as e:
        print(f"    !! {filename}: {str(e)[:120]}")
        return None


def main():
    print("=" * 70)
    print("Barchart (market-dashboard) -> parquet merge")
    print("=" * 70)

    if not PARQUET_PATH.exists():
        print(f"\n[FATAL] {PARQUET_PATH} not found.", file=sys.stderr)
        print("  Run fetch_ratio_universe.py first to create the base parquet.",
              file=sys.stderr)
        sys.exit(1)

    session = requests.Session()

    # --- Discover CSV files ---
    print(f"\n[1/4] Listing CSVs from {GH_OWNER}/{GH_REPO}/{GH_DIR}...")
    try:
        filenames = list_barchart_csvs(session)
    except Exception as e:
        print(f"  [FATAL] GitHub API listing failed: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"  Found {len(filenames)} CSV files")

    if not filenames:
        print("  [FATAL] No CSVs found — is the market-dashboard repo accessible?",
              file=sys.stderr)
        sys.exit(1)

    # --- Fetch each CSV ---
    print(f"\n[2/4] Fetching {len(filenames)} CSVs...")
    bc_cols = {}
    for fn in filenames:
        slug = slug_from_filename(fn)
        col_name = f"bc_{slug}"
        s = fetch_barchart_csv(session, fn)
        if s is not None and len(s) > 0:
            bc_cols[col_name] = s
            print(f"    {col_name:<35}: {len(s):>6} rows  "
                  f"{s.index.min().date()} -> {s.index.max().date()}  "
                  f"last={s.iloc[-1]:.3f}")
        time.sleep(0.2)  # polite pace vs GitHub raw

    if not bc_cols:
        print("\n[FATAL] No barchart series fetched.", file=sys.stderr)
        sys.exit(1)

    bc_df = pd.DataFrame(bc_cols)
    bc_df.index = pd.to_datetime(bc_df.index).normalize()
    bc_df.index.name = "date"
    print(f"  Combined: {bc_df.shape}")

    # --- Load existing parquet ---
    print(f"\n[3/4] Loading existing {PARQUET_PATH}...")
    df = pd.read_parquet(PARQUET_PATH)
    df.index = pd.to_datetime(df.index).normalize()
    print(f"  Before merge: {df.shape}")

    # Drop any pre-existing bc_* columns so we do a clean overwrite
    existing = [c for c in bc_df.columns if c in df.columns]
    if existing:
        print(f"  Removing {len(existing)} pre-existing bc_* cols for clean overwrite")
        df = df.drop(columns=existing)

    # --- Merge and save ---
    print(f"\n[4/4] Merging and saving...")
    merged = df.join(bc_df, how="outer").sort_index()
    print(f"  After merge: {merged.shape}")

    merged.to_parquet(PARQUET_PATH)
    print(f"  Saved: {PARQUET_PATH}")

    # --- Summary ---
    bc_in_merged = [c for c in merged.columns if c.startswith("bc_")]
    print(f"\nSummary: {len(bc_in_merged)} bc_* columns now in parquet")
    print(f"  Sample recent: ", end="")
    for c in ["bc_VIX", "bc_MOVE", "bc_SPX"]:
        if c in merged.columns:
            s = merged[c].dropna()
            if len(s) > 0:
                print(f"{c}={s.iloc[-1]:.2f} ", end="")
    print()

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[FATAL] {e}", file=sys.stderr)
        sys.exit(1)
