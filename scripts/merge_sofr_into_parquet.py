"""
merge_sofr_into_parquet.py
===========================
Fetches SOFR futures data from the rates-regime pipeline and merges
synthetic SOFR-OIS equivalents into regime_data_ratios.parquet.

DATA SOURCE (upstream):
  https://rates-regime.vercel.app/data/sofr.json      (primary)
  https://raw.githubusercontent.com/smallfishmacro-Git/rates-regime/main/
      smallfish-rates/public/data/sofr.json            (fallback)

WHAT THIS ADDS TO THE PARQUET:
  SOFR_OIS_2Y  = time-weighted avg implied rate over next 2Y of quarterly contracts
  SOFR_OIS_5Y  = time-weighted avg implied rate over next 5Y of quarterly contracts

NOTE ON SEMANTICS:
  These are SOFR-based (secured repo rate). Legacy BBG "OIS_2Y" etc. were
  Fed-Funds-based (unsecured). Expected difference: 5-15bp due to SOFR/FF
  basis plus futures convexity adjustment. Qualitative Fed-policy signal
  is identical; only the specific number shifts slightly.

WHY NOT OIS_10Y:
  rates-regime pulls 20 quarterly contracts = 5Y forward coverage.
  10Y would require 40 contracts. For the report, 10Y Treasury yield
  (from yfinance) is used as the long-end reference instead.

USAGE (runs in GitHub Actions or locally):
  python scripts/merge_sofr_into_parquet.py

DEPENDENCIES:
  pip install pandas pyarrow requests
"""
import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np
import requests

PARQUET_PATH = Path("regime_data_ratios.parquet")
PRIMARY_URL = "https://rates-regime.vercel.app/data/sofr.json"
FALLBACK_URL = ("https://raw.githubusercontent.com/smallfishmacro-Git/"
                "rates-regime/main/smallfish-rates/public/data/sofr.json")


def fetch_sofr_json():
    """Fetch the SOFR JSON, trying primary URL first, falling back to GitHub raw."""
    for url in [PRIMARY_URL, FALLBACK_URL]:
        try:
            print(f"  trying {url}...")
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            print(f"    ✓ got {data['count']} contracts, "
                  f"history_days={data.get('historyDays', 'n/a')}, "
                  f"timestamp={data.get('timestamp', 'n/a')}")
            return data
        except Exception as e:
            print(f"    ✗ failed: {e}")
    raise RuntimeError("Both SOFR URLs failed. Check rates-regime repo & Vercel deployment.")


def build_contract_history(data):
    """
    Returns:
      history_df: long-format DataFrame with columns [date, ticker, impRate, settlementDate]
      contracts_meta: dict of ticker -> settlementDate (pd.Timestamp)
    """
    records = []
    contracts_meta = {}
    for c in data["contracts"]:
        ticker = c["ticker"]
        settle = pd.Timestamp(c["settlementDate"])
        contracts_meta[ticker] = settle
        for h in c["history"]:
            records.append({
                "date": pd.Timestamp(h["date"]),
                "ticker": ticker,
                "impRate": 100.0 - h["close"],  # price → implied rate
                "settlementDate": settle,
            })
    hist_df = pd.DataFrame(records).sort_values(["date", "settlementDate"]).reset_index(drop=True)
    return hist_df, contracts_meta


def compute_synthetic_ois(hist_df, contracts_meta):
    """
    For each historical date, compute synthetic OIS tenors as a TIME-WEIGHTED
    average of forward quarterly contracts, covering exactly the tenor window.

    Method:
      For tenor T years from trade_date:
      - Horizon end = trade_date + T years
      - For each forward contract with settlement <= horizon_end:
        weight = contract's period length (capped at horizon_end for the last one)
      - If not enough contracts extend past horizon, extend with farther contracts
        (gracefully degrade to simple avg of available)
      - OIS = sum(weight * rate) / sum(weight)

    This is more accurate than simple averaging when the curve is sloped,
    because each contract's price represents the average SOFR over a
    specific 3-month window. Weighting by window length within the tenor
    gives a proper time-integrated forward rate.

    Returns: DataFrame indexed by date with SOFR_OIS_2Y, SOFR_OIS_5Y and _N columns.
    """
    TENOR_YEARS = {"SOFR_OIS_2Y": 2.0, "SOFR_OIS_5Y": 5.0}
    results = []

    for trade_date, group in hist_df.groupby("date"):
        forward = group[group["settlementDate"] > trade_date].copy()
        forward = forward.sort_values("settlementDate").reset_index(drop=True)
        if len(forward) == 0:
            continue

        row = {"date": trade_date}
        for tenor_col, tenor_yrs in TENOR_YEARS.items():
            horizon_end = trade_date + pd.Timedelta(days=int(tenor_yrs * 365.25))

            # Build time-weighted windows for each contract.
            # Each contract covers from the PRIOR contract's settlement to its own settlement.
            # The first contract's window starts at trade_date.
            weights = []
            rates = []
            prev_end = trade_date
            for _, c in forward.iterrows():
                window_start = prev_end
                window_end = c["settlementDate"]

                # If this contract starts after horizon, we're done
                if window_start >= horizon_end:
                    break

                # Cap the window at horizon_end
                effective_end = min(window_end, horizon_end)
                weight_days = (effective_end - window_start).days
                if weight_days <= 0:
                    prev_end = window_end
                    continue

                weights.append(weight_days)
                rates.append(c["impRate"])
                prev_end = window_end

                # If we've covered the full horizon, stop
                if effective_end >= horizon_end:
                    break

            if not weights:
                row[tenor_col] = np.nan
                row[f"{tenor_col}_N"] = 0
                continue

            total_weight = sum(weights)
            total_days_needed = (horizon_end - trade_date).days
            coverage = total_weight / total_days_needed

            # Require at least 50% coverage of the tenor window
            if coverage < 0.5:
                row[tenor_col] = np.nan
                row[f"{tenor_col}_N"] = len(rates)
                continue

            weighted_avg = sum(w * r for w, r in zip(weights, rates)) / total_weight
            row[tenor_col] = weighted_avg
            row[f"{tenor_col}_N"] = len(rates)

        results.append(row)

    ois_df = pd.DataFrame(results).set_index("date").sort_index()
    return ois_df


def merge_into_parquet(ois_df, parquet_path):
    """
    Merge synthetic OIS columns into regime_data_ratios.parquet.
    Uses an outer join on the date index so historical parquet dates are preserved
    (existing rows keep their data; SOFR_OIS_* columns only populated where available).
    """
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"{parquet_path} not found. Run fetch_ratio_universe.py first to create it."
        )

    df = pd.read_parquet(parquet_path)
    df.index = pd.to_datetime(df.index).normalize()
    ois_df.index = pd.to_datetime(ois_df.index).normalize()

    # Drop any pre-existing SOFR_OIS_* columns so we do a clean overwrite (no stale values)
    sofr_cols_existing = [c for c in df.columns if c.startswith("SOFR_OIS_")]
    if sofr_cols_existing:
        print(f"  removing pre-existing SOFR cols: {sofr_cols_existing}")
        df = df.drop(columns=sofr_cols_existing)

    # Outer join preserves all existing parquet dates
    merged = df.join(ois_df, how="outer")
    merged = merged.sort_index()

    # Write back
    merged.to_parquet(parquet_path)
    return merged


def validate(merged):
    """Print a sanity check comparing SOFR-derived OIS to any legacy BBG OIS columns."""
    last_date = merged.dropna(subset=["SOFR_OIS_2Y"]).index.max()
    if pd.isna(last_date):
        print("  ⚠ no SOFR_OIS_2Y data was written. Check data source.")
        return

    last = merged.loc[last_date]
    print(f"\n  Latest SOFR-derived curve (as of {last_date.date()}):")
    for tenor in ["SOFR_OIS_2Y", "SOFR_OIS_5Y"]:
        val = last.get(tenor)
        n = last.get(f"{tenor}_N")
        if pd.notna(val):
            print(f"    {tenor} = {val:.3f}%  (time-weighted over {int(n)} contracts)")

    # If legacy BBG columns exist, compare
    legacy_pairs = [("SOFR_OIS_2Y", "OIS_2Y"), ("SOFR_OIS_5Y", "OIS_5Y")]
    print("\n  Validation vs legacy BBG OIS (if present):")
    print("  NOTE: BBG OIS = Fed-Funds-OIS; our synthetic = SOFR-OIS.")
    print("        Expected gap 5-15bp due to SOFR/FF basis + futures convexity.")
    any_compared = False
    for sofr_col, bbg_col in legacy_pairs:
        if bbg_col in merged.columns:
            bbg_series = merged[bbg_col].dropna()
            if len(bbg_series) == 0:
                continue
            bbg_last_date = bbg_series.index.max()
            bbg_last_val = bbg_series.iloc[-1]
            sofr_on_that_date = merged.loc[bbg_last_date, sofr_col] \
                if bbg_last_date in merged.index else np.nan
            if pd.notna(sofr_on_that_date):
                diff_bp = (sofr_on_that_date - bbg_last_val) * 100
                # 20bp threshold given known SOFR/FF basis + convexity effects
                flag = "✓" if abs(diff_bp) < 20 else "⚠ LARGE GAP (check data)"
                print(f"    {sofr_col} vs {bbg_col} on {bbg_last_date.date()}: "
                      f"{sofr_on_that_date:.3f}% vs {bbg_last_val:.3f}% "
                      f"(diff {diff_bp:+.1f}bp) {flag}")
                any_compared = True
    if not any_compared:
        print("    (no legacy BBG OIS columns found — first run after BBG decommission)")


def main():
    print("=" * 70)
    print("SOFR → parquet merge")
    print("=" * 70)

    print("\n[1/4] Fetching sofr.json...")
    data = fetch_sofr_json()

    print("\n[2/4] Building contract history...")
    hist_df, contracts_meta = build_contract_history(data)
    print(f"  {hist_df['ticker'].nunique()} contracts, "
          f"{hist_df['date'].nunique()} dates, "
          f"range {hist_df['date'].min().date()} → {hist_df['date'].max().date()}")

    print("\n[3/4] Computing synthetic OIS tenors...")
    ois_df = compute_synthetic_ois(hist_df, contracts_meta)
    print(f"  computed {len(ois_df)} daily curve snapshots")

    print(f"\n[4/4] Merging into {PARQUET_PATH}...")
    merged = merge_into_parquet(ois_df, PARQUET_PATH)
    print(f"  merged parquet shape: {merged.shape}")

    validate(merged)

    print("\n" + "=" * 70)
    print("DONE — parquet now has SOFR_OIS_2Y and SOFR_OIS_5Y columns")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ FATAL: {e}", file=sys.stderr)
        sys.exit(1)
