"""
Data Fetcher Script

Responsible for fetching market data for Nifty indices, Gold, Silver, and USD/INR using yfinance.
Calculates professional gold metrics: Global Spot (USD/oz), Global INR (10g), and India Retail (10g).
Stores everything in a unified row-based CSV format.
"""

import os
import logging
from typing import Dict

import pandas as pd
import yfinance as yf

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ── Asset ticker → friendly name mapping ──────────────────────────────────────
ASSETS: Dict[str, str] = {
    "^NSEI":   "Nifty50",
    "^CNX100": "Nifty100",
    "^CNX150": "Nifty150",
    "^CNX200": "Nifty200",
    "^CNX500": "Nifty500",
    "GC=F":    "Gold",
    "SI=F":    "Silver",
    "USDINR=X": "USDINR",
}

FALLBACK_TICKERS: Dict[str, list[str]] = {
    "^CNX500": ["^CRSLDX"],
}

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR     = os.path.join(PROJECT_ROOT, "data")
OUTPUT_PATH  = os.path.join(DATA_DIR, "market_data_raw.csv")

REQUIRED_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume"]

TROY_OUNCE_TO_GRAMS = 31.1035
DATA_SOURCE = "Yahoo Finance (yfinance)"

VALIDATION_RANGES = {
    "Gold_INR_10g": (100000, 200000),
    "Silver_INR_g": (50, 150),
    "Nifty50": (10000, 35000),
    "Nifty100": (10000, 40000),
    "Nifty150": (8000, 30000),
    "Nifty200": (7000, 30000),
    "Nifty500": (5000, 30000),
}

def ensure_data_directory() -> None:
    """Create the data directory if it does not already exist."""
    os.makedirs(DATA_DIR, exist_ok=True)

def _log_fetch_summary(asset_name: str, ticker: str, df: pd.DataFrame) -> None:
    """Log asset-level fetch summary for observability."""
    if df.empty:
        return
    latest = df.sort_values("Date").iloc[-1]
    logger.info(
        "Fetched %s data (%s) | Rows: %s | Latest Close: %.4f | Source: %s",
        asset_name,
        ticker,
        f"{len(df):,}",
        latest["Close"],
        DATA_SOURCE,
    )

def _warn_if_out_of_range(name: str, value: float, min_value: float, max_value: float) -> None:
    """Warn when a key market metric looks unrealistic."""
    if pd.isna(value):
        return
    if value < min_value or value > max_value:
        logger.warning(
            "Validation warning: %s=%.2f is outside expected range [%.2f, %.2f].",
            name,
            value,
            min_value,
            max_value,
        )

def validate_latest_snapshot(combined_df: pd.DataFrame) -> None:
    """Run sanity checks on the latest asset prices without crashing the pipeline."""
    if combined_df.empty:
        return
    latest = combined_df.sort_values("Date").groupby("Asset").last()

    if "Gold" in latest.index:
        _warn_if_out_of_range(
            "Gold_INR_10g",
            latest.loc["Gold"].get("Gold_INR_10g"),
            *VALIDATION_RANGES["Gold_INR_10g"],
        )
    if "Silver" in latest.index:
        _warn_if_out_of_range(
            "Silver_INR_g",
            latest.loc["Silver"].get("Silver_INR_g"),
            *VALIDATION_RANGES["Silver_INR_g"],
        )

    for asset in ("Nifty50", "Nifty100", "Nifty150", "Nifty200", "Nifty500"):
        if asset in latest.index:
            _warn_if_out_of_range(
                asset,
                latest.loc[asset].get("Close"),
                *VALIDATION_RANGES[asset],
            )

def fetch_asset_data(ticker: str, asset_name: str, period: str = "1y") -> pd.DataFrame:
    """
    Fetch daily OHLCV data for a single asset and return a clean, flat DataFrame.
    """
    try:
        logger.info(f"Fetching data for {asset_name} ({ticker})...")

        raw = yf.download(
            ticker,
            period=period,
            interval="1d",
            progress=False,
            auto_adjust=True,
        )

        if raw.empty:
            logger.warning(f"No data returned for {asset_name} ({ticker}).")
            return pd.DataFrame()

        # Flatten MultiIndex columns
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw.reset_index()
        df = df.rename(columns={"Datetime": "Date"})

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)

        available = [c for c in REQUIRED_COLUMNS if c in df.columns]
        df = df[available].copy()
        df["Asset"] = asset_name
        _log_fetch_summary(asset_name=asset_name, ticker=ticker, df=df)
        return df

    except Exception as exc:
        logger.error(f"Error fetching data for {asset_name} ({ticker}): {exc}")
        return pd.DataFrame()

def fetch_data(period: str = "1y") -> None:
    """
    Fetch historical data for all assets and calculate gold metrics.
    """
    ensure_data_directory()

    asset_frames = {}
    for ticker, asset_name in ASSETS.items():
        df = fetch_asset_data(ticker=ticker, asset_name=asset_name, period=period)
        if df.empty and ticker in FALLBACK_TICKERS:
            for fallback_ticker in FALLBACK_TICKERS[ticker]:
                logger.warning(
                    "Primary ticker %s returned no data for %s. Retrying with fallback %s.",
                    ticker,
                    asset_name,
                    fallback_ticker,
                )
                df = fetch_asset_data(
                    ticker=fallback_ticker,
                    asset_name=asset_name,
                    period=period,
                )
                if not df.empty:
                    break
        if not df.empty:
            asset_frames[asset_name] = df

    if not asset_frames:
        logger.error("No data fetched for any asset. Exiting.")
        return

    # ── Build shared USD/INR series for commodity conversions ────────────────
    usdinr_df = asset_frames.get("USDINR", pd.DataFrame()).copy()
    if not usdinr_df.empty:
        usdinr_df = usdinr_df.sort_values("Date")
        usdinr_df = usdinr_df[["Date", "Close"]].rename(columns={"Close": "USD_INR"})
        usdinr_df["USD_INR"] = usdinr_df["USD_INR"].ffill().fillna(83.0)

    # ── Calculate Gold Metrics ────────────────────────────────────────────────
    if "Gold" in asset_frames and not usdinr_df.empty:
        gold_df = asset_frames["Gold"].copy().sort_values("Date")
        merged_gold = pd.merge_asof(gold_df, usdinr_df, on="Date", direction="backward")
        merged_gold["USD_INR"] = merged_gold["USD_INR"].ffill().fillna(83.0)

        merged_gold["Gold_USD_oz"] = merged_gold["Close"]
        merged_gold["Gold_INR_10g"] = (
            (merged_gold["Gold_USD_oz"] / TROY_OUNCE_TO_GRAMS) * merged_gold["USD_INR"] * 10
        )
        merged_gold["Gold_Retail_10g"] = merged_gold["Gold_INR_10g"] * 1.20

        latest_gold = merged_gold.iloc[-1]
        logger.info(
            "Fetched Gold data (GC=F) | Latest: $%.2f / oz | Converted: Rs %.0f / 10g",
            latest_gold["Gold_USD_oz"],
            latest_gold["Gold_INR_10g"],
        )
        asset_frames["Gold"] = merged_gold

    # ── Calculate Silver Metrics ──────────────────────────────────────────────
    if "Silver" in asset_frames and not usdinr_df.empty:
        silver_df = asset_frames["Silver"].copy().sort_values("Date")
        merged_silver = pd.merge_asof(silver_df, usdinr_df, on="Date", direction="backward")
        merged_silver["USD_INR"] = merged_silver["USD_INR"].ffill().fillna(83.0)

        merged_silver["Silver_USD_oz"] = merged_silver["Close"]
        merged_silver["Silver_INR_g"] = (
            merged_silver["Silver_USD_oz"] / TROY_OUNCE_TO_GRAMS
        ) * merged_silver["USD_INR"]
        merged_silver["Silver_INR_10g"] = merged_silver["Silver_INR_g"] * 10

        latest_silver = merged_silver.iloc[-1]
        logger.info(
            "Fetched Silver data (SI=F) | Latest: $%.2f / oz | Converted: Rs %.2f / g",
            latest_silver["Silver_USD_oz"],
            latest_silver["Silver_INR_g"],
        )
        asset_frames["Silver"] = merged_silver

    # ── Combine all frames ────────────────────────────────────────────────────
    combined_df = pd.concat(asset_frames.values(), ignore_index=True)

    # Ensure new columns exist for all rows (will be NaN for non-matching assets)
    for col in [
        "Gold_USD_oz",
        "Gold_INR_10g",
        "Gold_Retail_10g",
        "Silver_USD_oz",
        "Silver_INR_g",
        "Silver_INR_10g",
    ]:
        if col not in combined_df.columns:
            combined_df[col] = None

    combined_df = combined_df.sort_values(by=["Date", "Asset"]).reset_index(drop=True)
    validate_latest_snapshot(combined_df)

    try:
        combined_df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Saved {len(combined_df):,} rows to {OUTPUT_PATH}.")
        print(f"\nSuccess! {len(combined_df):,} rows saved to {OUTPUT_PATH}.")
    except Exception as exc:
        logger.error(f"Failed to save data: {exc}")

if __name__ == "__main__":
    fetch_data()
