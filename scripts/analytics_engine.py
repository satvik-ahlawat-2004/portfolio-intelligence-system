"""
Analytics Engine Script

Generates technical market analytics from the cleaned dataset.
Calculates moving averages, volatility, cumulative returns, and trend signals
for each asset group (Nifty50, Gold, Silver).
"""

import os
import pandas as pd
import logging
try:
    from scripts.google_sheets_db import sync_market_prices_from_analytics
except ModuleNotFoundError:
    from google_sheets_db import sync_market_prices_from_analytics

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR     = os.path.join(PROJECT_ROOT, "data")
INPUT_PATH   = os.path.join(DATA_DIR, "market_data_clean.csv")
OUTPUT_PATH  = os.path.join(DATA_DIR, "market_analytics.csv")


def load_data() -> pd.DataFrame:
    """Load the cleaned market dataset from CSV."""
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(
            f"Cleaned data file not found: {INPUT_PATH}. "
            "Please run data_cleaner.py first."
        )

    logger.info(f"Loading cleaned data from {INPUT_PATH}...")
    df = pd.read_csv(INPUT_PATH, parse_dates=["Date"])
    logger.info(f"  → {len(df):,} rows loaded across {df['Asset'].nunique()} assets.")
    return df


def compute_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate 20-day and 50-day moving averages of Close, grouped by Asset."""
    logger.info("Computing MA20 and MA50...")
    df["MA20"] = (
        df.groupby("Asset")["Close"]
        .transform(lambda x: x.rolling(window=20, min_periods=1).mean())
    )
    df["MA50"] = (
        df.groupby("Asset")["Close"]
        .transform(lambda x: x.rolling(window=50, min_periods=1).mean())
    )
    return df


def compute_volatility(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate 20-day rolling standard deviation of Daily_Return, grouped by Asset."""
    logger.info("Computing Volatility (rolling 20-day std of Daily_Return)...")
    df["Volatility"] = (
        df.groupby("Asset")["Daily_Return"]
        .transform(lambda x: x.rolling(window=20, min_periods=1).std())
    )
    return df


def compute_cumulative_return(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate cumulative return for each asset relative to its first Close price.

    Formula: (Close / first_close) - 1
    """
    logger.info("Computing Cumulative_Return...")
    df["Cumulative_Return"] = (
        df.groupby("Asset")["Close"]
        .transform(lambda x: (x / x.iloc[0]) - 1)
    )
    return df


def compute_trend(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign a Trend label based on Close vs MA50:
      - Close > MA50 → "Bullish"
      - Close < MA50 → "Bearish"
      - Close == MA50 → "Neutral"
    """
    logger.info("Computing Trend signals...")
    df["Trend"] = "Neutral"
    df.loc[df["Close"] > df["MA50"], "Trend"] = "Bullish"
    df.loc[df["Close"] < df["MA50"], "Trend"] = "Bearish"
    return df


def print_summary(df: pd.DataFrame, new_columns: list) -> None:
    """Print a concise analytics summary to the console."""
    print("\n" + "=" * 55)
    print(" 📈 ANALYTICS ENGINE REPORT")
    print("=" * 55)
    print(f"  Assets processed: {df['Asset'].unique().tolist()}")
    print(f"  Total rows:       {len(df):,}")
    print(f"  New columns:      {new_columns}")
    print("-" * 55)

    for asset, group in df.groupby("Asset"):
        latest = group.sort_values("Date").iloc[-1]
        print(f"\n  [{asset}]")
        print(f"    Latest Close:       {latest['Close']:.2f}")
        print(f"    MA20:               {latest['MA20']:.2f}")
        print(f"    MA50:               {latest['MA50']:.2f}")
        print(f"    Volatility (20d):   {latest['Volatility']:.4f}")
        print(f"    Cumulative Return:  {latest['Cumulative_Return']:.2%}")
        print(f"    Trend:              {latest['Trend']}")

    print("\n" + "=" * 55 + "\n")


def run_analytics() -> None:
    """Main entry point: load, compute analytics, save, and print summary."""
    try:
        df = load_data()
    except FileNotFoundError as e:
        logger.error(str(e))
        return

    # Track which columns we add
    original_columns = set(df.columns)

    # Run analytics steps
    df = compute_moving_averages(df)
    df = compute_volatility(df)
    df = compute_cumulative_return(df)
    df = compute_trend(df)

    usdinr = df[df["Asset"] == "USDINR"][["Date", "Close"]].copy()
    usdinr = usdinr.rename(columns={"Close": "USD_INR"})
    if not usdinr.empty:
        # Safer map join to guarantee column exists
        df["USD_INR"] = df["Date"].map(usdinr.set_index("Date")["USD_INR"])
        
        # Forward fill missing FX rates if timezone holidays mis-match
        df["USD_INR"] = df["USD_INR"].ffill().bfill()
        
        is_gold = df["Asset"] == "Gold"
        df.loc[is_gold, "Gold_USD_oz"] = df.loc[is_gold, "Close"]
        df.loc[is_gold, "Gold_INR_10g"] = (df.loc[is_gold, "Close"] / 31.1035) * df.loc[is_gold, "USD_INR"] * 10
        df.loc[is_gold, "Gold_Retail_10g"] = df.loc[is_gold, "Gold_INR_10g"] * 1.02

        is_silver = df["Asset"] == "Silver"
        df.loc[is_silver, "Silver_USD_oz"] = df.loc[is_silver, "Close"]
        sil_gram = (df.loc[is_silver, "Close"] / 31.1035) * df.loc[is_silver, "USD_INR"]
        df.loc[is_silver, "Silver_INR_10g"] = sil_gram * 10
        df.loc[is_silver, "Silver_INR_kg"] = sil_gram * 1000

        df = df.drop(columns=["USD_INR"])

    # Sort output by Date and Asset
    df = df.sort_values(by=["Date", "Asset"]).reset_index(drop=True)

    # Identify new columns added
    new_columns = [c for c in df.columns if c not in original_columns]

    # Save to output CSV
    try:
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Analytics saved to {OUTPUT_PATH}.")
    except Exception as e:
        logger.error(f"Failed to save analytics data: {e}")
        return

    # Keep CSV pipeline intact while pushing latest prices to Google Sheets backend.
    synced = sync_market_prices_from_analytics(OUTPUT_PATH)
    if synced:
        logger.info("Synced market_prices sheet from analytics output.")
    else:
        logger.warning("market_prices sheet sync skipped/failed; CSV output still available.")

    print_summary(df, new_columns)


if __name__ == "__main__":
    run_analytics()
