"""
Data Cleaner Script

Responsible for preprocessing and cleaning the fetched financial data.
"""

import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
INPUT_PATH = os.path.join(DATA_DIR, "market_data_raw.csv")
OUTPUT_PATH = os.path.join(DATA_DIR, "market_data_clean.csv")

def clean_data():
    """
    Main function to load raw data, clean it, calculate daily returns,
    and save the prepared data.
    """
    if not os.path.exists(INPUT_PATH):
        logger.error(f"Input file not found: {INPUT_PATH}. Please run data_fetcher.py first.")
        return

    try:
        logger.info(f"Loading raw data from {INPUT_PATH}...")
        df = pd.read_csv(INPUT_PATH)
    except Exception as e:
        logger.error(f"Failed to load raw data: {e}")
        return

    initial_rows = len(df)

    # 1. Ensure Date column is datetime format
    if "Date" not in df.columns:
        logger.error("Date column is missing from the dataset.")
        return
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # 2. Convert required pricing columns to numeric (Volume is intentionally excluded)
    logger.info("Cleaning and converting pricing columns to numeric types...")
    cols = [
        "Open",
        "High",
        "Low",
        "Close",
        "Gold_USD_oz",
        "Gold_INR_10g",
        "Gold_Retail_10g",
        "Silver_USD_oz",
        "Silver_INR_g",
        "Silver_INR_10g",
        "USD_INR",
    ]

    # Pre-clean: remove commas and strip whitespace before numeric conversion
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.strip()

    # Now safely convert to numeric; unparseable values become NaN
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3. Remove rows where Close is missing only
    df.dropna(subset=["Close"], inplace=True)
    close_removed = initial_rows - len(df)

    # 4. Remove duplicate rows
    pre_dedup = len(df)
    df.drop_duplicates(inplace=True)
    dupes_removed = pre_dedup - len(df)

    # 5. Sort data by Date and Asset
    df = df.sort_values(by=["Date", "Asset"]).reset_index(drop=True)

    # 6. Calculate Daily_Return grouped by Asset
    logger.info("Calculating Daily_Return for each asset...")
    df["Daily_Return"] = df.groupby("Asset")["Close"].pct_change()

    final_rows = len(df)

    # 7. Print summary report
    print("\n" + "=" * 50)
    print(" 📊 DATA CLEANING REPORT")
    print("=" * 50)
    print(f"  Total rows loaded:                   {initial_rows:,}")
    print(f"  Rows removed (missing Close values): {close_removed:,}")
    print(f"  Duplicate rows removed:              {dupes_removed:,}")
    print("-" * 50)
    print(f"  FINAL ROW COUNT:                     {final_rows:,}")
    print("=" * 50 + "\n")

    # 8. Save cleaned dataset
    try:
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Cleaned dataset saved to {OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"Failed to save cleaned data: {e}")

if __name__ == "__main__":
    clean_data()
