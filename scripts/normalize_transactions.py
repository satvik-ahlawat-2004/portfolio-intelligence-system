"""
Normalize and migrate Google Sheets transactions for PMS Operations.

Usage:
    python scripts/normalize_transactions.py
"""

from __future__ import annotations

from datetime import datetime
from typing import List

import pandas as pd

try:
    from scripts import google_sheets_db as sheets_db
    from scripts.portfolio_engine import PortfolioEngine
except ModuleNotFoundError:
    import google_sheets_db as sheets_db
    from portfolio_engine import PortfolioEngine


CANONICAL_HEADERS: List[str] = ["txn_id", "client_id", "stock", "type", "qty", "price", "date"]


def _normalize_transaction_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize transactions DataFrame to canonical schema."""
    if df is None or df.empty:
        return pd.DataFrame(columns=CANONICAL_HEADERS)

    normalized = df.copy()
    rename_map = {
        "stock_symbol": "stock",
        "symbol": "stock",
        "transaction_type": "type",
        "quantity": "qty",
    }
    normalized = normalized.rename(columns=rename_map)

    # Ensure all required canonical columns exist.
    for col in CANONICAL_HEADERS:
        if col not in normalized.columns:
            normalized[col] = ""

    normalized["txn_id"] = normalized["txn_id"].astype(str).str.strip()
    normalized["client_id"] = normalized["client_id"].astype(str).str.strip()
    normalized["stock"] = normalized["stock"].astype(str).str.strip()
    normalized["type"] = normalized["type"].astype(str).str.upper().str.strip()
    normalized["qty"] = pd.to_numeric(normalized["qty"], errors="coerce")
    normalized["price"] = pd.to_numeric(normalized["price"], errors="coerce")
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")

    # Drop invalid rows.
    normalized = normalized[normalized["client_id"].notna()]
    normalized = normalized[normalized["stock"].notna()]
    normalized = normalized[normalized["client_id"] != ""]
    normalized = normalized[normalized["stock"] != ""]
    normalized = normalized[normalized["client_id"].str.lower() != "nan"]
    normalized = normalized[normalized["stock"].str.lower() != "nan"]
    normalized = normalized[normalized["qty"].fillna(0) > 0]
    normalized = normalized[normalized["price"].fillna(0) > 0]

    # If date is missing/unparseable, assign today's date to keep row usable.
    normalized["date"] = normalized["date"].fillna(pd.Timestamp(datetime.now().date()))
    normalized["date"] = normalized["date"].dt.strftime("%Y-%m-%d")

    # Auto-generate missing txn_id values.
    numeric_ids = (
        normalized["txn_id"]
        .str.extract(r"^T(\d+)$", expand=False)
        .dropna()
        .astype(int)
    )
    next_id = int(numeric_ids.max()) if not numeric_ids.empty else 0

    missing_mask = (
        normalized["txn_id"].isna()
        | (normalized["txn_id"] == "")
        | (normalized["txn_id"].str.lower() == "nan")
    )
    for row_idx in normalized[missing_mask].index:
        next_id += 1
        normalized.at[row_idx, "txn_id"] = f"T{next_id:04d}"

    normalized = normalized[CANONICAL_HEADERS].reset_index(drop=True)
    return normalized


def _write_back_to_sheets(df: pd.DataFrame) -> None:
    """Overwrite transactions tab with normalized data."""
    ws = sheets_db._worksheet(sheets_db.SHEETS["transactions"])
    ws.clear()
    ws.update("A1:G1", [CANONICAL_HEADERS], value_input_option="RAW")
    if not df.empty:
        end_row = len(df) + 1
        ws.update(f"A2:G{end_row}", df[CANONICAL_HEADERS].values.tolist(), value_input_option="USER_ENTERED")


def main() -> None:
    print("Loading transactions from Google Sheets...")
    raw_df = sheets_db.load_transactions()
    print(f"Loaded rows: {len(raw_df):,}")

    cleaned_df = _normalize_transaction_df(raw_df)
    print(f"Normalized valid rows: {len(cleaned_df):,}")

    print("Writing cleaned transactions back to Google Sheets...")
    _write_back_to_sheets(cleaned_df)
    print("Transactions sheet updated.")

    print("Recalculating portfolio summary...")
    engine = PortfolioEngine()
    summary = engine.run()
    print(f"Portfolio summary regenerated for {len(summary):,} clients.")
    print("Done.")


if __name__ == "__main__":
    main()
