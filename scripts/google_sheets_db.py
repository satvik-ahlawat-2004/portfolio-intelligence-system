"""
Google Sheets backend connector for PMS Operations Database.
This module keeps Google Sheets as the ONLY primary operational datastore.
"""

import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    gspread = None
    Credentials = None

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
ANALYTICS_PATH = os.path.join(DATA_DIR, "market_analytics.csv")

SPREADSHEET_ID = "1T-z8k1HYUrfpTdm7x5XuOfZN93gflcIUl2YxUsjbUFs"
DEFAULT_CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "credentials.json")

SHEETS = {
    "clients": "clients",
    "kyc": "kyc_records",
    "instruments": "instruments",
    "market_prices": "market_prices",
    "transactions": "transactions",
    "alerts": "alerts",
    "ai_insights": "ai_insights",
}

REQUIRED_SYMBOLS = ["Gold", "Silver", "Nifty50", "Nifty100", "Nifty200", "Nifty500"]

CLIENT_COLUMNS = ["client_id", "full_name", "risk_profile", "status", "created_at"]
TRANSACTION_COLUMNS = ["txn_id", "client_id", "instrument", "type", "qty", "price", "date"]
KYC_COLUMNS = ["client_id", "pan", "kyc_status", "expiry_date"]
INSTRUMENT_COLUMNS = ["instrument", "asset_type", "exchange", "sector"]
MARKET_PRICE_COLUMNS = ["instrument", "price", "last_updated"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_gc = None
_spreadsheet = None

def get_spreadsheet():
    """Public accessor for the active spreadsheet object."""
    return _get_spreadsheet()

def _get_spreadsheet():
    global _gc, _spreadsheet
    if _spreadsheet is not None:
        return _spreadsheet
    if gspread is None or Credentials is None:
        raise RuntimeError("Google Sheets dependencies are missing. Install gspread and google-auth packages.")
    
    # Deployment Support: Try Streamlit Secrets first, then Environment Variable (JSON), then local file.
    creds = None
    try:
        import streamlit as st
        # Only check st.secrets if we are actually running inside a Streamlit app
        if hasattr(st, "runtime") and st.runtime.exists():
            if "GCP_SERVICE_ACCOUNT" in st.secrets:
                service_account_info = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
                creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
                logger.info("Loaded Google credentials from Streamlit Secrets.")
    except Exception:
        pass

    if not creds:
        # Check for JSON string in env (passed to subprocesses)
        json_creds = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
        if json_creds:
            try:
                import json
                service_account_info = json.loads(json_creds)
                creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
                logger.info("Loaded Google credentials from environment JSON string.")
            except Exception as e:
                logger.error(f"Failed to parse GCP_SERVICE_ACCOUNT_JSON: {e}")

    if not creds:
        raise ValueError("CRITICAL: No Google credentials found. Please ensure you have added the [GCP_SERVICE_ACCOUNT] section to your Streamlit Secrets on the Cloud Dashboard.")

    try:
        _gc = gspread.authorize(creds)
        _spreadsheet = _gc.open_by_key(SPREADSHEET_ID)
        return _spreadsheet
    except Exception as e:
        logger.error(f"GSpread Authorization/Open Failed: {e}")
        raise

def _worksheet(tab_name: str):
    return get_spreadsheet().worksheet(tab_name)

def _get_or_create_worksheet(sh, name, headers):
    try:
        return sh.worksheet(name)
    except:
        ws = sh.add_worksheet(title=name, rows="100", cols=str(len(headers)))
        ws.append_row(headers)
        return ws

def fetch_table(table_key: str) -> pd.DataFrame:
    """Generic fetch for any table defined in SHEETS."""
    tab_name = SHEETS.get(table_key, table_key)
    # Use empty defaults for col mapping if not specifically defined
    return _read_sheet(tab_name, [])

_CACHE = {}
CACHE_TTL = 60  # seconds

def _read_sheet(tab_name: str, expected_columns: List[str]) -> pd.DataFrame:
    global _CACHE
    now = time.time()
    
    # Return cache if within TTL to prevent 429 API blocks
    if tab_name in _CACHE and (now - _CACHE[tab_name]["time"]) < CACHE_TTL:
        return _CACHE[tab_name]["df"].copy()

    try:
        ws = _worksheet(tab_name)
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return pd.DataFrame(columns=expected_columns)
            
        headers = values[0]
        data_rows = values[1:]
        df = pd.DataFrame(data_rows, columns=headers)
        
        if not expected_columns:
            final_df = df
        else:
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = ""
            final_df = df[expected_columns]
        
        # Save to cache
        _CACHE[tab_name] = {"df": final_df.copy(), "time": now}
        return final_df
    except Exception as exc:
        logger.warning(f"Failed to read {tab_name} from Google Sheets: {exc}")
        # If API limited, aggressively return old cache if present
        if tab_name in _CACHE:
            return _CACHE[tab_name]["df"].copy()
        return pd.DataFrame(columns=expected_columns)

def load_clients() -> pd.DataFrame:
    df = _read_sheet(SHEETS["clients"], CLIENT_COLUMNS)
    df["client_id"] = df["client_id"].astype(str).str.strip()
    return df[df["client_id"] != ""]

def add_client(client_id: str, full_name: str, risk_profile: str, status: str = "Active", created_at: str = None) -> bool:
    try:
        if not created_at:
            created_at = datetime.utcnow().strftime("%Y-%m-%d")
        ws = _worksheet(SHEETS["clients"])
        ws.append_row([client_id, full_name, risk_profile, status, created_at])
        if SHEETS["clients"] in _CACHE:
            del _CACHE[SHEETS["clients"]]
        return True
    except Exception as exc:
        logger.error(f"Failed to append client to sheets: {exc}")
        return False

def remove_client(client_id: str) -> bool:
    try:
        ws = _worksheet(SHEETS["clients"])
        records = ws.get_all_records(expected_headers=CLIENT_COLUMNS) if hasattr(ws, 'get_all_records') else ws.get_all_values()[1:]
        
        row_to_delete = None
        # Values is a 2D array, row[0] is client_id
        values = ws.get_all_values()
        for i, row in enumerate(values):
            if i == 0: continue # Skip header
            if str(row[0]).strip() == client_id:
                row_to_delete = i + 1 # 1-indexed for apps script UI but gspread delete_rows is 1-indexed
                break
                
        if row_to_delete:
            ws.delete_rows(row_to_delete)
            if SHEETS["clients"] in _CACHE:
                del _CACHE[SHEETS["clients"]]
            return True
        return False
    except Exception as exc:
        logger.error(f"Failed to remove client from sheets: {exc}")
        return False

def load_transactions(client_id: Optional[str] = None) -> pd.DataFrame:
    df = _read_sheet(SHEETS["transactions"], TRANSACTION_COLUMNS)
    df["client_id"] = df["client_id"].astype(str).str.strip()
    
    df = df[df["client_id"] != ""]
    # Enforce constraints directly
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    
    # Only valid rows
    df = df[(df["qty"] > 0) & (df["price"] > 0)]
    
    if client_id:
        df = df[df["client_id"] == client_id]
    return df

def record_transaction(client_id: str, instrument: str, txn_type: str, qty: float, price: float, trade_date: Optional[str] = None) -> bool:
    txn_id = f"T{datetime.utcnow().strftime('%M%S%f')[:8]}"
    trade_date = trade_date or datetime.utcnow().strftime("%Y-%m-%d")
    try:
        ws = _worksheet(SHEETS["transactions"])
        ws.append_row([txn_id, client_id, instrument, str(txn_type).upper(), float(qty), float(price), trade_date])
        return True
    except Exception as exc:
        logger.error(f"Failed to record txn in sheets: {exc}")
        return False

def load_kyc_records(client_id: Optional[str] = None) -> pd.DataFrame:
    df = _read_sheet(SHEETS["kyc"], KYC_COLUMNS)
    df["client_id"] = df["client_id"].astype(str).str.strip()
    
    if client_id:
        df = df[df["client_id"] == client_id]
    return df

def upsert_kyc_record(client_id: str, kyc_status: str, expiry_date: str, pan: str = "") -> bool:
    try:
        ws = _worksheet(SHEETS["kyc"])
        records = ws.get_all_records()
        row_idx = None
        for i, rec in enumerate(records):
            if str(rec.get("client_id")).strip() == str(client_id):
                row_idx = i + 2 # +2 because 1 for header, 1 for 0-index
                break
        
        if row_idx:
            ws.update(f"A{row_idx}:D{row_idx}", [[client_id, pan, kyc_status, expiry_date]])
        else:
            ws.append_row([client_id, pan, kyc_status, expiry_date])
        return True
    except Exception as exc:
        logger.error(f"Failed to upsert KYC: {exc}")
        return False

def load_instruments() -> pd.DataFrame:
    return _read_sheet(SHEETS["instruments"], INSTRUMENT_COLUMNS)

def load_market_prices(symbols: Optional[List[str]] = None) -> pd.DataFrame:
    df = _read_sheet(SHEETS["market_prices"], MARKET_PRICE_COLUMNS)
    df["instrument"] = df["instrument"].astype(str).str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    
    if symbols:
        df = df[df["instrument"].isin(symbols)]
    return df

def get_market_price_map() -> Dict[str, float]:
    prices = load_market_prices()
    if prices.empty:
        return {}
    return prices.groupby("instrument")["price"].last().to_dict()

def sync_market_prices_from_analytics(analytics_path: str = ANALYTICS_PATH) -> bool:
    if not os.path.exists(analytics_path):
        return False
    try:
        df = pd.read_csv(analytics_path, parse_dates=["Date"])
        latest = (
            df[df["Asset"].isin(REQUIRED_SYMBOLS)]
            .sort_values("Date")
            .groupby("Asset", as_index=False)
            .last()[["Asset", "Close", "Date"]]
        )
        if latest.empty:
            return False
            
        latest["Date"] = pd.to_datetime(latest["Date"]).dt.strftime("%Y-%m-%d")
        
        ws = _worksheet(SHEETS["market_prices"])
        existing_records = ws.get_all_records()
        existing_df = pd.DataFrame(existing_records) if existing_records else pd.DataFrame(columns=MARKET_PRICE_COLUMNS)
        
        rows_to_update = []
        for _, row in latest.iterrows():
            inst, price, dt = row["Asset"], float(row["Close"]), row["Date"]
            
            # Map standard symbols
            found = False
            for i, rec in enumerate(existing_records):
                if str(rec.get("instrument")) == str(inst):
                    found = True
                    ws.update(f"A{i+2}:C{i+2}", [[inst, price, dt]])
                    break
            if not found:
                rows_to_update.append([inst, price, dt])
                
            # Handle additional explicit requested bullion fields
            if inst == "Gold" and pd.notna(row.get("Gold_INR_10g")):
                bullions = [
                    ("GOLD_10G", float(row["Gold_INR_10g"])),
                    ("GOLD_RETAIL_10G", float(row.get("Gold_Retail_10g", row["Gold_INR_10g"])))
                ]
                for b_inst, b_price in bullions:
                    b_found = False
                    for i, rec in enumerate(existing_records):
                        if str(rec.get("instrument")) == str(b_inst):
                            b_found = True
                            ws.update(f"A{i+2}:C{i+2}", [[b_inst, b_price, dt]])
                            break
                    if not b_found:
                        rows_to_update.append([b_inst, b_price, dt])

            if inst == "Silver" and pd.notna(row.get("Silver_INR_10g")):
                bullions = [
                    ("SILVER_10G", float(row["Silver_INR_10g"])),
                    ("SILVER_KG", float(row.get("Silver_INR_kg", row["Silver_INR_10g"] * 100)))
                ]
                for b_inst, b_price in bullions:
                    b_found = False
                    for i, rec in enumerate(existing_records):
                        if str(rec.get("instrument")) == str(b_inst):
                            b_found = True
                            ws.update(f"A{i+2}:C{i+2}", [[b_inst, b_price, dt]])
                            break
                    if not b_found:
                        rows_to_update.append([b_inst, b_price, dt])
                
        if rows_to_update:
            ws.append_rows(rows_to_update)
            
        return True
    except Exception as exc:
        logger.error(f"Failed to sync market prices: {exc}")
        return False
