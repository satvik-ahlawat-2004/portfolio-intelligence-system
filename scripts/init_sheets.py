import logging
import gspread
from google.oauth2.service_account import Credentials
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "credentials.json")
SPREADSHEET_ID = "1T-z8k1HYUrfpTdm7x5XuOfZN93gflcIUl2YxUsjbUFs"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
gc = gspread.authorize(creds)
ss = gc.open_by_key(SPREADSHEET_ID)

schemas = {
    "clients": ["client_id", "full_name", "risk_profile", "status", "created_at"],
    "transactions": ["txn_id", "client_id", "instrument", "type", "qty", "price", "date"],
    "instruments": ["instrument", "asset_type", "exchange", "sector"],
    "market_prices": ["instrument", "price", "last_updated"],
    "kyc_records": ["client_id", "pan", "kyc_status", "expiry_date"]
}

for sheet_name, cols in schemas.items():
    try:
        ws = ss.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=sheet_name, rows="1000", cols=str(len(cols)))
    
    # Let's check headers, if missing, write them
    headers = ws.row_values(1) if ws.row_count > 0 else []
    if headers != cols:
        print(f"Updating headers for {sheet_name}")
        ws.clear()
        ws.append_row(cols)
        
        # Insert example rows
        if sheet_name == "clients":
            ws.append_row(["C001", "Aarav Mehta", "Conservative", "Active", "2026-04-15"])
        elif sheet_name == "transactions":
            ws.append_row(["T001", "C001", "RELIANCE", "BUY", "10", "2450", "2026-04-10"])
        elif sheet_name == "instruments":
            ws.append_rows([
                ["RELIANCE", "Equity", "NSE", "Energy"],
                ["ITC", "Equity", "NSE", "FMCG"],
                ["NIFTYETF", "ETF", "NSE", "Index"],
                ["Gold", "Commodity", "Global", "Precious Metals"],
                ["Silver", "Commodity", "Global", "Precious Metals"],
                ["Nifty50", "Index", "NSE", "Broad Market"],
                ["Nifty100", "Index", "NSE", "Broad Market"],
                ["Nifty200", "Index", "NSE", "Broad Market"],
                ["Nifty500", "Index", "NSE", "Broad Market"]
            ])
        elif sheet_name == "market_prices":
            ws.append_row(["RELIANCE", "2715", "2026-04-15"])
        elif sheet_name == "kyc_records":
            ws.append_row(["C001", "ABCD1234P", "Valid", "2028-05-10"])

print("Schema initialization complete!")
