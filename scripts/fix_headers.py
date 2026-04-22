import sys
sys.path.insert(0, '.')
from scripts import google_sheets_db

db = google_sheets_db

# Just explicitly update "A1" for each sheet
def fix_headers(name, cols):
    ws = db._worksheet(name)
    # The columns are 1D array, we must pass 2D array [[c1, c2, c3]]
    ws.update("A1", [cols])
    print(f"Fixed headers for {name}")

fix_headers(db.SHEETS["clients"], db.CLIENT_COLUMNS)
fix_headers(db.SHEETS["kyc"], db.KYC_COLUMNS)
fix_headers(db.SHEETS["instruments"], db.INSTRUMENT_COLUMNS)
fix_headers(db.SHEETS["transactions"], db.TRANSACTION_COLUMNS)
fix_headers(db.SHEETS["market_prices"], db.MARKET_PRICE_COLUMNS)
