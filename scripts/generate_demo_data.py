import os
import random
import logging
from datetime import datetime, timedelta
import string

import google_sheets_db
from portfolio_engine import PortfolioEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_data():
    FIRST_NAMES = ["Aarav", "Vihaan", "Vivaan", "Ananya", "Diya", "Aditya", "Aryan", "Kavya", "Saanvi", "Neha", 
                   "Rohan", "Rahul", "Priya", "Amit", "Sneha", "Karan", "Kunal", "Meera", "Riya", "Vikram",
                   "Arjun", "Aditi", "Pooja", "Raj", "Nikhil", "Nisha", "Rishabh", "Ishita", "Anjali", "Suresh"]
    LAST_NAMES = ["Sharma", "Verma", "Gupta", "Mehta", "Iyer", "Desai", "Patil", "Jain", "Singh", "Reddy",
                  "Rao", "Nair", "Pillai", "Das", "Bose", "Chakraborty", "Kapoor", "Chopra", "Ahluwalia", "Shah"]

    # 1. Generate 100 Clients
    logger.info("Generating 100 Clients...")
    clients_rows = []
    client_ids = []
    
    for i in range(1, 101):
        cid = f"C{i:03d}"
        client_ids.append(cid)
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        
        # Risk profile: 40% Moderate, 30% Conservative, 30% Aggressive
        r = random.random()
        if r < 0.3: risk = "Conservative"
        elif r < 0.7: risk = "Moderate"
        else: risk = "Aggressive"
        
        created_at = (datetime(2026, 4, 15) - timedelta(days=random.randint(0, 100))).strftime("%Y-%m-%d")
        clients_rows.append([cid, name, risk, "Active", created_at])
        
    # 2. Generate 100 KYC Records
    logger.info("Generating KYC Records...")
    kyc_rows = []
    for cid in client_ids:
        pan = "".join(random.choices(string.ascii_uppercase, k=5)) + \
              "".join(random.choices(string.digits, k=4)) + \
              random.choice(string.ascii_uppercase)
              
        r = random.random()
        if r < 0.8:
            status = "Valid"
            expiry = (datetime(2026, 4, 15) + timedelta(days=random.randint(365, 1000))).strftime("%Y-%m-%d")
        elif r < 0.95:
            status = "Expiring Soon"
            expiry = (datetime(2026, 4, 15) + timedelta(days=random.randint(1, 29))).strftime("%Y-%m-%d")
        else:
            status = "Expired"
            expiry = (datetime(2026, 4, 15) - timedelta(days=random.randint(1, 60))).strftime("%Y-%m-%d")
            
        kyc_rows.append([cid, pan, status, expiry])
        
    # 3. Populate Instruments
    logger.info("Populating Instruments...")
    instruments_rows = [
        ["RELIANCE", "Equity", "NSE", "Energy"],
        ["HDFCBANK", "Equity", "NSE", "Banking"],
        ["ITC", "Equity", "NSE", "FMCG"],
        ["INFY", "Equity", "NSE", "Technology"],
        ["SBIN", "Equity", "NSE", "Banking"],
        ["TCS", "Equity", "NSE", "Technology"],
        ["LT", "Equity", "NSE", "Infrastructure"],
        ["AXISBANK", "Equity", "NSE", "Banking"],
        ["NIFTYETF", "ETF", "NSE", "Index"]
    ]
    symbols = [row[0] for row in instruments_rows]

    # Price ranges
    price_ranges = {
        "RELIANCE": (2300, 2800),
        "HDFCBANK": (1500, 1700),
        "ITC": (400, 470),
        "INFY": (1400, 1700),
        "SBIN": (700, 900),
        "TCS": (3500, 4200),
        "LT": (3000, 3700),
        "AXISBANK": (1000, 1300),
        "NIFTYETF": (200, 250) 
    }

    # 4. Generate Transactions
    logger.info("Generating Transactions...")
    txn_rows = []
    num_txns = random.randint(300, 500)
    
    # Keep track of client holdings so we don't sell what we don't own
    client_holdings = {cid: {sym: 0 for sym in symbols} for cid in client_ids}
    
    for i in range(1, num_txns + 1):
        tid = f"T{i:04d}"
        cid = random.choice(client_ids)
        sym = random.choice(symbols)
        
        prange = price_ranges[sym]
        price = round(random.uniform(prange[0], prange[1]), 2)
        
        date = (datetime(2026, 1, 1) + timedelta(days=random.randint(0, 100))).strftime("%Y-%m-%d")
        
        # Determine BUY or SELL
        if client_holdings[cid][sym] > 0:
            if random.random() < 0.3: # 30% chance to sell if we own
                txn_type = "SELL"
                qty = random.randint(1, int(client_holdings[cid][sym]))
                client_holdings[cid][sym] -= qty
            else:
                txn_type = "BUY"
                qty = random.randint(5, 100)
                client_holdings[cid][sym] += qty
        else:
            txn_type = "BUY"
            qty = random.randint(5, 100)
            client_holdings[cid][sym] += qty
            
        txn_rows.append([tid, cid, sym, txn_type, float(qty), float(price), date])
        
    # 5. Populate Market Prices
    logger.info("Generating Market Prices...")
    market_price_rows = []
    for sym in symbols:
        price = round(random.uniform(price_ranges[sym][0], price_ranges[sym][1]), 2)
        date = "2026-04-17"
        market_price_rows.append([sym, price, date])
        
    # WRITE TO SHEETS
    logger.info("Uploading to Google Sheets...")
    
    def upload_sheet(name, cols, data):
        ws = google_sheets_db._worksheet(name)
        ws.clear()
        ws.append_row(cols, value_input_option="RAW")
        if data:
            ws.append_rows(data, value_input_option="USER_ENTERED")
        logger.info(f"Loaded {len(data)} rows into {name}")

    upload_sheet(google_sheets_db.SHEETS["clients"], google_sheets_db.CLIENT_COLUMNS, clients_rows)
    upload_sheet(google_sheets_db.SHEETS["kyc"], google_sheets_db.KYC_COLUMNS, kyc_rows)
    upload_sheet(google_sheets_db.SHEETS["instruments"], google_sheets_db.INSTRUMENT_COLUMNS, instruments_rows)
    upload_sheet(google_sheets_db.SHEETS["transactions"], google_sheets_db.TRANSACTION_COLUMNS, txn_rows)
    upload_sheet(google_sheets_db.SHEETS["market_prices"], google_sheets_db.MARKET_PRICE_COLUMNS, market_price_rows)

    # 6. Run Portfolio Engine
    logger.info("Running Portfolio Engine...")
    engine = PortfolioEngine()
    engine.run()
    logger.info("Demo Data Generation Complete!")

if __name__ == "__main__":
    generate_data()
