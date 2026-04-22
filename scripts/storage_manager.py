import logging
from typing import Tuple
import pandas as pd
from datetime import datetime

from scripts import google_sheets_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StorageManager:
    """
    Manages data persistence for Clients, Transactions, and KYC exclusively via Google Sheets.
    No local CSVs are used for operational data.
    """
    def __init__(self):
        pass

    # --- Client Operations ---
    def get_clients(self) -> pd.DataFrame:
        """Fetch all clients from Google Sheets."""
        return google_sheets_db.load_clients()

    def add_client(self, client_id: str, full_name: str, risk_profile: str, status: str = "Active", kyc_expiry: str = None, initial_investment: float = 0.0, preferred_instrument: str = None) -> Tuple[bool, str]:
        """
        Create a new client in Google Sheets and optionally an initial investment.
        """
        clients = self.get_clients()
        if not clients.empty and client_id in clients['client_id'].astype(str).values:
            return False, "Client ID already exists"
        
        created_at = datetime.utcnow().strftime("%Y-%m-%d")
        success = google_sheets_db.add_client(client_id, full_name, risk_profile, status, created_at)
        if not success:
            return False, "Failed to write client to Google Sheets"

        # Default KYC structure
        if not kyc_expiry:
            kyc_expiry = f"{datetime.utcnow().year + 5}-12-31" 
        google_sheets_db.upsert_kyc_record(client_id, "Valid", kyc_expiry, "")

        # Optional initial investment
        if initial_investment > 0 and preferred_instrument:
            prices = google_sheets_db.get_market_price_map()
            price = prices.get(preferred_instrument, 1000.0) # default fallback
            qty = initial_investment / price
            self.add_transaction(client_id, preferred_instrument, "BUY", qty, price, created_at)

        return True, "Client successfully created in Google Sheets"

    def remove_client(self, client_id: str) -> Tuple[bool, str]:
        """Remove a client from Google Sheets."""
        success = google_sheets_db.remove_client(client_id)
        if success:
            return True, f"Client {client_id} successfully removed."
        return False, f"Failed to remove client {client_id}. They might not exist."

    # --- Transaction Operations ---
    def get_transactions(self, client_id: str = None) -> pd.DataFrame:
        """Fetch transactions from Google Sheets."""
        return google_sheets_db.load_transactions(client_id=client_id)

    def add_transaction(self, client_id: str, instrument: str, txn_type: str, qty: float, price: float, date: str = None) -> Tuple[bool, str]:
        """Record a transaction in Google Sheets. Enforces strict qty > 0 and price > 0."""
        if qty <= 0 or price <= 0:
            return False, "Quantity and price must be greater than zero."
            
        instruments_df = google_sheets_db.load_instruments()
        if instrument not in instruments_df['instrument'].values:
            return False, f"Instrument '{instrument}' is not recognized in the master list."

        success = google_sheets_db.record_transaction(client_id, instrument, txn_type, qty, price, trade_date=date)
        if success:
            return True, "Transaction recorded successfully in Google Sheets."
        return False, "Failed to record transaction in Google Sheets."

    # --- Instrument & Market Price Operations ---
    def get_instruments(self) -> pd.DataFrame:
        """Fetch instrument master list."""
        return google_sheets_db.load_instruments()
        
    def get_market_prices(self) -> pd.DataFrame:
        """Fetch latest market prices."""
        return google_sheets_db.load_market_prices()

    # --- KYC Operations ---
    def get_kyc_records(self, client_id: str = None) -> pd.DataFrame:
        return google_sheets_db.load_kyc_records(client_id=client_id)

    def update_kyc_record(self, client_id: str, kyc_status: str, expiry_date: str) -> bool:
        return google_sheets_db.upsert_kyc_record(client_id, kyc_status, expiry_date)
