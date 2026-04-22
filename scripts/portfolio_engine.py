"""
Portfolio Engine v2.0 - Extended for PMS Operations
Calculates multi-client portfolio metrics, returns, and holdings.
Data is read strictly from Google Sheets via StorageManager.
"""

import os
import pandas as pd
import logging
import yfinance as yf
from typing import Dict, List, Optional
from datetime import datetime
try:
    from scripts.storage_manager import StorageManager
    from scripts import google_sheets_db as sheets_db
except ModuleNotFoundError:
    from storage_manager import StorageManager
    import google_sheets_db as sheets_db

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR     = os.path.join(PROJECT_ROOT, "data")
ANALYTICS_PATH = os.path.join(DATA_DIR, "market_analytics.csv")
PERFORMANCE_PATH = os.path.join(DATA_DIR, "portfolio_performance.csv")

class PortfolioEngine:
    def __init__(self):
        self.storage = StorageManager()
        self.market_cache = {}
        self.load_market_data()

    def load_market_data(self):
        """Load latest prices, prioritizing Google Sheets market_prices."""
        self.market_cache = {}
        sheet_prices = sheets_db.get_market_price_map()
        if sheet_prices:
            self.market_cache.update(sheet_prices)
            logger.info("Market cache updated with %s assets from Google Sheets.", len(sheet_prices))
            return

        if os.path.exists(ANALYTICS_PATH):
            df = pd.read_csv(ANALYTICS_PATH)
            latest = df.sort_values("Date").groupby("Asset")["Close"].last().to_dict()
            self.market_cache.update(latest)
            logger.info("Market cache fallback from analytics CSV with %s assets.", len(latest))

    def get_current_price(self, symbol: str) -> float:
        """Fetch current price with caching."""
        if symbol in self.market_cache:
            return self.market_cache[symbol]
        
        # Try yfinance for unknown symbols
        try:
            # For Indian stocks, append .NS if not present
            yf_symbol = symbol if "." in symbol else f"{symbol}.NS"
            ticker = yf.Ticker(yf_symbol)
            # Use fast_info if available or just history
            price = ticker.history(period="1d")["Close"].iloc[-1]
            self.market_cache[symbol] = price
            return price
        except Exception as e:
            logger.warning(f"Could not fetch price for {symbol}: {e}")
            return 0.0

    def calculate_client_portfolio(self, client_id: str, all_txns: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Calculates holdings for a specific client dynamically from transactions.
        Returns a DataFrame with [Stock, Quantity, Average Price, Current Price, Market Value, P&L %]
        """
        txns = all_txns if all_txns is not None else self.storage.get_transactions(client_id)
        if txns.empty:
            return pd.DataFrame(columns=['Stock', 'Quantity', 'Average Price', 'Current Price', 'Market Value', 'P&L %'])

        # filter to client if all_txns
        if all_txns is not None:
            txns = txns[txns["client_id"].astype(str).str.strip() == client_id]

        holdings_map = {}
        for _, row in txns.iterrows():
            sym = str(row['instrument']).strip()
            qty = float(row['qty'])
            price = float(row['price'])
            t_type = str(row['type']).upper()

            if sym not in holdings_map:
                holdings_map[sym] = {'qty': 0.0, 'total_cost': 0.0}

            if t_type == 'BUY':
                holdings_map[sym]['qty'] += qty
                holdings_map[sym]['total_cost'] += qty * price
            elif t_type == 'SELL':
                if holdings_map[sym]['qty'] > 0:
                    avg = holdings_map[sym]['total_cost'] / holdings_map[sym]['qty']
                    holdings_map[sym]['total_cost'] -= qty * avg
                holdings_map[sym]['qty'] -= qty

        results = []
        for sym, data in holdings_map.items():
            qty = data['qty']
            if qty <= 0: continue
            
            avg_price = data['total_cost'] / qty if qty > 0 else 0
            current_price = self.get_current_price(sym)
            if current_price <= 0:
                current_price = avg_price
            
            market_value = qty * current_price
            invested_value = qty * avg_price
            p_and_l = market_value - invested_value
            p_and_l_pct = (p_and_l / invested_value) if invested_value != 0 else 0
            
            results.append({
                'Stock': sym,
                'Quantity': qty,
                'Average Price': avg_price,
                'Current Price': current_price,
                'Market Value': market_value,
                'P&L %': p_and_l_pct
            })
            
        return pd.DataFrame(results)

    def calculate_client_returns(self, client_id: str, all_txns: Optional[pd.DataFrame] = None) -> Dict:
        """Calculates aggregate metrics for a client."""
        holdings_df = self.calculate_client_portfolio(client_id, all_txns)
        if holdings_df.empty:
            return {
                'portfolio_value': 0,
                'total_invested': 0,
                'total_return': 0,
                'return_pct': 0
            }
        
        total_market_value = holdings_df['Market Value'].sum()
        total_invested = (holdings_df['Quantity'] * holdings_df['Average Price']).sum()
        total_return = total_market_value - total_invested
        return_pct = (total_return / total_invested) if total_invested != 0 else 0
        
        return {
            'portfolio_value': total_market_value,
            'total_invested': total_invested,
            'total_return': total_return,
            'return_pct': return_pct
        }

    def generate_portfolio_summary(self) -> pd.DataFrame:
        """Generated a summary of all clients for the dashboard."""
        clients = self.storage.get_clients()
        all_txns = self.storage.get_transactions()
        all_kyc = self.storage.get_kyc_records()
        summary = []
        
        for _, client in clients.iterrows():
            cid = str(client['client_id']).strip()
            metrics = self.calculate_client_returns(cid, all_txns)
            
            k = all_kyc[all_kyc['client_id'].astype(str).str.strip() == cid]
            kyc_status = k.iloc[0]['kyc_status'] if not k.empty else "Pending"
            
            client_name = client.get("full_name", "Unknown")
            
            summary.append({
                'ClientID': cid,
                'ClientName': client_name,
                'RiskProfile': client['risk_profile'],
                'KYCStatus': kyc_status,
                'PortfolioValue': metrics['portfolio_value'],
                'TotalReturn': metrics['total_return'],
                'ReturnPct': metrics['return_pct'],
                'InvestmentAmount': metrics['total_invested']
            })
            
        df = pd.DataFrame(summary)
        if not df.empty:
            df.to_csv(PERFORMANCE_PATH, index=False)
        return df

    def get_compliance_metrics(self) -> Dict:
        """Calculates compliance metrics for the dashboard."""
        clients = self.storage.get_clients()
        all_kyc = self.storage.get_kyc_records()
        all_txns = self.storage.get_transactions()

        
        today = datetime.now().date()
        
        expired = 0
        expiring_soon = 0
        active_clients = len(clients)
        zero_portfolio = 0
        
        for _, client in clients.iterrows():
            cid = str(client['client_id']).strip()
            k = all_kyc[all_kyc['client_id'].astype(str).str.strip() == cid]
            
            # Check zero portfolio
            metrics = self.calculate_client_returns(cid, all_txns)
            if metrics['portfolio_value'] == 0:
                zero_portfolio += 1
            
            if k.empty:
                expired += 1
                continue
                
            expiry_str = k.iloc[0]['expiry_date']
            try:
                expiry = datetime.strptime(expiry_str, '%Y-%m-%d').date()
                days_left = (expiry - today).days
                if days_left < 0:
                    expired += 1
                elif days_left < 30:
                    expiring_soon += 1
            except:
                expired += 1
                
        return {
            'Expired KYC': expired,
            'KYC Expiring Soon': expiring_soon,
            'Active Clients': active_clients,
            'Zero Portfolio': zero_portfolio
        }

    def run(self) -> pd.DataFrame:
        """Refresh market cache and recompute latest portfolio summary."""
        self.load_market_data()
        return self.generate_portfolio_summary()

def run_portfolio_engine():
    engine = PortfolioEngine()
    logger.info("Running Portfolio Engine...")
    summary = engine.generate_portfolio_summary()
    print(summary)
    
if __name__ == "__main__":
    run_portfolio_engine()
