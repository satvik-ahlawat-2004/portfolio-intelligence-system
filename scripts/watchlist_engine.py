import pandas as pd
import yfinance as yf
from scripts import google_sheets_db as db

def get_watchlists() -> pd.DataFrame:
    """Retrieve all watchlists from database"""
    return db.fetch_table("watchlists")

def add_to_watchlist(name: str, symbol: str) -> tuple[bool, str]:
    """Add a symbol to a specific watchlist"""
    try:
        sh = db.get_spreadsheet()
        ws = db._get_or_create_worksheet(sh, "watchlists", ["watchlist_name", "symbol"])
        ws.append_row([name, symbol])
        return True, "Added successfully"
    except Exception as e:
        return False, str(e)

def get_watchlist_market_data(name: str) -> pd.DataFrame:
    """Fetch live data for symbols in a watchlist"""
    df = get_watchlists()
    if df.empty or "symbol" not in df.columns:
        return pd.DataFrame()
    
    symbols = df[df["watchlist_name"] == name]["symbol"].tolist()
    if not symbols: return pd.DataFrame()
    
    try:
        data = yf.download(symbols, period="5d", progress=False)
        results = []
        for sym in symbols:
            if isinstance(data.columns, pd.MultiIndex):
                if sym in data['Close'].columns:
                    close = data['Close'][sym].dropna().values
                    if len(close) > 1:
                        results.append({
                            "Symbol": sym,
                            "Price": close[-1],
                            "Change %": (close[-1] - close[-2]) / close[-2],
                            "High 52W": data['High'][sym].max(),
                            "Volume": data['Volume'][sym].dropna().values[-1] if 'Volume' in data else 0
                        })
            else:
                 if not data.empty and 'Close' in data:
                     close = data['Close'].dropna().values
                     if len(close) > 1:
                        results.append({
                            "Symbol": sym,
                            "Price": close[-1],
                            "Change %": (close[-1] - close[-2]) / close[-2],
                            "High 52W": data['High'].max(),
                            "Volume": data['Volume'].iloc[-1]
                        })
        return pd.DataFrame(results)
    except:
        return pd.DataFrame()
