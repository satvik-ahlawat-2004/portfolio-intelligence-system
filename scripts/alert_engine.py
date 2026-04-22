import pandas as pd
from scripts import google_sheets_db as db
import uuid
from datetime import datetime

def get_alerts() -> pd.DataFrame:
    df = db.fetch_table("alerts")
    if df.empty: return pd.DataFrame(columns=["alert_id", "symbol", "condition", "threshold", "status", "last_triggered"])
    return df

def create_alert(symbol: str, condition: str, threshold: float) -> tuple[bool, str]:
    aid = str(uuid.uuid4())[:8]
    try:
        sh = db.get_spreadsheet()
        ws = db._get_or_create_worksheet(sh, "alerts", ["alert_id", "symbol", "condition", "threshold", "status", "last_triggered"])
        ws.append_row([aid, symbol, condition, threshold, "active", ""])
        return True, "Alert created"
    except Exception as e:
        return False, str(e)

def run_alert_checks(latest_prices: dict, portfolio_metrics: dict = None) -> list:
    """ Advanced Engine Triggers handling institutional volatility bounds"""
    active_triggers = []
    if portfolio_metrics:
        if portfolio_metrics.get("drawdown", 0) > 0.05:
            active_triggers.append({"Symbol": "PORTFOLIO", "Condition": "Drawdown > 5%", "Timestamp": datetime.now().strftime("%H:%M:%S"), "Status": "CRITICAL"})
        if portfolio_metrics.get("volatility_spike", False):
            active_triggers.append({"Symbol": "PORTFOLIO", "Condition": "Vol Spike > 2x", "Timestamp": datetime.now().strftime("%H:%M:%S"), "Status": "WARNING"})
    
    if latest_prices.get("Gold", 0) > 160000:
        active_triggers.append({"Symbol": "GOLD", "Condition": "Price > ₹160,000", "Timestamp": datetime.now().strftime("%H:%M:%S"), "Status": "TRIGGERED"})
    
    if latest_prices.get("Nifty50_Drop", 0) > 0.03:
        active_triggers.append({"Symbol": "NIFTY50", "Condition": "Drop > 3%", "Timestamp": datetime.now().strftime("%H:%M:%S"), "Status": "CRITICAL"})
        
    return active_triggers

def get_advanced_mock_alerts() -> pd.DataFrame:
    return pd.DataFrame([
        {"Symbol": "PORTFOLIO", "Trigger Condition": "Portfolio drawdown > 5%", "Timestamp": "10:42 AM", "Status": "🚨 HIGH RISK"},
        {"Symbol": "NIFTY50", "Trigger Condition": "Nifty50 drop > 3%", "Timestamp": "11:15 AM", "Status": "⚠️ VOLATILITY"},
        {"Symbol": "GOLD", "Trigger Condition": "Gold > ₹1,60,000", "Timestamp": "12:05 PM", "Status": "✅ TARGET HIT"}
    ])
