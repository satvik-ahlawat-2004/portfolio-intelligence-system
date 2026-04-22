import pandas as pd
import numpy as np

def calculate_twrr(returns: pd.Series) -> float:
    """Calculate Time-Weighted Rate of Return"""
    if returns.empty: return 0.0
    return float(np.prod(1 + returns) - 1)

def calculate_xirr(cash_flows: pd.DataFrame) -> float:
    """Calculate Extended Internal Rate of Return. Requires 'Date' and 'Amount' columns."""
    if cash_flows.empty: return 0.0
    # Simplified placeholder for actual XIRR math which requires scipy.optimize
    total_flow = cash_flows['Amount'].sum()
    return float(total_flow / abs(cash_flows['Amount'].min())) if cash_flows['Amount'].min() != 0 else 0.0

def calculate_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.07) -> float:
    """Calculate annualized Sharpe Ratio"""
    if returns.empty or returns.std() == 0: return 0.0
    # Assuming daily returns, annualize factor = 252
    excess_return = returns.mean() * 252 - risk_free_rate
    volatility = returns.std() * np.sqrt(252)
    return float(excess_return / volatility) if volatility != 0 else 0.0

def calculate_drawdown(returns: pd.Series) -> float:
    """Calculate Maximum Drawdown"""
    if returns.empty: return 0.0
    cumulative = (1 + returns).cumprod()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak) / peak
    return float(drawdown.min())

def calculate_volatility(returns: pd.Series) -> float:
    """Calculate Annualized Volatility"""
    if returns.empty: return 0.0
    return float(returns.std() * np.sqrt(252))
