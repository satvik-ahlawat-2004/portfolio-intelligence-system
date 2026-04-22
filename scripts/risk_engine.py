import pandas as pd
import numpy as np

def calculate_var(returns: pd.Series, confidence_level=0.95) -> float:
    """Calculate Value at Risk (VaR) using historical method"""
    if returns.empty: return 0.0
    return float(np.percentile(returns, (1 - confidence_level) * 100))

def calculate_expected_shortfall(returns: pd.Series, confidence_level=0.95) -> float:
    """Calculate Expected Shortfall (CVaR)"""
    if returns.empty: return 0.0
    var = calculate_var(returns, confidence_level)
    return float(returns[returns <= var].mean())

def calculate_sortino_ratio(returns: pd.Series, risk_free_rate=0.07, target_return=0.0) -> float:
    """Calculate Sortino Ratio"""
    if returns.empty: return 0.0
    downside_returns = returns[returns < target_return]
    downside_volatility = downside_returns.std() * np.sqrt(252)
    if downside_volatility == 0 or pd.isna(downside_volatility): return 0.0
    excess_return = returns.mean() * 252 - risk_free_rate
    return float(excess_return / downside_volatility)

def simulate_stress_scenario(portfolio_weights: dict, scenario_shocks: dict) -> float:
    """
    Simulate impact of a shock scenario.
    portfolio_weights: {'IT': 0.3, 'Gold': 0.2, ...}
    scenario_shocks: {'IT': -0.05, 'Gold': 0.03, ...}
    """
    impact = 0.0
    for asset, weight in portfolio_weights.items():
        if asset in scenario_shocks:
            impact += weight * scenario_shocks[asset]
    return impact
