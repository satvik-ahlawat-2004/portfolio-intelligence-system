import numpy as np
import pandas as pd

def run_simulation(initial_portfolio_value: float, mean_return: float, volatility: float, period_days=252, simulations=10000) -> dict:
    """Run Monte Carlo simulation for portfolio values using Geometric Brownian Motion"""
    dt = 1.0 / period_days
    
    # Calculate geometric random walk paths
    paths = np.zeros((period_days, simulations))
    paths[0] = initial_portfolio_value
    
    # Generate normally distributed random shocks
    shocks = np.random.normal((mean_return - 0.5 * volatility**2) * dt, volatility * np.sqrt(dt), size=(period_days - 1, simulations))
    paths[1:] = initial_portfolio_value * np.exp(np.cumsum(shocks, axis=0))
    
    final_values = paths[-1]
    
    return {
        "paths": pd.DataFrame(paths[:, :100]), # Subseletion of 100 paths for visualization performance
        "expected": float(np.mean(final_values)),
        "best_95": float(np.percentile(final_values, 95)),
        "worst_5": float(np.percentile(final_values, 5)),
        "median": float(np.median(final_values)),
        "distribution": final_values
    }
