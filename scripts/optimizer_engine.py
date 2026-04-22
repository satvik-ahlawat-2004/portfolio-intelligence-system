import numpy as np
import pandas as pd
import scipy.optimize as sco

def calculate_expected_returns(returns: pd.DataFrame) -> pd.Series:
    return returns.mean() * 252

def calculate_covariance_matrix(returns: pd.DataFrame) -> pd.DataFrame:
    return returns.cov() * 252

def _portfolio_performance(weights, returns, cov_matrix, risk_free_rate=0.07):
    p_ret = np.sum(returns.mean() * weights) * 252
    p_vol = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
    return p_ret, p_vol

def _neg_sharpe_ratio(weights, returns, cov_matrix, risk_free_rate=0.07):
    p_ret, p_vol = _portfolio_performance(weights, returns, cov_matrix, risk_free_rate)
    return -(p_ret - risk_free_rate) / p_vol if p_vol > 0 else 0

def calculate_optimal_portfolio(returns: pd.DataFrame) -> dict:
    if returns.empty: return {"weights": {}}
    num_assets = len(returns.columns)
    args = (returns, calculate_covariance_matrix(returns))
    constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
    bounds = tuple((0, 1) for _ in range(num_assets))
    
    # Starting weights evenly distributed
    init_guess = num_assets * [1. / num_assets,]
    
    result = sco.minimize(_neg_sharpe_ratio, init_guess, args=args, bounds=bounds, constraints=constraints)
    
    ret, vol = _portfolio_performance(result.x, returns, args[1])
    return {
        "weights": dict(zip(returns.columns, result.x)),
        "return": ret,
        "volatility": vol,
        "sharpe": -result.fun
    }

def generate_efficient_frontier(returns: pd.DataFrame, num_portfolios=100) -> pd.DataFrame:
    if returns.empty: return pd.DataFrame()
    ret_arr = []
    vol_arr = []
    cov_matrix = calculate_covariance_matrix(returns)
    num_assets = len(returns.columns)
    
    for _ in range(num_portfolios):
        weights = np.random.random(num_assets)
        weights /= np.sum(weights)
        pr, pv = _portfolio_performance(weights, returns, cov_matrix)
        ret_arr.append(pr)
        vol_arr.append(pv)
        
    return pd.DataFrame({"Return": ret_arr, "Volatility": vol_arr})
