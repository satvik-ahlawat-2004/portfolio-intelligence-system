"""
Data-grounded tools for the AI Portfolio Assistant.
These tools only use internal engines/data sources.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

import pandas as pd

from scripts.portfolio_engine import PortfolioEngine
from scripts.storage_manager import StorageManager
from scripts import risk_engine
from scripts.optimizer_engine import calculate_optimal_portfolio
from scripts.performance_engine import calculate_drawdown, calculate_sharpe_ratio, calculate_volatility

try:
    import streamlit as st
except Exception:  # pragma: no cover - Streamlit may be unavailable in non-UI runs.
    st = None

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
PERFORMANCE_PATH = os.path.join(DATA_DIR, "portfolio_performance.csv")
MARKET_ANALYTICS_PATH = os.path.join(DATA_DIR, "market_analytics.csv")

ASSETS_FOR_OPTIMIZER = ["Nifty50", "Nifty100", "Nifty200", "Nifty500", "Gold", "Silver"]
RISK_PROXY_PREFERENCE = ["Nifty50", "NIFTY50", "NIFTY 50", "Nifty100", "Nifty200", "Nifty500"]

logger = logging.getLogger(__name__)


def _cache_data(ttl: int):
    """Use Streamlit cache in app context, no-op otherwise."""

    def decorator(func):
        if st is None:
            return func
        return st.cache_data(ttl=ttl)(func)

    return decorator


def _round_if_number(value: Any, digits: int = 6) -> Any:
    if isinstance(value, (int, float)):
        return round(float(value), digits)
    return value


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _log_tool_output(tool_name: str, payload: Dict[str, Any]) -> None:
    logger.info("%s -> %s", tool_name, payload)


def _classify_asset_bucket(symbol: str, instruments_df: pd.DataFrame) -> str:
    s = str(symbol).strip()
    if not instruments_df.empty and {"instrument", "asset_type"}.issubset(instruments_df.columns):
        match = instruments_df[instruments_df["instrument"].astype(str).str.strip().str.lower() == s.lower()]
        if not match.empty:
            raw_asset_type = str(match.iloc[0].get("asset_type", "")).strip().lower()
            if "gold" in raw_asset_type:
                return "Gold"
            if "silver" in raw_asset_type:
                return "Silver"
            if "cash" in raw_asset_type or "liquid" in raw_asset_type:
                return "Cash"
            return "Equity"

    s_lower = s.lower()
    if "gold" in s_lower:
        return "Gold"
    if "silver" in s_lower:
        return "Silver"
    if "cash" in s_lower:
        return "Cash"
    return "Equity"


def _returns_from_market_prices(storage: StorageManager) -> tuple[pd.Series, str]:
    prices_df = storage.get_market_prices()
    if prices_df.empty or not {"instrument", "price"}.issubset(prices_df.columns):
        return pd.Series(dtype=float), ""

    df = prices_df.copy()
    df["instrument"] = df["instrument"].astype(str).str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    if df.empty:
        return pd.Series(dtype=float), ""

    if "last_updated" in df.columns:
        df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
        df = df.dropna(subset=["last_updated"])
    else:
        return pd.Series(dtype=float), ""

    if df.empty:
        return pd.Series(dtype=float), ""

    for proxy in RISK_PROXY_PREFERENCE:
        sdf = df[df["instrument"].str.lower() == proxy.lower()]
        if sdf.empty:
            continue
        series = (
            sdf.sort_values("last_updated")
            .groupby("last_updated", as_index=True)["price"]
            .last()
            .pct_change()
            .dropna()
        )
        if len(series) >= 2:
            return series.astype(float), proxy
    return pd.Series(dtype=float), ""


def _returns_from_holdings(engine: PortfolioEngine, storage: StorageManager) -> pd.Series:
    clients = storage.get_clients()
    txns = storage.get_transactions()
    if clients.empty or txns.empty:
        return pd.Series(dtype=float)

    holding_returns: List[float] = []
    for _, row in clients.iterrows():
        client_id = str(row.get("client_id", "")).strip()
        if not client_id:
            continue
        holdings = engine.calculate_client_portfolio(client_id, all_txns=txns)
        if holdings.empty or "P&L %" not in holdings.columns:
            continue
        values = pd.to_numeric(holdings["P&L %"], errors="coerce").dropna().tolist()
        holding_returns.extend(float(v) for v in values)
    if not holding_returns:
        return pd.Series(dtype=float)
    return pd.Series(holding_returns, dtype=float)


def _load_market_analytics() -> pd.DataFrame:
    if not os.path.exists(MARKET_ANALYTICS_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(MARKET_ANALYTICS_PATH, parse_dates=["Date"])
    except Exception:
        return pd.DataFrame()
    if "Date" in df.columns:
        df = df.sort_values("Date")
    return df


@_cache_data(ttl=180)
def get_portfolio_summary() -> Dict[str, Any]:
    """
    Return aggregated live portfolio metrics using Google Sheets-backed storage.
    """
    storage = StorageManager()
    engine = PortfolioEngine()
    clients = storage.get_clients()
    txns = storage.get_transactions()

    if clients.empty:
        payload = {
            "status": "error",
            "message": "No clients found in the portfolio system. Please add a client to get started.",
        }
        _log_tool_output("get_portfolio_summary", payload)
        return payload

    if txns.empty:
        payload = {
            "status": "error",
            "message": "No transactions found. Please add investments to generate portfolio analytics.",
            "client_count": int(len(clients)),
        }
        _log_tool_output("get_portfolio_summary", payload)
        return payload

    total_value = 0.0
    total_invested = 0.0
    total_return = 0.0
    active_clients = 0
    for _, row in clients.iterrows():
        client_id = str(row.get("client_id", "")).strip()
        if not client_id:
            continue
        metrics = engine.calculate_client_returns(client_id, all_txns=txns)
        value = _safe_float(metrics.get("portfolio_value"))
        invested = _safe_float(metrics.get("total_invested"))
        ret = _safe_float(metrics.get("total_return"))
        if value > 0 or invested > 0:
            active_clients += 1
        total_value += value
        total_invested += invested
        total_return += ret

    if total_invested <= 0 and total_value <= 0:
        payload = {
            "status": "error",
            "message": "No active holdings found yet. Please add buy transactions to build a portfolio.",
            "client_count": int(len(clients)),
        }
        _log_tool_output("get_portfolio_summary", payload)
        return payload

    portfolio_return = (total_return / total_invested) if total_invested else 0.0

    payload = {
        "status": "ok",
        "total_value": _round_if_number(total_value, 2),
        "total_investment": _round_if_number(total_invested, 2),
        "returns": _round_if_number(total_return, 2),
        "returns_pct": _round_if_number(portfolio_return),
        # Backward-compatible keys consumed by existing prompts/UI.
        "portfolio_value": _round_if_number(total_value, 2),
        "total_invested": _round_if_number(total_invested, 2),
        "total_return": _round_if_number(total_return, 2),
        "portfolio_return": _round_if_number(portfolio_return),
        "client_count": int(len(clients)),
        "active_clients": int(active_clients),
    }
    _log_tool_output("get_portfolio_summary", payload)
    return payload


@_cache_data(ttl=180)
def get_portfolio_allocation() -> Dict[str, Any]:
    """
    Aggregate live allocation into Equity/Gold/Silver/Cash buckets.
    """
    storage = StorageManager()
    engine = PortfolioEngine()
    clients = storage.get_clients()
    all_txns = storage.get_transactions()
    instruments_df = storage.get_instruments()
    if clients.empty:
        payload = {
            "status": "error",
            "message": "No clients found in the portfolio system. Please add a client to view allocation.",
        }
        _log_tool_output("get_portfolio_allocation", payload)
        return payload
    if all_txns.empty:
        payload = {
            "status": "error",
            "message": "No transactions found. Please add investments to compute allocation.",
        }
        _log_tool_output("get_portfolio_allocation", payload)
        return payload

    allocations: List[Dict[str, Any]] = []
    all_holdings: List[pd.DataFrame] = []
    for _, row in clients.iterrows():
        client_id = str(row.get("client_id", "")).strip()
        if not client_id:
            continue
        holdings = engine.calculate_client_portfolio(client_id, all_txns=all_txns)
        if not holdings.empty:
            all_holdings.append(holdings)

    if not all_holdings:
        payload = {
            "status": "error",
            "message": "No active holdings available yet. Add buy transactions to compute allocation.",
        }
        _log_tool_output("get_portfolio_allocation", payload)
        return payload

    combined = pd.concat(all_holdings, ignore_index=True)
    combined["asset_bucket"] = combined["Stock"].apply(lambda s: _classify_asset_bucket(s, instruments_df))
    grouped = combined.groupby("asset_bucket", as_index=False)["Market Value"].sum()
    total_value = float(grouped["Market Value"].sum())
    if total_value <= 0:
        payload = {
            "status": "error",
            "message": "Unable to compute allocation because total portfolio value is zero.",
        }
        _log_tool_output("get_portfolio_allocation", payload)
        return payload

    grouped["weight_pct"] = grouped["Market Value"] / total_value
    grouped = grouped.sort_values("weight_pct", ascending=False)

    for _, row in grouped.iterrows():
        allocations.append(
            {
                "asset": str(row["asset_bucket"]),
                "market_value": _round_if_number(row["Market Value"], 2),
                "weight_pct": _round_if_number(row["weight_pct"]),
                "weight_percent": _round_if_number(float(row["weight_pct"]) * 100, 2),
            }
        )

    payload = {
        "status": "ok",
        "total_portfolio_value": _round_if_number(total_value, 2),
        "allocation": allocations,
    }
    _log_tool_output("get_portfolio_allocation", payload)
    return payload


@_cache_data(ttl=180)
def get_portfolio_risk_metrics() -> Dict[str, Any]:
    """
    Risk metrics computed from Google Sheets-backed market/portfolio data.
    """
    storage = StorageManager()
    engine = PortfolioEngine()

    returns, source = _returns_from_market_prices(storage)
    if returns.empty:
        returns = _returns_from_holdings(engine, storage)
        source = "portfolio_holdings_pnl_proxy"

    if returns.empty or len(returns) < 2:
        payload = {
            "status": "error",
            "message": "Not enough risk data points yet. Add more transactions and market price history to compute VaR/Volatility/Sharpe.",
        }
        _log_tool_output("get_portfolio_risk_metrics", payload)
        return payload

    var_95 = risk_engine.calculate_var(returns, confidence_level=0.95)
    volatility = calculate_volatility(returns)
    sharpe_ratio = calculate_sharpe_ratio(returns)
    sortino_ratio = risk_engine.calculate_sortino_ratio(returns)
    max_drawdown = calculate_drawdown(returns) if len(returns) >= 2 else 0.0

    payload = {
        "status": "ok",
        "proxy_asset": source,
        "VaR": _round_if_number(var_95),
        "Volatility": _round_if_number(volatility),
        "Sharpe Ratio": _round_if_number(sharpe_ratio),
        "var_95": _round_if_number(var_95),
        "volatility_annualized": _round_if_number(volatility),
        "sharpe_ratio": _round_if_number(sharpe_ratio),
        "max_drawdown": _round_if_number(max_drawdown),
        "sortino_ratio": _round_if_number(sortino_ratio),
        "data_points": int(len(returns)),
    }
    _log_tool_output("get_portfolio_risk_metrics", payload)
    return payload


@_cache_data(ttl=180)
def get_market_prices() -> Dict[str, Any]:
    """
    Return latest key market prices from Google Sheets market_prices tab.
    """
    storage = StorageManager()
    df = storage.get_market_prices()
    if df.empty or not {"instrument", "price"}.issubset(df.columns):
        payload = {
            "status": "error",
            "message": "No market prices found in Google Sheets. Please sync live market data first.",
        }
        _log_tool_output("get_market_prices", payload)
        return payload

    prices = df.copy()
    prices["instrument"] = prices["instrument"].astype(str).str.strip()
    prices["price"] = pd.to_numeric(prices["price"], errors="coerce")
    prices = prices.dropna(subset=["price"])
    if prices.empty:
        payload = {
            "status": "error",
            "message": "Market prices tab has no valid numeric prices. Please run market sync.",
        }
        _log_tool_output("get_market_prices", payload)
        return payload

    if "last_updated" in prices.columns:
        prices["last_updated"] = pd.to_datetime(prices["last_updated"], errors="coerce")
        prices = prices.sort_values("last_updated")
        latest = prices.groupby("instrument", as_index=False).last()
    else:
        latest = prices.groupby("instrument", as_index=False).last()

    latest_map = {str(row["instrument"]): row for _, row in latest.iterrows()}

    def _first_price(candidates: List[str]) -> Any:
        for symbol in candidates:
            row = latest_map.get(symbol)
            if row is not None:
                return _round_if_number(row.get("price"), 2)
        return None

    payload = {
        "status": "ok",
        "gold_price_inr_10g": _first_price(["GOLD_10G", "Gold"]),
        "silver_price_inr_10g": _first_price(["SILVER_10G", "Silver"]),
        "nifty50": _first_price(["Nifty50", "NIFTY50", "NIFTY 50"]),
        "nifty100": _first_price(["Nifty100"]),
        "nifty200": _first_price(["Nifty200"]),
        "nifty500": _first_price(["Nifty500"]),
        "available_instruments": sorted(list(latest_map.keys())),
    }
    _log_tool_output("get_market_prices", payload)
    return payload


@_cache_data(ttl=180)
def run_portfolio_optimizer() -> Dict[str, Any]:
    """
    Build optimizer input from internal market return series and run MPT optimizer.
    """
    mdf = _load_market_analytics()
    if mdf.empty or not {"Asset", "Date", "Daily_Return"}.issubset(mdf.columns):
        return {"status": "error", "message": "Data not available."}

    sub = mdf[mdf["Asset"].isin(ASSETS_FOR_OPTIMIZER)][["Date", "Asset", "Daily_Return"]].dropna()
    if sub.empty:
        return {"status": "error", "message": "Data not available."}

    returns_df = sub.pivot(index="Date", columns="Asset", values="Daily_Return").dropna(how="all")
    returns_df = returns_df.fillna(0.0)
    if returns_df.shape[1] < 2:
        return {"status": "error", "message": "Data not available."}

    opt = calculate_optimal_portfolio(returns_df)
    if not opt or not opt.get("weights"):
        return {"status": "error", "message": "Data not available."}

    return {
        "status": "ok",
        "weights": {k: _round_if_number(v) for k, v in opt["weights"].items()},
        "expected_return": _round_if_number(opt.get("return")),
        "volatility": _round_if_number(opt.get("volatility")),
        "sharpe": _round_if_number(opt.get("sharpe")),
    }


@_cache_data(ttl=180)
def run_stress_test() -> Dict[str, Any]:
    """
    Run stress scenario using current allocation weights and predefined shocks.
    """
    allocation = get_portfolio_allocation()
    if allocation.get("status") != "ok":
        return {"status": "error", "message": "Data not available."}

    weights = {row["asset"]: float(row["weight_pct"]) for row in allocation.get("allocation", [])}
    if not weights:
        return {"status": "error", "message": "Data not available."}

    # Conservative macro shock assumptions.
    scenario_shocks = {
        "Nifty50": -0.05,
        "Nifty100": -0.045,
        "Nifty200": -0.04,
        "Nifty500": -0.05,
        "Gold": 0.03,
        "Silver": 0.02,
    }
    portfolio_impact = risk_engine.simulate_stress_scenario(weights, scenario_shocks)

    return {
        "status": "ok",
        "scenario_name": "Equity Shock with Precious Metals Cushion",
        "scenario_shocks": {k: _round_if_number(v) for k, v in scenario_shocks.items()},
        "estimated_portfolio_impact": _round_if_number(portfolio_impact),
    }
