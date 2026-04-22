import pandas as pd
from scripts import google_sheets_db as db

def get_insights(portfolio_id: str) -> pd.DataFrame:
    df = db.fetch_table("ai_insights")
    if df.empty: return pd.DataFrame(columns=["date", "portfolio_id", "commentary"])
    return df[df["portfolio_id"] == portfolio_id]

def generate_insights_commentary(metrics_dict: dict) -> str:
    # LLM API stub placeholder backing for Phase-1 UI rendering
    pretend = metrics_dict.get('portfolio_return', 0.0)
    bench = metrics_dict.get('benchmark_return', 0.0)
    diff = pretend - bench
    if diff > 0:
        return f"Your portfolio outperformed the benchmark this week by {diff*100:.1f}%. The allocation strategy shows strong resilience with managed volatility exposure."
    else:
        return f"Your portfolio trailed the benchmark by {abs(diff)*100:.1f}%. Consider rebalancing the top holding allocations to reduce sector cluster risk."
