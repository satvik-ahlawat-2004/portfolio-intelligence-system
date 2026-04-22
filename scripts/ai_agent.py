"""
Gemini-powered, data-grounded AI Portfolio Assistant.
Uses internal tools first, then asks Gemini to explain results.
SDK: google-genai (new)   Model: gemini-2.0-flash
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from dotenv import load_dotenv

# ── Environment ───────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
load_dotenv()

# ── API Key Resolution ────────────────────────────────────────────────────────
def _resolve_gemini_key() -> str:
    # 1. Try Streamlit Secrets (for Cloud Deployment)
    try:
        import streamlit as st
        if "GEMINI_API_KEY" in st.secrets:
            return st.secrets["GEMINI_API_KEY"]
    except Exception:
        pass
    
    # 2. Try Environment Variable (Local/Docker)
    return os.getenv("GEMINI_API_KEY", "")

GEMINI_API_KEY = _resolve_gemini_key()
_KEY_VALID = bool(GEMINI_API_KEY) and GEMINI_API_KEY != "YOUR_GEMINI_API_KEY"

logger = logging.getLogger(__name__)
if _KEY_VALID:
    logger.info("Gemini API key loaded successfully.")
else:
    logger.warning("Gemini API key missing or is still the placeholder value.")

# ── SDK: google-genai (new, replacing deprecated google-generativeai) ─────────
_GEMINI_MODEL = "gemini-2.0-flash"

try:
    from google import genai as _genai_sdk
    from google.genai import types as _genai_types

    _genai_client: Any = None

    def _get_client() -> Any:
        global _genai_client
        if _genai_client is None:
            _genai_client = _genai_sdk.Client(api_key=GEMINI_API_KEY)
        return _genai_client

    GEMINI_SDK_AVAILABLE = True
except Exception as _sdk_err:
    logger.warning("google-genai SDK not available: %s", _sdk_err)
    GEMINI_SDK_AVAILABLE = False

GEMINI_ENABLED = GEMINI_SDK_AVAILABLE and _KEY_VALID

# ── Tool imports ──────────────────────────────────────────────────────────────
from scripts.agent_tools import (
    get_market_prices,
    get_portfolio_allocation,
    get_portfolio_risk_metrics,
    get_portfolio_summary,
    run_portfolio_optimizer,
    run_stress_test,
)

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None


def _cache_data(ttl: int):
    """Use Streamlit cache when running in app context."""
    def decorator(func):
        if st is None:
            return func
        return st.cache_data(ttl=ttl)(func)
    return decorator


# ── Intent detection ──────────────────────────────────────────────────────────
def classify_query(query: str) -> str:
    q = query.lower().strip()

    if q in {"hi", "hello", "hey", "hi!", "hello!", "hey!"} or q.startswith(("hi ", "hello ", "hey ")):
        return "greeting"
    if q in {"what is risk", "define risk", "risk meaning", "explain risk"}:
        return "risk_definition"
    if any(k in q for k in ["optimize", "optimal allocation", "optimizer", "efficient frontier", "mpt"]):
        return "optimizer"
    if any(k in q for k in ["stress", "scenario", "shock", "crash", "crisis"]):
        return "stress_test"
    if any(k in q for k in ["allocation", "weights", "asset mix", "holdings mix", "breakdown", "portfolio breakdown"]):
        return "allocation"
    if "risk" in q or any(k in q for k in ["value at risk", "var", "volatility", "drawdown", "sortino", "expected shortfall", "cvar", "sharpe"]):
        return "risk"
    if "portfolio" in q or "value" in q or "returns" in q or "performance" in q:
        return "portfolio"
    if any(k in q for k in ["gold", "silver", "nifty", "market", "indices", "index", "price"]):
        return "market"
    return "ai"


def _detect_intent(query: str) -> Tuple[str, str]:
    q = query.lower().strip()

    if any(k in q for k in ["optimize", "optimal allocation", "optimizer", "efficient frontier", "mpt"]):
        return "optimizer", "run_portfolio_optimizer"
    if any(k in q for k in ["stress", "scenario", "shock", "crash", "crisis"]):
        return "stress_test", "run_stress_test"
    if any(k in q for k in ["allocation", "weights", "asset mix", "holdings mix", "breakdown", "portfolio breakdown"]):
        return "allocation", "get_portfolio_allocation"
    if any(k in q for k in [
        "value at risk", "var", "volatility", "drawdown", "sortino",
        "risk", "expected shortfall", "cvar", "sharpe",
    ]):
        return "risk", "get_portfolio_risk_metrics"
    if any(k in q for k in [
        "gold", "silver", "nifty", "market", "indices", "index",
        "price", "sector", "commodity",
    ]):
        return "market", "get_market_prices"
    if any(k in q for k in [
        "summary", "overview", "portfolio return", "portfolio value",
        "benchmark", "client returns", "total value", "returns",
        "performance", "how is", "how am", "how are",
    ]):
        return "portfolio_summary", "get_portfolio_summary"

    return "portfolio_summary", "get_portfolio_summary"


# ── Tool executor ─────────────────────────────────────────────────────────────
@_cache_data(ttl=60)
def _cached_portfolio_summary() -> Dict[str, Any]:
    return get_portfolio_summary()


@_cache_data(ttl=60)
def _cached_market_prices() -> Dict[str, Any]:
    return get_market_prices()


@_cache_data(ttl=60)
def _cached_portfolio_allocation() -> Dict[str, Any]:
    return get_portfolio_allocation()


@_cache_data(ttl=60)
def _cached_portfolio_risk_metrics() -> Dict[str, Any]:
    return get_portfolio_risk_metrics()


def _invoke_tool(tool_name: str) -> Dict[str, Any]:
    tool_map = {
        "get_portfolio_summary": _cached_portfolio_summary,
        "get_portfolio_allocation": _cached_portfolio_allocation,
        "get_portfolio_risk_metrics": _cached_portfolio_risk_metrics,
        "get_market_prices": _cached_market_prices,
        "run_portfolio_optimizer": run_portfolio_optimizer,
        "run_stress_test": run_stress_test,
    }
    fn = tool_map.get(tool_name)
    if not fn:
        return {"status": "error", "message": f"Unknown tool: {tool_name}."}
    try:
        payload = fn()
        logger.info("Tool %s returned: %s", tool_name, payload)
        return payload or {"status": "error", "message": "Tool returned empty response."}
    except Exception as exc:
        logger.error("Tool %s raised: %s", tool_name, exc, exc_info=True)
        return {"status": "error", "message": f"Tool execution failed: {exc}"}


# ── Built-in data formatter (no Gemini dependency) ────────────────────────────
def _format_data_response(intent: str, data: Dict[str, Any]) -> str:
    """Produce a clean human-readable answer purely from tool data."""
    lines: list[str] = []

    if intent in ("portfolio_summary",):
        pv  = data.get("total_value") or data.get("portfolio_value", 0)
        inv = data.get("total_investment") or data.get("total_invested", 0)
        ret = data.get("returns") or data.get("total_return", 0)
        pct = data.get("returns_pct") or data.get("portfolio_return", 0)
        cc  = data.get("client_count", 0)
        ac  = data.get("active_clients", cc)
        lines += [
            "Portfolio Summary",
            f"  Total Portfolio Value : ₹{pv:,.2f}",
            f"  Total Invested        : ₹{inv:,.2f}",
            f"  Total Returns         : ₹{ret:,.2f}",
            f"  Return %              : {pct*100:.2f}%",
            f"  Total Clients         : {cc}  (Active: {ac})",
        ]

    elif intent == "allocation":
        tv = data.get("total_portfolio_value", 0)
        lines.append(f"Portfolio Allocation  (Total ₹{tv:,.2f})")
        for row in data.get("allocation", []):
            pct = float(row.get("weight_percent", float(row.get("weight_pct", 0)) * 100))
            mv  = row.get("market_value", 0)
            lines.append(f"  {row['asset']:<10}  ₹{mv:>15,.2f}   ({pct:.1f}%)")

    elif intent in ("risk", "risk_page_explanation"):
        lines += [
            "Risk Metrics (Proxy: %s)" % data.get("proxy_asset", "portfolio"),
            f"  VaR 95%            : {data.get('var_95', data.get('VaR', 0)):.4f}",
            f"  Volatility (ann.)  : {data.get('volatility_annualized', data.get('Volatility', 0)):.4f}",
            f"  Sharpe Ratio       : {data.get('sharpe_ratio', data.get('Sharpe Ratio', 0)):.4f}",
            f"  Sortino Ratio      : {data.get('sortino_ratio', 0):.4f}",
            f"  Max Drawdown       : {data.get('max_drawdown', 0):.4f}",
            f"  Data Points        : {data.get('data_points', 0)}",
        ]

    elif intent == "market":
        lines.append("Latest Market Prices")
        if data.get("gold_price_inr_10g"):
            lines.append(f"  Gold  (10g)  : ₹{data['gold_price_inr_10g']:,.2f}")
        if data.get("silver_price_inr_10g"):
            lines.append(f"  Silver (10g) : ₹{data['silver_price_inr_10g']:,.2f}")
        for idx in ["nifty50", "nifty100", "nifty200", "nifty500"]:
            val = data.get(idx)
            if val:
                lines.append(f"  {idx.upper():<10} : {val:,.2f}")

    elif intent == "optimizer":
        lines.append("Optimal Portfolio Weights (Markowitz MPT)")
        for asset, w in (data.get("weights") or {}).items():
            lines.append(f"  {asset:<12} : {float(w)*100:.1f}%")
        if data.get("expected_return"):
            lines.append(f"  Expected Return  : {float(data['expected_return'])*100:.2f}%")
        if data.get("volatility"):
            lines.append(f"  Volatility       : {float(data['volatility'])*100:.2f}%")
        if data.get("sharpe"):
            lines.append(f"  Sharpe Ratio     : {float(data['sharpe']):.4f}")

    elif intent == "stress_test":
        lines.append("Stress Test — %s" % data.get("scenario_name", ""))
        impact = data.get("estimated_portfolio_impact", 0)
        lines.append(f"  Estimated Portfolio Impact : {float(impact)*100:.2f}%")
        lines.append("  Scenario Shocks:")
        for asset, shock in (data.get("scenario_shocks") or {}).items():
            lines.append(f"    {asset:<12}: {float(shock)*100:+.1f}%")

    else:
        return json.dumps(data, indent=2)

    return "\n".join(lines)


# ── Gemini explanation (with data-formatter fallback) ─────────────────────────
def _explain_with_gemini(user_query: str, intent: str, tool_name: str, data: Dict[str, Any]) -> str:
    formatted = _format_data_response(intent, data)

    if not GEMINI_ENABLED:
        notice = (
            "Note: Gemini AI is not active (API key missing or placeholder). "
            "Showing raw data summary instead.\n\n"
        ) if not _KEY_VALID else ""
        return notice + formatted

    prompt = f"""You are an expert portfolio analyst assistant for a Portfolio Intelligence System.

A non-technical investor asked: "{user_query}"

You have access to the following live data retrieved from the backend:

{formatted}

Full JSON data for reference:
{json.dumps(data, indent=2)}

Instructions:
- Answer the question directly using ONLY the numbers above.
- Do NOT invent or estimate numbers not present.
- Be concise (3-6 sentences max).
- Mention units (INR, %, etc.) wherever relevant.
- If the numbers look unusual, note it and suggest a possible reason.
- End with one actionable suggestion for the investor.
"""

    try:
        def _call_gemini() -> str:
            client = _get_client()
            response = client.models.generate_content(
                model=_GEMINI_MODEL,
                contents=prompt,
            )
            return (response.text or "").strip()

        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(_call_gemini)
            text = fut.result(timeout=3.0)

        logger.info("Gemini responded (%d chars)", len(text))
        if text:
            return text
        logger.warning("Gemini returned empty text. Falling back to formatter.")
    except FuturesTimeoutError:
        logger.warning("Gemini call timed out after 3s. Returning formatted summary.")
    except Exception as exc:
        logger.error("Gemini generate_content failed: %s", exc, exc_info=True)

    return formatted


# ── Routing helpers ────────────────────────────────────────────────────────────
def ai_agent_requires_gemini(user_query: str) -> bool:
    return classify_query(user_query) == "ai"


def _local_fast_response(intent: str) -> str:
    if intent == "greeting":
        return "Hi! I’m your AI Portfolio Copilot. Ask me about your portfolio, risk, or market trends."
    if intent == "risk_definition":
        return "Risk is the possibility of losing money in an investment. I can calculate your portfolio risk if data is available."
    return ""


# ── Public entrypoint ─────────────────────────────────────────────────────────
def ai_agent(user_query: str) -> str:
    """Main assistant entrypoint called by Streamlit UI."""
    if not user_query or not user_query.strip():
        return (
            "Please ask me something — for example:\n"
            "  • 'Show portfolio summary'\n"
            "  • 'What is my portfolio allocation?'\n"
            "  • 'What are the risk metrics?'\n"
            "  • 'What are current market prices?'"
        )

    intent = classify_query(user_query)
    local_reply = _local_fast_response(intent)
    if local_reply:
        return local_reply

    if intent == "portfolio":
        tool_name = "get_portfolio_summary"
    elif intent == "allocation":
        tool_name = "get_portfolio_allocation"
    elif intent == "risk":
        tool_name = "get_portfolio_risk_metrics"
    elif intent == "market":
        tool_name = "get_market_prices"
    elif intent == "optimizer":
        tool_name = "run_portfolio_optimizer"
    elif intent == "stress_test":
        tool_name = "run_stress_test"
    else:
        _, tool_name = _detect_intent(user_query)

    data = _invoke_tool(tool_name)

    if data.get("status") != "ok":
        msg = str(data.get("message", "")).strip()
        return (
            f"{msg}\n\n"
            "Tip: Make sure clients and transactions have been added, "
            "and that market data has been synced."
            if msg else
            "Could not fetch analytics. Please check that clients, transactions, "
            "and market prices are available in Google Sheets."
        )

    # Fast path: tool-first local summaries for common intents.
    if intent in {"portfolio", "allocation", "risk", "market", "optimizer", "stress_test"}:
        mapped_intent = {
            "portfolio": "portfolio_summary",
            "allocation": "allocation",
            "risk": "risk",
            "market": "market",
            "optimizer": "optimizer",
            "stress_test": "stress_test",
        }.get(intent, intent)
        return _format_data_response(mapped_intent, data)

    # Complex queries only: use Gemini with 3s timeout and formatter fallback.
    explain_intent = {
        "get_portfolio_summary": "portfolio_summary",
        "get_portfolio_allocation": "allocation",
        "get_portfolio_risk_metrics": "risk",
        "get_market_prices": "market",
        "run_portfolio_optimizer": "optimizer",
        "run_stress_test": "stress_test",
    }.get(tool_name, "portfolio_summary")

    return _explain_with_gemini(
        user_query=user_query,
        intent=explain_intent,
        tool_name=tool_name,
        data=data,
    )
