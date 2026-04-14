"""
RiskLens — Synthetic Data Generator

Generates realistic FRTB-flavored internal data:
- Trade book (5 desks, 10,000 trades/day)
- Market data (prices, vol surfaces)
- Risk outputs (VaR, ES, P&L vectors)
- Reference data (instruments, counterparties, currencies)
- Pipeline job logs
- Data quality results
- Dataset ownership registry

Usage:
    python ingestion/synthetic/generate.py --days 30 --output-dir /tmp/synthetic
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ── Constants ────────────────────────────────────────────────────────────────

DESKS = ["Rates", "FX", "Credit", "Equities", "Commodities"]

INSTRUMENTS = {
    "Rates":       ["IRS_USD_5Y", "IRS_EUR_10Y", "CDS_JPMORGAN", "SWAPTION_USD_1Y5Y", "BOND_UST_10Y"],
    "FX":          ["EURUSD_SPOT", "GBPUSD_SPOT", "USDJPY_FWD_1M", "USDCHF_SPOT", "AUDUSD_SPOT"],
    "Credit":      ["CDS_BARC_5Y", "CDS_DB_5Y", "CDX_NA_IG", "ITRAXX_EUR", "CDS_GS_5Y"],
    "Equities":    ["SPX_PUT_3M", "ESTX50_CALL_1M", "GOOGL_STOCK", "JPM_STOCK", "VOL_SWAP_SPX"],
    "Commodities": ["WTI_FUT_1M", "BRENT_FUT_2M", "GOLD_SPOT", "SILVER_FUT_3M", "NATGAS_FUT_1M"],
}

COUNTERPARTIES = [
    "JPMorgan Chase", "Goldman Sachs", "Deutsche Bank", "Barclays",
    "BNP Paribas", "UBS", "Credit Suisse", "HSBC", "Morgan Stanley", "Citigroup"
]

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "SEK"]

BOOKS = {
    "Rates":       ["RATES_BOOK_01", "RATES_BOOK_02", "RATES_HEDGE"],
    "FX":          ["FX_SPOT_BOOK", "FX_FWD_BOOK"],
    "Credit":      ["CREDIT_BOOK_01", "CREDIT_HY_BOOK"],
    "Equities":    ["EQ_DERIV_BOOK", "EQ_DELTA_BOOK"],
    "Commodities": ["CMDTY_BOOK_01"],
}

OWNERS = [
    {"name": "Sarah Chen",    "team": "Market Risk Data",    "email": "s.chen@risklens.io"},
    {"name": "James Okafor",  "team": "Regulatory Reporting","email": "j.okafor@risklens.io"},
    {"name": "Priya Sharma",  "team": "Reference Data",      "email": "p.sharma@risklens.io"},
    {"name": "Tom Brennan",   "team": "Data Engineering",    "email": "t.brennan@risklens.io"},
    {"name": "Anita Kovacs",  "team": "Quant Analytics",     "email": "a.kovacs@risklens.io"},
]

ASSETS = [
    # Bronze
    {"asset_id": "bronze_dtcc_trades",    "name": "DTCC SDR Trades (Raw)",         "domain": "market_data",  "layer": "bronze"},
    {"asset_id": "bronze_fred_rates",     "name": "FRED Rates (Raw)",              "domain": "market_data",  "layer": "bronze"},
    {"asset_id": "bronze_yahoo_prices",   "name": "Yahoo Finance Prices (Raw)",    "domain": "market_data",  "layer": "bronze"},
    {"asset_id": "bronze_risk_outputs",   "name": "Synthetic Risk Outputs (Raw)",  "domain": "risk",         "layer": "bronze"},
    # Silver
    {"asset_id": "silver_trades",         "name": "Trades (Cleaned)",              "domain": "market_data",  "layer": "silver"},
    {"asset_id": "silver_rates",          "name": "Rates (Cleaned)",               "domain": "market_data",  "layer": "silver"},
    {"asset_id": "silver_prices",         "name": "Prices (Cleaned)",              "domain": "market_data",  "layer": "silver"},
    {"asset_id": "silver_risk_outputs",   "name": "Risk Outputs (Cleaned)",        "domain": "risk",         "layer": "silver"},
    # Gold
    {"asset_id": "gold_trade_positions",  "name": "Enriched Trade Positions",      "domain": "risk",         "layer": "gold"},
    {"asset_id": "gold_var_outputs",      "name": "VaR Outputs by Desk",           "domain": "risk",         "layer": "gold"},
    {"asset_id": "gold_es_outputs",       "name": "Expected Shortfall by Desk",    "domain": "risk",         "layer": "gold"},
    {"asset_id": "gold_pnl_vectors",      "name": "P&L Vectors (100 scenarios)",   "domain": "risk",         "layer": "gold"},
    {"asset_id": "gold_risk_summary",     "name": "Daily Risk Summary Report",     "domain": "regulatory",   "layer": "gold"},
    # Reference
    {"asset_id": "ref_instrument_master", "name": "Instrument Master",             "domain": "reference",    "layer": "gold"},
    {"asset_id": "ref_counterparty",      "name": "Counterparty Master",           "domain": "reference",    "layer": "gold"},
    {"asset_id": "ref_currency",          "name": "Currency Reference",            "domain": "reference",    "layer": "gold"},
]

ASSET_DESCRIPTIONS = {
    "bronze_dtcc_trades":    "Raw OTC derivatives trade data ingested from DTCC Swap Data Repository. Contains all publicly reported trades across asset classes. No transforms applied — immutable raw landing zone.",
    "bronze_fred_rates":     "Raw macroeconomic time-series data from Federal Reserve Economic Data (FRED) API. Includes Fed Funds rate, 10Y treasury yield, HY credit spread, and EUR/USD FX rate.",
    "bronze_yahoo_prices":   "Raw daily OHLCV price data from Yahoo Finance for instruments matching DTCC trade universe. Includes equity prices, commodity futures, and FX spot rates.",
    "bronze_risk_outputs":   "Synthetic VaR, Expected Shortfall, and P&L vector outputs generated from enriched trade positions. Seeded from real FRED and Yahoo data inputs.",
    "silver_trades":         "Cleaned and normalized trade data. Nulls removed, schema standardized, duplicates eliminated. Desk, instrument, notional, counterparty, and book fields validated.",
    "silver_rates":          "Cleaned FRED rate series. Outliers flagged, gaps forward-filled, schema normalized to standard time-series format.",
    "silver_prices":         "Cleaned Yahoo Finance prices. OHLCV validated, split-adjusted, schema normalized. Instruments mapped to internal instrument IDs.",
    "silver_risk_outputs":   "Cleaned risk outputs. VaR and ES values validated against historical bounds. P&L vector dimension checks passed.",
    "gold_trade_positions":  "Enriched trade positions joining silver_trades with silver_prices and silver_rates. Final positions used as input to VaR and ES calculations.",
    "gold_var_outputs":      "Daily 99% VaR outputs per desk and at firm level. Calculated using Historical Simulation over 250 trading days. Primary FRTB-IMA regulatory metric.",
    "gold_es_outputs":       "Daily 97.5% Expected Shortfall per desk. Regulatory capital metric under FRTB Internal Models Approach. Fed into daily capital adequacy report.",
    "gold_pnl_vectors":      "100-scenario P&L attribution vectors per trade per day. Used for P&L Attribution Test (PLAT) under FRTB-IMA. Desk-level aggregation available.",
    "gold_risk_summary":     "Daily consolidated risk summary report. Aggregates VaR, ES, and PLAT results across all desks. Regulatory submission-ready format.",
    "ref_instrument_master": "Master reference for all traded instruments. Contains ISIN, CUSIP, asset class, currency, and desk mapping. Source of truth for instrument classification.",
    "ref_counterparty":      "Master reference for all trading counterparties. Contains LEI, credit rating, jurisdiction, and netting agreement details.",
    "ref_currency":          "Currency reference table. ISO codes, decimal places, and settlement calendars for all currencies in the trade universe.",
}

LINEAGE_EDGES = [
    ("dtcc_sdr_source",    "bronze_dtcc_trades",    "feeds",      "bronze_trades_job"),
    ("fred_api_source",    "bronze_fred_rates",     "feeds",      "bronze_rates_job"),
    ("yahoo_api_source",   "bronze_yahoo_prices",   "feeds",      "bronze_prices_job"),
    ("bronze_dtcc_trades", "silver_trades",         "transforms", "silver_transform_job"),
    ("bronze_fred_rates",  "silver_rates",          "transforms", "silver_transform_job"),
    ("bronze_yahoo_prices","silver_prices",         "transforms", "silver_transform_job"),
    ("bronze_risk_outputs","silver_risk_outputs",   "transforms", "silver_transform_job"),
    ("silver_trades",      "gold_trade_positions",  "aggregates", "gold_aggregate_job"),
    ("silver_prices",      "gold_trade_positions",  "aggregates", "gold_aggregate_job"),
    ("silver_rates",       "gold_trade_positions",  "aggregates", "gold_aggregate_job"),
    ("gold_trade_positions","gold_var_outputs",     "aggregates", "gold_aggregate_job"),
    ("gold_trade_positions","gold_es_outputs",      "aggregates", "gold_aggregate_job"),
    ("gold_trade_positions","gold_pnl_vectors",     "aggregates", "gold_aggregate_job"),
    ("gold_var_outputs",   "gold_risk_summary",     "aggregates", "gold_aggregate_job"),
    ("gold_es_outputs",    "gold_risk_summary",     "aggregates", "gold_aggregate_job"),
    ("gold_pnl_vectors",   "gold_risk_summary",     "aggregates", "gold_aggregate_job"),
]

LINEAGE_NODES = [
    ("dtcc_sdr_source",     "DTCC SDR API",          "source"),
    ("fred_api_source",     "FRED API",              "source"),
    ("yahoo_api_source",    "Yahoo Finance API",     "source"),
    ("bronze_trades_job",   "Spark Bronze Trades",   "pipeline"),
    ("bronze_rates_job",    "Spark Bronze Rates",    "pipeline"),
    ("bronze_prices_job",   "Spark Bronze Prices",   "pipeline"),
    ("silver_transform_job","Spark Silver Transform","pipeline"),
    ("gold_aggregate_job",  "Spark Gold Aggregate",  "pipeline"),
] + [(a["asset_id"], a["name"], "table") for a in ASSETS]


# ── Generators ───────────────────────────────────────────────────────────────

def gen_trades(date: datetime, n: int = 2000) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        desk = random.choice(DESKS)
        instrument = random.choice(INSTRUMENTS[desk])
        notional = round(random.uniform(1_000_000, 500_000_000), 2)
        rows.append({
            "trade_id":       str(uuid.uuid4()),
            "trade_date":     date.strftime("%Y-%m-%d"),
            "desk":           desk,
            "instrument_id":  instrument,
            "notional_usd":   notional,
            "currency":       random.choice(CURRENCIES),
            "counterparty":   random.choice(COUNTERPARTIES),
            "book":           random.choice(BOOKS[desk]),
            "direction":      random.choice(["BUY", "SELL"]),
            "maturity_date":  (date + timedelta(days=random.randint(30, 3650))).strftime("%Y-%m-%d"),
            "ingested_at":    datetime.utcnow().isoformat(),
        })
    return pd.DataFrame(rows)


def gen_var_es(date: datetime) -> pd.DataFrame:
    rows = []
    for desk in DESKS:
        base_var = random.uniform(1_000_000, 50_000_000)
        base_es  = base_var * random.uniform(1.2, 1.6)
        rows.append({
            "calc_date":    date.strftime("%Y-%m-%d"),
            "desk":         desk,
            "var_99_1d":    round(base_var, 2),
            "var_99_10d":   round(base_var * np.sqrt(10), 2),
            "es_975_1d":    round(base_es, 2),
            "es_975_10d":   round(base_es * np.sqrt(10), 2),
            "confidence":   0.99,
            "horizon_days": 1,
            "method":       "Historical Simulation",
            "scenarios":    250,
            "calculated_at":datetime.utcnow().isoformat(),
        })
    return pd.DataFrame(rows)


def gen_pnl_vectors(date: datetime) -> pd.DataFrame:
    rows = []
    for desk in DESKS:
        scenarios = np.random.normal(0, 1_000_000, 100).round(2).tolist()
        rows.append({
            "calc_date":    date.strftime("%Y-%m-%d"),
            "desk":         desk,
            "scenarios":    json.dumps(scenarios),
            "num_scenarios":100,
            "mean_pnl":     round(float(np.mean(scenarios)), 2),
            "std_pnl":      round(float(np.std(scenarios)), 2),
            "calculated_at":datetime.utcnow().isoformat(),
        })
    return pd.DataFrame(rows)


def gen_pipeline_logs(date: datetime) -> pd.DataFrame:
    jobs = [
        "bronze_trades_job", "bronze_rates_job", "bronze_prices_job",
        "bronze_synthetic_job", "silver_transform_job", "gold_aggregate_job",
    ]
    rows = []
    for job in jobs:
        status  = random.choices(["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"], weights=[8, 8, 8, 1])[0]
        rows.append({
            "job_id":       str(uuid.uuid4()),
            "job_name":     job,
            "run_date":     date.strftime("%Y-%m-%d"),
            "status":       status,
            "rows_read":    random.randint(10_000, 500_000),
            "rows_written": random.randint(9_000, 490_000),
            "duration_secs":random.randint(30, 600),
            "error_msg":    "Null constraint violation on notional_usd" if status == "FAILED" else None,
            "started_at":   (date.replace(hour=6) + timedelta(minutes=random.randint(0, 60))).isoformat(),
        })
    return pd.DataFrame(rows)


def gen_ownership() -> pd.DataFrame:
    rows = []
    for i, asset in enumerate(ASSETS):
        owner = OWNERS[i % len(OWNERS)]
        rows.append({
            "asset_id":      asset["asset_id"],
            "owner_name":    owner["name"],
            "team":          owner["team"],
            "steward":       OWNERS[(i + 1) % len(OWNERS)]["name"],
            "email":         owner["email"],
            "assigned_date": (datetime.utcnow() - timedelta(days=random.randint(30, 365))).strftime("%Y-%m-%d"),
        })
    # Leave 2 assets without owners (to show governance gaps)
    rows[-1]["owner_name"] = None
    rows[-2]["owner_name"] = None
    return pd.DataFrame(rows)


def gen_quality_scores(date: datetime) -> pd.DataFrame:
    rows = []
    for asset in ASSETS:
        null_rate = round(random.uniform(0.0, 0.05), 4)
        freshness = "fresh" if null_rate < 0.02 else ("stale" if null_rate < 0.04 else "critical")
        rows.append({
            "asset_id":         asset["asset_id"],
            "null_rate":        null_rate,
            "schema_drift":     random.random() < 0.05,
            "freshness_status": freshness,
            "duplicate_rate":   round(random.uniform(0.0, 0.01), 4),
            "last_checked":     date.isoformat(),
        })
    return pd.DataFrame(rows)


def gen_sla_status(date: datetime) -> pd.DataFrame:
    rows = []
    for asset in ASSETS:
        expected = date.replace(hour=8, minute=0, second=0)
        delay    = random.randint(-30, 90)   # minutes: negative = early, positive = late
        actual   = expected + timedelta(minutes=delay)
        breach   = delay > 30
        rows.append({
            "asset_id":             asset["asset_id"],
            "expected_refresh":     expected.isoformat(),
            "actual_refresh":       actual.isoformat(),
            "breach_flag":          breach,
            "breach_duration_mins": max(0, delay - 30) if breach else 0,
            "checked_at":           date.isoformat(),
        })
    return pd.DataFrame(rows)


def gen_assets_catalog() -> pd.DataFrame:
    rows = []
    for asset in ASSETS:
        rows.append({
            "asset_id":   asset["asset_id"],
            "name":       asset["name"],
            "type":       "table",
            "domain":     asset["domain"],
            "layer":      asset["layer"],
            "description":ASSET_DESCRIPTIONS.get(asset["asset_id"], ""),
            "tags":       json.dumps([asset["domain"], asset["layer"], "frtb"]),
            "row_count":  random.randint(10_000, 5_000_000),
            "size_bytes": random.randint(1_000_000, 500_000_000),
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        })
    return pd.DataFrame(rows)


def gen_schema_registry() -> pd.DataFrame:
    """One row per column per asset — column-level metadata for all catalog assets."""
    # Column definitions per asset_id
    SCHEMAS = {
        "bronze_dtcc_trades": [
            ("dissemination_id", "STRING", False, "Unique DTCC trade identifier", "1234567890"),
            ("asset_class", "STRING", False, "Derivative asset class", "RATES"),
            ("sub_asset_class", "STRING", True, "Sub-classification of the asset class", "Fixed-to-Float"),
            ("execution_timestamp", "STRING", True, "Trade execution timestamp (ISO 8601)", "2026-01-15T14:23:00Z"),
            ("notional_currency_1", "STRING", True, "Primary notional currency (ISO 4217)", "USD"),
            ("rounded_notional_amount_1", "STRING", True, "Rounded notional amount in primary currency", "100000000+"),
            ("underlying_asset_1", "STRING", True, "Primary underlying asset or reference rate", "USD-LIBOR-BBA"),
            ("settlement_currency", "STRING", True, "Settlement currency (ISO 4217)", "USD"),
            ("effective_date", "STRING", True, "Trade effective / start date", "2026-01-20"),
            ("end_date", "STRING", True, "Trade termination / maturity date", "2031-01-20"),
            ("trade_date", "STRING", False, "DTCC publication date (partition key)", "2026-01-15"),
            ("ingested_at", "TIMESTAMP", False, "Pipeline ingestion timestamp", None),
        ],
        "bronze_fred_rates": [
            ("series_id", "STRING", False, "FRED series identifier", "DGS10"),
            ("series_name", "STRING", False, "Human-readable series name", "10Y Treasury Yield"),
            ("frequency", "STRING", False, "Observation frequency: daily or monthly", "daily"),
            ("domain", "STRING", False, "Rate domain: rates, fx, credit, macro, volatility", "rates"),
            ("date", "DATE", False, "Observation date", "2026-01-15"),
            ("value", "FLOAT64", True, "Rate value (percent or index level)", "4.42"),
            ("trade_date", "STRING", False, "Ingestion run date (partition key)", "2026-01-15"),
            ("ingested_at", "TIMESTAMP", False, "Pipeline ingestion timestamp", None),
        ],
        "bronze_yahoo_prices": [
            ("ticker", "STRING", False, "Yahoo Finance ticker symbol", "JPM"),
            ("name", "STRING", False, "Instrument full name", "JPMorgan Chase"),
            ("asset_class", "STRING", False, "Asset class: equity, etf, fx, commodity, volatility", "equity"),
            ("currency", "STRING", False, "Instrument native currency (ISO 4217)", "USD"),
            ("date", "DATE", False, "Trading date", "2026-01-15"),
            ("open", "FLOAT64", True, "Opening price", "245.10"),
            ("high", "FLOAT64", True, "Intraday high price", "247.80"),
            ("low", "FLOAT64", True, "Intraday low price", "244.30"),
            ("close", "FLOAT64", True, "Closing price", "246.50"),
            ("adj_close", "FLOAT64", True, "Split and dividend-adjusted closing price", "246.50"),
            ("volume", "INTEGER", True, "Trading volume (shares)", "8234000"),
            ("trade_date", "STRING", False, "Ingestion run date (partition key)", "2026-01-15"),
            ("ingested_at", "TIMESTAMP", False, "Pipeline ingestion timestamp", None),
        ],
        "bronze_risk_outputs": [
            ("desk", "STRING", False, "Trading desk name", "Rates"),
            ("calc_date", "DATE", False, "Risk calculation date", "2026-01-15"),
            ("var_99_1d", "FLOAT64", True, "99th percentile 1-day VaR (USD millions)", "2.34"),
            ("var_99_10d", "FLOAT64", True, "99th percentile 10-day VaR (USD millions)", "7.40"),
            ("es_975_1d", "FLOAT64", True, "97.5% 1-day Expected Shortfall (USD millions)", "2.69"),
            ("es_975_10d", "FLOAT64", True, "97.5% 10-day Expected Shortfall (USD millions)", "8.51"),
            ("method", "STRING", True, "VaR calculation method", "Historical Simulation"),
            ("scenarios", "STRING", True, "Number of scenarios used", "500"),
            ("num_scenarios", "INTEGER", True, "Parsed scenario count", "500"),
            ("mean_pnl", "FLOAT64", True, "Mean scenario P&L (USD millions)", "0.05"),
            ("std_pnl", "FLOAT64", True, "Standard deviation of scenario P&L", "0.82"),
            ("trade_date", "STRING", False, "Ingestion run date (partition key)", "2026-01-15"),
            ("ingested_at", "TIMESTAMP", False, "Pipeline ingestion timestamp", None),
        ],
        "silver_trades": [
            ("dissemination_id", "STRING", False, "Deduplicated DTCC trade identifier", "1234567890"),
            ("asset_class", "STRING", False, "Normalised asset class", "RATES"),
            ("currency", "STRING", True, "Notional currency (ISO 4217)", "USD"),
            ("notional_amount", "FLOAT64", True, "Notional amount (numeric)", "100000000.0"),
            ("underlying", "STRING", True, "Underlying asset or reference rate", "USD-LIBOR-BBA"),
            ("effective_date", "DATE", True, "Trade effective date", "2026-01-20"),
            ("end_date", "DATE", True, "Trade maturity date", "2031-01-20"),
            ("execution_timestamp", "TIMESTAMP", True, "Parsed execution timestamp", None),
            ("trade_date", "STRING", False, "Source trade date", "2026-01-15"),
            ("processed_at", "TIMESTAMP", False, "Silver transform timestamp", None),
        ],
        "silver_rates": [
            ("series_id", "STRING", False, "FRED series identifier", "DGS10"),
            ("series_name", "STRING", False, "Series name", "10Y Treasury Yield"),
            ("frequency", "STRING", False, "Observation frequency", "daily"),
            ("domain", "STRING", False, "Rate domain", "rates"),
            ("date", "DATE", False, "Observation date", "2026-01-15"),
            ("value", "FLOAT64", False, "Rate value (nulls removed in silver)", "4.42"),
            ("is_outlier", "BOOL", False, "True if value > 3 std from series mean", "false"),
            ("trade_date", "STRING", False, "Source trade date", "2026-01-15"),
            ("processed_at", "TIMESTAMP", False, "Silver transform timestamp", None),
        ],
        "silver_prices": [
            ("ticker", "STRING", False, "Yahoo Finance ticker", "JPM"),
            ("name", "STRING", False, "Instrument name", "JPMorgan Chase"),
            ("asset_class", "STRING", False, "Asset class", "equity"),
            ("currency", "STRING", False, "Currency", "USD"),
            ("date", "DATE", False, "Trading date", "2026-01-15"),
            ("open", "FLOAT64", True, "Opening price (OHLCV-validated)", "245.10"),
            ("high", "FLOAT64", True, "High price (OHLCV-validated)", "247.80"),
            ("low", "FLOAT64", True, "Low price (OHLCV-validated)", "244.30"),
            ("close", "FLOAT64", True, "Close price (OHLCV-validated)", "246.50"),
            ("adj_close", "FLOAT64", True, "Adjusted close", "246.50"),
            ("volume", "INTEGER", True, "Volume", "8234000"),
            ("trade_date", "STRING", False, "Source trade date", "2026-01-15"),
            ("processed_at", "TIMESTAMP", False, "Silver transform timestamp", None),
        ],
        "silver_risk_outputs": [
            ("desk", "STRING", False, "Trading desk", "Rates"),
            ("calc_date", "DATE", False, "Calculation date (deduplicated)", "2026-01-15"),
            ("var_99_1d", "FLOAT64", True, "Validated 1-day VaR", "2.34"),
            ("var_99_10d", "FLOAT64", True, "Validated 10-day VaR", "7.40"),
            ("es_975_1d", "FLOAT64", True, "Validated 1-day ES", "2.69"),
            ("es_975_10d", "FLOAT64", True, "Validated 10-day ES", "8.51"),
            ("method", "STRING", True, "VaR method", "Historical Simulation"),
            ("mean_pnl", "FLOAT64", True, "Mean scenario P&L", "0.05"),
            ("std_pnl", "FLOAT64", True, "Std dev of scenario P&L", "0.82"),
            ("trade_date", "STRING", False, "Source trade date", "2026-01-15"),
            ("processed_at", "TIMESTAMP", False, "Silver transform timestamp", None),
        ],
        "gold_trade_positions": [
            ("trade_id", "STRING", False, "DTCC dissemination ID", "1234567890"),
            ("currency", "STRING", True, "Trade currency", "USD"),
            ("asset_class", "STRING", True, "Asset class", "RATES"),
            ("notional_amount", "FLOAT64", True, "Notional amount", "100000000.0"),
            ("mark_price", "FLOAT64", True, "EOD mark price", "246.50"),
            ("market_value_usd", "FLOAT64", True, "Market value in USD", "24650000.0"),
            ("discount_rate", "FLOAT64", True, "Fed Funds rate used as discount", "0.0533"),
            ("pv_usd", "FLOAT64", True, "Present value in USD", "23419848.0"),
            ("trade_date", "STRING", False, "Trade date", "2026-01-15"),
            ("processed_at", "TIMESTAMP", False, "Gold aggregate timestamp", None),
        ],
        "gold_var_outputs": [
            ("calc_date", "DATE", False, "VaR calculation date", "2026-01-15"),
            ("desk", "STRING", False, "Desk name or FIRM for firm-wide aggregate", "Rates"),
            ("var_99_1d", "FLOAT64", True, "99th pct 1-day VaR (USD millions)", "2.34"),
            ("var_99_10d", "FLOAT64", True, "99th pct 10-day VaR (USD millions)", "7.40"),
            ("method", "STRING", True, "VaR calculation method", "Historical Simulation"),
            ("scenarios", "STRING", True, "Scenario count", "500"),
            ("trade_date", "STRING", False, "Source trade date", "2026-01-15"),
            ("processed_at", "TIMESTAMP", False, "Gold aggregate timestamp", None),
        ],
        "gold_es_outputs": [
            ("calc_date", "DATE", False, "ES calculation date", "2026-01-15"),
            ("desk", "STRING", False, "Desk name or FIRM", "Rates"),
            ("es_975_1d", "FLOAT64", True, "97.5% 1-day Expected Shortfall (USD millions)", "2.69"),
            ("es_975_10d", "FLOAT64", True, "97.5% 10-day Expected Shortfall (USD millions)", "8.51"),
            ("trade_date", "STRING", False, "Source trade date", "2026-01-15"),
            ("processed_at", "TIMESTAMP", False, "Gold aggregate timestamp", None),
        ],
        "gold_pnl_vectors": [
            ("calc_date", "DATE", False, "P&L calculation date", "2026-01-15"),
            ("desk", "STRING", False, "Trading desk", "Rates"),
            ("scenarios", "STRING", True, "Scenario count label", "500"),
            ("num_scenarios", "INTEGER", True, "Number of scenarios", "500"),
            ("mean_pnl", "FLOAT64", True, "Mean scenario P&L (USD millions)", "0.05"),
            ("std_pnl", "FLOAT64", True, "Std dev of scenario P&L", "0.82"),
            ("trade_date", "STRING", False, "Source trade date", "2026-01-15"),
            ("processed_at", "TIMESTAMP", False, "Gold aggregate timestamp", None),
        ],
        "gold_risk_summary": [
            ("calc_date", "DATE", False, "Report date", "2026-01-15"),
            ("desk", "STRING", False, "Desk or FIRM", "Rates"),
            ("var_99_1d", "FLOAT64", True, "1-day VaR", "2.34"),
            ("var_99_10d", "FLOAT64", True, "10-day VaR", "7.40"),
            ("es_975_1d", "FLOAT64", True, "1-day ES", "2.69"),
            ("es_975_10d", "FLOAT64", True, "10-day ES", "8.51"),
            ("mean_pnl", "FLOAT64", True, "Mean P&L", "0.05"),
            ("std_pnl", "FLOAT64", True, "P&L std dev", "0.82"),
            ("num_scenarios", "INTEGER", True, "Scenario count", "500"),
            ("plat_pass", "BOOL", True, "P&L Attribution Test pass/fail", "true"),
            ("method", "STRING", True, "VaR method", "Historical Simulation"),
            ("trade_date", "STRING", False, "Source trade date", "2026-01-15"),
            ("report_generated_at", "TIMESTAMP", False, "Report generation timestamp", None),
        ],
    }

    rows = []
    for asset_id, columns in SCHEMAS.items():
        for col_name, data_type, nullable, description, sample_value in columns:
            rows.append({
                "asset_id":     asset_id,
                "column_name":  col_name,
                "data_type":    data_type,
                "nullable":     nullable,
                "description":  description,
                "sample_value": sample_value,
            })
    return pd.DataFrame(rows)


def gen_lineage() -> tuple[pd.DataFrame, pd.DataFrame]:
    nodes = pd.DataFrame([
        {"node_id": n[0], "name": n[1], "type": n[2],
         "domain": "frtb", "layer": "", "metadata": "{}",
         "created_at": datetime.utcnow().isoformat()}
        for n in LINEAGE_NODES
    ])
    edges = pd.DataFrame([
        {"edge_id":      str(uuid.uuid4()),
         "from_node_id": e[0], "to_node_id": e[1],
         "relationship": e[2], "pipeline_job": e[3],
         "created_at":   datetime.utcnow().isoformat()}
        for e in LINEAGE_EDGES
    ])
    return nodes, edges


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days",       type=int, default=30,        help="Number of days of data to generate")
    parser.add_argument("--output-dir", default="/tmp/risklens/synthetic", help="Output directory")
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    end_date   = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=args.days)

    print(f"Generating {args.days} days of synthetic data → {out}")

    # Daily data
    all_trades, all_var_es, all_pnl, all_logs, all_quality, all_sla = [], [], [], [], [], []

    current = start_date
    while current <= end_date:
        if current.weekday() < 5:   # weekdays only
            all_trades.append(gen_trades(current))
            all_var_es.append(gen_var_es(current))
            all_pnl.append(gen_pnl_vectors(current))
            all_logs.append(gen_pipeline_logs(current))
            all_quality.append(gen_quality_scores(current))
            all_sla.append(gen_sla_status(current))
        current += timedelta(days=1)

    pd.concat(all_trades).to_parquet(out / "trades.parquet",       index=False)
    pd.concat(all_var_es).to_parquet(out / "var_es.parquet",       index=False)
    pd.concat(all_pnl).to_parquet(out    / "pnl_vectors.parquet",  index=False)
    pd.concat(all_logs).to_parquet(out   / "pipeline_logs.parquet",index=False)
    pd.concat(all_quality).to_parquet(out/ "quality_scores.parquet",index=False)
    pd.concat(all_sla).to_parquet(out    / "sla_status.parquet",   index=False)

    # Static data
    gen_ownership().to_parquet(out      / "ownership.parquet",     index=False)
    gen_assets_catalog().to_parquet(out / "assets.parquet",        index=False)

    nodes, edges = gen_lineage()
    nodes.to_parquet(out / "lineage_nodes.parquet", index=False)
    edges.to_parquet(out / "lineage_edges.parquet", index=False)

    print(f"\n  trades.parquet        → {len(pd.concat(all_trades)):,} rows")
    print(f"  var_es.parquet        → {len(pd.concat(all_var_es)):,} rows")
    print(f"  pnl_vectors.parquet   → {len(pd.concat(all_pnl)):,} rows")
    print(f"  pipeline_logs.parquet → {len(pd.concat(all_logs)):,} rows")
    print(f"  quality_scores.parquet→ {len(pd.concat(all_quality)):,} rows")
    print(f"  sla_status.parquet    → {len(pd.concat(all_sla)):,} rows")
    print(f"  ownership.parquet     → {len(gen_ownership()):,} rows")
    print(f"  assets.parquet        → {len(gen_assets_catalog()):,} rows")
    print(f"  lineage_nodes.parquet → {len(nodes):,} rows")
    print(f"  lineage_edges.parquet → {len(edges):,} rows")
    print(f"\n✓ Synthetic data generation complete.")


if __name__ == "__main__":
    main()
