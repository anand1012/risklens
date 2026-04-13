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
