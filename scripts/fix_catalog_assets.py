"""
RiskLens — Fix catalog.assets
Drops the corrupted (3-column) catalog.assets table, recreates it with the
proper 10-column schema, and seeds all 19 canonical asset rows.

Run once to unblock RAG indexing.

Usage:
    python scripts/fix_catalog_assets.py --project risklens-frtb-2026
"""

import argparse
from datetime import datetime, timezone
from google.cloud import bigquery

NOW = datetime.now(timezone.utc).isoformat()

ASSETS = [
    # Bronze
    {"asset_id": "bronze_dtcc_trades",    "name": "DTCC SDR Trades (Raw)",                  "domain": "market_data",  "layer": "bronze"},
    {"asset_id": "bronze_fred_rates",     "name": "FRED Rates (Raw)",                        "domain": "market_data",  "layer": "bronze"},
    {"asset_id": "bronze_yahoo_prices",   "name": "Yahoo Finance Prices (Raw)",              "domain": "market_data",  "layer": "bronze"},
    {"asset_id": "bronze_risk_outputs",   "name": "Synthetic Risk Outputs (Raw)",            "domain": "risk",         "layer": "bronze"},
    # Silver
    {"asset_id": "silver_trades",         "name": "Trades (Cleaned)",                        "domain": "market_data",  "layer": "silver"},
    {"asset_id": "silver_rates",          "name": "Rates (Cleaned)",                         "domain": "market_data",  "layer": "silver"},
    {"asset_id": "silver_prices",         "name": "Prices (Cleaned)",                        "domain": "market_data",  "layer": "silver"},
    {"asset_id": "silver_risk_outputs",   "name": "Risk Outputs (Cleaned)",                  "domain": "risk",         "layer": "silver"},
    {"asset_id": "silver_risk_enriched",  "name": "Risk Outputs Enriched (with Market Data)","domain": "risk",         "layer": "silver"},
    # Gold
    {"asset_id": "gold_trade_positions",  "name": "Enriched Trade Positions",                "domain": "risk",         "layer": "gold"},
    {"asset_id": "gold_backtesting",      "name": "VaR Back-Testing + Traffic Light",        "domain": "risk",         "layer": "gold"},
    {"asset_id": "gold_es_outputs",       "name": "ES 97.5% by Desk × Risk Class",           "domain": "risk",         "layer": "gold"},
    {"asset_id": "gold_pnl_vectors",      "name": "Hypothetical + Actual P&L Vectors",       "domain": "risk",         "layer": "gold"},
    {"asset_id": "gold_plat_results",     "name": "P&L Attribution Test Results",            "domain": "regulatory",   "layer": "gold"},
    {"asset_id": "gold_capital_charge",   "name": "Regulatory Capital Charge (IMA)",         "domain": "regulatory",   "layer": "gold"},
    {"asset_id": "gold_rfet_results",     "name": "Risk Factor Eligibility Test",            "domain": "regulatory",   "layer": "gold"},
    {"asset_id": "gold_risk_summary",     "name": "Daily Risk Summary Report",               "domain": "regulatory",   "layer": "gold"},
    # Reference
    {"asset_id": "ref_instrument_master", "name": "Instrument Master",                       "domain": "reference",    "layer": "gold"},
    {"asset_id": "ref_counterparty",      "name": "Counterparty Master",                     "domain": "reference",    "layer": "gold"},
    {"asset_id": "ref_currency",          "name": "Currency Reference",                      "domain": "reference",    "layer": "gold"},
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
    "silver_risk_enriched":  "Risk outputs enriched with market context (SOFR rate, VIX level, HY credit spread). Joins silver.risk_outputs with silver.rates on calc_date. Input to gold capital computations.",
    "gold_trade_positions":  "Enriched trade positions joining silver_trades with silver_prices and silver_rates. Includes present value using SOFR discount curve. Input to ES and PLAT calculations.",
    "gold_backtesting":      "VaR 99% 1-day back-testing results per desk (BCBS 457 ¶351-368). Compares modeled VaR against hypothetical and actual P&L over a rolling 250-day window. Assigns traffic light zone (GREEN/AMBER/RED) and capital multiplier. NOTE: VaR is for back-testing ONLY; ES 97.5% is the regulatory capital metric.",
    "gold_es_outputs":       "Expected Shortfall 97.5% per desk, risk class, and liquidity horizon (BCBS 457 ¶21-34). Each desk maps to its FRTB risk class (GIRR/FX/CSR_NS/EQ/COMM) with risk-class-specific liquidity horizon scaling (10-40 days). This is the primary regulatory capital metric under FRTB IMA.",
    "gold_pnl_vectors":      "Daily hypothetical P&L (risk-factor shocks only) and actual P&L (realized including unexplained) per desk. pnl_unexplained = actual - hypothetical. Input to the P&L Attribution Test (PLAT) under BCBS 457 ¶329-345.",
    "gold_plat_results":     "P&L Attribution Test (PLAT) results per desk over a rolling 12-month window (BCBS 457 ¶329-345). Three statistical tests: (1) UPL ratio |actual-hyp|/std < 0.95, (2) Spearman rank correlation > 0.40, (3) KS test statistic < 0.20. Desks failing PLAT face additional capital surcharges.",
    "gold_capital_charge":   "Regulatory capital charge per desk: ES_scaled × (3.0 + traffic_light_multiplier). Minimum 3.0× (GREEN zone), maximum 4.0× (RED zone, 10+ back-testing exceptions). This is the final number submitted to the regulator under FRTB IMA (BCBS 457 ¶180-186).",
    "gold_rfet_results":     "Risk Factor Eligibility Test (RFET) results per risk factor (BCBS 457 ¶76-80). Each risk factor must have ≥75 real observations in 12 months OR ≥25 in 90 days to be used in IMA models. Failed factors must be replaced with a Risk Committee-approved proxy.",
    "gold_risk_summary":     "Daily consolidated regulatory submission report. Aggregates ES 97.5% capital metric, PLAT pass/fail, back-testing traffic light, and final capital charge across all desks. Regulatory submission-ready format.",
    "ref_instrument_master": "Master reference for all traded instruments. Contains ISIN, CUSIP, asset class, currency, and desk mapping. Source of truth for instrument classification.",
    "ref_counterparty":      "Master reference for all trading counterparties. Contains LEI, credit rating, jurisdiction, and netting agreement details.",
    "ref_currency":          "Currency reference table. ISO codes, decimal places, and settlement calendars for all currencies in the trade universe.",
}

ASSET_TAGS = {
    "bronze_dtcc_trades":    ["market_data", "bronze", "frtb", "otc", "dtcc"],
    "bronze_fred_rates":     ["market_data", "bronze", "frtb", "macro", "fred"],
    "bronze_yahoo_prices":   ["market_data", "bronze", "frtb", "prices", "ohlcv"],
    "bronze_risk_outputs":   ["risk", "bronze", "frtb", "var", "es"],
    "silver_trades":         ["market_data", "silver", "frtb", "cleaned"],
    "silver_rates":          ["market_data", "silver", "frtb", "cleaned"],
    "silver_prices":         ["market_data", "silver", "frtb", "cleaned"],
    "silver_risk_outputs":   ["risk", "silver", "frtb", "cleaned"],
    "silver_risk_enriched":  ["risk", "silver", "frtb", "enriched", "sofr", "vix"],
    "gold_trade_positions":  ["risk", "gold", "frtb", "positions", "mtm"],
    "gold_backtesting":      ["risk", "gold", "frtb", "backtesting", "traffic-light"],
    "gold_es_outputs":       ["risk", "gold", "frtb", "es", "capital"],
    "gold_pnl_vectors":      ["risk", "gold", "frtb", "pnl", "plat"],
    "gold_plat_results":     ["regulatory", "gold", "frtb", "plat"],
    "gold_capital_charge":   ["regulatory", "gold", "frtb", "capital"],
    "gold_rfet_results":     ["regulatory", "gold", "frtb", "rfet", "ima"],
    "gold_risk_summary":     ["regulatory", "gold", "frtb", "summary"],
    "ref_instrument_master": ["reference", "gold", "frtb", "instruments"],
    "ref_counterparty":      ["reference", "gold", "frtb", "counterparty"],
    "ref_currency":          ["reference", "gold", "frtb", "currency"],
}

SCHEMA = [
    bigquery.SchemaField("asset_id",    "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("name",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("type",        "STRING"),
    bigquery.SchemaField("domain",      "STRING"),
    bigquery.SchemaField("layer",       "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("tags",        "STRING",    mode="REPEATED"),
    bigquery.SchemaField("row_count",   "INTEGER"),
    bigquery.SchemaField("size_bytes",  "INTEGER"),
    bigquery.SchemaField("created_at",  "TIMESTAMP"),
    bigquery.SchemaField("updated_at",  "TIMESTAMP"),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="GCP project ID")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    table_ref = f"{args.project}.risklens_catalog.assets"

    # 1. Drop existing table
    print(f"Dropping {table_ref} ...")
    client.delete_table(table_ref, not_found_ok=True)
    print("  Dropped.")

    # 2. Recreate with proper schema
    print("Recreating with proper 11-column schema ...")
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table.clustering_fields = ["domain", "layer", "type"]
    client.create_table(table)
    print("  Created.")

    # 3. Insert all asset rows
    print(f"Inserting {len(ASSETS)} asset rows ...")
    rows = []
    for a in ASSETS:
        rows.append({
            "asset_id":    a["asset_id"],
            "name":        a["name"],
            "type":        "table",
            "domain":      a["domain"],
            "layer":       a["layer"],
            "description": ASSET_DESCRIPTIONS.get(a["asset_id"], ""),
            "tags":        ASSET_TAGS.get(a["asset_id"], [a["domain"], a["layer"], "frtb"]),
            "row_count":   None,
            "size_bytes":  None,
            "created_at":  NOW,
            "updated_at":  NOW,
        })

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"  Insert errors: {errors}")
        return 1

    print(f"  Inserted {len(rows)} rows successfully.")
    print("\ncatalog.assets repaired. RAG indexing should now succeed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
