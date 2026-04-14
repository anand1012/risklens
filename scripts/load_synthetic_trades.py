"""
RiskLens — Synthetic Trades Loader
Generates DTCC-compatible OTC trade records and loads them into
risklens_bronze.trades_r directly via BigQuery client (no Spark needed).

Produces realistic FRTB IMA trade data across 5 asset classes × 5 desks.
All required fields for silver_transform.py are populated.

Usage:
    python scripts/load_synthetic_trades.py --project risklens-frtb-2026 --days 30
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

PROJECT = "risklens-frtb-2026"
DATASET = "risklens_bronze"
TABLE   = "trades_r"

# ── FRTB-relevant asset class mapping ────────────────────────────────────────
ASSET_CLASSES = {
    "RATES":       ["IRS_USD_5Y", "IRS_EUR_10Y", "OIS_USD_1Y", "SWAPTION_USD_1Y5Y", "IRS_GBP_10Y"],
    "FOREX":       ["EURUSD_FWD_1M", "GBPUSD_SPOT", "USDJPY_FWD_3M", "USDCHF_SPOT", "AUDUSD_FWD_1M"],
    "CREDITS":     ["CDS_JPMORGAN_5Y", "CDS_BARC_5Y", "CDX_NA_IG_5Y", "ITRAXX_EUR_5Y", "CDS_GS_5Y"],
    "EQUITIES":    ["SPX_PUT_3M", "ESTX50_CALL_1M", "VOL_SWAP_SPX", "VARIANCE_SWAP_SPX", "NIKKEI_FWD_1M"],
    "COMMODITIES": ["WTI_FUT_1M", "BRENT_FUT_2M", "GOLD_SPOT", "SILVER_FUT_3M", "NATGAS_FUT_1M"],
}

SUB_ASSET_CLASSES = {
    "RATES":       ["IR_SWAP", "SWAPTION", "OIS", "XCCY_SWAP"],
    "FOREX":       ["FX_SPOT", "FX_FORWARD", "FX_OPTION", "XCCY"],
    "CREDITS":     ["SINGLE_NAME_CDS", "CDS_INDEX", "TRANCHE"],
    "EQUITIES":    ["EQ_OPTION", "VARIANCE_SWAP", "EQ_FORWARD", "EQ_SWAP"],
    "COMMODITIES": ["COMMODITY_SWAP", "COMMODITY_OPTION", "COMMODITY_FORWARD"],
}

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD"]
SETTLEMENT_CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]  # USD-heavy

COUNTERPARTIES = [
    "JPMorgan Chase", "Goldman Sachs", "Deutsche Bank", "Barclays",
    "BNP Paribas", "UBS", "HSBC", "Morgan Stanley", "Citigroup", "Bank of America"
]

ACTIONS = ["NEW", "NEW", "NEW", "CORRECT", "CANCEL"]  # mostly NEW


def gen_trades_for_date(trade_date: datetime, n_per_asset_class: int = 400) -> list[dict]:
    """Generate n_per_asset_class trades per asset class for a given date."""
    rows = []
    date_str = trade_date.strftime("%Y-%m-%d")
    ingested_at = datetime.now(timezone.utc).isoformat()

    for asset_class, instruments in ASSET_CLASSES.items():
        for _ in range(n_per_asset_class):
            diss_id = str(uuid.uuid4())
            action = random.choice(ACTIONS)
            # CORRECT/CANCEL reference a prior dissemination_id
            orig_diss_id = str(uuid.uuid4()) if action in ("CORRECT", "CANCEL") else diss_id

            # Execution time: random time during trading hours
            exec_hour = random.randint(7, 21)
            exec_min  = random.randint(0, 59)
            exec_ts   = f"{date_str}T{exec_hour:02d}:{exec_min:02d}:00Z"

            notional = round(random.uniform(1_000_000, 500_000_000), 2)

            rows.append({
                "dissemination_id":          diss_id,
                "original_dissemination_id": orig_diss_id,
                "action":                    action,
                "execution_timestamp":       exec_ts,
                "cleared":                   random.choice(["Y", "N"]),
                "asset_class":               asset_class,
                "sub_asset_class":           random.choice(SUB_ASSET_CLASSES[asset_class]),
                "product_name":              random.choice(instruments),
                "notional_currency_1":       random.choice(CURRENCIES),
                "rounded_notional_amount_1": str(notional),
                "underlying_asset_1":        random.choice(instruments),
                "effective_date":            date_str,
                "end_date":                  (trade_date + timedelta(days=random.randint(30, 3650))).strftime("%Y-%m-%d"),
                "ingested_at":               ingested_at,
                "source_file":               f"gs://risklens-raw-risklens-frtb-2026/synthetic/trades/{date_str}/{asset_class.lower()}_trades.csv",
                "trade_date":                date_str,
            })

    return rows


def load_to_bigquery(rows: list[dict], client: bigquery.Client, project: str) -> int:
    """Insert rows into BigQuery bronze.trades_r."""
    table_ref = f"{project}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Load synthetic trades into bronze.trades_r")
    parser.add_argument("--project",            default=PROJECT)
    parser.add_argument("--days",               type=int, default=30, help="Number of trading days back to generate")
    parser.add_argument("--trades-per-class",   type=int, default=400, help="Trades per asset class per day (5 classes → 2000/day)")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    dates = []
    d = today
    while len(dates) < args.days:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # skip weekends
            dates.append(d)
    dates.reverse()  # oldest first

    total = 0
    for trade_date in dates:
        rows = gen_trades_for_date(trade_date, args.trades_per_class)
        # Insert in batches of 500 (BigQuery streaming limit)
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            load_to_bigquery(batch, client, args.project)
        total += len(rows)
        print(f"  ✓ {trade_date.strftime('%Y-%m-%d')}  {len(rows):,} rows  (total so far: {total:,})")

    print(f"\nDone. Loaded {total:,} synthetic trades into {args.project}.{DATASET}.{TABLE}")
    print(f"→ {args.days} trading days × {args.trades_per_class * 5} trades/day = {args.days * args.trades_per_class * 5:,} expected")


if __name__ == "__main__":
    main()
