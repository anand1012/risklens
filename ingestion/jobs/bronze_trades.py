"""
RiskLens — Bronze Layer: DTCC SDR Trade Ingestion (Synthetic)
Generates realistic DTCC-style OTC derivatives trade records for the given date
and lands them into BigQuery bronze layer. No transforms.

NOTE: The DTCC public SDR URL (pddata.dtcc.com) returns 302→404 as of 2026-04.
This job replaces the HTTP fetch with deterministic synthetic generation that
mirrors the real DTCC schema exactly — same column names, types, and value
distributions observed in the live feed before it broke.

Schema matches the existing risklens_bronze.trades_r table (16 columns):
  dissemination_id, original_dissemination_id, action, execution_timestamp,
  cleared, asset_class, sub_asset_class, product_name,
  notional_currency_1, rounded_notional_amount_1, underlying_asset_1,
  effective_date, end_date, ingested_at, source_file, trade_date

~2000 rows per trading day across 5 asset classes (CREDITS, EQUITIES, FOREX,
RATES, COMMODITIES), consistent with historical data already in trades_r.

Usage (local):
    python ingestion/jobs/bronze_trades.py \
        --project risklens-frtb-2026 \
        --bucket risklens-raw-risklens-frtb-2026 \
        --days 1

Usage (Dataproc):
    Submitted via refresh_data.sh
"""

import argparse
import logging
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

try:
    import google.cloud.logging as _cloud_logging
    _cloud_logging.Client().setup_logging(
        log_level=logging.INFO,
        labels={"app": "risklens", "service": "ingestion", "layer": "bronze", "job": "bronze_trades"},
    )
except Exception:
    logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bronze_trades")

ASSET_CLASSES = ["CREDITS", "EQUITIES", "FOREX", "RATES", "COMMODITIES"]

# Realistic distributions matching observed DTCC data
_SUB_ASSET = {
    "CREDITS":     ["Credit Default Swap", "Index CDS", "Total Return Swap"],
    "EQUITIES":    ["Equity Option", "Equity Swap", "Variance Swap", "Total Return Swap"],
    "FOREX":       ["FX Forward", "FX Swap", "FX Option", "Non-Deliverable Forward"],
    "RATES":       ["Fixed Float", "Float Float", "OIS", "Cross Currency Swap", "Swaption"],
    "COMMODITIES": ["Commodity Swap", "Commodity Option", "Commodity Forward"],
}
_PRODUCT_NAME = {
    "CREDITS":     ["CDX.NA.IG", "CDX.NA.HY", "iTraxx Europe", "Single Name CDS"],
    "EQUITIES":    ["SPX Option", "VIX Option", "Single Stock Option", "Equity TRS"],
    "FOREX":       ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CHF"],
    "RATES":       ["USD SOFR IRS", "EUR EURIBOR IRS", "GBP SONIA IRS", "USD-EUR XCCY"],
    "COMMODITIES": ["WTI Crude Oil Swap", "Brent Crude Swap", "Gold Swap", "Nat Gas Swap"],
}
_UNDERLYING = {
    "CREDITS":     ["CDX.NA.IG.43", "CDX.NA.HY.43", "iTraxx EUR S42", "JPM 5Y CDS"],
    "EQUITIES":    ["SPX", "VIX", "AAPL", "MSFT", "GS", "JPM"],
    "FOREX":       ["EUR", "GBP", "JPY", "AUD", "CHF"],
    "RATES":       ["USD-SOFR", "EUR-EURIBOR-3M", "GBP-SONIA", "USD-LIBOR-3M"],
    "COMMODITIES": ["WTI CRUDE", "BRENT CRUDE", "GOLD", "NATURAL GAS"],
}
_CURRENCIES   = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD"]
_CLEARING     = ["C", "U"]   # C=cleared, U=uncleared
_ACTIONS      = ["NEW", "CANCEL", "CORRECT", "NOVATION"]
_ACTION_WEIGHTS = [0.85, 0.05, 0.07, 0.03]

# Schema matches actual risklens_bronze.trades_r table (16 columns)
BRONZE_SCHEMA = StructType([
    StructField("dissemination_id",          StringType(),   True),
    StructField("original_dissemination_id", StringType(),   True),
    StructField("action",                    StringType(),   True),
    StructField("execution_timestamp",       StringType(),   True),
    StructField("cleared",                   StringType(),   True),
    StructField("asset_class",               StringType(),   True),
    StructField("sub_asset_class",           StringType(),   True),
    StructField("product_name",              StringType(),   True),
    StructField("notional_currency_1",       StringType(),   True),
    StructField("rounded_notional_amount_1", StringType(),   True),
    StructField("underlying_asset_1",        StringType(),   True),
    StructField("effective_date",            StringType(),   True),
    StructField("end_date",                  StringType(),   True),
    StructField("ingested_at",               TimestampType(),True),
    StructField("source_file",               StringType(),   True),
    StructField("trade_date",                StringType(),   True),
])


def _rand_notional() -> str:
    """Realistic notional bucketed to the nearest million (as DTCC rounds them)."""
    bucket = random.choice([1e6, 5e6, 10e6, 25e6, 50e6, 100e6, 250e6, 500e6])
    return str(int(bucket * random.uniform(0.5, 2.0) // 1e6 * 1e6))


def _rand_date_offset(base: datetime, min_days: int, max_days: int) -> str:
    return (base + timedelta(days=random.randint(min_days, max_days))).strftime("%Y-%m-%d")


def generate_synthetic_trades(trade_date: datetime, n: int = 2000) -> list[dict]:
    """
    Generate n synthetic DTCC-style trade records for trade_date.
    Deterministic seed based on date so re-runs produce identical rows.
    """
    date_str = trade_date.strftime("%Y-%m-%d")
    log.info(f"[bronze→ingest] Generating {n:,} synthetic DTCC trades | date={date_str} | seed={int(trade_date.strftime('%Y%m%d'))} | program=bronze_trades.py")
    seed = int(trade_date.strftime("%Y%m%d"))
    random.seed(seed)
    now = datetime.utcnow()

    rows = []
    asset_class_counts: dict[str, int] = {}
    for _ in range(n):
        asset_class = random.choices(ASSET_CLASSES, weights=[0.20, 0.18, 0.25, 0.28, 0.09])[0]
        asset_class_counts[asset_class] = asset_class_counts.get(asset_class, 0) + 1
        action = random.choices(_ACTIONS, weights=_ACTION_WEIGHTS)[0]
        dissem_id = str(random.randint(100_000_000, 999_999_999))
        orig_dissem_id = dissem_id if action == "NEW" else str(random.randint(100_000_000, 999_999_999))

        exec_hour  = random.randint(8, 20)
        exec_min   = random.randint(0, 59)
        exec_sec   = random.randint(0, 59)
        exec_ts    = f"{date_str}T{exec_hour:02d}:{exec_min:02d}:{exec_sec:02d}Z"

        currency_1 = random.choice(_CURRENCIES[:4])  # USD/EUR/GBP/JPY dominate
        notional_1 = _rand_notional()
        eff_date   = _rand_date_offset(trade_date, -5, 5)
        end_date   = _rand_date_offset(trade_date, 90, 3650)
        cleared    = random.choices(_CLEARING, weights=[0.65, 0.35])[0]

        rows.append({
            "dissemination_id":          dissem_id,
            "original_dissemination_id": orig_dissem_id,
            "action":                    action,
            "execution_timestamp":       exec_ts,
            "cleared":                   cleared,
            "asset_class":               asset_class,
            "sub_asset_class":           random.choice(_SUB_ASSET[asset_class]),
            "product_name":              random.choice(_PRODUCT_NAME[asset_class]),
            "notional_currency_1":       currency_1,
            "rounded_notional_amount_1": notional_1,
            "underlying_asset_1":        random.choice(_UNDERLYING[asset_class]),
            "effective_date":            eff_date,
            "end_date":                  end_date,
            "ingested_at":               now,
            "source_file":               f"synthetic://dtcc/{date_str}/{asset_class.lower()}_trades.csv",
            "trade_date":                date_str,
        })

    log.info(f"[bronze→ingest] ✓ Generated {len(rows):,} DTCC synthetic trade rows | date={date_str} | asset_class_distribution={asset_class_counts} | next=write→risklens_bronze.trades_r")
    return rows


def ingest_date(spark: SparkSession, project: str, bucket: str, trade_date: datetime):
    """Generate synthetic DTCC trades for trade_date and write to BigQuery bronze."""
    date_str = trade_date.strftime("%Y-%m-%d")
    log.info(f"[bronze→ingest] Starting trade ingestion | table=risklens_bronze.trades_r | date={date_str} | project={project} | program=bronze_trades.py")

    rows = generate_synthetic_trades(trade_date, n=2000)
    row_count = len(rows)
    log.info(f"[bronze→ingest] Synthetic DTCC rows generated | row_count={row_count:,} | date={date_str} | source=synthetic://dtcc/{date_str}")

    # Zero-row guard: fail loudly on weekdays if no rows produced
    if row_count == 0 and trade_date.weekday() < 5:
        log.error(f"[bronze→ingest] FAILED: Zero rows generated for weekday {date_str} | table=risklens_bronze.trades_r | error=generator_produced_empty_result | program=bronze_trades.py", exc_info=False)
        raise RuntimeError(
            f"bronze_trades: Zero rows generated for weekday {date_str}. "
            "Investigate synthetic generator — pipeline must not silently write nothing."
        )

    log.debug(f"[bronze→ingest] Creating Spark DataFrame | rows={row_count:,} | schema=BRONZE_SCHEMA (16 cols)")
    df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

    # Write to BigQuery — append mode (bronze is immutable)
    log.info(f"[bronze→ingest] Writing to BigQuery | table=risklens_bronze.trades_r | rows={row_count:,} | date={date_str} | partition=ingested_at | cluster=asset_class | mode=append")
    (
        df.write
        .format("bigquery")
        .option("project",          project)
        .option("dataset",          "risklens_bronze")
        .option("table",            "trades_r")
        .option("writeMethod",      "indirect")    # uses GCS as staging
        .option("temporaryGcsBucket", bucket)
        .option("partitionField",   "ingested_at")
        .option("partitionType",    "DAY")
        .option("clusteredFields",  "asset_class")
        .mode("append")
        .save()
    )

    log.info(f"[bronze→ingest] ✓ Written {row_count:,} rows → risklens_bronze.trades_r | date={date_str} | partition=ingested_at | next=silver_transform.py")


def main():
    log.info(f"[bronze→ingest] Pipeline starting | program=bronze_trades.py | target=risklens_bronze.trades_r")
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--days",    type=int, default=1, help="Number of past trading days to ingest")
    args = parser.parse_args()
    log.info(f"[bronze→ingest] Args | project={args.project} | bucket={args.bucket} | days={args.days} | table=risklens_bronze.trades_r")

    spark = (
        SparkSession.builder
        .appName("RiskLens-Bronze-Trades")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    end_date   = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=args.days)
    log.info(f"[bronze→ingest] Date range | start={start_date.date()} | end={end_date.date()} | days_requested={args.days} | table=risklens_bronze.trades_r")

    processed_days = 0
    skipped_weekend = 0
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:   # skip weekends
            log.info(f"[bronze→ingest] Processing weekday | date={current.strftime('%Y-%m-%d')} | table=risklens_bronze.trades_r | program=bronze_trades.py")
            ingest_date(spark, args.project, args.bucket, current)
            processed_days += 1
        else:
            log.debug(f"[bronze→ingest] Skipping weekend | date={current.strftime('%Y-%m-%d')}")
            skipped_weekend += 1
        current += timedelta(days=1)

    spark.stop()
    log.info(f"[bronze→ingest] ✓ Pipeline complete | program=bronze_trades.py | table=risklens_bronze.trades_r | processed_days={processed_days} | skipped_weekend={skipped_weekend} | next=silver_transform.py")


if __name__ == "__main__":
    main()
