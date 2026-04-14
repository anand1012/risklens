"""
RiskLens — Bronze Layer: FRED API Rate Ingestion
Fetches macro/market rate series from Federal Reserve Economic Data (FRED).
Lands raw data as-is into BigQuery bronze layer. No transforms.

FRED series ingested:
  DFF       — Fed Funds Rate (daily)
  DGS10     — 10-Year Treasury Yield (daily)
  DGS2      — 2-Year Treasury Yield (daily)
  DGS30     — 30-Year Treasury Yield (daily)
  BAMLH0A0HYM2 — ICE BofA HY Credit Spread (daily)
  DEXUSEU   — USD/EUR Exchange Rate (daily)
  DEXUSUK   — USD/GBP Exchange Rate (daily)
  DEXJPUS   — JPY/USD Exchange Rate (daily)
  CPIAUCSL  — CPI (monthly)
  VIXCLS    — CBOE Volatility Index (daily)

API key stored in GCP Secret Manager: risklens-fred-api-key

Usage (local):
    python ingestion/jobs/bronze_rates.py \
        --project risklens-frtb-2026 \
        --bucket risklens-raw-risklens-frtb-2026 \
        --days 30

Usage (Dataproc):
    Submitted via refresh_data.sh
"""

import argparse
import logging
from datetime import datetime, timedelta

from fredapi import Fred
from google.cloud import secretmanager
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, StringType, StructField,
                                StructType, TimestampType)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bronze_rates")

FRED_SERIES = {
    # GIRR — USD Risk-Free Rates (post-LIBOR transition, SOFR-based)
    "SOFR":         {"name": "SOFR (Secured Overnight Financing Rate)", "frequency": "daily",   "domain": "rates"},
    "SOFR30DAYAVG": {"name": "SOFR 30-Day Average",                    "frequency": "daily",   "domain": "rates"},
    "SOFR90DAYAVG": {"name": "SOFR 90-Day Average",                    "frequency": "daily",   "domain": "rates"},
    # GIRR — USD Treasury Curve (benchmark risk factors for FRTB IMA GIRR)
    "DFF":          {"name": "Fed Funds Rate (pre-SOFR reference)",     "frequency": "daily",   "domain": "rates"},
    "DGS2":         {"name": "2Y Treasury Yield",                       "frequency": "daily",   "domain": "rates"},
    "DGS10":        {"name": "10Y Treasury Yield",                      "frequency": "daily",   "domain": "rates"},
    "DGS30":        {"name": "30Y Treasury Yield",                      "frequency": "daily",   "domain": "rates"},
    # CSR_NS — Credit Spread Risk (Non-Securitisation)
    "BAMLH0A0HYM2": {"name": "HY Credit Spread ICE BofA (CSR_NS)",     "frequency": "daily",   "domain": "credit"},
    "BAMLC0A0CM":   {"name": "IG Credit Spread ICE BofA (CSR_NS)",     "frequency": "daily",   "domain": "credit"},
    # FX — spot exchange rates
    "DEXUSEU":      {"name": "USD/EUR Exchange Rate",                   "frequency": "daily",   "domain": "fx"},
    "DEXUSUK":      {"name": "USD/GBP Exchange Rate",                   "frequency": "daily",   "domain": "fx"},
    "DEXJPUS":      {"name": "JPY/USD Exchange Rate",                   "frequency": "daily",   "domain": "fx"},
    # COMM — Commodity prices
    "DCOILWTICO":   {"name": "WTI Crude Oil Spot Price",                "frequency": "daily",   "domain": "commodity"},
    # EQ — Equity volatility proxy
    "VIXCLS":       {"name": "CBOE VIX (EQ volatility)",               "frequency": "daily",   "domain": "volatility"},
    # MACRO — Inflation risk factor
    "CPIAUCSL":     {"name": "CPI All Urban Consumers (Inflation)",     "frequency": "monthly", "domain": "macro"},
}

BRONZE_SCHEMA = StructType([
    StructField("series_id",    StringType(),   True),
    StructField("series_name",  StringType(),   True),
    StructField("frequency",    StringType(),   True),
    StructField("domain",       StringType(),   True),
    StructField("date",         DateType(),     True),
    StructField("value",        DoubleType(),   True),
    StructField("ingested_at",  TimestampType(),True),
    StructField("trade_date",   StringType(),   True),
])


def get_fred_api_key(project: str) -> str:
    """Fetch FRED API key from GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project}/secrets/risklens-fred-api-key/versions/latest"
    resp   = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8").strip()


def fetch_series(fred: Fred, series_id: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """Fetch a single FRED series and return as list of dicts."""
    try:
        meta   = FRED_SERIES[series_id]
        data   = fred.get_series(
            series_id,
            observation_start=start_date.strftime("%Y-%m-%d"),
            observation_end=end_date.strftime("%Y-%m-%d"),
        )
        rows = []
        for date, value in data.items():
            rows.append({
                "series_id":   series_id,
                "series_name": meta["name"],
                "frequency":   meta["frequency"],
                "domain":      meta["domain"],
                "date":        date.date(),
                "value":       None if str(value) == "." else float(value),
                "ingested_at": datetime.utcnow(),
                "trade_date":  end_date.strftime("%Y-%m-%d"),
            })
        log.info(f"  {series_id}: {len(rows)} observations")
        return rows
    except Exception as e:
        log.warning(f"  Failed to fetch {series_id}: {e}")
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--days",    type=int, default=1)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("RiskLens-Bronze-Rates")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=args.days + 5)  # +5 buffer for non-trading days

    log.info(f"Fetching FRED series from {start_date.date()} to {end_date.date()}")

    fred_key = get_fred_api_key(args.project)
    fred     = Fred(api_key=fred_key)

    all_rows = []
    for series_id in FRED_SERIES:
        rows = fetch_series(fred, series_id, start_date, end_date)
        all_rows.extend(rows)

    if not all_rows:
        log.warning("No FRED data fetched.")
        spark.stop()
        return

    df = spark.createDataFrame(all_rows, schema=BRONZE_SCHEMA)
    row_count = df.count()
    log.info(f"Writing {row_count:,} rows to BigQuery risklens_bronze.rates_r")

    (
        df.write
        .format("bigquery")
        .option("project",          args.project)
        .option("dataset",          "risklens_bronze")
        .option("table",            "rates_r")
        .option("writeMethod",      "indirect")
        .option("temporaryGcsBucket", args.bucket)
        .option("partitionField",   "date")
        .option("partitionType",    "DAY")
        .option("clusteredFields",  "series_id,domain")
        .mode("append")
        .save()
    )

    log.info(f"Done: {row_count:,} rows written to risklens_bronze.rates_r")
    spark.stop()


if __name__ == "__main__":
    main()
