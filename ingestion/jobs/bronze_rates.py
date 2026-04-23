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

try:
    import google.cloud.logging as _cloud_logging
    _cloud_logging.Client().setup_logging(
        log_level=logging.INFO,
        labels={"app": "risklens", "service": "ingestion", "layer": "bronze", "job": "bronze_rates"},
    )
except Exception:
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
    log.info(f"[bronze→ingest] Fetching FRED API key | secret=risklens-fred-api-key | project={project} | program=bronze_rates.py")
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{project}/secrets/risklens-fred-api-key/versions/latest"
    try:
        resp   = client.access_secret_version(request={"name": name})
        key = resp.payload.data.decode("utf-8").strip()
        log.info(f"[bronze→ingest] FRED API key retrieved | key_length={len(key)} | secret=risklens-fred-api-key")
        return key
    except Exception as e:
        log.error(f"[bronze→ingest] FAILED: Could not retrieve FRED API key | secret=risklens-fred-api-key | project={project} | error={e}", exc_info=True)
        raise


def fetch_series(fred: Fred, series_id: str, start_date: datetime, end_date: datetime) -> list[dict]:
    """Fetch a single FRED series and return as list of dicts."""
    meta = FRED_SERIES[series_id]
    log.info(f"[bronze→ingest] Fetching FRED series | series_id={series_id} | name={meta['name']} | domain={meta['domain']} | frequency={meta['frequency']} | start={start_date.date()} | end={end_date.date()} | program=bronze_rates.py")
    try:
        data   = fred.get_series(
            series_id,
            observation_start=start_date.strftime("%Y-%m-%d"),
            observation_end=end_date.strftime("%Y-%m-%d"),
        )
        rows = []
        null_values = 0
        for date, value in data.items():
            is_null = str(value) == "."
            if is_null:
                null_values += 1
            rows.append({
                "series_id":   series_id,
                "series_name": meta["name"],
                "frequency":   meta["frequency"],
                "domain":      meta["domain"],
                "date":        date.date(),
                "value":       None if is_null else float(value),
                "ingested_at": datetime.utcnow(),
                "trade_date":  end_date.strftime("%Y-%m-%d"),
            })
        log.info(f"[bronze→ingest] ✓ FRED series fetched | series_id={series_id} | observations={len(rows)} | null_missing={null_values} | date_range={start_date.date()}→{end_date.date()}")
        if null_values > 0:
            log.debug(f"[bronze→ingest] {series_id}: {null_values} missing observations (FRED '.' sentinel values in date range)")
        return rows
    except Exception as e:
        log.error(f"[bronze→ingest] FAILED: FRED API call failed | series_id={series_id} | name={meta['name']} | error={e}", exc_info=True)
        return []


def main():
    log.info(f"[bronze→ingest] Pipeline starting | program=bronze_rates.py | target=risklens_bronze.rates_r | series_count={len(FRED_SERIES)}")
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--days",    type=int, default=1)
    args = parser.parse_args()
    log.info(f"[bronze→ingest] Args | project={args.project} | bucket={args.bucket} | days={args.days} | table=risklens_bronze.rates_r")

    spark = (
        SparkSession.builder
        .appName("RiskLens-Bronze-Rates")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=args.days + 5)  # +5 buffer for non-trading days

    log.info(f"[bronze→ingest] Fetching {len(FRED_SERIES)} FRED series | start={start_date.date()} | end={end_date.date()} | series={list(FRED_SERIES.keys())} | program=bronze_rates.py")

    fred_key = get_fred_api_key(args.project)
    fred     = Fred(api_key=fred_key)

    all_rows = []
    series_results: dict[str, int] = {}
    for series_id in FRED_SERIES:
        rows = fetch_series(fred, series_id, start_date, end_date)
        series_results[series_id] = len(rows)
        all_rows.extend(rows)

    log.info(f"[bronze→ingest] FRED fetch complete | series_fetched={len(series_results)} | total_rows={len(all_rows):,} | per_series={series_results}")

    if not all_rows:
        log.warning(f"[bronze→ingest] No FRED data fetched for any series | date_range={start_date.date()}→{end_date.date()} | skipping risklens_bronze.rates_r write")
        spark.stop()
        return

    df = spark.createDataFrame(all_rows, schema=BRONZE_SCHEMA)
    row_count = df.count()
    log.info(f"[bronze→ingest] Writing to BigQuery | table=risklens_bronze.rates_r | rows={row_count:,} | partition=date | cluster=series_id,domain | mode=append")

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

    log.info(f"[bronze→ingest] ✓ Written {row_count:,} rows → risklens_bronze.rates_r | series={len(FRED_SERIES)} | date_range={start_date.date()}→{end_date.date()} | next=silver_transform.py")
    spark.stop()


if __name__ == "__main__":
    main()
