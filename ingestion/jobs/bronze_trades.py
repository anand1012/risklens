"""
RiskLens — Bronze Layer: DTCC SDR Trade Ingestion
Fetches OTC derivatives trade data from DTCC Swap Data Repository (public).
Lands raw data as-is into BigQuery bronze layer. No transforms.

DTCC SDR public data: https://pddata.dtcc.com/gtr/cftc/
Files are published daily as zip/CSV for each asset class:
  - CREDITS  (credit default swaps)
  - EQUITIES (equity derivatives)
  - FOREX    (FX derivatives)
  - RATES    (interest rate swaps)
  - COMMODITIES

Usage (local):
    python ingestion/jobs/bronze_trades.py \
        --project risklens-frtb-2026 \
        --bucket risklens-raw-risklens-frtb-2026 \
        --days 1

Usage (Dataproc):
    Submitted via refresh_data.sh
"""

import argparse
import io
import logging
import zipfile
from datetime import datetime, timedelta

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

# DTCC SDR public base URL
DTCC_BASE_URL = "https://pddata.dtcc.com/gtr/cftc"

ASSET_CLASSES = ["CREDITS", "EQUITIES", "FOREX", "RATES", "COMMODITIES"]

# Raw bronze schema — accept everything as string (immutable landing)
BRONZE_SCHEMA = StructType([
    StructField("dissemination_id",     StringType(), True),
    StructField("original_dissemination_id", StringType(), True),
    StructField("action",               StringType(), True),
    StructField("execution_timestamp",  StringType(), True),
    StructField("cleared",              StringType(), True),
    StructField("indication_of_collat", StringType(), True),
    StructField("indication_of_end_usr", StringType(), True),
    StructField("indication_of_othr_prc", StringType(), True),
    StructField("day_count_convention", StringType(), True),
    StructField("settlement_currency",  StringType(), True),
    StructField("asset_class",          StringType(), True),
    StructField("sub_asset_class",      StringType(), True),
    StructField("product_name",         StringType(), True),
    StructField("contract_type",        StringType(), True),
    StructField("price_forming_continuation_data", StringType(), True),
    StructField("underlying_asset_1",   StringType(), True),
    StructField("underlying_asset_2",   StringType(), True),
    StructField("price_notation_type",  StringType(), True),
    StructField("price_notation",       StringType(), True),
    StructField("additional_price_notation_type", StringType(), True),
    StructField("additional_price_notation", StringType(), True),
    StructField("notional_currency_1",  StringType(), True),
    StructField("notional_currency_2",  StringType(), True),
    StructField("rounded_notional_amount_1", StringType(), True),
    StructField("rounded_notional_amount_2", StringType(), True),
    StructField("payment_frequency_1",  StringType(), True),
    StructField("payment_frequency_2",  StringType(), True),
    StructField("reset_frequency_1",    StringType(), True),
    StructField("reset_frequency_2",    StringType(), True),
    StructField("embeds_option",        StringType(), True),
    StructField("option_strike_price",  StringType(), True),
    StructField("option_type",          StringType(), True),
    StructField("option_family",        StringType(), True),
    StructField("option_currency",      StringType(), True),
    StructField("option_premium",       StringType(), True),
    StructField("option_lock_period",   StringType(), True),
    StructField("option_expiration_date", StringType(), True),
    StructField("option_premium_currency", StringType(), True),
    StructField("effective_date",       StringType(), True),
    StructField("end_date",             StringType(), True),
    StructField("day_count_convention_2", StringType(), True),
    StructField("settlement_currency_2", StringType(), True),
    StructField("exchange_rate",        StringType(), True),
    StructField("exchange_rate_basis",  StringType(), True),
    StructField("ingested_at",          TimestampType(), True),
    StructField("source_file",          StringType(), True),
    StructField("trade_date",           StringType(), True),
])


def build_dtcc_url(asset_class: str, trade_date: datetime) -> str:
    """Build DTCC SDR download URL for a given asset class and date."""
    date_str = trade_date.strftime("%Y_%m_%d")
    return f"{DTCC_BASE_URL}/{asset_class.lower()}/CFTC_{asset_class}_{date_str}.zip"


def fetch_dtcc_file(url: str) -> bytes | None:
    """Download DTCC zip file. Returns None if not available (weekends/holidays)."""
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            return resp.content
        log.warning(f"DTCC file not available: {url} (HTTP {resp.status_code})")
        return None
    except requests.RequestException as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return None


def extract_csv_from_zip(zip_bytes: bytes) -> str | None:
    """Extract the CSV content from DTCC zip file."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            if not csv_files:
                return None
            return zf.read(csv_files[0]).decode("utf-8", errors="replace")
    except zipfile.BadZipFile as e:
        log.warning(f"Bad zip file: {e}")
        return None


def upload_to_gcs(content: str, bucket: str, gcs_path: str) -> str:
    """Upload CSV content to GCS raw zone."""
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(bucket).blob(gcs_path)
    blob.upload_from_string(content, content_type="text/csv")
    return f"gs://{bucket}/{gcs_path}"


def ingest_date(spark: SparkSession, project: str, bucket: str, trade_date: datetime):
    """Fetch all asset classes for a given trade date and write to BigQuery bronze."""
    date_str = trade_date.strftime("%Y-%m-%d")
    log.info(f"Ingesting DTCC trades for {date_str}")

    all_gcs_paths = []

    for asset_class in ASSET_CLASSES:
        url = build_dtcc_url(asset_class, trade_date)
        log.info(f"  Fetching {asset_class}: {url}")

        zip_bytes = fetch_dtcc_file(url)
        if not zip_bytes:
            log.info(f"  No data for {asset_class} on {date_str} — skipping.")
            continue

        csv_content = extract_csv_from_zip(zip_bytes)
        if not csv_content:
            log.warning(f"  Could not extract CSV from {asset_class} zip.")
            continue

        gcs_path = f"dtcc/{date_str}/{asset_class.lower()}_trades.csv"
        gcs_uri = upload_to_gcs(csv_content, bucket, gcs_path)
        all_gcs_paths.append((gcs_uri, asset_class, date_str))
        log.info(f"  Uploaded: {gcs_uri}")

    if not all_gcs_paths:
        log.info(f"No DTCC data available for {date_str} (weekend/holiday).")
        return

    # Read all CSVs from GCS into Spark
    gcs_uris = [p[0] for p in all_gcs_paths]
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # bronze: all strings
        .option("mode", "PERMISSIVE")     # bronze: accept all records
        .csv(gcs_uris)
    )

    # Add audit columns
    df = df.withColumn("ingested_at", F.current_timestamp()) \
           .withColumn("source_file",  F.input_file_name()) \
           .withColumn("trade_date",   F.lit(date_str))

    # Normalize column names (lowercase, replace spaces with underscores)
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip().lower().replace(" ", "_").replace("-", "_"))

    row_count = df.count()
    log.info(f"  Writing {row_count:,} rows to BigQuery bronze for {date_str}")

    # Write to BigQuery — append mode (bronze is immutable)
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

    log.info(f"  Done: {row_count:,} rows written to risklens_bronze.trades_r")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--days",    type=int, default=1, help="Number of past trading days to ingest")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("RiskLens-Bronze-Trades")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    end_date   = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=args.days)

    current = start_date
    while current <= end_date:
        if current.weekday() < 5:   # skip weekends
            ingest_date(spark, args.project, args.bucket, current)
        current += timedelta(days=1)

    spark.stop()
    log.info("Bronze trades ingestion complete.")


if __name__ == "__main__":
    main()
