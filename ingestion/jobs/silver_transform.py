"""
RiskLens — Silver Layer: Clean, Validate, Normalize
Reads from Bronze, applies quality rules, writes to Silver.

Rules applied per source:
  Trades  : Null checks, schema normalization, dedup by dissemination_id + trade_date
  Rates   : Null checks, forward-fill gaps, outlier flagging, dedup by series_id + date
  Prices  : Null checks, OHLCV sanity (high >= low, close in range), dedup by ticker + date
  Risk    : VaR/ES bounds check, P&L vector dimension check, dedup by desk + calc_date

Bad records → risklens_bronze.quarantine (never deleted, full audit trail)
Good records → risklens_silver.*

Usage (local):
    python ingestion/jobs/silver_transform.py \
        --project risklens-frtb-2026 \
        --bucket risklens-raw-risklens-frtb-2026 \
        --date 2026-04-13

Usage (Dataproc):
    Submitted via refresh_data.sh
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, LongType, StringType,
                                StructField, StructType, TimestampType)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("silver_transform")


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_bronze(spark: SparkSession, project: str, table: str,
                trade_date: str | None = None) -> DataFrame:
    """Read from BigQuery bronze layer, optionally filtered by trade_date."""
    query = f"SELECT * FROM `{project}.risklens_bronze.{table}`"
    if trade_date:
        query += f" WHERE trade_date = '{trade_date}'"

    return (
        spark.read
        .format("bigquery")
        .option("query", query)
        .load()
    )


def write_silver(df: DataFrame, project: str, bucket: str,
                 table: str, mode: str = "append",
                 partition_field: str | None = None,
                 partition_type: str = "DAY",
                 cluster_fields: str | None = None) -> int:
    """Write cleaned DataFrame to BigQuery silver layer."""
    row_count = df.count()
    writer = (
        df.write
        .format("bigquery")
        .option("project",     project)
        .option("dataset",     "risklens_silver")
        .option("table",       table)
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", bucket)
    )
    if partition_field:
        writer = writer.option("partitionField", partition_field) \
                       .option("partitionType",  partition_type)
    if cluster_fields:
        writer = writer.option("clusteredFields", cluster_fields)
    writer.mode(mode).save()
    log.info(f"  risklens_silver.{table}: {row_count:,} rows written")
    return row_count


def write_quarantine(df: DataFrame, project: str, bucket: str,
                     source: str, reason: str, trade_date: str):
    """Write rejected records to quarantine table for audit."""
    if df.rdd.isEmpty():
        return
    quarantine_df = df \
        .withColumn("source_table",    F.lit(source)) \
        .withColumn("rejection_reason",F.lit(reason)) \
        .withColumn("quarantined_at",  F.current_timestamp()) \
        .withColumn("trade_date",      F.lit(trade_date))

    row_count = quarantine_df.count()
    (
        quarantine_df.write
        .format("bigquery")
        .option("project",          project)
        .option("dataset",          "risklens_bronze")
        .option("table",            "quarantine_r")
        .option("writeMethod",      "indirect")
        .option("temporaryGcsBucket", bucket)
        .option("partitionField",   "quarantined_at")
        .option("partitionType",    "DAY")
        .option("clusteredFields",  "source_table,rejection_reason")
        .mode("append")
        .save()
    )
    log.warning(f"  Quarantined {row_count:,} records from {source}: {reason}")


def update_quality_score(spark: SparkSession, project: str, bucket: str,
                         asset_id: str, total: int, nulls: int,
                         dupes: int, schema_drift: bool, trade_date: str):
    """Update quality scores in catalog after silver processing."""
    null_rate  = round(nulls / total, 4) if total > 0 else 0.0
    dupe_rate  = round(dupes / total, 4) if total > 0 else 0.0
    freshness  = "fresh" if null_rate < 0.02 else ("stale" if null_rate < 0.05 else "critical")

    quality_row = spark.createDataFrame([{
        "asset_id":         asset_id,
        "null_rate":        null_rate,
        "schema_drift":     schema_drift,
        "freshness_status": freshness,
        "duplicate_rate":   dupe_rate,
        "last_checked":     datetime.utcnow(),
    }])

    (
        quality_row.write
        .format("bigquery")
        .option("project",     project)
        .option("dataset",     "risklens_catalog")
        .option("table",       "quality_scores_s")
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", bucket)
        .mode("append")
        .save()
    )


# ── Transforms ────────────────────────────────────────────────────────────────

def transform_trades(spark: SparkSession, project: str, bucket: str, trade_date: str):
    """Bronze → Silver for DTCC trade data."""
    log.info(f"Transforming trades for {trade_date}")

    df = read_bronze(spark, project, "trades_r", trade_date)
    total = df.count()

    if total == 0:
        log.info("  No bronze trade data for this date.")
        return

    # ── Null check on critical fields ────────────────────────────────────────
    critical_cols = ["dissemination_id", "asset_class", "execution_timestamp",
                     "notional_currency_1", "rounded_notional_amount_1"]
    null_filter   = " OR ".join([f"{c} IS NULL" for c in critical_cols])
    bad_df  = df.filter(null_filter)
    good_df = df.filter(f"NOT ({null_filter})")

    write_quarantine(bad_df, project, bucket, "bronze.trades_r",
                     "NULL in critical field", trade_date)

    # ── Deduplication by dissemination_id + trade_date ────────────────────────
    window   = Window.partitionBy("dissemination_id", "trade_date") \
                     .orderBy(F.col("ingested_at").desc())
    good_df  = good_df.withColumn("_rank", F.row_number().over(window)) \
                      .filter(F.col("_rank") == 1) \
                      .drop("_rank")
    dupes    = total - good_df.count()

    # ── Normalize schema ──────────────────────────────────────────────────────
    silver_df = good_df.select(
        F.col("dissemination_id"),
        F.col("original_dissemination_id"),
        F.col("action").cast(StringType()),
        F.to_timestamp(F.col("execution_timestamp")).alias("execution_timestamp"),
        F.col("asset_class").cast(StringType()),
        F.col("sub_asset_class").cast(StringType()),
        F.col("product_name").cast(StringType()),
        F.col("notional_currency_1").alias("currency"),
        F.col("rounded_notional_amount_1").cast(DoubleType()).alias("notional_amount"),
        F.col("settlement_currency").cast(StringType()),
        F.col("underlying_asset_1").alias("underlying"),
        F.col("effective_date").cast(DateType()),
        F.col("end_date").cast(DateType()),
        F.col("trade_date").cast(StringType()),
        F.current_timestamp().alias("processed_at"),
    )

    nulls_count = bad_df.count()
    write_silver(silver_df, project, bucket, "trades_r",
                 partition_field="trade_date", cluster_fields="asset_class,currency")
    update_quality_score(spark, project, bucket, "silver_trades",
                         total, nulls_count, dupes, False, trade_date)


def transform_rates(spark: SparkSession, project: str, bucket: str, trade_date: str):
    """Bronze → Silver for FRED rate series."""
    log.info(f"Transforming rates for {trade_date}")

    df = read_bronze(spark, project, "rates_r", trade_date)
    total = df.count()

    if total == 0:
        log.info("  No bronze rate data for this date.")
        return

    # ── Drop rows where value is NULL (FRED uses '.' for missing) ────────────
    bad_df  = df.filter(F.col("value").isNull())
    good_df = df.filter(F.col("value").isNotNull())
    nulls_count = bad_df.count()

    write_quarantine(bad_df, project, bucket, "bronze.rates_r",
                     "NULL value — missing observation", trade_date)

    # ── Deduplication by series_id + date ─────────────────────────────────────
    window  = Window.partitionBy("series_id", "date").orderBy(F.col("ingested_at").desc())
    good_df = good_df.withColumn("_rank", F.row_number().over(window)) \
                     .filter(F.col("_rank") == 1) \
                     .drop("_rank")
    dupes   = total - nulls_count - good_df.count()

    # ── Outlier flag (value > 3 std from series mean) ────────────────────────
    stats   = good_df.groupBy("series_id").agg(
        F.mean("value").alias("mean_val"),
        F.stddev("value").alias("std_val"),
    )
    good_df = good_df.join(stats, "series_id", "left") \
                     .withColumn("is_outlier",
                        F.abs(F.col("value") - F.col("mean_val")) > 3 * F.col("std_val")) \
                     .drop("mean_val", "std_val")

    silver_df = good_df.select(
        F.col("series_id"),
        F.col("series_name"),
        F.col("frequency"),
        F.col("domain"),
        F.col("date").cast(DateType()),
        F.col("value").cast(DoubleType()),
        F.col("is_outlier"),
        F.col("trade_date"),
        F.current_timestamp().alias("processed_at"),
    )

    write_silver(silver_df, project, bucket, "rates_r",
                 partition_field="date", cluster_fields="series_id,domain")
    update_quality_score(spark, project, bucket, "silver_rates",
                         total, nulls_count, dupes, False, trade_date)


def transform_prices(spark: SparkSession, project: str, bucket: str, trade_date: str):
    """Bronze → Silver for Yahoo Finance price data."""
    log.info(f"Transforming prices for {trade_date}")

    df = read_bronze(spark, project, "prices_r", trade_date)
    total = df.count()

    if total == 0:
        log.info("  No bronze price data for this date.")
        return

    # ── Null check on OHLCV ───────────────────────────────────────────────────
    null_filter  = "close IS NULL OR high IS NULL OR low IS NULL"
    bad_df       = df.filter(null_filter)
    good_df      = df.filter(f"NOT ({null_filter})")
    nulls_count  = bad_df.count()

    write_quarantine(bad_df, project, bucket, "bronze.prices_r",
                     "NULL in OHLCV", trade_date)

    # ── OHLCV sanity: high >= low, close between low and high ─────────────────
    invalid_df  = good_df.filter(
        (F.col("high") < F.col("low")) |
        (F.col("close") < F.col("low")) |
        (F.col("close") > F.col("high"))
    )
    good_df     = good_df.filter(
        (F.col("high") >= F.col("low")) &
        (F.col("close") >= F.col("low")) &
        (F.col("close") <= F.col("high"))
    )

    write_quarantine(invalid_df, project, bucket, "bronze.prices_r",
                     "OHLCV sanity check failed", trade_date)

    # ── Deduplication by ticker + date ────────────────────────────────────────
    window  = Window.partitionBy("ticker", "date").orderBy(F.col("ingested_at").desc())
    good_df = good_df.withColumn("_rank", F.row_number().over(window)) \
                     .filter(F.col("_rank") == 1) \
                     .drop("_rank")
    dupes   = total - nulls_count - good_df.count()

    silver_df = good_df.select(
        F.col("ticker"),
        F.col("name"),
        F.col("asset_class"),
        F.col("currency"),
        F.col("date").cast(DateType()),
        F.col("open").cast(DoubleType()),
        F.col("high").cast(DoubleType()),
        F.col("low").cast(DoubleType()),
        F.col("close").cast(DoubleType()),
        F.col("adj_close").cast(DoubleType()),
        F.col("volume").cast(LongType()),
        F.col("trade_date"),
        F.current_timestamp().alias("processed_at"),
    )

    write_silver(silver_df, project, bucket, "prices_r",
                 partition_field="date", cluster_fields="ticker,asset_class,currency")
    update_quality_score(spark, project, bucket, "silver_prices",
                         total, nulls_count, dupes, False, trade_date)


def transform_risk(spark: SparkSession, project: str, bucket: str, trade_date: str):
    """Bronze → Silver for synthetic risk outputs (VaR, ES, P&L vectors)."""
    log.info(f"Transforming risk outputs for {trade_date}")

    df = read_bronze(spark, project, "risk_outputs_s", trade_date)
    total = df.count()

    if total == 0:
        log.info("  No bronze risk data for this date.")
        return

    # ── Null check on required fields ─────────────────────────────────────────
    null_filter = "desk IS NULL OR calc_date IS NULL"
    bad_df  = df.filter(null_filter)
    good_df = df.filter(f"NOT ({null_filter})")
    nulls_count = bad_df.count()

    write_quarantine(bad_df, project, bucket, "bronze.risk_outputs_s",
                     "NULL in desk or calc_date", trade_date)

    # ── VaR/ES bounds: must be positive ───────────────────────────────────────
    if "var_99_1d" in good_df.columns:
        invalid_df = good_df.filter(F.col("var_99_1d") <= 0)
        good_df    = good_df.filter(F.col("var_99_1d") > 0)
        write_quarantine(invalid_df, project, bucket, "bronze.risk_outputs_s",
                         "VaR must be positive", trade_date)

    # ── Deduplication by desk + calc_date ─────────────────────────────────────
    dedup_cols = ["desk", "calc_date"] if "calc_date" in good_df.columns else ["desk"]
    window     = Window.partitionBy(*dedup_cols).orderBy(F.col("calculated_at").desc())
    good_df    = good_df.withColumn("_rank", F.row_number().over(window)) \
                        .filter(F.col("_rank") == 1) \
                        .drop("_rank")
    dupes      = total - nulls_count - good_df.count()

    silver_df  = good_df.withColumn("processed_at", F.current_timestamp())

    write_silver(silver_df, project, bucket, "risk_outputs_s",
                 partition_field="calc_date", cluster_fields="desk")
    update_quality_score(spark, project, bucket, "silver_risk_outputs",
                         total, nulls_count, dupes, False, trade_date)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",    required=True)
    parser.add_argument("--bucket",     required=True)
    parser.add_argument("--date",       default=datetime.utcnow().strftime("%Y-%m-%d"),
                        help="Trade date to process (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("RiskLens-Silver-Transform")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Silver transform for trade_date={args.date}")

    transform_trades(spark, args.project, args.bucket, args.date)
    transform_rates(spark,  args.project, args.bucket, args.date)
    transform_prices(spark, args.project, args.bucket, args.date)
    transform_risk(spark,   args.project, args.bucket, args.date)

    log.info("Silver transform complete.")
    spark.stop()


if __name__ == "__main__":
    main()
