"""
RiskLens — Silver Layer (Job 1 of 2): Clean, Validate, Normalize
Reads from Bronze, applies quality rules, writes to Silver.
Run this BEFORE silver_enrich.py.

Rules applied per source:
  Trades  : Null checks, schema normalization, dedup by dissemination_id + trade_date
  Rates   : Null checks, forward-fill gaps, outlier flagging, dedup by series_id + date
  Prices  : Null checks, OHLCV sanity (high >= low, close in range), dedup by ticker + date
  Risk    : VaR/ES bounds check, P&L vector dimension check, dedup by desk + calc_date

Bad records → risklens_bronze.quarantine_r (never deleted, full audit trail)
Good records → risklens_silver.{trades, rates, prices, risk_outputs}

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

try:
    import google.cloud.logging as _cloud_logging
    _cloud_logging.Client().setup_logging(
        log_level=logging.INFO,
        labels={"app": "risklens", "service": "ingestion", "layer": "silver", "job": "silver_transform"},
    )
except Exception:
    logging.basicConfig(level=logging.INFO)
log = logging.getLogger("silver_transform")


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_bronze(spark: SparkSession, project: str, table: str,
                trade_date: str | None = None) -> DataFrame:
    """Read from BigQuery bronze layer, optionally filtered by trade_date."""
    log.info(f"[bronze→silver] Reading bronze table | table=risklens_bronze.{table} | date_filter={trade_date} | program=silver_transform.py")
    df = (
        spark.read
        .format("bigquery")
        .option("project", project)
        .option("dataset", "risklens_bronze")
        .option("table", table)
        .load()
    )
    if trade_date:
        df = df.filter(F.col("trade_date") == trade_date)
    log.info(f"[bronze→silver] Bronze read complete | table=risklens_bronze.{table} | date={trade_date}")
    return df


def write_silver(df: DataFrame, project: str, bucket: str,
                 table: str, mode: str = "append",
                 partition_field: str | None = None,
                 partition_type: str = "DAY",
                 cluster_fields: str | None = None) -> int:
    """Write cleaned DataFrame to BigQuery silver layer."""
    log.info(f"[bronze→silver] Writing to silver | table=risklens_silver.{table} | mode={mode} | partition={partition_field} | cluster={cluster_fields}")
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
    log.info(f"[bronze→silver] ✓ Written {row_count:,} rows → risklens_silver.{table} | partition={partition_field} | program=silver_transform.py")
    return row_count


def write_quarantine(df: DataFrame, project: str, bucket: str,
                     source: str, reason: str, trade_date: str):
    """Write rejected records to quarantine table for audit."""
    log.info(f"[bronze→silver] Quarantine check | source={source} | reason={reason} | date={trade_date}")
    if df.rdd.isEmpty():
        log.debug(f"[bronze→silver] No quarantine records | source={source} | reason={reason}")
        return
    row_count = df.count()
    # Select only metadata cols — all sources have different schemas so we
    # cannot store full rows in a single table without schema conflicts.
    quarantine_df = df.select(
        F.lit(source).alias("source_table"),
        F.lit(reason).alias("rejection_reason"),
        F.current_timestamp().alias("quarantined_at"),
        F.lit(trade_date).alias("trade_date"),
        F.lit(row_count).alias("rejected_count"),
    )
    (
        quarantine_df.write
        .format("bigquery")
        .option("project",          project)
        .option("dataset",          "risklens_bronze")
        .option("table",            "quarantine_r")
        .option("writeMethod",      "indirect")
        .option("temporaryGcsBucket", bucket)
        .mode("append")
        .save()
    )
    log.warning(f"[bronze→silver] Quarantined {row_count:,} bad rows → risklens_bronze.quarantine_r | source={source} | reason={reason} | date={trade_date}")


def update_quality_score(spark: SparkSession, project: str, bucket: str,
                         asset_id: str, total: int, nulls: int,
                         dupes: int, schema_drift: bool, trade_date: str):
    """Update quality scores in catalog after silver processing."""
    log.info(f"[bronze→silver] Recording quality scores | asset_id={asset_id} | total_rows={total:,} | nulls={nulls} | dupes={dupes} | date={trade_date} | table=risklens_catalog.quality_scores")
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
        .option("table",       "quality_scores")
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", bucket)
        .mode("append")
        .save()
    )
    log.info(f"[bronze→silver] ✓ Quality score recorded | asset_id={asset_id} | null_rate={null_rate} | dupe_rate={dupe_rate} | freshness={freshness} | date={trade_date}")


# ── Transforms ────────────────────────────────────────────────────────────────

def transform_trades(spark: SparkSession, project: str, bucket: str, trade_date: str):
    """Bronze → Silver for DTCC trade data."""
    log.info(f"[bronze→silver] Starting trade transform | source=risklens_bronze.trades_r | target=risklens_silver.trades | date={trade_date} | program=silver_transform.py")

    df = read_bronze(spark, project, "trades_r", trade_date)
    total = df.count()
    log.info(f"[bronze→silver] trades_r read complete | total_rows={total:,} | date={trade_date} | source=risklens_bronze.trades_r")

    if total == 0:
        log.warning(f"[bronze→silver] No bronze trade data for date={trade_date} | table=risklens_bronze.trades_r — skipping risklens_silver.trades")
        return

    # ── Null check on critical fields ────────────────────────────────────────
    critical_cols = ["dissemination_id", "asset_class", "execution_timestamp",
                     "notional_currency_1", "rounded_notional_amount_1"]
    null_filter   = " OR ".join([f"{c} IS NULL" for c in critical_cols])
    bad_df  = df.filter(null_filter)
    good_df = df.filter(f"NOT ({null_filter})")
    nulls_before_quarantine = bad_df.count()
    log.info(f"[bronze→silver] Null-check trades | critical_cols={critical_cols} | bad_rows={nulls_before_quarantine} | good_rows={total - nulls_before_quarantine} | date={trade_date}")

    write_quarantine(bad_df, project, bucket, "bronze.trades_r",
                     "NULL in critical field", trade_date)

    # ── Deduplication by dissemination_id + trade_date ────────────────────────
    window   = Window.partitionBy("dissemination_id", "trade_date") \
                     .orderBy(F.col("ingested_at").desc())
    good_df  = good_df.withColumn("_rank", F.row_number().over(window)) \
                      .filter(F.col("_rank") == 1) \
                      .drop("_rank")
    dupes    = total - nulls_before_quarantine - good_df.count()
    log.info(f"[bronze→silver] Dedup trades | key=dissemination_id+trade_date | dupes_removed={dupes} | clean_rows={good_df.count()} | date={trade_date}")

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

    nulls_count = nulls_before_quarantine
    write_silver(silver_df, project, bucket, "trades",
                 partition_field="trade_date", cluster_fields="asset_class,currency")
    update_quality_score(spark, project, bucket, "silver_trades",
                         total, nulls_count, dupes, False, trade_date)
    log.info(f"[bronze→silver] ✓ Trade transform complete | date={trade_date} | total_bronze={total:,} | quarantined={nulls_count} | deduped={dupes} | written_silver=risklens_silver.trades | next=silver_enrich.py")


def transform_rates(spark: SparkSession, project: str, bucket: str, trade_date: str):
    """Bronze → Silver for FRED rate series."""
    log.info(f"[bronze→silver] Starting rates transform | source=risklens_bronze.rates_r | target=risklens_silver.rates | date={trade_date} | program=silver_transform.py")

    df = read_bronze(spark, project, "rates_r", trade_date)
    total = df.count()
    log.info(f"[bronze→silver] rates_r read complete | total_rows={total:,} | date={trade_date} | source=risklens_bronze.rates_r")

    if total == 0:
        log.warning(f"[bronze→silver] No bronze rate data for date={trade_date} | table=risklens_bronze.rates_r — skipping risklens_silver.rates")
        return

    # ── Drop rows where value is NULL (FRED uses '.' for missing) ────────────
    bad_df  = df.filter(F.col("value").isNull())
    good_df = df.filter(F.col("value").isNotNull())
    nulls_count = bad_df.count()
    log.info(f"[bronze→silver] Null-check rates | null_value_rows={nulls_count} | valid_rows={total-nulls_count} | date={trade_date} | FRED_missing_sentinel=dot")

    write_quarantine(bad_df, project, bucket, "bronze.rates_r",
                     "NULL value — missing observation", trade_date)

    # ── Deduplication by series_id + date ─────────────────────────────────────
    window  = Window.partitionBy("series_id", "date").orderBy(F.col("ingested_at").desc())
    good_df = good_df.withColumn("_rank", F.row_number().over(window)) \
                     .filter(F.col("_rank") == 1) \
                     .drop("_rank")
    dupes   = total - nulls_count - good_df.count()
    log.info(f"[bronze→silver] Dedup rates | key=series_id+date | dupes_removed={dupes} | clean_rows={good_df.count()} | date={trade_date}")

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

    write_silver(silver_df, project, bucket, "rates",
                 partition_field="date", cluster_fields="series_id,domain")
    update_quality_score(spark, project, bucket, "silver_rates",
                         total, nulls_count, dupes, False, trade_date)
    log.info(f"[bronze→silver] ✓ Rates transform complete | date={trade_date} | total_bronze={total:,} | quarantined={nulls_count} | deduped={dupes} | outliers_flagged=true | written_silver=risklens_silver.rates | next=silver_enrich.py")


def transform_prices(spark: SparkSession, project: str, bucket: str, trade_date: str):
    """Bronze → Silver for Yahoo Finance price data."""
    log.info(f"[bronze→silver] Starting prices transform | source=risklens_bronze.prices_r | target=risklens_silver.prices | date={trade_date} | program=silver_transform.py")

    df = read_bronze(spark, project, "prices_r", trade_date)
    total = df.count()
    log.info(f"[bronze→silver] prices_r read complete | total_rows={total:,} | date={trade_date} | source=risklens_bronze.prices_r")

    if total == 0:
        log.warning(f"[bronze→silver] No bronze price data for date={trade_date} | table=risklens_bronze.prices_r — skipping risklens_silver.prices")
        return

    # ── Null check on OHLCV ───────────────────────────────────────────────────
    null_filter  = "close IS NULL OR high IS NULL OR low IS NULL"
    bad_df       = df.filter(null_filter)
    good_df      = df.filter(f"NOT ({null_filter})")
    nulls_count  = bad_df.count()
    log.info(f"[bronze→silver] OHLCV null-check | null_ohlcv_rows={nulls_count} | valid_rows={total-nulls_count} | date={trade_date}")

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
    ohlcv_sanity_fails = invalid_df.count()
    log.info(f"[bronze→silver] OHLCV sanity check (high>=low, close in [low,high]) | failed_rows={ohlcv_sanity_fails} | passed_rows={good_df.count()} | date={trade_date}")

    write_quarantine(invalid_df, project, bucket, "bronze.prices_r",
                     "OHLCV sanity check failed", trade_date)

    # ── Deduplication by ticker + date ────────────────────────────────────────
    window  = Window.partitionBy("ticker", "date").orderBy(F.col("ingested_at").desc())
    good_df = good_df.withColumn("_rank", F.row_number().over(window)) \
                     .filter(F.col("_rank") == 1) \
                     .drop("_rank")
    dupes   = total - nulls_count - ohlcv_sanity_fails - good_df.count()
    log.info(f"[bronze→silver] Dedup prices | key=ticker+date | dupes_removed={dupes} | clean_rows={good_df.count()} | date={trade_date}")

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

    write_silver(silver_df, project, bucket, "prices",
                 partition_field="date", cluster_fields="ticker,asset_class,currency")
    update_quality_score(spark, project, bucket, "silver_prices",
                         total, nulls_count, dupes, False, trade_date)
    log.info(f"[bronze→silver] ✓ Prices transform complete | date={trade_date} | total_bronze={total:,} | null_quarantined={nulls_count} | ohlcv_fails={ohlcv_sanity_fails} | deduped={dupes} | written_silver=risklens_silver.prices | next=silver_enrich.py")


def transform_risk(spark: SparkSession, project: str, bucket: str, trade_date: str):
    """Bronze → Silver for synthetic risk outputs (VaR, ES, P&L vectors)."""
    log.info(f"[bronze→silver] Starting risk transform | source=risklens_bronze.risk_outputs_s | target=risklens_silver.risk_outputs | date={trade_date} | program=silver_transform.py")

    df = read_bronze(spark, project, "risk_outputs_s", trade_date)
    total = df.count()
    log.info(f"[bronze→silver] risk_outputs_s read complete | total_rows={total:,} | date={trade_date} | source=risklens_bronze.risk_outputs_s")

    if total == 0:
        log.warning(f"[bronze→silver] No bronze risk data for date={trade_date} | table=risklens_bronze.risk_outputs_s — skipping risklens_silver.risk_outputs")
        return

    # ── Null check on required fields ─────────────────────────────────────────
    null_filter = "desk IS NULL OR calc_date IS NULL"
    bad_df  = df.filter(null_filter)
    good_df = df.filter(f"NOT ({null_filter})")
    nulls_count = bad_df.count()
    log.info(f"[bronze→silver] Risk null-check | null_desk_or_calcdate={nulls_count} | valid_rows={total-nulls_count} | date={trade_date}")

    write_quarantine(bad_df, project, bucket, "bronze.risk_outputs_s",
                     "NULL in desk or calc_date", trade_date)

    # ── VaR/ES bounds: must be positive ───────────────────────────────────────
    if "var_99_1d" in good_df.columns:
        invalid_df = good_df.filter(F.col("var_99_1d") <= 0)
        invalid_count = invalid_df.count()
        good_df    = good_df.filter(F.col("var_99_1d") > 0)
        log.info(f"[bronze→silver] VaR bounds check (var_99_1d > 0) | invalid_rows={invalid_count} | date={trade_date}")
        write_quarantine(invalid_df, project, bucket, "bronze.risk_outputs_s",
                         "VaR must be positive", trade_date)

    # ── Deduplication by desk + calc_date ─────────────────────────────────────
    dedup_cols = ["desk", "calc_date"] if "calc_date" in good_df.columns else ["desk"]
    window     = Window.partitionBy(*dedup_cols).orderBy(F.col("calculated_at").desc())
    good_df    = good_df.withColumn("_rank", F.row_number().over(window)) \
                        .filter(F.col("_rank") == 1) \
                        .drop("_rank")
    dupes      = total - nulls_count - good_df.count()
    log.info(f"[bronze→silver] Dedup risk outputs | key=desk+calc_date | dupes_removed={dupes} | clean_rows={good_df.count()} | date={trade_date}")

    silver_df  = good_df.withColumn("processed_at", F.current_timestamp())

    write_silver(silver_df, project, bucket, "risk_outputs",
                 partition_field="calc_date", cluster_fields="desk")
    update_quality_score(spark, project, bucket, "silver_risk_outputs",
                         total, nulls_count, dupes, False, trade_date)
    log.info(f"[bronze→silver] ✓ Risk transform complete | date={trade_date} | total_bronze={total:,} | quarantined={nulls_count} | deduped={dupes} | written_silver=risklens_silver.risk_outputs | next=silver_enrich.py")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(f"[bronze→silver] Pipeline starting | program=silver_transform.py | sources=risklens_bronze.[trades_r,rates_r,prices_r,risk_outputs_s] | targets=risklens_silver.[trades,rates,prices,risk_outputs]")
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",    required=True)
    parser.add_argument("--bucket",     required=True)
    parser.add_argument("--date",       default=datetime.utcnow().strftime("%Y-%m-%d"),
                        help="Trade date to process (YYYY-MM-DD)")
    args = parser.parse_args()
    log.info(f"[bronze→silver] Args | project={args.project} | bucket={args.bucket} | date={args.date}")

    spark = (
        SparkSession.builder
        .appName("RiskLens-Silver-Transform")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"[bronze→silver] Starting all 4 transforms | date={args.date} | order=trades→rates→prices→risk")

    transform_trades(spark, args.project, args.bucket, args.date)
    transform_rates(spark,  args.project, args.bucket, args.date)
    transform_prices(spark, args.project, args.bucket, args.date)
    transform_risk(spark,   args.project, args.bucket, args.date)

    log.info(f"[bronze→silver] ✓ All transforms complete | date={args.date} | program=silver_transform.py | next=silver_enrich.py")
    spark.stop()


if __name__ == "__main__":
    main()
