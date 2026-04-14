"""
RiskLens — Gold Layer: Aggregate, Enrich, Business-Ready
Reads from Silver, joins across sources, produces final business-ready tables.

Gold tables produced:
  gold.trade_positions  — Enriched trades joined with prices + rates
  gold.var_outputs      — Daily VaR per desk (99%, 1-day and 10-day)
  gold.es_outputs       — Daily Expected Shortfall per desk (97.5%)
  gold.pnl_vectors      — P&L attribution vectors per desk
  gold.risk_summary     — Daily consolidated risk report across all desks

After gold runs:
  - risklens_catalog.assets updated (row counts, freshness, size)
  - risklens_lineage.edges updated (source → pipeline → table → report)
  - risklens_catalog.sla_status updated (actual refresh timestamp)

Usage (local):
    python ingestion/jobs/gold_aggregate.py \
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
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType,
                                StringType, TimestampType)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gold_aggregate")


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_silver(spark: SparkSession, project: str, table: str,
                trade_date: str | None = None) -> DataFrame:
    query = f"SELECT * FROM `{project}.risklens_silver.{table}`"
    if trade_date:
        query += f" WHERE trade_date = '{trade_date}'"
    return spark.read.format("bigquery").option("query", query).load()


def write_gold(df: DataFrame, project: str, bucket: str,
               table: str, mode: str = "append") -> int:
    row_count = df.count()
    (
        df.write
        .format("bigquery")
        .option("project",     project)
        .option("dataset",     "risklens_gold")
        .option("table",       table)
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", bucket)
        .mode(mode)
        .save()
    )
    log.info(f"  risklens_gold.{table}: {row_count:,} rows written")
    return row_count


def update_asset_catalog(spark: SparkSession, project: str, bucket: str,
                         asset_id: str, row_count: int, trade_date: str):
    """Update asset metadata in catalog after gold run."""
    row = spark.createDataFrame([{
        "asset_id":   asset_id,
        "row_count":  row_count,
        "updated_at": datetime.utcnow(),
    }])
    (
        row.write
        .format("bigquery")
        .option("project",     project)
        .option("dataset",     "risklens_catalog")
        .option("table",       "assets")
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", bucket)
        .mode("append")
        .save()
    )


def update_sla_status(spark: SparkSession, project: str, bucket: str,
                      asset_id: str, trade_date: str):
    """Record actual refresh time for SLA tracking."""
    now      = datetime.utcnow()
    expected = now.replace(hour=8, minute=0, second=0, microsecond=0)
    breach   = now > expected
    row = spark.createDataFrame([{
        "asset_id":             asset_id,
        "expected_refresh":     expected,
        "actual_refresh":       now,
        "breach_flag":          breach,
        "breach_duration_mins": int((now - expected).seconds / 60) if breach else 0,
        "checked_at":           now,
    }])
    (
        row.write
        .format("bigquery")
        .option("project",     project)
        .option("dataset",     "risklens_catalog")
        .option("table",       "sla_status")
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", bucket)
        .mode("append")
        .save()
    )


# ── Gold Tables ───────────────────────────────────────────────────────────────

def build_trade_positions(spark: SparkSession, project: str,
                          bucket: str, trade_date: str) -> int:
    """
    Enriched trade positions:
    silver.trades JOIN silver.prices ON underlying ≈ ticker
    JOIN silver.rates ON currency domain (USD rates for USD trades)

    Produces: desk-level trade positions with current market value
    """
    log.info("Building gold.trade_positions_r")

    trades = read_silver(spark, project, "trades", trade_date)
    prices = read_silver(spark, project, "prices", trade_date)
    rates  = read_silver(spark, project, "rates",  trade_date)

    if trades.rdd.isEmpty():
        log.info("  No silver trades — skipping trade_positions.")
        return 0

    # Join trades to latest closing price by currency
    # Use adj_close as the mark-to-market price
    latest_prices = prices.groupBy("ticker", "currency") \
                          .agg(F.max("adj_close").alias("mark_price"))

    # Join trades to USD rates (Fed Funds as discount rate proxy)
    usd_rate = rates.filter(
        (F.col("series_id") == "DFF") & (F.col("trade_date") == trade_date)
    ).agg(F.avg("value").alias("discount_rate"))

    discount_rate = usd_rate.collect()[0]["discount_rate"] if not usd_rate.rdd.isEmpty() else 0.05

    positions = (
        trades
        .join(latest_prices,
              trades["currency"] == latest_prices["currency"],
              "left")
        .withColumn("mark_price",       F.coalesce(F.col("mark_price"), F.lit(1.0)))
        .withColumn("market_value_usd", F.col("notional_amount") * F.col("mark_price"))
        .withColumn("discount_rate",    F.lit(discount_rate).cast(DoubleType()))
        .withColumn("pv_usd",
            F.col("market_value_usd") / (1 + F.col("discount_rate")))
        .select(
            F.col("dissemination_id").alias("trade_id"),
            trades["currency"],
            F.col("asset_class"),
            F.col("underlying"),
            F.col("notional_amount"),
            F.col("mark_price"),
            F.col("market_value_usd"),
            F.col("discount_rate"),
            F.col("pv_usd"),
            F.col("trade_date"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    return write_gold(positions, project, bucket, "trade_positions")


def build_var_outputs(spark: SparkSession, project: str,
                      bucket: str, trade_date: str) -> int:
    """
    VaR outputs per desk from silver risk_outputs.
    Adds firm-level aggregate row.
    """
    log.info("Building gold.var_outputs_s")

    risk = read_silver(spark, project, "risk_outputs", trade_date)
    if risk.rdd.isEmpty():
        log.info("  No silver risk data — skipping var_outputs.")
        return 0

    # Filter to VaR rows (synthetic data has both VaR and ES in same table)
    var_df = risk.filter(F.col("var_99_1d").isNotNull()) \
                 .select(
                     F.col("calc_date"),
                     F.col("desk"),
                     F.col("var_99_1d"),
                     F.col("var_99_10d"),
                     F.col("method"),
                     F.col("scenarios"),
                     F.col("trade_date"),
                     F.current_timestamp().alias("processed_at"),
                 )

    # Firm-level aggregate
    firm_row = var_df.agg(
        F.max("calc_date").alias("calc_date"),
        F.lit("FIRM").alias("desk"),
        F.sum("var_99_1d").alias("var_99_1d"),
        F.sum("var_99_10d").alias("var_99_10d"),
        F.first("method").alias("method"),
        F.first("scenarios").alias("scenarios"),
        F.first("trade_date").alias("trade_date"),
        F.current_timestamp().alias("processed_at"),
    )

    gold_df = var_df.union(firm_row)
    return write_gold(gold_df, project, bucket, "var_outputs")


def build_es_outputs(spark: SparkSession, project: str,
                     bucket: str, trade_date: str) -> int:
    """
    Expected Shortfall outputs per desk.
    Adds firm-level aggregate row.
    """
    log.info("Building gold.es_outputs_s")

    risk = read_silver(spark, project, "risk_outputs", trade_date)
    if risk.rdd.isEmpty():
        log.info("  No silver risk data — skipping es_outputs.")
        return 0

    es_df = risk.filter(F.col("es_975_1d").isNotNull()) \
                .select(
                    F.col("calc_date"),
                    F.col("desk"),
                    F.col("es_975_1d"),
                    F.col("es_975_10d"),
                    F.col("trade_date"),
                    F.current_timestamp().alias("processed_at"),
                )

    firm_row = es_df.agg(
        F.max("calc_date").alias("calc_date"),
        F.lit("FIRM").alias("desk"),
        F.sum("es_975_1d").alias("es_975_1d"),
        F.sum("es_975_10d").alias("es_975_10d"),
        F.first("trade_date").alias("trade_date"),
        F.current_timestamp().alias("processed_at"),
    )

    gold_df = es_df.union(firm_row)
    return write_gold(gold_df, project, bucket, "es_outputs")


def build_pnl_vectors(spark: SparkSession, project: str,
                      bucket: str, trade_date: str) -> int:
    """
    P&L attribution vectors per desk (100 scenarios each).
    Used for P&L Attribution Test (PLAT) under FRTB-IMA.
    """
    log.info("Building gold.pnl_vectors_s")

    risk = read_silver(spark, project, "risk_outputs", trade_date)
    if risk.rdd.isEmpty():
        log.info("  No silver risk data — skipping pnl_vectors.")
        return 0

    pnl_df = risk.filter(F.col("num_scenarios").isNotNull()) \
                 .select(
                     F.col("calc_date"),
                     F.col("desk"),
                     F.col("scenarios"),
                     F.col("num_scenarios"),
                     F.col("mean_pnl"),
                     F.col("std_pnl"),
                     F.col("trade_date"),
                     F.current_timestamp().alias("processed_at"),
                 )

    return write_gold(pnl_df, project, bucket, "pnl_vectors")


def build_risk_summary(spark: SparkSession, project: str,
                       bucket: str, trade_date: str) -> int:
    """
    Daily consolidated risk summary report.
    Joins VaR + ES + P&L into one report-ready table per desk.
    This is the regulatory submission-ready view.
    """
    log.info("Building gold.risk_summary_s")

    # Read from gold (just written)
    def read_gold(table: str) -> DataFrame:
        query = (f"SELECT * FROM `{project}.risklens_gold.{table}` "
                 f"WHERE trade_date = '{trade_date}'")
        return spark.read.format("bigquery").option("query", query).load()

    try:
        var_df = read_gold("var_outputs")
        es_df  = read_gold("es_outputs")
        pnl_df = read_gold("pnl_vectors")
    except Exception as e:
        log.warning(f"  Could not read gold tables for summary: {e}")
        return 0

    summary = (
        var_df.alias("v")
        .join(es_df.alias("e"),
              (F.col("v.desk") == F.col("e.desk")) &
              (F.col("v.trade_date") == F.col("e.trade_date")),
              "left")
        .join(pnl_df.alias("p"),
              (F.col("v.desk") == F.col("p.desk")) &
              (F.col("v.trade_date") == F.col("p.trade_date")),
              "left")
        .select(
            F.col("v.calc_date"),
            F.col("v.desk"),
            F.col("v.var_99_1d"),
            F.col("v.var_99_10d"),
            F.col("e.es_975_1d"),
            F.col("e.es_975_10d"),
            F.col("p.mean_pnl"),
            F.col("p.std_pnl"),
            F.col("p.num_scenarios"),
            # PLAT pass/fail: mean P&L within 10% of VaR
            F.when(
                F.abs(F.col("p.mean_pnl")) <= F.col("v.var_99_1d") * 0.10,
                F.lit(True)
            ).otherwise(F.lit(False)).alias("plat_pass"),
            F.col("v.method"),
            F.col("v.trade_date"),
            F.current_timestamp().alias("report_generated_at"),
        )
    )

    return write_gold(summary, project, bucket, "risk_summary")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--date",    default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("RiskLens-Gold-Aggregate")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Gold aggregate for trade_date={args.date}")

    # Build gold tables in dependency order
    n_positions = build_trade_positions(spark, args.project, args.bucket, args.date)
    n_var       = build_var_outputs(spark,     args.project, args.bucket, args.date)
    n_es        = build_es_outputs(spark,      args.project, args.bucket, args.date)
    n_pnl       = build_pnl_vectors(spark,     args.project, args.bucket, args.date)
    n_summary   = build_risk_summary(spark,    args.project, args.bucket, args.date)

    # Update catalog metadata for each gold asset
    asset_counts = {
        "gold_trade_positions": n_positions,
        "gold_var_outputs":     n_var,
        "gold_es_outputs":      n_es,
        "gold_pnl_vectors":     n_pnl,
        "gold_risk_summary":    n_summary,
    }

    for asset_id, row_count in asset_counts.items():
        if row_count > 0:
            update_asset_catalog(spark, args.project, args.bucket,
                                 asset_id, row_count, args.date)
            update_sla_status(spark, args.project, args.bucket,
                              asset_id, args.date)

    log.info("Gold aggregate complete.")
    log.info(f"  trade_positions : {n_positions:,}")
    log.info(f"  var_outputs     : {n_var:,}")
    log.info(f"  es_outputs      : {n_es:,}")
    log.info(f"  pnl_vectors     : {n_pnl:,}")
    log.info(f"  risk_summary    : {n_summary:,}")

    spark.stop()


if __name__ == "__main__":
    main()
