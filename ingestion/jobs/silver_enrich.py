"""
RiskLens — Silver Layer (Job 2 of 2): Join, Enrich, Aggregate
Reads from already-cleaned Silver tables, produces enriched/joined Silver tables.
Run this AFTER silver_transform.py.

Enriched tables produced:
  silver.positions     : silver.trades × silver.prices × silver.rates
                         Aggregates to asset_class/currency level, attaches current
                         mark-to-market price and SOFR-discounted present value.
                         → Input to gold.trade_positions (no join needed in gold)

  silver.risk_enriched : silver.risk_outputs × silver.rates (SOFR, VIX, HY spread)
                         Attaches the market rate context that was in force on each
                         calc_date — SOFR (discount rate), VIX (equity vol regime),
                         HY spread (credit regime).
                         → Input to gold FRTB IMA tables (backtesting, ES, PnL, PLAT)

Usage (local):
    python ingestion/jobs/silver_enrich.py \
        --project risklens-frtb-2026 \
        --bucket risklens-raw-risklens-frtb-2026 \
        --date 2026-04-13

Usage (Dataproc):
    Submitted via refresh_data.sh (runs after silver_transform.py)
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("silver_enrich")


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_silver(spark: SparkSession, project: str, table: str,
                trade_date: str | None = None) -> DataFrame:
    """Read from cleaned silver layer, optionally filtered by trade_date."""
    df = (
        spark.read
        .format("bigquery")
        .option("project", project)
        .option("dataset", "risklens_silver")
        .option("table", table)
        .load()
    )
    if trade_date:
        df = df.filter(F.col("trade_date") == trade_date)
    return df


def write_silver_enriched(df: DataFrame, project: str, bucket: str,
                          table: str, partition_field: str | None = None,
                          cluster_fields: str | None = None) -> int:
    """Write enriched DataFrame to BigQuery silver layer."""
    row_count = df.count()
    if row_count == 0:
        log.info(f"  risklens_silver.{table}: 0 rows — skipping write")
        return 0

    writer = (
        df.write
        .format("bigquery")
        .option("project",          project)
        .option("dataset",          "risklens_silver")
        .option("table",            table)
        .option("writeMethod",      "indirect")
        .option("temporaryGcsBucket", bucket)
    )
    if partition_field:
        writer = writer.option("partitionField", partition_field) \
                       .option("partitionType", "DAY")
    if cluster_fields:
        writer = writer.option("clusteredFields", cluster_fields)
    writer.mode("append").save()
    log.info(f"  risklens_silver.{table}: {row_count:,} rows written")
    return row_count


# ── Enrichment Jobs ───────────────────────────────────────────────────────────

def enrich_positions(spark: SparkSession, project: str,
                     bucket: str, trade_date: str) -> int:
    """
    Build silver.positions: trades aggregated to asset_class/currency level,
    joined with latest mark price and SOFR-discounted present value.

    Sources:
      silver.trades  → notional amounts per asset_class/currency
      silver.prices  → latest adj_close as mark price (matched by currency)
      silver.rates   → SOFR or DFF as discount rate for PV calculation
    """
    log.info("Building silver.positions")

    trades = read_silver(spark, project, "trades", trade_date)
    prices = read_silver(spark, project, "prices", trade_date)
    rates  = read_silver(spark, project, "rates",  trade_date)

    if trades.rdd.isEmpty():
        log.info("  No silver trades — skipping positions.")
        return 0

    # Aggregate trades to asset_class × currency level
    positions = trades.groupBy("asset_class", "currency", "trade_date").agg(
        F.sum("notional_amount").alias("total_notional"),
        F.countDistinct("dissemination_id").alias("trade_count"),
    )

    # Latest price per currency — use max adj_close as mark price proxy
    latest_prices = prices.groupBy("currency").agg(
        F.max("adj_close").alias("mark_price"),
    )

    # SOFR (or DFF fallback) as discount rate for present value
    sofr = rates.filter(
        F.col("series_id").isin("SOFR", "DFF") & F.col("is_outlier").isin([False])
    ).agg(F.avg("value").alias("sofr_rate"))

    sofr_rate_val = sofr.collect()[0]["sofr_rate"]
    sofr_rate = float(sofr_rate_val) if sofr_rate_val is not None else 0.05

    enriched = (
        positions
        .join(latest_prices, "currency", "left")
        .withColumn("mark_price",       F.coalesce(F.col("mark_price"), F.lit(1.0)))
        .withColumn("market_value_usd", F.col("total_notional") * F.col("mark_price"))
        .withColumn("sofr_rate",        F.lit(sofr_rate).cast(DoubleType()))
        .withColumn(
            # Flat-rate PV discounting: MV / (1 + SOFR)
            "pv_usd",
            F.col("market_value_usd") / (F.lit(1.0) + F.col("sofr_rate")),
        )
        .select(
            F.col("asset_class"),
            F.col("currency"),
            F.col("total_notional"),
            F.col("trade_count"),
            F.col("mark_price"),
            F.col("market_value_usd"),
            F.col("sofr_rate"),
            F.col("pv_usd"),
            F.col("trade_date"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    return write_silver_enriched(enriched, project, bucket, "positions",
                                 partition_field="trade_date",
                                 cluster_fields="asset_class,currency")


def enrich_risk(spark: SparkSession, project: str,
                bucket: str, trade_date: str) -> int:
    """
    Build silver.risk_enriched: risk outputs joined with market rate context.

    Attaches to each desk's calc_date:
      sofr_rate  — SOFR/DFF on calc_date (discount rate, GIRR risk factor)
      vix_level  — VIX on calc_date (equity vol regime indicator)
      hy_spread  — ICE BofA HY spread on calc_date (credit regime indicator)

    Used by all FRTB IMA gold builders (backtesting, ES, PnL, PLAT) so that
    gold layer only does metric computation and never touches bronze.

    Sources:
      silver.risk_outputs  → desk-level VaR, ES, P&L distribution params
      silver.rates         → market rates for the same calc_date window
    """
    log.info("Building silver.risk_enriched")

    risk  = read_silver(spark, project, "risk_outputs", trade_date)
    # Read all rates history so we can match any calc_date in risk
    rates = read_silver(spark, project, "rates", None)

    if risk.rdd.isEmpty():
        log.info("  No silver risk data — skipping risk_enriched.")
        return 0

    # Pivot key rate series to columns, one row per date
    # Filter to only the 3 series we need before pivoting for efficiency
    key_rates = rates.filter(
        F.col("series_id").isin("SOFR", "DFF", "VIXCLS", "BAMLH0A0HYM2")
    ).select(
        F.col("date").cast("string").alias("rate_date"),
        F.col("series_id"),
        F.col("value"),
    )

    # Use coalesce for SOFR → DFF fallback (SOFR data starts 2018; DFF goes back further)
    sofr_df = key_rates.filter(F.col("series_id") == "SOFR") \
                       .select(F.col("rate_date"), F.col("value").alias("sofr_raw"))
    dff_df  = key_rates.filter(F.col("series_id") == "DFF") \
                       .select(F.col("rate_date"), F.col("value").alias("dff_raw"))
    vix_df  = key_rates.filter(F.col("series_id") == "VIXCLS") \
                       .select(F.col("rate_date"), F.col("value").alias("vix_level"))
    hy_df   = key_rates.filter(F.col("series_id") == "BAMLH0A0HYM2") \
                       .select(F.col("rate_date"), F.col("value").alias("hy_spread"))

    # Merge rate columns, match risk rows by calc_date → rate_date
    rates_wide = sofr_df \
        .join(dff_df,  "rate_date", "outer") \
        .join(vix_df,  "rate_date", "left") \
        .join(hy_df,   "rate_date", "left") \
        .withColumn("sofr_rate",
                    F.coalesce(F.col("sofr_raw"), F.col("dff_raw"))) \
        .drop("sofr_raw", "dff_raw")

    enriched = (
        risk
        .withColumn("calc_date_str", F.col("calc_date").cast("string"))
        .join(rates_wide, risk["calc_date"].cast("string") == rates_wide["rate_date"], "left")
        .drop("rate_date", "calc_date_str")
        .withColumn("sofr_rate", F.coalesce(F.col("sofr_rate"), F.lit(0.05)))
        .withColumn("vix_level", F.coalesce(F.col("vix_level"), F.lit(20.0)))
        .withColumn("hy_spread", F.coalesce(F.col("hy_spread"), F.lit(3.5)))
        .select(
            # Core risk columns
            F.col("desk"),
            F.col("calc_date"),
            F.col("var_99_1d"),
            F.col("var_99_10d"),
            F.col("es_975_1d"),
            F.col("es_975_10d"),
            F.col("method"),
            F.col("scenarios"),
            F.col("num_scenarios"),
            F.col("mean_pnl"),
            F.col("std_pnl"),
            # Market context columns added here
            F.col("sofr_rate"),   # GIRR risk factor — discount rate in effect
            F.col("vix_level"),   # EQ vol regime: low <20, elevated 20-30, stress >30
            F.col("hy_spread"),   # Credit regime: IG-like <3%, stress >5%
            F.col("trade_date"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    return write_silver_enriched(enriched, project, bucket, "risk_enriched",
                                 partition_field="calc_date",
                                 cluster_fields="desk")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--date",    default=datetime.utcnow().strftime("%Y-%m-%d"),
                        help="Trade date to process (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("RiskLens-Silver-Enrich")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Silver enrich for trade_date={args.date}")

    enrich_positions(spark, args.project, args.bucket, args.date)
    enrich_risk(spark, args.project, args.bucket, args.date)

    log.info("Silver enrich complete.")
    spark.stop()


if __name__ == "__main__":
    main()
