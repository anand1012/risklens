"""
RiskLens — Gold Layer: Aggregate, Enrich, Business-Ready
FRTB IMA (BCBS 457, January 2019) aligned table definitions.

Gold tables produced:
  gold.trade_positions  — Enriched trades: silver.trades × prices × rates
  gold.backtesting      — VaR 99% back-testing + traffic light (BCBS 457 ¶351-368)
                          NOTE: VaR is for back-testing ONLY. ES 97.5% is the capital metric.
  gold.es_outputs       — ES 97.5% per desk × risk class × liquidity horizon (¶21-34)
  gold.pnl_vectors      — Hypothetical + actual P&L per desk (PLAT input, ¶329-345)
  gold.plat_results     — P&L Attribution Test: UPL ratio, Spearman, KS (¶329-345)
  gold.capital_charge   — Regulatory capital: ES × (3.0 + traffic_light_multiplier)
  gold.rfet_results     — Risk Factor Eligibility Test observation counts (¶76-80)
  gold.risk_summary     — Daily consolidated ES + PLAT + capital report per desk

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
import math
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType,
                                StringType, TimestampType)

try:
    import google.cloud.logging as _cloud_logging
    _cloud_logging.Client().setup_logging(
        log_level=logging.INFO,
        labels={"app": "risklens", "service": "ingestion", "layer": "gold", "job": "gold_aggregate"},
    )
except Exception:
    logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gold_aggregate")

# FRTB IMA risk class → liquidity horizon mapping (BCBS 457 ¶33-34)
# Desk-level risk class assignment for RiskLens synthetic desks
RISK_CLASS_MAP = {
    "Rates":       ("GIRR",   10),   # General Interest Rate Risk
    "FX":          ("FX",     10),   # FX Risk
    "Credit":      ("CSR_NS", 20),   # Credit Spread Risk Non-Securitization
    "Equities":    ("EQ",     10),   # Equity Risk
    "Commodities": ("COMM",   10),   # Commodity Risk
    "FIRM":        ("FIRM",   10),   # Firm-level aggregate
}

# FRED series → FRTB risk class mapping for RFET
RISK_FACTOR_MAP = {
    "DFF":          "GIRR",    "DGS2":         "GIRR",
    "DGS10":        "GIRR",    "DGS30":        "GIRR",
    "SOFR":         "GIRR",    "SOFR3M":       "GIRR",
    "SOFR6M":       "GIRR",
    "BAMLH0A0HYM2": "CSR_NS",
    "DEXUSEU":      "FX",      "DEXUSUK":      "FX",
    "DEXJPUS":      "FX",
    "VIXCLS":       "EQ",
    "DCOILWTICO":   "COMM",
    "CPIAUCSL":     "MACRO",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_silver(spark: SparkSession, project: str, table: str,
                trade_date: str | None = None) -> DataFrame | None:
    """Read from silver layer. Returns None if the table does not exist."""
    log.info(f"[silver→gold] Reading silver source | table=risklens_silver.{table} | date_filter={trade_date} | program=gold_aggregate.py")
    try:
        df = (
            spark.read
            .format("bigquery")
            .option("project", project)
            .option("dataset", "risklens_silver")
            .option("table", table)
            .load()
        )
    except Exception as e:
        if "not found" in str(e).lower():
            log.warning(f"[silver→gold] Silver table not found | table=risklens_silver.{table} — returning None")
            return None
        log.error(f"[silver→gold] FAILED: Read error | table=risklens_silver.{table} | error={e}", exc_info=True)
        raise
    if trade_date:
        df = df.filter(F.col("trade_date") == trade_date)
    log.info(f"[silver→gold] Silver read complete | table=risklens_silver.{table} | date={trade_date}")
    return df


def read_gold_table(spark: SparkSession, project: str,
                    table: str, trade_date: str) -> DataFrame | None:
    """Read from gold layer. Returns None if the table does not exist."""
    log.info(f"[silver→gold] Reading gold table (cross-table join) | table=risklens_gold.{table} | date={trade_date} | program=gold_aggregate.py")
    try:
        df = (
            spark.read
            .format("bigquery")
            .option("project", project)
            .option("dataset", "risklens_gold")
            .option("table", table)
            .load()
            .filter(F.col("trade_date") == trade_date)
        )
        row_count = df.count()
        log.info(f"[silver→gold] Gold table read complete | table=risklens_gold.{table} | rows={row_count:,} | date={trade_date}")
        return df
    except Exception as e:
        if "not found" in str(e).lower():
            log.warning(f"[silver→gold] Gold table not found | table=risklens_gold.{table} — returning None")
            return None
        log.error(f"[silver→gold] FAILED: Read error | table=risklens_gold.{table} | error={e}", exc_info=True)
        raise


def write_gold(df: DataFrame, project: str, bucket: str,
               table: str, mode: str = "append",
               partition_field: str | None = None,
               partition_type: str = "DAY",
               cluster_fields: str | None = None) -> int:
    log.info(f"[silver→gold] Writing gold table | table=risklens_gold.{table} | mode={mode} | partition={partition_field} | cluster={cluster_fields} | program=gold_aggregate.py")
    row_count = df.count()
    if row_count == 0:
        log.warning(f"[silver→gold] 0 rows — skipping write | table=risklens_gold.{table} | mode={mode}")
        return 0
    writer = (
        df.write
        .format("bigquery")
        .option("project",     project)
        .option("dataset",     "risklens_gold")
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
    log.info(f"[silver→gold] ✓ Written {row_count:,} rows → risklens_gold.{table} | partition={partition_field} | program=gold_aggregate.py")
    return row_count


def update_asset_catalog(spark: SparkSession, project: str, bucket: str,
                         asset_id: str, row_count: int, trade_date: str):
    """
    Upsert row_count / updated_at for an existing asset row.

    Uses a BigQuery MERGE (WHEN MATCHED only) via the Python client rather than
    a Spark DataFrame append, because `.mode("append")` creates a new row on
    every pipeline run — which is how `gold_rfet_results` and
    `silver_risk_enriched` ended up with duplicate rows in I-11. Assets must
    already be registered by scripts/fix_catalog_assets.py (or equivalent
    one-shot seeder); if not, this call is a no-op by design.
    """
    log.info(f"[silver→gold] Updating asset catalog | asset_id={asset_id} | row_count={row_count:,} | trade_date={trade_date} | table=risklens_catalog.assets | program=gold_aggregate.py")
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    sql = f"""
        MERGE `{project}.risklens_catalog.assets` T
        USING (
            SELECT
                @asset_id  AS asset_id,
                @row_count AS row_count,
                CURRENT_TIMESTAMP() AS updated_at
        ) S
        ON T.asset_id = S.asset_id
        WHEN MATCHED THEN UPDATE SET
            T.row_count  = S.row_count,
            T.updated_at = S.updated_at
    """
    client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("asset_id",  "STRING", asset_id),
                bigquery.ScalarQueryParameter("row_count", "INT64",  int(row_count)),
            ]
        ),
    ).result()
    log.info(f"[silver→gold] ✓ Asset catalog updated | asset_id={asset_id} | row_count={row_count:,} | trade_date={trade_date}")


def update_sla_status(spark: SparkSession, project: str, bucket: str,
                      asset_id: str, trade_date: str):
    log.info(f"[silver→gold] Updating SLA status | asset_id={asset_id} | trade_date={trade_date} | table=risklens_catalog.sla_status | program=gold_aggregate.py")
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
    log.info(f"[silver→gold] ✓ SLA status written | asset_id={asset_id} | breach={breach} | trade_date={trade_date}")


# ── Gold Tables ───────────────────────────────────────────────────────────────

def build_trade_positions(spark: SparkSession, project: str,
                          bucket: str, trade_date: str) -> int:
    """
    Gold trade positions from silver.positions.
    silver_enrich.py already joined trades × prices × rates into silver.positions,
    so this job just promotes the enriched silver table to gold with no additional join.
    """
    log.info(f"[silver→gold] Building trade_positions | source=risklens_silver.positions | target=risklens_gold.trade_positions | date={trade_date} | program=gold_aggregate.py")

    positions = read_silver(spark, project, "positions", trade_date)

    if positions is None or positions.rdd.isEmpty():
        log.warning(f"[silver→gold] No silver positions for date={trade_date} | table=risklens_silver.positions — skipping trade_positions build")
        return 0

    positions_count = positions.count()
    log.info(f"[silver→gold] Silver positions loaded | rows={positions_count:,} | date={trade_date} | promoting to risklens_gold.trade_positions (no additional join)")
    gold_df = positions.withColumn("processed_at", F.current_timestamp())

    n = write_gold(gold_df, project, bucket, "trade_positions",
                   partition_field="trade_date",
                   cluster_fields="asset_class,currency")
    log.info(f"[silver→gold] ✓ trade_positions complete | date={trade_date} | rows_written={n:,} | source=risklens_silver.positions | target=risklens_gold.trade_positions")
    return n


def build_backtesting(spark: SparkSession, project: str,
                      bucket: str, trade_date: str) -> int:
    """
    FRTB IMA back-testing table (BCBS 457 ¶351-368).

    VaR 99% 1-day is compared against hypothetical P&L (risk-factor-only P&L)
    and actual P&L (realized trader P&L). Exceptions are counted over a
    rolling 250-day window. Traffic light zone determines capital add-on:
      GREEN  (0-4  exceptions) → multiplier 0.00, charge = ES × 3.00
      AMBER  (5-9  exceptions) → multiplier 0.75, charge = ES × 3.75
      RED    (10+  exceptions) → multiplier 1.00, charge = ES × 4.00

    NOTE: VaR is stored here for back-testing ONLY.
    ES 97.5% (gold.es_outputs) is the regulatory capital metric.
    """
    log.info(f"[silver→gold] Building backtesting | source=risklens_silver.risk_enriched | target=risklens_gold.backtesting | date={trade_date} | bcbs=BCBS457¶351-368 | program=gold_aggregate.py")

    risk = read_silver(spark, project, "risk_enriched", trade_date)
    if risk is None or risk.rdd.isEmpty():
        log.warning(f"[silver→gold] No silver risk_enriched data for date={trade_date} | table=risklens_silver.risk_enriched — skipping backtesting build")
        return 0

    risk_count = risk.count()
    log.info(f"[silver→gold] Risk enriched loaded | rows={risk_count:,} | date={trade_date} | computing hypothetical_pnl, actual_pnl, exceptions")
    # Simulate hypothetical P&L from distribution N(mean_pnl, std_pnl)
    # In production: join against actual trade-level P&L vectors
    bt_df = (
        risk.filter(F.col("var_99_1d").isNotNull())
        .withColumn(
            "hypothetical_pnl",
            F.col("mean_pnl") + F.col("std_pnl") * F.randn(42),
        )
        .withColumn(
            # Actual P&L = hypothetical + small unexplained term (model error, slippage)
            "actual_pnl",
            F.col("hypothetical_pnl") + F.col("std_pnl") * F.randn(17) * 0.05,
        )
        .withColumn(
            # Exception: daily P&L loss exceeds VaR 99% threshold
            "hypothetical_exception",
            (F.col("hypothetical_pnl") < -F.col("var_99_1d")).cast(IntegerType()),
        )
        .withColumn(
            "actual_exception",
            (F.col("actual_pnl") < -F.col("var_99_1d")).cast(IntegerType()),
        )
        .select(
            F.col("calc_date"),
            F.col("desk"),
            F.col("var_99_1d"),
            F.col("var_99_10d"),
            F.col("hypothetical_pnl"),
            F.col("actual_pnl"),
            F.col("hypothetical_exception"),
            F.col("actual_exception"),
            F.col("method"),
            F.col("trade_date"),
        )
    )

    # Rolling exception counts: count over all available history
    # (In production: window over last 250 trading days)
    exception_totals = bt_df.groupBy("desk").agg(
        F.sum("hypothetical_exception").alias("total_hyp_exceptions"),
        F.sum("actual_exception").alias("total_act_exceptions"),
        F.count("calc_date").alias("observation_days"),
    ).withColumn(
        # Scale to 250-day equivalent to normalise for partial history
        "exception_count_250d",
        F.least(
            (F.col("total_hyp_exceptions") * F.lit(250.0)
             / F.col("observation_days")).cast(IntegerType()),
            F.lit(20),  # cap at 20 for display
        ),
    ).withColumn(
        "traffic_light_zone",
        F.when(F.col("exception_count_250d") <= 4, F.lit("GREEN"))
         .when(F.col("exception_count_250d") <= 9, F.lit("AMBER"))
         .otherwise(F.lit("RED")),
    ).withColumn(
        # BCBS 457 ¶352: capital multiplier based on traffic light zone
        "capital_multiplier",
        F.when(F.col("traffic_light_zone") == "GREEN", F.lit(0.00))
         .when(F.col("traffic_light_zone") == "AMBER", F.lit(0.75))
         .otherwise(F.lit(1.00)),
    )

    # Filter to today's data and join exception counts
    today_df = bt_df.filter(F.col("trade_date") == trade_date)
    result = today_df.join(exception_totals, "desk", "left")

    # Firm-level aggregate row
    firm_row = result.groupBy("trade_date").agg(
        F.max("calc_date").alias("calc_date"),
        F.lit("FIRM").alias("desk"),
        F.sum("var_99_1d").alias("var_99_1d"),
        F.sum("var_99_10d").alias("var_99_10d"),
        F.sum("hypothetical_pnl").alias("hypothetical_pnl"),
        F.sum("actual_pnl").alias("actual_pnl"),
        F.max("hypothetical_exception").alias("hypothetical_exception"),
        F.max("actual_exception").alias("actual_exception"),
        F.first("method").alias("method"),
        F.sum("exception_count_250d").alias("exception_count_250d"),
        F.max("traffic_light_zone").alias("traffic_light_zone"),
        F.max("capital_multiplier").alias("capital_multiplier"),
    )

    gold_df = result.unionByName(firm_row, allowMissingColumns=True) \
                    .withColumn("processed_at", F.current_timestamp())

    # Log traffic light distribution before writing
    zone_dist = gold_df.groupBy("traffic_light_zone").count().collect()
    zone_str = " | ".join(f"{r['traffic_light_zone']}={r['count']}" for r in zone_dist if r['traffic_light_zone'])
    log.info(f"[silver→gold] Backtesting traffic light distribution | {zone_str} | date={trade_date}")

    n = write_gold(gold_df, project, bucket, "backtesting",
                   partition_field="calc_date",
                   cluster_fields="desk,traffic_light_zone")
    log.info(f"[silver→gold] ✓ backtesting complete | date={trade_date} | rows_written={n:,} | source=risklens_silver.risk_enriched | target=risklens_gold.backtesting | zones={zone_str}")
    return n


def build_es_outputs(spark: SparkSession, project: str,
                     bucket: str, trade_date: str) -> int:
    """
    Expected Shortfall 97.5% per desk — the FRTB IMA regulatory capital metric.
    (BCBS 457 ¶21-22: ES replaces VaR for capital calculation.)

    Each desk is mapped to its FRTB risk class with the correct liquidity horizon:
      GIRR / FX / EQ / COMM  → 10 days
      CSR Non-Securitisation → 20 days
      CSR Securitisation     → 40 days  (not in scope for RiskLens MVP)
    """
    log.info(f"[silver→gold] Building es_outputs | source=risklens_silver.risk_enriched | target=risklens_gold.es_outputs | date={trade_date} | bcbs=BCBS457¶21-22 | metric=ES_97.5pct | program=gold_aggregate.py")

    risk = read_silver(spark, project, "risk_enriched", trade_date)
    if risk is None or risk.rdd.isEmpty():
        log.warning(f"[silver→gold] No silver risk_enriched data for date={trade_date} | table=risklens_silver.risk_enriched — skipping es_outputs build")
        return 0

    # Risk class and liquidity horizon per desk
    risk_class_expr = (
        F.when(F.col("desk") == "Rates",       F.lit("GIRR"))
         .when(F.col("desk") == "FX",          F.lit("FX"))
         .when(F.col("desk") == "Credit",      F.lit("CSR_NS"))
         .when(F.col("desk") == "Equities",    F.lit("EQ"))
         .when(F.col("desk") == "Commodities", F.lit("COMM"))
         .otherwise(F.lit("FIRM"))
    )
    lh_expr = (
        F.when(F.col("desk") == "Credit", F.lit(20))
         .otherwise(F.lit(10))
    )

    es_df = (
        risk.filter(F.col("es_975_1d").isNotNull())
        .withColumn("risk_class",        risk_class_expr)
        .withColumn("liquidity_horizon", lh_expr)
        .withColumn(
            # ES scaled to risk-class liquidity horizon: ES_1d × sqrt(LH)
            # (BCBS 457 ¶33: holding period scaling)
            "es_975_scaled",
            F.col("es_975_1d") * F.sqrt(F.col("liquidity_horizon").cast(DoubleType())),
        )
        .select(
            F.col("calc_date"),
            F.col("desk"),
            F.col("risk_class"),
            F.col("liquidity_horizon"),
            F.col("es_975_1d"),
            F.col("es_975_10d"),
            F.col("es_975_scaled"),
            F.col("trade_date"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    # Firm-level aggregate
    firm_row = es_df.groupBy("calc_date", "trade_date").agg(
        F.lit("FIRM").alias("desk"),
        F.lit("FIRM").alias("risk_class"),
        F.lit(10).alias("liquidity_horizon"),
        F.sum("es_975_1d").alias("es_975_1d"),
        F.sum("es_975_10d").alias("es_975_10d"),
        F.sum("es_975_scaled").alias("es_975_scaled"),
        F.current_timestamp().alias("processed_at"),
    ).select(es_df.columns)

    gold_df = es_df.union(firm_row)
    es_count = gold_df.count()
    log.info(f"[silver→gold] ES outputs computed | desk_rows={es_count - 1} | plus_firm_aggregate=1 | total={es_count} | scaling=ES_1d×sqrt(LH) | date={trade_date}")
    n = write_gold(gold_df, project, bucket, "es_outputs",
                   partition_field="calc_date",
                   cluster_fields="desk,risk_class")
    log.info(f"[silver→gold] ✓ es_outputs complete | date={trade_date} | rows_written={n:,} | source=risklens_silver.risk_enriched | target=risklens_gold.es_outputs | risk_classes=GIRR,FX,CSR_NS,EQ,COMM,FIRM")
    return n


def build_pnl_vectors(spark: SparkSession, project: str,
                      bucket: str, trade_date: str) -> int:
    """
    P&L vectors per desk — input to the P&L Attribution Test (PLAT).
    (BCBS 457 ¶329-345)

    Stores both hypothetical P&L (from risk factors only) and actual P&L
    (realized including model error, slippage, reserves).
    pnl_unexplained = actual_pnl - hypothetical_pnl
    """
    log.info(f"[silver→gold] Building pnl_vectors | source=risklens_silver.risk_enriched | target=risklens_gold.pnl_vectors | date={trade_date} | bcbs=BCBS457¶329-345 | program=gold_aggregate.py")

    risk = read_silver(spark, project, "risk_enriched", trade_date)
    if risk is None or risk.rdd.isEmpty():
        log.warning(f"[silver→gold] No silver risk_enriched data for date={trade_date} | table=risklens_silver.risk_enriched — skipping pnl_vectors build")
        return 0

    risk_class_expr = (
        F.when(F.col("desk") == "Rates",       F.lit("GIRR"))
         .when(F.col("desk") == "FX",          F.lit("FX"))
         .when(F.col("desk") == "Credit",      F.lit("CSR_NS"))
         .when(F.col("desk") == "Equities",    F.lit("EQ"))
         .when(F.col("desk") == "Commodities", F.lit("COMM"))
         .otherwise(F.lit("FIRM"))
    )

    pnl_df = (
        risk.filter(F.col("num_scenarios").isNotNull())
        .withColumn("risk_class", risk_class_expr)
        .withColumn(
            # Hypothetical P&L: risk-factor-only P&L (no model error)
            # Simulated as N(mean_pnl, std_pnl) — in production: sum of
            # risk-factor sensitivities × factor shocks
            "hypothetical_pnl",
            F.col("mean_pnl") + F.col("std_pnl") * F.randn(99),
        )
        .withColumn(
            # Actual P&L: realized trader P&L including unexplained component
            # Unexplained ≈ 3-8% of std_pnl (model error, new deals, reserves)
            "actual_pnl",
            F.col("hypothetical_pnl") + F.col("std_pnl") * F.randn(13) * 0.06,
        )
        .withColumn(
            "pnl_unexplained",
            F.col("actual_pnl") - F.col("hypothetical_pnl"),
        )
        .select(
            F.col("calc_date"),
            F.col("desk"),
            F.col("risk_class"),
            F.col("hypothetical_pnl"),
            F.col("actual_pnl"),
            F.col("pnl_unexplained"),
            F.col("scenarios"),
            F.col("num_scenarios"),
            F.col("mean_pnl"),
            F.col("std_pnl"),
            F.col("trade_date"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    n = write_gold(pnl_df, project, bucket, "pnl_vectors",
                   partition_field="calc_date",
                   cluster_fields="desk,risk_class")
    log.info(f"[silver→gold] ✓ pnl_vectors complete | date={trade_date} | rows_written={n:,} | columns=hypothetical_pnl,actual_pnl,pnl_unexplained,scenarios | source=risklens_silver.risk_enriched | target=risklens_gold.pnl_vectors")
    return n


def build_plat_results(spark: SparkSession, project: str,
                       bucket: str, trade_date: str) -> int:
    """
    P&L Attribution Test (PLAT) results. (BCBS 457 ¶329-345)

    Three statistical tests per desk over a rolling 12-month window:
      1. UPL ratio: |mean(actual_pnl) - mean(hypothetical_pnl)| / std(hypothetical_pnl)
                    PASS if < 0.95
      2. Spearman rank correlation between actual_pnl and hypothetical_pnl
                    PASS if > 0.40
      3. Kolmogorov-Smirnov test: hypothetical P&L distribution vs. modeled
                    PASS if KS statistic < 0.20

    PLAT passes only if all three tests pass.
    Desks that fail PLAT may be subject to additional capital surcharges.
    """
    log.info(f"[silver→gold] Building plat_results | sources=risklens_gold.pnl_vectors+risklens_silver.risk_enriched(full_history) | target=risklens_gold.plat_results | date={trade_date} | bcbs=BCBS457¶329-345 | tests=UPL_ratio,Spearman,KS | program=gold_aggregate.py")

    pnl_df = read_gold_table(spark, project, "pnl_vectors", trade_date)

    if pnl_df is None or pnl_df.rdd.isEmpty():
        log.warning(f"[silver→gold] No pnl_vectors data for date={trade_date} | table=risklens_gold.pnl_vectors — skipping plat_results build")
        return 0

    # Aggregate over the 12-month window available in silver
    # In production: window over 250 trading days of daily P&L observations
    pnl_all = read_silver(spark, project, "risk_enriched", None)  # full history
    if pnl_all is None or pnl_all.rdd.isEmpty():
        log.warning(f"[silver→gold] No risk_enriched full history | table=risklens_silver.risk_enriched — skipping plat_results build")
        return 0

    window_agg = pnl_all.groupBy("desk").agg(
        F.count("calc_date").alias("observation_count"),
        F.avg("mean_pnl").alias("hyp_pnl_mean"),
        F.stddev("std_pnl").alias("hyp_pnl_std"),
        F.avg(F.col("mean_pnl") * F.lit(1.06)).alias("actual_pnl_mean"),
        F.min("calc_date").alias("window_start_date"),
        F.max("calc_date").alias("window_end_date"),
    )

    plat_df = (
        window_agg
        .withColumn(
            # UPL ratio: |mean actual - mean hypothetical| / std hypothetical
            # (BCBS 457 ¶333: threshold ~0.95 for amber zone)
            "upl_ratio",
            F.abs(F.col("actual_pnl_mean") - F.col("hyp_pnl_mean"))
            / F.greatest(F.col("hyp_pnl_std"), F.lit(1.0)),
        )
        .withColumn("upl_pass",  F.col("upl_ratio") < F.lit(0.95))
        .withColumn(
            # Spearman rank correlation (BCBS 457 ¶339)
            # Simulated from desk hash — in production: computed over 250-day series
            "spearman_correlation",
            F.lit(0.65) + (F.hash("desk").cast(DoubleType()) % F.lit(100)) / F.lit(1000),
        )
        .withColumn("spearman_pass", F.col("spearman_correlation") > F.lit(0.40))
        .withColumn(
            # KS statistic (BCBS 457 ¶340)
            # Simulated — in production: scipy.stats.ks_2samp on daily P&L vectors
            "ks_statistic",
            F.lit(0.09) + (F.abs(F.hash("desk").cast(DoubleType())) % F.lit(50)) / F.lit(1000),
        )
        .withColumn("ks_pass", F.col("ks_statistic") < F.lit(0.20))
        .withColumn(
            "plat_pass",
            F.col("upl_pass") & F.col("spearman_pass") & F.col("ks_pass"),
        )
        .withColumn(
            "notes",
            F.when(~F.col("plat_pass"),
                   F.concat(
                       F.when(~F.col("upl_pass"), F.lit("UPL_FAIL ")).otherwise(F.lit("")),
                       F.when(~F.col("spearman_pass"), F.lit("SPEARMAN_FAIL ")).otherwise(F.lit("")),
                       F.when(~F.col("ks_pass"), F.lit("KS_FAIL")).otherwise(F.lit("")),
                   )
            ).otherwise(F.lit("ALL_PASS")),
        )
        .select(
            F.to_date(F.lit(trade_date)).alias("calc_date"),
            F.col("desk"),
            F.col("window_start_date"),
            F.col("window_end_date"),
            F.col("observation_count"),
            F.col("hyp_pnl_mean"),
            F.col("hyp_pnl_std"),
            F.col("actual_pnl_mean"),
            F.col("upl_ratio"),
            F.col("upl_pass"),
            F.col("spearman_correlation"),
            F.col("spearman_pass"),
            F.col("ks_statistic"),
            F.col("ks_pass"),
            F.col("plat_pass"),
            F.col("notes"),
            F.lit(trade_date).alias("trade_date"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    # Log PLAT pass/fail distribution
    plat_dist = plat_df.groupBy("plat_pass").count().collect()
    plat_str = " | ".join(f"plat_pass={r['plat_pass']}:{r['count']}" for r in plat_dist)
    log.info(f"[silver→gold] PLAT results computed | {plat_str} | tests=upl_ratio<0.95,spearman>0.40,ks<0.20 | date={trade_date}")

    n = write_gold(plat_df, project, bucket, "plat_results",
                   partition_field="calc_date",
                   cluster_fields="desk,plat_pass")
    log.info(f"[silver→gold] ✓ plat_results complete | date={trade_date} | rows_written={n:,} | {plat_str} | source=risklens_silver.risk_enriched+risklens_gold.pnl_vectors | target=risklens_gold.plat_results")
    return n


def build_capital_charge(spark: SparkSession, project: str,
                         bucket: str, trade_date: str) -> int:
    """
    Regulatory capital charge per desk. (BCBS 457 ¶180-186)

    Capital charge = ES 97.5% × (3.0 + traffic_light_multiplier)
      Minimum multiplier: 3.0 (GREEN zone, no back-testing exceptions)
      Maximum multiplier: 4.0 (RED zone, 10+ back-testing exceptions)

    This is the final regulatory number submitted to the supervisor.
    """
    log.info(f"[silver→gold] Building capital_charge | sources=risklens_gold.es_outputs+risklens_gold.backtesting | target=risklens_gold.capital_charge | date={trade_date} | bcbs=BCBS457¶180-186 | formula=ES×(3.0+traffic_light_multiplier) | program=gold_aggregate.py")

    es_df = read_gold_table(spark, project, "es_outputs", trade_date)
    bt_df = read_gold_table(spark, project, "backtesting", trade_date)

    if es_df is None or bt_df is None or es_df.rdd.isEmpty() or bt_df.rdd.isEmpty():
        log.warning(f"[silver→gold] Missing ES or backtesting data for date={trade_date} | tables=risklens_gold.es_outputs,risklens_gold.backtesting — skipping capital_charge build")
        return 0

    bt_slim = bt_df.select(
        "desk", "traffic_light_zone", "capital_multiplier", "exception_count_250d"
    ).dropDuplicates(["desk"])

    charge_df = (
        es_df.join(bt_slim, "desk", "left")
        .withColumn(
            "capital_multiplier",
            F.coalesce(F.col("capital_multiplier"), F.lit(0.0)),
        )
        .withColumn(
            # BCBS 457 ¶183: minimum multiplier is 3, maximum 4
            "regulatory_floor",   F.col("es_975_scaled") * F.lit(3.0),
        )
        .withColumn(
            "capital_charge_usd", F.col("es_975_scaled") * (F.lit(3.0) + F.col("capital_multiplier")),
        )
        .select(
            F.col("calc_date"),
            F.col("desk"),
            F.col("risk_class"),
            F.col("liquidity_horizon"),
            F.col("es_975_1d"),
            F.col("es_975_scaled"),
            F.col("traffic_light_zone"),
            F.col("capital_multiplier"),
            F.col("regulatory_floor"),
            F.col("capital_charge_usd"),
            F.col("exception_count_250d"),
            F.col("trade_date"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    # Log capital charge summary by traffic light zone
    zone_dist = charge_df.groupBy("traffic_light_zone").agg(
        F.count("desk").alias("desk_count"),
        F.sum("capital_charge_usd").alias("total_capital"),
    ).collect()
    zone_cap_str = " | ".join(f"{r['traffic_light_zone']}={r['desk_count']}desks/${r['total_capital']:,.0f}" for r in zone_dist if r['traffic_light_zone'])
    log.info(f"[silver→gold] Capital charge computed | {zone_cap_str} | formula=ES_scaled×(3.0+multiplier) | date={trade_date}")

    n = write_gold(charge_df, project, bucket, "capital_charge",
                   partition_field="calc_date",
                   cluster_fields="desk,risk_class")
    log.info(f"[silver→gold] ✓ capital_charge complete | date={trade_date} | rows_written={n:,} | {zone_cap_str} | sources=risklens_gold.es_outputs+risklens_gold.backtesting | target=risklens_gold.capital_charge")
    return n


def build_rfet_results(spark: SparkSession, project: str,
                       bucket: str, trade_date: str) -> int:
    """
    Risk Factor Eligibility Test (RFET) results. (BCBS 457 ¶76-80)

    Each risk factor must pass either:
      - 75+ real observations in the last 12 months, OR
      - 25+ real observations in the last 90 days

    Risk factors that fail RFET cannot be used in IMA models and must be
    replaced with a proxy approved by the Risk Committee.

    Source: silver.rates (cleaned FRED series — outliers flagged but not dropped,
    so all real observations are counted; bronze-only missing values already removed)
    """
    log.info(f"[silver→gold] Building rfet_results | source=risklens_silver.rates(full_history) | target=risklens_gold.rfet_results | date={trade_date} | bcbs=BCBS457¶76-80 | thresholds=75obs_12m_or_25obs_90d | program=gold_aggregate.py")

    cutoff_12m = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")
    cutoff_90d = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")

    try:
        # Read full silver.rates history (no trade_date filter) to count all observations
        rates = (
            spark.read.format("bigquery")
            .option("project", project)
            .option("dataset", "risklens_silver")
            .option("table", "rates")
            .load()
            .select("series_id", "date")
        )
        rates_count = rates.count()
        log.info(f"[silver→gold] RFET: silver rates loaded | table=risklens_silver.rates | rows={rates_count:,} | cutoff_12m={cutoff_12m} | cutoff_90d={cutoff_90d}")
    except Exception as e:
        log.error(f"[silver→gold] FAILED: Could not read risklens_silver.rates for RFET | error={e}", exc_info=True)
        return 0

    if rates.rdd.isEmpty():
        log.warning(f"[silver→gold] No silver rates data | table=risklens_silver.rates — skipping rfet_results build")
        return 0

    counts = rates.groupBy("series_id").agg(
        F.sum(
            F.when(F.col("date") >= F.lit(cutoff_12m), F.lit(1)).otherwise(F.lit(0))
        ).alias("obs_12m_count"),
        F.sum(
            F.when(F.col("date") >= F.lit(cutoff_90d), F.lit(1)).otherwise(F.lit(0))
        ).alias("obs_90d_count"),
        F.max("date").alias("last_observation_date"),
    )

    # Map series_id → FRTB risk class
    rf_map = spark.createDataFrame(
        [(k, v) for k, v in RISK_FACTOR_MAP.items()],
        ["series_id", "risk_class"],
    )

    rfet_df = (
        counts
        .join(rf_map, "series_id", "left")
        .withColumn("risk_class", F.coalesce(F.col("risk_class"), F.lit("OTHER")))
        .withColumn("obs_12m_pass", F.col("obs_12m_count") >= F.lit(75))
        .withColumn("obs_90d_pass", F.col("obs_90d_count") >= F.lit(25))
        .withColumn(
            "rfet_pass",
            F.col("obs_12m_pass") | F.col("obs_90d_pass"),
        )
        .withColumn("eligible_for_ima", F.col("rfet_pass"))
        .withColumn(
            "staleness_days",
            F.datediff(F.lit(trade_date), F.col("last_observation_date")),
        )
        .withColumn(
            "failure_reason",
            F.when(~F.col("rfet_pass"),
                   F.concat(
                       F.lit("Insufficient observations: 12m="),
                       F.col("obs_12m_count").cast(StringType()),
                       F.lit(" (need 75), 90d="),
                       F.col("obs_90d_count").cast(StringType()),
                       F.lit(" (need 25)"),
                   )
            ).otherwise(F.lit(None).cast(StringType())),
        )
        .select(
            F.to_date(F.lit(trade_date)).alias("rfet_date"),
            F.col("series_id").alias("risk_factor_id"),
            F.col("risk_class"),
            F.col("obs_12m_count"),
            F.col("obs_90d_count"),
            F.col("obs_12m_pass"),
            F.col("obs_90d_pass"),
            F.col("rfet_pass"),
            F.col("eligible_for_ima"),
            F.col("last_observation_date"),
            F.col("staleness_days"),
            F.col("failure_reason"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    # Log RFET pass/fail by risk class
    rfet_dist = rfet_df.groupBy("rfet_pass", "risk_class").count().collect()
    rfet_pass_count = sum(r['count'] for r in rfet_dist if r['rfet_pass'])
    rfet_fail_count = sum(r['count'] for r in rfet_dist if not r['rfet_pass'])
    log.info(f"[silver→gold] RFET eligibility results | rfet_pass={rfet_pass_count} | rfet_fail={rfet_fail_count} | eligible_for_ima={rfet_pass_count} | date={trade_date}")

    n = write_gold(rfet_df, project, bucket, "rfet_results",
                   partition_field="rfet_date",
                   cluster_fields="risk_class,rfet_pass")
    log.info(f"[silver→gold] ✓ rfet_results complete | date={trade_date} | rows_written={n:,} | rfet_pass={rfet_pass_count} | rfet_fail={rfet_fail_count} | source=risklens_silver.rates | target=risklens_gold.rfet_results")
    return n


def build_risk_summary(spark: SparkSession, project: str,
                       bucket: str, trade_date: str) -> int:
    """
    Daily consolidated risk summary — ES capital + PLAT + back-testing per desk.
    This is the regulatory submission-ready view.
    """
    log.info(f"[silver→gold] Building risk_summary | sources=risklens_gold.backtesting+es_outputs+plat_results+capital_charge | target=risklens_gold.risk_summary | date={trade_date} | program=gold_aggregate.py")

    bt_df   = read_gold_table(spark, project, "backtesting",    trade_date)
    es_df   = read_gold_table(spark, project, "es_outputs",     trade_date)
    plat_df = read_gold_table(spark, project, "plat_results",   trade_date)
    cap_df  = read_gold_table(spark, project, "capital_charge", trade_date)

    missing = [name for name, df in [("backtesting", bt_df), ("es_outputs", es_df), ("plat_results", plat_df), ("capital_charge", cap_df)] if df is None]
    if missing:
        log.warning(f"[silver→gold] Missing gold tables for risk_summary | missing={missing} | date={trade_date} — skipping risk_summary build")
        return 0

    bt_slim   = bt_df.select(
        "desk", "calc_date", "trade_date", "var_99_1d", "var_99_10d",
        "traffic_light_zone", "exception_count_250d",
    ).dropDuplicates(["desk", "calc_date"])

    es_slim   = es_df.select(
        "desk", "calc_date", "risk_class", "liquidity_horizon",
        "es_975_1d", "es_975_10d", "es_975_scaled",
    ).dropDuplicates(["desk", "calc_date"])

    plat_slim = plat_df.select(
        "desk", "plat_pass", "upl_ratio", "spearman_correlation",
        "ks_statistic", "notes",
    ).dropDuplicates(["desk"])

    cap_slim  = cap_df.select(
        "desk", "calc_date", "capital_charge_usd", "capital_multiplier",
    ).dropDuplicates(["desk", "calc_date"])

    summary = (
        bt_slim.alias("b")
        .join(es_slim.alias("e"),
              (F.col("b.desk") == F.col("e.desk")) &
              (F.col("b.calc_date") == F.col("e.calc_date")), "left")
        .join(plat_slim.alias("p"), F.col("b.desk") == F.col("p.desk"), "left")
        .join(cap_slim.alias("c"),
              (F.col("b.desk") == F.col("c.desk")) &
              (F.col("b.calc_date") == F.col("c.calc_date")), "left")
        .select(
            F.col("b.calc_date"),
            F.col("b.desk"),
            F.col("e.risk_class"),
            F.col("e.liquidity_horizon"),
            # Back-testing (for reference only, not capital)
            F.col("b.var_99_1d"),
            F.col("b.var_99_10d"),
            F.col("b.traffic_light_zone"),
            F.col("b.exception_count_250d"),
            # ES 97.5% — the capital metric
            F.col("e.es_975_1d"),
            F.col("e.es_975_10d"),
            F.col("e.es_975_scaled"),
            # PLAT
            F.col("p.plat_pass"),
            F.col("p.upl_ratio"),
            F.col("p.spearman_correlation"),
            F.col("p.ks_statistic"),
            F.col("p.notes").alias("plat_notes"),
            # Capital charge
            F.col("c.capital_charge_usd"),
            F.col("c.capital_multiplier"),
            F.col("b.trade_date"),
            F.current_timestamp().alias("report_generated_at"),
        )
    )

    n = write_gold(summary, project, bucket, "risk_summary",
                   partition_field="calc_date",
                   cluster_fields="desk,risk_class")
    log.info(f"[silver→gold] ✓ risk_summary complete | date={trade_date} | rows_written={n:,} | sources=risklens_gold.backtesting+es_outputs+plat_results+capital_charge | target=risklens_gold.risk_summary | regulatory_submission_ready=true")
    return n


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(f"[silver→gold] Pipeline starting | program=gold_aggregate.py | targets=risklens_gold.(trade_positions,backtesting,es_outputs,pnl_vectors,plat_results,capital_charge,rfet_results,risk_summary)")
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--date",    default=datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    log.info(f"[silver→gold] Args | project={args.project} | bucket={args.bucket} | date={args.date}")

    spark = (
        SparkSession.builder
        .appName("RiskLens-Gold-Aggregate")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"[silver→gold] Running 8 gold builders | date={args.date} | order=trade_positions→backtesting→es_outputs→pnl_vectors→plat_results→capital_charge→rfet_results→risk_summary")

    # Dependency order: positions → ES → backtesting → PnL → PLAT → capital → summary
    n_positions = build_trade_positions(spark, args.project, args.bucket, args.date)
    n_bt        = build_backtesting(   spark, args.project, args.bucket, args.date)
    n_es        = build_es_outputs(    spark, args.project, args.bucket, args.date)
    n_pnl       = build_pnl_vectors(   spark, args.project, args.bucket, args.date)
    n_plat      = build_plat_results(  spark, args.project, args.bucket, args.date)
    n_cap       = build_capital_charge(spark, args.project, args.bucket, args.date)
    n_rfet      = build_rfet_results(  spark, args.project, args.bucket, args.date)
    try:
        n_summary = build_risk_summary(spark, args.project, args.bucket, args.date)
    except Exception as e:
        log.error(f"[silver→gold] FAILED: risk_summary build failed | date={args.date} | error={e}", exc_info=True)
        n_summary = 0

    asset_counts = {
        "gold_trade_positions": n_positions,
        "gold_backtesting":     n_bt,
        "gold_es_outputs":      n_es,
        "gold_pnl_vectors":     n_pnl,
        "gold_plat_results":    n_plat,
        "gold_capital_charge":  n_cap,
        "gold_rfet_results":    n_rfet,
        "gold_risk_summary":    n_summary,
    }

    for asset_id, row_count in asset_counts.items():
        if row_count > 0:
            update_asset_catalog(spark, args.project, args.bucket,
                                 asset_id, row_count, args.date)
            update_sla_status(spark, args.project, args.bucket,
                              asset_id, args.date)

    # Keep catalog-management tables themselves registered in the asset catalog
    # so the RAG indexer picks up their schema. Without this, the AI chat never
    # sees columns for sla_status / quality_scores and hallucinates field names.
    # (I-6 / I-9 — see docs/issue_log.md.)
    catalog_mgmt_assets = {
        "sla_status":     "Per-asset SLA tracker written by this job",
        "quality_scores": "Per-asset DQ scores written by silver_transform",
    }
    for catalog_asset, _note in catalog_mgmt_assets.items():
        update_asset_catalog(spark, args.project, args.bucket,
                             catalog_asset, 0, args.date)

    total_rows = sum(asset_counts.values())
    log.info(f"[silver→gold] ✓ Pipeline complete | program=gold_aggregate.py | date={args.date} | total_rows_written={total_rows:,}")
    for name, count in asset_counts.items():
        log.info(f"[silver→gold]   risklens_gold.{name:<28}: {count:,} rows")

    spark.stop()


if __name__ == "__main__":
    main()
