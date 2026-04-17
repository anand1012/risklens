"""
RiskLens — Bronze Layer: Synthetic Internal Data Ingestion
Runs the synthetic data generator and loads results into BigQuery bronze layer.

IMPORTANT: Run this AFTER bronze_rates.py and bronze_prices.py have completed.
This job reads SOFR, VIX, and HY spread from bronze.rates_r to seed the
synthetic VaR/ES numbers so they reflect real market conditions.

Synthetic assets loaded:
  - Risk outputs  : VaR, ES, P&L vectors (seeded from real FRED/Yahoo inputs)
  - Pipeline logs : Spark job run history
  - Ownership     : Dataset owner/steward registry
  - Quality scores: Null rates, schema drift, freshness
  - SLA status    : Expected vs actual refresh times
  - Assets catalog: All data asset metadata
  - Lineage       : Nodes and edges of the data lineage graph

Usage (local):
    python ingestion/jobs/bronze_synthetic.py \
        --project risklens-frtb-2026 \
        --bucket risklens-raw-risklens-frtb-2026 \
        --days 30

Usage (Dataproc):
    Submitted via refresh_data.sh (after bronze_rates + bronze_prices)
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bronze_synthetic")

# On Dataproc, generate.py is added as a --py-file (flat import).
# Locally it lives at ingestion/synthetic/generate.py.
try:
    from generate import (
        gen_assets_catalog,
        gen_desk_registry,
        gen_lineage,
        gen_ownership,
        gen_pipeline_logs,
        gen_pnl_vectors,
        gen_quality_scores,
        gen_schema_registry,
        gen_sla_status,
        gen_trades,
        gen_var_es,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from ingestion.synthetic.generate import (
        gen_assets_catalog,
        gen_desk_registry,
        gen_lineage,
        gen_ownership,
        gen_pipeline_logs,
        gen_pnl_vectors,
        gen_quality_scores,
        gen_schema_registry,
        gen_sla_status,
        gen_trades,
        gen_var_es,
    )

import pandas as pd
from datetime import timedelta


BRONZE_TABLES = {
    "risk_outputs":   "risklens_bronze.risk_outputs_s",
    "pipeline_logs":  "risklens_bronze.pipeline_logs_s",
    "ownership":      "risklens_catalog.ownership",
    "quality_scores": "risklens_catalog.quality_scores",
    "sla_status":     "risklens_catalog.sla_status",
    "assets":         "risklens_catalog.assets",
    "lineage_nodes":  "risklens_lineage.nodes",
    "lineage_edges":  "risklens_lineage.edges",
}


def write_to_bigquery(spark: SparkSession, df_pd: pd.DataFrame,
                      table: str, project: str, bucket: str, mode: str = "overwrite"):
    """Convert pandas DataFrame to Spark and write to BigQuery."""
    if df_pd.empty:
        log.warning(f"  Empty DataFrame for {table} — skipping.")
        return

    # Convert list/dict columns to JSON strings for BigQuery compatibility
    for col in df_pd.columns:
        if df_pd[col].dtype == object:
            sample = df_pd[col].dropna().head(1)
            if len(sample) > 0 and isinstance(sample.iloc[0], (list, dict)):
                df_pd[col] = df_pd[col].apply(
                    lambda x: str(x) if x is not None else None
                )

    # Fix mixed-type columns (e.g. str + NaN float from concat) that crash
    # Spark schema inference.  Convert any object column containing NaN to
    # nullable string so createDataFrame sees a uniform type.
    for col in df_pd.columns:
        if df_pd[col].dtype == object and df_pd[col].isna().any():
            df_pd[col] = df_pd[col].where(df_pd[col].notna(), None)

    # Fix mixed int+string columns (e.g. scenarios: 250 from gen_var_es
    # vs scenarios: "[1.2, ...]" from gen_pnl_vectors).  Cast to string
    # so Spark sees a uniform StringType.
    for col in df_pd.columns:
        if df_pd[col].dtype == object:
            non_null = df_pd[col].dropna()
            if len(non_null) > 0:
                has_numeric = any(isinstance(x, (int, float)) for x in non_null)
                has_string = any(isinstance(x, str) for x in non_null)
                if has_numeric and has_string:
                    df_pd[col] = df_pd[col].apply(
                        lambda x: str(x) if x is not None else None
                    )

    # Convert date/datetime-like string columns so Spark infers DateType
    # or TimestampType matching BQ's DATE/DATETIME column types.
    import re
    _date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    _datetime_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:")
    for col in df_pd.columns:
        if df_pd[col].dtype == object:
            sample = df_pd[col].dropna().head(3)
            if len(sample) == 0:
                continue
            sample_strs = [str(v) for v in sample]
            if all(_date_re.match(s) for s in sample_strs):
                df_pd[col] = pd.to_datetime(df_pd[col]).dt.date
            elif all(_datetime_re.match(s) for s in sample_strs):
                df_pd[col] = pd.to_datetime(df_pd[col])

    dataset, table_name = table.split(".")
    df_spark = spark.createDataFrame(df_pd)
    row_count = df_spark.count()

    (
        df_spark.write
        .format("bigquery")
        .option("project",     project)
        .option("dataset",     dataset)
        .option("table",       table_name)
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", bucket)
        .mode(mode)
        .save()
    )
    log.info(f"  {table}: {row_count:,} rows written")


def load_market_params(spark: SparkSession, project: str,
                       start_date: datetime, end_date: datetime) -> dict:
    """
    Read SOFR/DFF, VIX, and HY spread from bronze.rates_r for each date
    in [start_date, end_date]. Returns dict keyed by date string.

    Falls back to neutral defaults (SOFR=5, VIX=20, HY=3.5) for any missing date
    so synthetic generation always succeeds even with sparse real data.
    """
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    try:
        df = (
            spark.read.format("bigquery")
            .option("project", project)
            .option("dataset", "risklens_bronze")
            .option("table",   "rates_r")
            .load()
            .filter(
                (F.col("series_id").isin("SOFR", "DFF", "VIXCLS", "BAMLH0A0HYM2"))
                & (F.col("date").between(start_str, end_str))
                & (F.col("value").isNotNull())
            )
            .select(
                F.col("date").cast("string").alias("date"),
                "series_id",
                "value",
            )
        )
        rows = df.collect()
        log.info(f"  Loaded {len(rows)} market param rows from bronze.rates_r")
    except Exception as e:
        log.warning(f"  Could not read bronze.rates_r for seeding: {e}. Using neutral defaults.")
        return {}

    # Build per-date param dict
    params: dict[str, dict] = {}
    for row in rows:
        d = row["date"]
        if d not in params:
            params[d] = {}
        sid = row["series_id"]
        val = float(row["value"]) if row["value"] is not None else None
        if val is None:
            continue
        if sid in ("SOFR", "DFF") and "sofr" not in params[d]:
            params[d]["sofr"] = val           # prefer SOFR, fall back to DFF
        elif sid == "SOFR":
            params[d]["sofr"] = val           # SOFR always wins over DFF
        elif sid == "VIXCLS":
            params[d]["vix"] = val
        elif sid == "BAMLH0A0HYM2":
            params[d]["hy_spread"] = val

    # Forward-fill missing dates from nearest prior date
    defaults = {"sofr": 5.0, "vix": 20.0, "hy_spread": 3.5}
    last_known = dict(defaults)
    current = start_date
    while current <= end_date:
        d = current.strftime("%Y-%m-%d")
        if d in params:
            last_known = {**last_known, **params[d]}
        params[d] = dict(last_known)
        current += timedelta(days=1)

    log.info(f"  Market params ready for {len(params)} dates")
    return params


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--days",    type=int, default=1)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("RiskLens-Bronze-Synthetic")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    end_date   = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=args.days)

    log.info(f"Generating synthetic data for {args.days} days")

    # ── Load real market params to seed synthetic risk numbers ────────────────
    log.info("Loading market params from bronze.rates_r...")
    market_params = load_market_params(spark, args.project, start_date, end_date)

    # ── Daily data (generated per trading day) ────────────────────────────────
    all_var_es, all_pnl, all_logs, all_quality, all_sla = [], [], [], [], []

    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            date_str = current.strftime("%Y-%m-%d")
            params   = market_params.get(date_str)  # None falls back to defaults inside generators
            all_var_es.append(gen_var_es(current, params))
            all_pnl.append(gen_pnl_vectors(current, params))
            all_logs.append(gen_pipeline_logs(current))
            all_quality.append(gen_quality_scores(current))
            all_sla.append(gen_sla_status(current))
        current += timedelta(days=1)

    if all_var_es:
        risk_df = pd.concat(all_var_es + all_pnl, ignore_index=True)
        log.info(f"Writing risk outputs ({len(risk_df):,} rows)...")
        write_to_bigquery(spark, risk_df, "risklens_bronze.risk_outputs_s", args.project, args.bucket)

    if all_logs:
        logs_df = pd.concat(all_logs, ignore_index=True)
        log.info(f"Writing pipeline logs ({len(logs_df):,} rows)...")
        write_to_bigquery(spark, logs_df, "risklens_bronze.pipeline_logs_s", args.project, args.bucket)

    if all_quality:
        quality_df = pd.concat(all_quality, ignore_index=True)
        log.info(f"Writing quality scores ({len(quality_df):,} rows)...")
        write_to_bigquery(spark, quality_df, "risklens_catalog.quality_scores", args.project, args.bucket)

    if all_sla:
        sla_df = pd.concat(all_sla, ignore_index=True)
        log.info(f"Writing SLA status ({len(sla_df):,} rows)...")
        write_to_bigquery(spark, sla_df, "risklens_catalog.sla_status", args.project, args.bucket)

    # ── Static data (overwrite on each run) ───────────────────────────────────
    log.info("Writing ownership registry...")
    write_to_bigquery(spark, gen_ownership(), "risklens_catalog.ownership",
                      args.project, args.bucket, mode="overwrite")

    log.info("Writing asset catalog...")
    write_to_bigquery(spark, gen_assets_catalog(), "risklens_catalog.assets",
                      args.project, args.bucket, mode="overwrite")

    log.info("Writing lineage graph...")
    nodes, edges = gen_lineage()
    write_to_bigquery(spark, nodes, "risklens_lineage.nodes",
                      args.project, args.bucket, mode="overwrite")
    write_to_bigquery(spark, edges, "risklens_lineage.edges",
                      args.project, args.bucket, mode="overwrite")

    log.info("Writing schema registry...")
    write_to_bigquery(spark, gen_schema_registry(), "risklens_catalog.schema_registry",
                      args.project, args.bucket, mode="overwrite")

    log.info("Writing desk registry...")
    write_to_bigquery(spark, gen_desk_registry(), "risklens_catalog.desk_registry",
                      args.project, args.bucket, mode="overwrite")

    log.info("Bronze synthetic ingestion complete.")
    spark.stop()


if __name__ == "__main__":
    main()
