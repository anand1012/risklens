"""
RiskLens — Bronze Layer: Synthetic Internal Data Ingestion
Runs the synthetic data generator and loads results into BigQuery bronze layer.

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
    Submitted via refresh_data.sh
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bronze_synthetic")

# Add project root to path so we can import generate.py
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion.synthetic.generate import (
    gen_assets_catalog,
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
    "ownership":      "risklens_catalog.ownership_s",
    "quality_scores": "risklens_catalog.quality_scores_s",
    "sla_status":     "risklens_catalog.sla_status_s",
    "assets":         "risklens_catalog.assets_s",
    "lineage_nodes":  "risklens_lineage.nodes_s",
    "lineage_edges":  "risklens_lineage.edges_s",
}


def write_to_bigquery(spark: SparkSession, df_pd: pd.DataFrame,
                      table: str, project: str, bucket: str, mode: str = "append"):
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

    # ── Daily data (generated per trading day) ────────────────────────────────
    all_var_es, all_pnl, all_logs, all_quality, all_sla = [], [], [], [], []

    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            all_var_es.append(gen_var_es(current))
            all_pnl.append(gen_pnl_vectors(current))
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
        write_to_bigquery(spark, quality_df, "risklens_catalog.quality_scores_s", args.project, args.bucket)

    if all_sla:
        sla_df = pd.concat(all_sla, ignore_index=True)
        log.info(f"Writing SLA status ({len(sla_df):,} rows)...")
        write_to_bigquery(spark, sla_df, "risklens_catalog.sla_status_s", args.project, args.bucket)

    # ── Static data (overwrite on each run) ───────────────────────────────────
    log.info("Writing ownership registry...")
    write_to_bigquery(spark, gen_ownership(), "risklens_catalog.ownership_s",
                      args.project, args.bucket, mode="overwrite")

    log.info("Writing asset catalog...")
    write_to_bigquery(spark, gen_assets_catalog(), "risklens_catalog.assets_s",
                      args.project, args.bucket, mode="overwrite")

    log.info("Writing lineage graph...")
    nodes, edges = gen_lineage()
    write_to_bigquery(spark, nodes, "risklens_lineage.nodes_s",
                      args.project, args.bucket, mode="overwrite")
    write_to_bigquery(spark, edges, "risklens_lineage.edges_s",
                      args.project, args.bucket, mode="overwrite")

    log.info("Writing schema registry...")
    write_to_bigquery(spark, gen_schema_registry(), "risklens_catalog.schema_registry_s",
                      args.project, args.bucket, mode="overwrite")

    log.info("Bronze synthetic ingestion complete.")
    spark.stop()


if __name__ == "__main__":
    main()
