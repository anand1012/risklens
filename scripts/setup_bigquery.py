"""
RiskLens — BigQuery Schema Setup
Run once to create all datasets and tables with proper partitioning and clustering.

BigQuery partition/cluster strategy:
  - Partition: time-based (DAY) on the primary date column — enables partition pruning
  - Cluster: up to 4 high-cardinality filter columns — acts as z-ordering / bucketing
  - No partition on static/small lookup tables (ownership, schema_registry, lineage nodes)

Usage:
    python scripts/setup_bigquery.py --project YOUR_GCP_PROJECT_ID
"""

import argparse
from google.cloud import bigquery


def create_datasets(client: bigquery.Client, project: str):
    datasets = [
        "risklens_bronze",
        "risklens_silver",
        "risklens_gold",
        "risklens_catalog",
        "risklens_lineage",
        "risklens_embeddings",
    ]
    for ds_id in datasets:
        dataset = bigquery.Dataset(f"{project}.{ds_id}")
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"  Dataset ready: {ds_id}")


def _make_table(client: bigquery.Client, table_ref: str,
                schema: list[bigquery.SchemaField],
                partition_field: str | None = None,
                partition_type: str = "DAY",
                cluster_fields: list[str] | None = None):
    """Create a BigQuery table with optional partitioning and clustering."""
    table = bigquery.Table(table_ref, schema=schema)
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type,
            field=partition_field,
        )
    if cluster_fields:
        table.clustering_fields = cluster_fields
    client.create_table(table, exists_ok=True)
    label = table_ref.split(".", 1)[1]   # drop project prefix for display
    print(f"  Table ready: {label}")


# ── Bronze ────────────────────────────────────────────────────────────────────

def create_bronze_tables(client: bigquery.Client, project: str):
    base = f"{project}.risklens_bronze"

    _make_table(client, f"{base}.prices_r", [
        bigquery.SchemaField("ticker",       "STRING"),
        bigquery.SchemaField("name",         "STRING"),
        bigquery.SchemaField("asset_class",  "STRING"),
        bigquery.SchemaField("currency",     "STRING"),
        bigquery.SchemaField("date",         "DATE"),
        bigquery.SchemaField("open",         "FLOAT64"),
        bigquery.SchemaField("high",         "FLOAT64"),
        bigquery.SchemaField("low",          "FLOAT64"),
        bigquery.SchemaField("close",        "FLOAT64"),
        bigquery.SchemaField("adj_close",    "FLOAT64"),
        bigquery.SchemaField("volume",       "INT64"),
        bigquery.SchemaField("ingested_at",  "TIMESTAMP"),
        bigquery.SchemaField("trade_date",   "STRING"),
    ], partition_field="date", cluster_fields=["ticker", "asset_class", "currency"])

    _make_table(client, f"{base}.rates_r", [
        bigquery.SchemaField("series_id",    "STRING"),
        bigquery.SchemaField("series_name",  "STRING"),
        bigquery.SchemaField("frequency",    "STRING"),
        bigquery.SchemaField("domain",       "STRING"),
        bigquery.SchemaField("date",         "DATE"),
        bigquery.SchemaField("value",        "FLOAT64"),
        bigquery.SchemaField("ingested_at",  "TIMESTAMP"),
        bigquery.SchemaField("trade_date",   "STRING"),
    ], partition_field="date", cluster_fields=["series_id", "domain"])

    _make_table(client, f"{base}.trades_r", [
        bigquery.SchemaField("dissemination_id",          "STRING"),
        bigquery.SchemaField("original_dissemination_id", "STRING"),
        bigquery.SchemaField("action",                    "STRING"),
        bigquery.SchemaField("execution_timestamp",       "STRING"),
        bigquery.SchemaField("cleared",                   "STRING"),
        bigquery.SchemaField("asset_class",               "STRING"),
        bigquery.SchemaField("sub_asset_class",           "STRING"),
        bigquery.SchemaField("product_name",              "STRING"),
        bigquery.SchemaField("notional_currency_1",       "STRING"),
        bigquery.SchemaField("rounded_notional_amount_1", "STRING"),
        bigquery.SchemaField("underlying_asset_1",        "STRING"),
        bigquery.SchemaField("effective_date",            "STRING"),
        bigquery.SchemaField("end_date",                  "STRING"),
        bigquery.SchemaField("ingested_at",               "TIMESTAMP"),
        bigquery.SchemaField("source_file",               "STRING"),
        bigquery.SchemaField("trade_date",                "STRING"),
    ], partition_field="ingested_at", cluster_fields=["asset_class"])

    _make_table(client, f"{base}.risk_outputs_s", [
        bigquery.SchemaField("desk",          "STRING"),
        bigquery.SchemaField("calc_date",     "DATE"),
        bigquery.SchemaField("var_99_1d",     "FLOAT64"),
        bigquery.SchemaField("var_99_10d",    "FLOAT64"),
        bigquery.SchemaField("es_975_1d",     "FLOAT64"),
        bigquery.SchemaField("es_975_10d",    "FLOAT64"),
        bigquery.SchemaField("method",        "STRING"),
        bigquery.SchemaField("scenarios",     "STRING"),
        bigquery.SchemaField("num_scenarios", "INT64"),
        bigquery.SchemaField("mean_pnl",      "FLOAT64"),
        bigquery.SchemaField("std_pnl",       "FLOAT64"),
        bigquery.SchemaField("calculated_at", "TIMESTAMP"),
        bigquery.SchemaField("trade_date",    "STRING"),
    ], partition_field="calc_date", cluster_fields=["desk"])

    _make_table(client, f"{base}.pipeline_logs_s", [
        bigquery.SchemaField("run_id",       "STRING"),
        bigquery.SchemaField("job_name",     "STRING"),
        bigquery.SchemaField("status",       "STRING"),
        bigquery.SchemaField("rows_written", "INT64"),
        bigquery.SchemaField("duration_sec", "FLOAT64"),
        bigquery.SchemaField("run_date",     "DATE"),
        bigquery.SchemaField("started_at",   "TIMESTAMP"),
        bigquery.SchemaField("finished_at",  "TIMESTAMP"),
        bigquery.SchemaField("error_msg",    "STRING"),
    ], partition_field="run_date", cluster_fields=["job_name", "status"])

    _make_table(client, f"{base}.quarantine_r", [
        bigquery.SchemaField("source_table",      "STRING"),
        bigquery.SchemaField("rejection_reason",  "STRING"),
        bigquery.SchemaField("quarantined_at",    "TIMESTAMP"),
        bigquery.SchemaField("trade_date",        "STRING"),
    ], partition_field="quarantined_at", cluster_fields=["source_table", "rejection_reason"])


# ── Silver ────────────────────────────────────────────────────────────────────

def create_silver_tables(client: bigquery.Client, project: str):
    base = f"{project}.risklens_silver"

    _make_table(client, f"{base}.prices", [
        bigquery.SchemaField("ticker",      "STRING"),
        bigquery.SchemaField("name",        "STRING"),
        bigquery.SchemaField("asset_class", "STRING"),
        bigquery.SchemaField("currency",    "STRING"),
        bigquery.SchemaField("date",        "DATE"),
        bigquery.SchemaField("open",        "FLOAT64"),
        bigquery.SchemaField("high",        "FLOAT64"),
        bigquery.SchemaField("low",         "FLOAT64"),
        bigquery.SchemaField("close",       "FLOAT64"),
        bigquery.SchemaField("adj_close",   "FLOAT64"),
        bigquery.SchemaField("volume",      "INT64"),
        bigquery.SchemaField("trade_date",  "STRING"),
        bigquery.SchemaField("processed_at","TIMESTAMP"),
    ], partition_field="date", cluster_fields=["ticker", "asset_class", "currency"])

    _make_table(client, f"{base}.rates", [
        bigquery.SchemaField("series_id",   "STRING"),
        bigquery.SchemaField("series_name", "STRING"),
        bigquery.SchemaField("frequency",   "STRING"),
        bigquery.SchemaField("domain",      "STRING"),
        bigquery.SchemaField("date",        "DATE"),
        bigquery.SchemaField("value",       "FLOAT64"),
        bigquery.SchemaField("is_outlier",  "BOOL"),
        bigquery.SchemaField("trade_date",  "STRING"),
        bigquery.SchemaField("processed_at","TIMESTAMP"),
    ], partition_field="date", cluster_fields=["series_id", "domain"])

    _make_table(client, f"{base}.trades", [
        bigquery.SchemaField("dissemination_id",          "STRING"),
        bigquery.SchemaField("original_dissemination_id", "STRING"),
        bigquery.SchemaField("action",                    "STRING"),
        bigquery.SchemaField("execution_timestamp",       "TIMESTAMP"),
        bigquery.SchemaField("asset_class",               "STRING"),
        bigquery.SchemaField("sub_asset_class",           "STRING"),
        bigquery.SchemaField("product_name",              "STRING"),
        bigquery.SchemaField("currency",                  "STRING"),
        bigquery.SchemaField("notional_amount",           "FLOAT64"),
        bigquery.SchemaField("settlement_currency",       "STRING"),
        bigquery.SchemaField("underlying",                "STRING"),
        bigquery.SchemaField("effective_date",            "DATE"),
        bigquery.SchemaField("end_date",                  "DATE"),
        bigquery.SchemaField("trade_date",                "STRING"),
        bigquery.SchemaField("processed_at",              "TIMESTAMP"),
    ], partition_field="execution_timestamp", cluster_fields=["asset_class", "currency"])

    _make_table(client, f"{base}.risk_outputs", [
        bigquery.SchemaField("desk",          "STRING"),
        bigquery.SchemaField("calc_date",     "DATE"),
        bigquery.SchemaField("var_99_1d",     "FLOAT64"),
        bigquery.SchemaField("var_99_10d",    "FLOAT64"),
        bigquery.SchemaField("es_975_1d",     "FLOAT64"),
        bigquery.SchemaField("es_975_10d",    "FLOAT64"),
        bigquery.SchemaField("method",        "STRING"),
        bigquery.SchemaField("scenarios",     "STRING"),
        bigquery.SchemaField("num_scenarios", "INT64"),
        bigquery.SchemaField("mean_pnl",      "FLOAT64"),
        bigquery.SchemaField("std_pnl",       "FLOAT64"),
        bigquery.SchemaField("calculated_at", "TIMESTAMP"),
        bigquery.SchemaField("trade_date",    "STRING"),
        bigquery.SchemaField("processed_at",  "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk"])


# ── Gold ──────────────────────────────────────────────────────────────────────

def create_gold_tables(client: bigquery.Client, project: str):
    base = f"{project}.risklens_gold"

    _make_table(client, f"{base}.trade_positions", [
        bigquery.SchemaField("trade_id",         "STRING"),
        bigquery.SchemaField("currency",         "STRING"),
        bigquery.SchemaField("asset_class",      "STRING"),
        bigquery.SchemaField("underlying",       "STRING"),
        bigquery.SchemaField("notional_amount",  "FLOAT64"),
        bigquery.SchemaField("mark_price",       "FLOAT64"),
        bigquery.SchemaField("market_value_usd", "FLOAT64"),
        bigquery.SchemaField("discount_rate",    "FLOAT64"),
        bigquery.SchemaField("pv_usd",           "FLOAT64"),
        bigquery.SchemaField("trade_date",       "STRING"),
        bigquery.SchemaField("processed_at",     "TIMESTAMP"),
    ], partition_field="processed_at", cluster_fields=["asset_class", "currency"])

    _make_table(client, f"{base}.var_outputs", [
        bigquery.SchemaField("calc_date",    "DATE"),
        bigquery.SchemaField("desk",         "STRING"),
        bigquery.SchemaField("var_99_1d",    "FLOAT64"),
        bigquery.SchemaField("var_99_10d",   "FLOAT64"),
        bigquery.SchemaField("method",       "STRING"),
        bigquery.SchemaField("scenarios",    "STRING"),
        bigquery.SchemaField("trade_date",   "STRING"),
        bigquery.SchemaField("processed_at", "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk", "method"])

    _make_table(client, f"{base}.es_outputs", [
        bigquery.SchemaField("calc_date",    "DATE"),
        bigquery.SchemaField("desk",         "STRING"),
        bigquery.SchemaField("es_975_1d",    "FLOAT64"),
        bigquery.SchemaField("es_975_10d",   "FLOAT64"),
        bigquery.SchemaField("trade_date",   "STRING"),
        bigquery.SchemaField("processed_at", "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk"])

    _make_table(client, f"{base}.pnl_vectors", [
        bigquery.SchemaField("calc_date",     "DATE"),
        bigquery.SchemaField("desk",          "STRING"),
        bigquery.SchemaField("scenarios",     "STRING"),
        bigquery.SchemaField("num_scenarios", "INT64"),
        bigquery.SchemaField("mean_pnl",      "FLOAT64"),
        bigquery.SchemaField("std_pnl",       "FLOAT64"),
        bigquery.SchemaField("trade_date",    "STRING"),
        bigquery.SchemaField("processed_at",  "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk"])

    _make_table(client, f"{base}.risk_summary", [
        bigquery.SchemaField("calc_date",          "DATE"),
        bigquery.SchemaField("desk",               "STRING"),
        bigquery.SchemaField("var_99_1d",          "FLOAT64"),
        bigquery.SchemaField("var_99_10d",         "FLOAT64"),
        bigquery.SchemaField("es_975_1d",          "FLOAT64"),
        bigquery.SchemaField("es_975_10d",         "FLOAT64"),
        bigquery.SchemaField("mean_pnl",           "FLOAT64"),
        bigquery.SchemaField("std_pnl",            "FLOAT64"),
        bigquery.SchemaField("num_scenarios",      "INT64"),
        bigquery.SchemaField("plat_pass",          "BOOL"),
        bigquery.SchemaField("method",             "STRING"),
        bigquery.SchemaField("trade_date",         "STRING"),
        bigquery.SchemaField("report_generated_at","TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk", "method"])


# ── Catalog ───────────────────────────────────────────────────────────────────

def create_catalog_tables(client: bigquery.Client, project: str):
    base = f"{project}.risklens_catalog"

    # assets: no partition (small, ~13 rows); cluster by domain + layer for filter performance
    _make_table(client, f"{base}.assets", [
        bigquery.SchemaField("asset_id",    "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name",        "STRING", mode="REQUIRED"),
        bigquery.SchemaField("type",        "STRING"),
        bigquery.SchemaField("domain",      "STRING"),
        bigquery.SchemaField("layer",       "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("tags",        "STRING", mode="REPEATED"),
        bigquery.SchemaField("row_count",   "INTEGER"),
        bigquery.SchemaField("size_bytes",  "INTEGER"),
        bigquery.SchemaField("created_at",  "TIMESTAMP"),
        bigquery.SchemaField("updated_at",  "TIMESTAMP"),
    ], cluster_fields=["domain", "layer", "type"])

    # ownership_s: no partition (static lookup)
    _make_table(client, f"{base}.ownership", [
        bigquery.SchemaField("asset_id",      "STRING", mode="REQUIRED"),
        bigquery.SchemaField("owner_name",    "STRING"),
        bigquery.SchemaField("team",          "STRING"),
        bigquery.SchemaField("steward",       "STRING"),
        bigquery.SchemaField("email",         "STRING"),
        bigquery.SchemaField("assigned_date", "DATE"),
    ], cluster_fields=["team"])

    # quality_scores_s: partition by last_checked (appended each pipeline run)
    _make_table(client, f"{base}.quality_scores", [
        bigquery.SchemaField("asset_id",         "STRING", mode="REQUIRED"),
        bigquery.SchemaField("null_rate",         "FLOAT64"),
        bigquery.SchemaField("schema_drift",      "BOOL"),
        bigquery.SchemaField("freshness_status",  "STRING"),
        bigquery.SchemaField("duplicate_rate",    "FLOAT64"),
        bigquery.SchemaField("last_checked",      "TIMESTAMP"),
    ], partition_field="last_checked", cluster_fields=["asset_id", "freshness_status"])

    # sla_status_s: partition by checked_at
    _make_table(client, f"{base}.sla_status", [
        bigquery.SchemaField("asset_id",             "STRING", mode="REQUIRED"),
        bigquery.SchemaField("expected_refresh",     "TIMESTAMP"),
        bigquery.SchemaField("actual_refresh",       "TIMESTAMP"),
        bigquery.SchemaField("breach_flag",          "BOOL"),
        bigquery.SchemaField("breach_duration_mins", "INTEGER"),
        bigquery.SchemaField("checked_at",           "TIMESTAMP"),
    ], partition_field="checked_at", cluster_fields=["asset_id", "breach_flag"])

    # schema_registry_s: no partition (static, ~120 rows)
    _make_table(client, f"{base}.schema_registry", [
        bigquery.SchemaField("asset_id",     "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column_name",  "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_type",    "STRING"),
        bigquery.SchemaField("nullable",     "BOOL"),
        bigquery.SchemaField("sample_value", "STRING"),
        bigquery.SchemaField("description",  "STRING"),
    ], cluster_fields=["asset_id"])

    # access_log_r: partition by timestamp (high-volume event stream)
    _make_table(client, f"{base}.access_log", [
        bigquery.SchemaField("event_id",   "STRING", mode="REQUIRED"),
        bigquery.SchemaField("page",       "STRING"),
        bigquery.SchemaField("action",     "STRING"),
        bigquery.SchemaField("detail",     "STRING"),
        bigquery.SchemaField("ip_address", "STRING"),
        bigquery.SchemaField("country",    "STRING"),
        bigquery.SchemaField("city",       "STRING"),
        bigquery.SchemaField("browser",    "STRING"),
        bigquery.SchemaField("os",         "STRING"),
        bigquery.SchemaField("referrer",   "STRING"),
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("timestamp",  "TIMESTAMP"),
    ], partition_field="timestamp", cluster_fields=["page", "action"])


# ── Lineage ───────────────────────────────────────────────────────────────────

def create_lineage_tables(client: bigquery.Client, project: str):
    base = f"{project}.risklens_lineage"

    # nodes_s: no partition (small, ~13 nodes); cluster for type-based traversal
    _make_table(client, f"{base}.nodes", [
        bigquery.SchemaField("node_id",    "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name",       "STRING", mode="REQUIRED"),
        bigquery.SchemaField("type",       "STRING"),
        bigquery.SchemaField("domain",     "STRING"),
        bigquery.SchemaField("layer",      "STRING"),
        bigquery.SchemaField("metadata",   "JSON"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ], cluster_fields=["type", "domain"])

    # edges_s: cluster by relationship type for graph traversal queries
    _make_table(client, f"{base}.edges", [
        bigquery.SchemaField("edge_id",      "STRING", mode="REQUIRED"),
        bigquery.SchemaField("from_node_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("to_node_id",   "STRING", mode="REQUIRED"),
        bigquery.SchemaField("relationship", "STRING"),
        bigquery.SchemaField("pipeline_job", "STRING"),
        bigquery.SchemaField("created_at",   "TIMESTAMP"),
    ], cluster_fields=["relationship"])


# ── Embeddings ────────────────────────────────────────────────────────────────

def create_embeddings_tables(client: bigquery.Client, project: str):
    base = f"{project}.risklens_embeddings"

    _make_table(client, f"{base}.chunks", [
        bigquery.SchemaField("chunk_id",    "STRING", mode="REQUIRED"),
        bigquery.SchemaField("asset_id",    "STRING"),
        bigquery.SchemaField("text",        "STRING"),
        bigquery.SchemaField("source_type", "STRING"),
        bigquery.SchemaField("domain",      "STRING"),
        bigquery.SchemaField("created_at",  "TIMESTAMP"),
    ], cluster_fields=["source_type", "domain"])

    _make_table(client, f"{base}.vectors", [
        bigquery.SchemaField("chunk_id",  "STRING", mode="REQUIRED"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="GCP project ID")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    print("\n=== Creating datasets ===")
    create_datasets(client, args.project)

    print("\n=== Creating bronze tables ===")
    create_bronze_tables(client, args.project)

    print("\n=== Creating silver tables ===")
    create_silver_tables(client, args.project)

    print("\n=== Creating gold tables ===")
    create_gold_tables(client, args.project)

    print("\n=== Creating catalog tables ===")
    create_catalog_tables(client, args.project)

    print("\n=== Creating lineage tables ===")
    create_lineage_tables(client, args.project)

    print("\n=== Creating embeddings tables ===")
    create_embeddings_tables(client, args.project)

    print("\n✓ BigQuery schema setup complete.")


if __name__ == "__main__":
    main()
