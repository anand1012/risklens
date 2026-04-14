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

    # backtesting: VaR 99% for back-testing ONLY (BCBS 457 ¶351-368)
    # Not used for capital calculation — ES 97.5% is the capital metric
    _make_table(client, f"{base}.backtesting", [
        bigquery.SchemaField("calc_date",              "DATE"),
        bigquery.SchemaField("desk",                   "STRING"),
        bigquery.SchemaField("var_99_1d",              "FLOAT64"),
        bigquery.SchemaField("var_99_10d",             "FLOAT64"),
        bigquery.SchemaField("hypothetical_pnl",       "FLOAT64"),
        bigquery.SchemaField("actual_pnl",             "FLOAT64"),
        bigquery.SchemaField("hypothetical_exception", "INT64"),
        bigquery.SchemaField("actual_exception",       "INT64"),
        bigquery.SchemaField("exception_count_250d",   "INT64"),
        bigquery.SchemaField("traffic_light_zone",     "STRING"),  # GREEN/AMBER/RED
        bigquery.SchemaField("capital_multiplier",     "FLOAT64"), # 0.00/0.75/1.00
        bigquery.SchemaField("method",                 "STRING"),
        bigquery.SchemaField("trade_date",             "STRING"),
        bigquery.SchemaField("processed_at",           "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk", "traffic_light_zone"])

    # es_outputs: ES 97.5% per desk × risk class × liquidity horizon (BCBS 457 ¶21-34)
    _make_table(client, f"{base}.es_outputs", [
        bigquery.SchemaField("calc_date",         "DATE"),
        bigquery.SchemaField("desk",              "STRING"),
        bigquery.SchemaField("risk_class",        "STRING"),  # GIRR/FX/CSR_NS/EQ/COMM
        bigquery.SchemaField("liquidity_horizon", "INT64"),   # 10/20/40/60 days
        bigquery.SchemaField("es_975_1d",         "FLOAT64"),
        bigquery.SchemaField("es_975_10d",        "FLOAT64"),
        bigquery.SchemaField("es_975_scaled",     "FLOAT64"), # ES × sqrt(liquidity_horizon)
        bigquery.SchemaField("trade_date",        "STRING"),
        bigquery.SchemaField("processed_at",      "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk", "risk_class"])

    # pnl_vectors: hypothetical + actual P&L per desk (PLAT input, BCBS 457 ¶329-345)
    _make_table(client, f"{base}.pnl_vectors", [
        bigquery.SchemaField("calc_date",         "DATE"),
        bigquery.SchemaField("desk",              "STRING"),
        bigquery.SchemaField("risk_class",        "STRING"),
        bigquery.SchemaField("hypothetical_pnl",  "FLOAT64"),  # risk-factor P&L only
        bigquery.SchemaField("actual_pnl",        "FLOAT64"),  # realized trader P&L
        bigquery.SchemaField("pnl_unexplained",   "FLOAT64"),  # actual - hypothetical
        bigquery.SchemaField("scenarios",         "STRING"),
        bigquery.SchemaField("num_scenarios",     "INT64"),
        bigquery.SchemaField("mean_pnl",          "FLOAT64"),
        bigquery.SchemaField("std_pnl",           "FLOAT64"),
        bigquery.SchemaField("trade_date",        "STRING"),
        bigquery.SchemaField("processed_at",      "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk", "risk_class"])

    # plat_results: P&L Attribution Test — 3 statistical tests (BCBS 457 ¶329-345)
    _make_table(client, f"{base}.plat_results", [
        bigquery.SchemaField("calc_date",             "DATE"),
        bigquery.SchemaField("desk",                  "STRING"),
        bigquery.SchemaField("window_start_date",     "DATE"),
        bigquery.SchemaField("window_end_date",       "DATE"),
        bigquery.SchemaField("observation_count",     "INT64"),
        bigquery.SchemaField("hyp_pnl_mean",          "FLOAT64"),
        bigquery.SchemaField("hyp_pnl_std",           "FLOAT64"),
        bigquery.SchemaField("actual_pnl_mean",       "FLOAT64"),
        bigquery.SchemaField("upl_ratio",             "FLOAT64"), # pass if < 0.95
        bigquery.SchemaField("upl_pass",              "BOOL"),
        bigquery.SchemaField("spearman_correlation",  "FLOAT64"), # pass if > 0.40
        bigquery.SchemaField("spearman_pass",         "BOOL"),
        bigquery.SchemaField("ks_statistic",          "FLOAT64"), # pass if < 0.20
        bigquery.SchemaField("ks_pass",               "BOOL"),
        bigquery.SchemaField("plat_pass",             "BOOL"),    # True if all 3 pass
        bigquery.SchemaField("notes",                 "STRING"),
        bigquery.SchemaField("trade_date",            "STRING"),
        bigquery.SchemaField("processed_at",          "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk", "plat_pass"])

    # capital_charge: regulatory capital = ES × (3.0 + multiplier) (BCBS 457 ¶180-186)
    _make_table(client, f"{base}.capital_charge", [
        bigquery.SchemaField("calc_date",          "DATE"),
        bigquery.SchemaField("desk",               "STRING"),
        bigquery.SchemaField("risk_class",         "STRING"),
        bigquery.SchemaField("liquidity_horizon",  "INT64"),
        bigquery.SchemaField("es_975_1d",          "FLOAT64"),
        bigquery.SchemaField("es_975_scaled",      "FLOAT64"),
        bigquery.SchemaField("traffic_light_zone", "STRING"),
        bigquery.SchemaField("capital_multiplier", "FLOAT64"),
        bigquery.SchemaField("regulatory_floor",   "FLOAT64"), # ES × 3.0
        bigquery.SchemaField("capital_charge_usd", "FLOAT64"), # ES × (3.0 + multiplier)
        bigquery.SchemaField("exception_count_250d","INT64"),
        bigquery.SchemaField("trade_date",         "STRING"),
        bigquery.SchemaField("processed_at",       "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk", "risk_class"])

    # rfet_results: Risk Factor Eligibility Test (BCBS 457 ¶76-80)
    _make_table(client, f"{base}.rfet_results", [
        bigquery.SchemaField("rfet_date",             "DATE"),
        bigquery.SchemaField("risk_factor_id",        "STRING"),
        bigquery.SchemaField("risk_class",            "STRING"),
        bigquery.SchemaField("obs_12m_count",         "INT64"),   # need >= 75
        bigquery.SchemaField("obs_90d_count",         "INT64"),   # need >= 25
        bigquery.SchemaField("obs_12m_pass",          "BOOL"),
        bigquery.SchemaField("obs_90d_pass",          "BOOL"),
        bigquery.SchemaField("rfet_pass",             "BOOL"),
        bigquery.SchemaField("eligible_for_ima",      "BOOL"),
        bigquery.SchemaField("last_observation_date", "DATE"),
        bigquery.SchemaField("staleness_days",        "INT64"),
        bigquery.SchemaField("failure_reason",        "STRING"),
        bigquery.SchemaField("processed_at",          "TIMESTAMP"),
    ], partition_field="rfet_date", cluster_fields=["risk_class", "rfet_pass"])

    # risk_summary: consolidated daily report (ES capital + PLAT + back-testing)
    _make_table(client, f"{base}.risk_summary", [
        bigquery.SchemaField("calc_date",             "DATE"),
        bigquery.SchemaField("desk",                  "STRING"),
        bigquery.SchemaField("risk_class",            "STRING"),
        bigquery.SchemaField("liquidity_horizon",     "INT64"),
        bigquery.SchemaField("var_99_1d",             "FLOAT64"),   # back-testing ref only
        bigquery.SchemaField("var_99_10d",            "FLOAT64"),
        bigquery.SchemaField("traffic_light_zone",    "STRING"),
        bigquery.SchemaField("exception_count_250d",  "INT64"),
        bigquery.SchemaField("es_975_1d",             "FLOAT64"),   # capital metric
        bigquery.SchemaField("es_975_10d",            "FLOAT64"),
        bigquery.SchemaField("es_975_scaled",         "FLOAT64"),
        bigquery.SchemaField("plat_pass",             "BOOL"),
        bigquery.SchemaField("upl_ratio",             "FLOAT64"),
        bigquery.SchemaField("spearman_correlation",  "FLOAT64"),
        bigquery.SchemaField("ks_statistic",          "FLOAT64"),
        bigquery.SchemaField("plat_notes",            "STRING"),
        bigquery.SchemaField("capital_charge_usd",    "FLOAT64"),
        bigquery.SchemaField("capital_multiplier",    "FLOAT64"),
        bigquery.SchemaField("trade_date",            "STRING"),
        bigquery.SchemaField("report_generated_at",   "TIMESTAMP"),
    ], partition_field="calc_date", cluster_fields=["desk", "risk_class"])


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

    # desk_registry: FRTB IMA desk-level model approval and governance
    _make_table(client, f"{base}.desk_registry", [
        bigquery.SchemaField("desk_id",           "STRING", mode="REQUIRED"),
        bigquery.SchemaField("desk_name",         "STRING"),
        bigquery.SchemaField("risk_class",        "STRING"),  # primary risk class
        bigquery.SchemaField("business_line",     "STRING"),
        bigquery.SchemaField("approved_by",       "STRING"),
        bigquery.SchemaField("approval_date",     "DATE"),
        bigquery.SchemaField("model_type",        "STRING"),  # Historical Sim / Monte Carlo
        bigquery.SchemaField("num_scenarios",     "INT64"),
        bigquery.SchemaField("risk_limit_usd",    "FLOAT64"),
        bigquery.SchemaField("traffic_light_zone","STRING"),
        bigquery.SchemaField("last_reviewed_date","DATE"),
        bigquery.SchemaField("created_at",        "TIMESTAMP"),
    ], cluster_fields=["risk_class", "business_line"])

    # access_log: partition by timestamp (high-volume event stream)
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
