"""
RiskLens — BigQuery Schema Setup
Run once to create all datasets and tables.

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


def create_catalog_tables(client: bigquery.Client, project: str):
    tables = {
        "assets_s": [
            bigquery.SchemaField("asset_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING"),        # table, feed, report, model
            bigquery.SchemaField("domain", "STRING"),      # risk, market_data, reference, regulatory
            bigquery.SchemaField("layer", "STRING"),       # bronze, silver, gold
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            bigquery.SchemaField("row_count", "INTEGER"),
            bigquery.SchemaField("size_bytes", "INTEGER"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ],
        "ownership_s": [
            bigquery.SchemaField("asset_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("owner_name", "STRING"),
            bigquery.SchemaField("team", "STRING"),
            bigquery.SchemaField("steward", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("assigned_date", "DATE"),
        ],
        "quality_scores_s": [
            bigquery.SchemaField("asset_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("null_rate", "FLOAT64"),
            bigquery.SchemaField("schema_drift", "BOOL"),
            bigquery.SchemaField("freshness_status", "STRING"),  # fresh, stale, critical
            bigquery.SchemaField("duplicate_rate", "FLOAT64"),
            bigquery.SchemaField("last_checked", "TIMESTAMP"),
        ],
        "sla_status_s": [
            bigquery.SchemaField("asset_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("expected_refresh", "TIMESTAMP"),
            bigquery.SchemaField("actual_refresh", "TIMESTAMP"),
            bigquery.SchemaField("breach_flag", "BOOL"),
            bigquery.SchemaField("breach_duration_mins", "INTEGER"),
            bigquery.SchemaField("checked_at", "TIMESTAMP"),
        ],
        "schema_registry_s": [
            bigquery.SchemaField("asset_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("data_type", "STRING"),
            bigquery.SchemaField("nullable", "BOOL"),
            bigquery.SchemaField("sample_value", "STRING"),
            bigquery.SchemaField("description", "STRING"),
        ],
        "access_log_r": [
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("page", "STRING"),
            bigquery.SchemaField("action", "STRING"),    # page_view, asset_click, chat_query, search
            bigquery.SchemaField("detail", "STRING"),    # asset_id, query text, search term
            bigquery.SchemaField("ip_address", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("browser", "STRING"),
            bigquery.SchemaField("os", "STRING"),
            bigquery.SchemaField("referrer", "STRING"),
            bigquery.SchemaField("session_id", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ],
    }

    for table_name, schema in tables.items():
        table_ref = f"{project}.risklens_catalog.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"  Table ready: risklens_catalog.{table_name}")


def create_lineage_tables(client: bigquery.Client, project: str):
    tables = {
        "nodes_s": [
            bigquery.SchemaField("node_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING"),   # source, pipeline, table, report
            bigquery.SchemaField("domain", "STRING"),
            bigquery.SchemaField("layer", "STRING"),
            bigquery.SchemaField("metadata", "JSON"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        "edges_s": [
            bigquery.SchemaField("edge_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("from_node_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("to_node_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("relationship", "STRING"),   # feeds, transforms, aggregates
            bigquery.SchemaField("pipeline_job", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
    }

    for table_name, schema in tables.items():
        table_ref = f"{project}.risklens_lineage.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"  Table ready: risklens_lineage.{table_name}")


def create_embeddings_tables(client: bigquery.Client, project: str):
    tables = {
        "chunks_s": [
            bigquery.SchemaField("chunk_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("asset_id", "STRING"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("source_type", "STRING"),  # asset_desc, schema_doc, fred_desc, pipeline_doc
            bigquery.SchemaField("domain", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ],
        "vectors_s": [
            bigquery.SchemaField("chunk_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
        ],
    }

    for table_name, schema in tables.items():
        table_ref = f"{project}.risklens_embeddings.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"  Table ready: risklens_embeddings.{table_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="GCP project ID")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    print("\n=== Creating datasets ===")
    create_datasets(client, args.project)

    print("\n=== Creating catalog tables ===")
    create_catalog_tables(client, args.project)

    print("\n=== Creating lineage tables ===")
    create_lineage_tables(client, args.project)

    print("\n=== Creating embeddings tables ===")
    create_embeddings_tables(client, args.project)

    print("\n✓ BigQuery schema setup complete.")


if __name__ == "__main__":
    main()
