#!/usr/bin/env python3
"""Dump BigQuery table stats for all RiskLens datasets to docs/table_stats.{json,md}.

Reproducible generator for the local stats cache. Run whenever table state changes:

    python3 scripts/dump_table_stats.py

Reads from BQ via ADC; writes docs/table_stats.json and docs/table_stats.md.
The JSON is the source of truth; the markdown is a human-readable rendering.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

PROJECT = "risklens-frtb-2026"
DATASETS = [
    "risklens_bronze",
    "risklens_silver",
    "risklens_gold",
    "risklens_catalog",
    "risklens_lineage",
    "risklens_embeddings",
]
REPO_ROOT = Path(__file__).resolve().parent.parent
JSON_PATH = REPO_ROOT / "docs" / "table_stats.json"
MD_PATH = REPO_ROOT / "docs" / "table_stats.md"


def collect() -> dict:
    client = bigquery.Client(project=PROJECT)
    out: dict = {
        "project": PROJECT,
        "collected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "datasets": {},
    }
    for ds_name in DATASETS:
        ds_ref = f"{PROJECT}.{ds_name}"
        tables_out: dict = {}
        for t in sorted(client.list_tables(ds_ref), key=lambda x: x.table_id):
            full = client.get_table(f"{ds_ref}.{t.table_id}")
            tables_out[t.table_id] = {
                "full_id": f"{ds_name}.{t.table_id}",
                "num_rows": full.num_rows or 0,
                "num_bytes": full.num_bytes or 0,
                "size_mb": round((full.num_bytes or 0) / (1024 * 1024), 3),
                "created": full.created.isoformat() if full.created else None,
                "modified": full.modified.isoformat() if full.modified else None,
                "partition_field": full.time_partitioning.field if full.time_partitioning else None,
                "partition_type": full.time_partitioning.type_ if full.time_partitioning else None,
                "clustering_fields": ", ".join(full.clustering_fields) if full.clustering_fields else None,
                "num_columns": len(full.schema),
                "columns": [
                    {
                        "name": f.name,
                        "type": f.field_type,
                        "mode": f.mode,
                        "description": f.description or "",
                    }
                    for f in full.schema
                ],
            }
        out["datasets"][ds_name] = {"tables": tables_out}
    return out


def render_markdown(stats: dict) -> str:
    total_tables = sum(len(ds["tables"]) for ds in stats["datasets"].values())
    total_rows = sum(
        t["num_rows"] for ds in stats["datasets"].values() for t in ds["tables"].values()
    )
    total_mb = sum(
        t["size_mb"] for ds in stats["datasets"].values() for t in ds["tables"].values()
    )

    lines: list[str] = []
    lines.append("# RiskLens BigQuery Table Stats\n")
    lines.append(f"**Project:** `{stats['project']}`  ")
    lines.append(
        f"**Summary:** {total_tables} tables across {len(stats['datasets'])} datasets "
        f"| {total_rows:,} total rows | {total_mb:,.3f} MB total  "
    )
    lines.append(f"**Collected:** {stats['collected_at']}  \n")

    for ds_name, ds in stats["datasets"].items():
        ds_rows = sum(t["num_rows"] for t in ds["tables"].values())
        ds_mb = sum(t["size_mb"] for t in ds["tables"].values())
        lines.append(f"## {ds_name}")
        lines.append(
            f"**{len(ds['tables'])} tables** | {ds_rows:,} total rows | {ds_mb:,.3f} MB total\n"
        )
        lines.append(
            "| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |"
        )
        lines.append("|-------|------|-----------|----------------|------------|------|-------------|")
        for tbl_name, t in ds["tables"].items():
            part = f"`{t['partition_field']}`" if t["partition_field"] else "`-`"
            clust = t["clustering_fields"] or "-"
            key_cols = ", ".join(c["name"] for c in t["columns"][:5])
            lines.append(
                f"| `{tbl_name}` | {t['num_rows']:,} | {t['size_mb']} | {part} | {clust} | {t['num_columns']} | {key_cols} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    print(f"Collecting stats from {PROJECT}...")
    stats = collect()
    JSON_PATH.write_text(json.dumps(stats, indent=2, default=str))
    MD_PATH.write_text(render_markdown(stats))
    total_tables = sum(len(ds["tables"]) for ds in stats["datasets"].values())
    total_rows = sum(
        t["num_rows"] for ds in stats["datasets"].values() for t in ds["tables"].values()
    )
    total_mb = sum(
        t["size_mb"] for ds in stats["datasets"].values() for t in ds["tables"].values()
    )
    print(f"Wrote {JSON_PATH.relative_to(REPO_ROOT)}")
    print(f"Wrote {MD_PATH.relative_to(REPO_ROOT)}")
    print(f"Total: {total_tables} tables | {total_rows:,} rows | {total_mb:,.3f} MB")


if __name__ == "__main__":
    main()
