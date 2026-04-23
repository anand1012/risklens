"""
RiskLens — Document Chunker
Pulls catalog/lineage metadata from BigQuery and produces ChunkDoc objects
ready for embedding and BM25 indexing.

Sources:
  - risklens_catalog.assets       → one chunk per asset  (source_type=asset_desc)
  - risklens_catalog.schema_registry → one chunk per table schema (source_type=schema_doc)
  - risklens_lineage.nodes + edges   → pipeline lineage narrative (source_type=pipeline_doc)

Usage:
    from indexing.chunker import build_chunks
    chunks = build_chunks(project="risklens-frtb-2026")
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from google.cloud import bigquery

logger = logging.getLogger(__name__)


@dataclass
class ChunkDoc:
    chunk_id: str          # deterministic SHA-256 of text
    asset_id: str          # FK to catalog asset (or node_id for lineage)
    text: str              # the prose that gets embedded / BM25-indexed
    source_type: str       # asset_desc | schema_doc | pipeline_doc
    domain: str            # risk | market_data | reference | regulatory | lineage
    metadata: dict = field(default_factory=dict)  # extra info for retrieval context


def _chunk_id(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Chunk builders
# ---------------------------------------------------------------------------

def _chunk_catalog_assets(client: bigquery.Client, project: str) -> list[ChunkDoc]:
    """One chunk per catalog asset — name, type, domain, layer, description, tags."""
    logger.info("→ _chunk_catalog_assets called", extra={"json_fields": {"project": project}})
    query = f"""
        SELECT
            a.asset_id,
            a.name,
            a.type,
            a.domain,
            a.layer,
            a.description,
            a.tags,
            o.owner_name,
            o.team
        FROM `{project}.risklens_catalog.assets` a
        LEFT JOIN `{project}.risklens_catalog.ownership` o USING (asset_id)
    """
    rows = list(client.query(query).result())
    logger.debug("_chunk_catalog_assets: query returned %d asset rows", len(rows))
    chunks: list[ChunkDoc] = []

    for row in rows:
        tags_str = ", ".join(row.tags) if row.tags else "none"
        text = (
            f"Asset: {row.name}\n"
            f"Type: {row.type or 'unknown'}\n"
            f"Domain: {row.domain or 'unknown'}\n"
            f"Layer: {row.layer or 'unknown'}\n"
            f"Description: {row.description or 'No description provided.'}\n"
            f"Tags: {tags_str}\n"
            f"Owner: {row.owner_name or 'unassigned'} ({row.team or 'unknown team'})"
        )
        chunks.append(ChunkDoc(
            chunk_id=_chunk_id(text),
            asset_id=row.asset_id,
            text=text,
            source_type="asset_desc",
            domain=row.domain or "unknown",
            metadata={
                "name": row.name,
                "type": row.type,
                "layer": row.layer,
                "owner": row.owner_name,
                "team": row.team,
            },
        ))

    logger.info(
        "← _chunk_catalog_assets done",
        extra={"json_fields": {"chunk_count": len(chunks)}},
    )
    if not chunks:
        logger.warning("_chunk_catalog_assets produced 0 chunks — is risklens_catalog.assets populated?")
    return chunks


def _chunk_schema_registry(client: bigquery.Client, project: str) -> list[ChunkDoc]:
    """One chunk per table — all columns collapsed into a single schema narrative."""
    logger.info("→ _chunk_schema_registry called", extra={"json_fields": {"project": project}})
    query = f"""
        SELECT
            sr.asset_id,
            a.name AS asset_name,
            a.domain,
            a.layer,
            ARRAY_AGG(
                STRUCT(sr.column_name, sr.data_type, sr.nullable, sr.description, sr.sample_value)
                ORDER BY sr.column_name
            ) AS columns
        FROM `{project}.risklens_catalog.schema_registry` sr
        LEFT JOIN `{project}.risklens_catalog.assets` a USING (asset_id)
        GROUP BY sr.asset_id, a.name, a.domain, a.layer
    """
    rows = list(client.query(query).result())
    logger.debug("_chunk_schema_registry: query returned %d table rows", len(rows))
    chunks: list[ChunkDoc] = []

    for row in rows:
        col_lines = []
        for col in row.columns:
            # BQ returns STRUCT elements as Row objects or dicts depending on client version
            _get = (lambda f: col.get(f)) if isinstance(col, dict) else (lambda f: getattr(col, f, None))
            nullable = "nullable" if _get("nullable") else "required"
            desc = _get("description") or ""
            sample_val = _get("sample_value")
            sample = f" (e.g. {sample_val})" if sample_val else ""
            col_lines.append(
                f"  - {_get('column_name')} ({_get('data_type')}, {nullable}){sample}: {desc}"
            )
        columns_text = "\n".join(col_lines)
        text = (
            f"Schema for {row.asset_name or row.asset_id}:\n"
            f"Domain: {row.domain or 'unknown'} | Layer: {row.layer or 'unknown'}\n"
            f"Columns:\n{columns_text}"
        )
        col_count = len(row.columns)
        logger.debug("_chunk_schema_registry: asset=%s columns=%d", row.asset_id, col_count)
        chunks.append(ChunkDoc(
            chunk_id=_chunk_id(text),
            asset_id=row.asset_id,
            text=text,
            source_type="schema_doc",
            domain=row.domain or "unknown",
            metadata={
                "asset_name": row.asset_name,
                "column_count": col_count,
            },
        ))

    logger.info(
        "← _chunk_schema_registry done",
        extra={"json_fields": {"chunk_count": len(chunks)}},
    )
    if not chunks:
        logger.warning("_chunk_schema_registry produced 0 chunks")
    return chunks


def _chunk_lineage(client: bigquery.Client, project: str) -> list[ChunkDoc]:
    """One chunk per lineage node augmented with its upstream/downstream edges."""
    logger.info("→ _chunk_lineage called", extra={"json_fields": {"project": project}})
    nodes_query = f"""
        SELECT node_id, name, type, domain, layer, metadata
        FROM `{project}.risklens_lineage.nodes`
    """
    edges_query = f"""
        SELECT from_node_id, to_node_id, relationship, pipeline_job
        FROM `{project}.risklens_lineage.edges`
    """

    nodes = {row.node_id: row for row in client.query(nodes_query).result()}
    edges = list(client.query(edges_query).result())
    logger.debug("_chunk_lineage: loaded %d nodes and %d edges", len(nodes), len(edges))

    # Build adjacency: upstream (in-edges) and downstream (out-edges) per node
    upstream: dict[str, list[str]] = {nid: [] for nid in nodes}
    downstream: dict[str, list[str]] = {nid: [] for nid in nodes}
    pipelines: dict[str, list[str]] = {nid: [] for nid in nodes}

    for e in edges:
        if e.to_node_id in downstream:
            upstream[e.to_node_id].append(
                f"{nodes[e.from_node_id].name if e.from_node_id in nodes else e.from_node_id}"
                f" [{e.relationship}]"
            )
        if e.from_node_id in downstream:
            downstream[e.from_node_id].append(
                f"{nodes[e.to_node_id].name if e.to_node_id in nodes else e.to_node_id}"
                f" [{e.relationship}]"
            )
            if e.pipeline_job:
                pipelines[e.from_node_id].append(e.pipeline_job)

    chunks: list[ChunkDoc] = []
    for node_id, row in nodes.items():
        meta_str = ""
        if row.metadata:
            try:
                meta_dict = json.loads(row.metadata) if isinstance(row.metadata, str) else row.metadata
                meta_str = "\n".join(f"  {k}: {v}" for k, v in meta_dict.items())
            except Exception:
                meta_str = str(row.metadata)

        up_str = ", ".join(upstream[node_id]) or "none"
        down_str = ", ".join(downstream[node_id]) or "none"
        pipe_str = ", ".join(set(pipelines[node_id])) or "none"

        text = (
            f"Lineage node: {row.name}\n"
            f"Type: {row.type or 'unknown'} | Domain: {row.domain or 'unknown'} | Layer: {row.layer or 'unknown'}\n"
            f"Upstream sources: {up_str}\n"
            f"Downstream consumers: {down_str}\n"
            f"Pipeline jobs: {pipe_str}"
        )
        if meta_str:
            text += f"\nMetadata:\n{meta_str}"

        chunks.append(ChunkDoc(
            chunk_id=_chunk_id(text),
            asset_id=node_id,
            text=text,
            source_type="pipeline_doc",
            domain=row.domain or "lineage",
            metadata={
                "name": row.name,
                "type": row.type,
                "layer": row.layer,
                "upstream_count": len(upstream[node_id]),
                "downstream_count": len(downstream[node_id]),
            },
        ))

    logger.info(
        "← _chunk_lineage done",
        extra={"json_fields": {"chunk_count": len(chunks), "node_count": len(nodes), "edge_count": len(edges)}},
    )
    if not chunks:
        logger.warning("_chunk_lineage produced 0 chunks")
    return chunks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_chunks(project: str) -> list[ChunkDoc]:
    """Pull all BigQuery metadata and return the full corpus of ChunkDocs."""
    logger.info("→ build_chunks called", extra={"json_fields": {"project": project}})
    client = bigquery.Client(project=project)
    logger.info("Building document corpus from BigQuery project: %s", project)

    chunks: list[ChunkDoc] = []

    catalog_chunks = _chunk_catalog_assets(client, project)
    chunks.extend(catalog_chunks)
    logger.debug("build_chunks: after catalog assets: %d chunks total", len(chunks))

    schema_chunks = _chunk_schema_registry(client, project)
    chunks.extend(schema_chunks)
    logger.debug("build_chunks: after schema registry: %d chunks total", len(chunks))

    lineage_chunks = _chunk_lineage(client, project)
    chunks.extend(lineage_chunks)
    logger.debug("build_chunks: after lineage: %d chunks total", len(chunks))

    # Deduplicate by chunk_id (same text from multiple sources)
    seen: set[str] = set()
    unique: list[ChunkDoc] = []
    dupes = 0
    for c in chunks:
        if c.chunk_id not in seen:
            seen.add(c.chunk_id)
            unique.append(c)
        else:
            dupes += 1

    if dupes:
        logger.warning("build_chunks: deduplicated %d duplicate chunks", dupes)
    logger.info(
        "← build_chunks done",
        extra={"json_fields": {
            "total_before_dedup": len(chunks),
            "unique_chunks": len(unique),
            "duplicates_removed": dupes,
            "catalog_chunks": len(catalog_chunks),
            "schema_chunks": len(schema_chunks),
            "lineage_chunks": len(lineage_chunks),
        }},
    )
    return unique
