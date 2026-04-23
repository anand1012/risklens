"""
RiskLens — Vertex AI Embedder
Embeds ChunkDocs using Vertex AI text-embedding-004 and writes results to BigQuery:
  - risklens_embeddings.chunks  — text + metadata
  - risklens_embeddings.vectors — chunk_id + embedding (FLOAT64 REPEATED, 768-dim)

Uses Workload Identity — no API key required.

Usage:
    from indexing.embedder import embed_and_store
    embed_and_store(chunks, project="risklens-frtb-2026")
"""

import logging
import os
import time
from datetime import datetime, timezone

import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from google.cloud import bigquery

from indexing.chunker import ChunkDoc

logger = logging.getLogger(__name__)

_EMBED_MODEL = "text-embedding-004"
_LOCATION = os.environ.get("GCP_REGION", "us-central1")
# text-embedding-004 limits: 250 texts per call, 20,000 tokens per call.
# With rich UI/workflow chunks averaging ~300 tokens each, batch_size=20
# keeps every call well under the 20k token ceiling (~6,000 tokens max).
_BATCH_SIZE = 20
# Hard character cap per chunk to avoid per-text token overflow (2,048 tokens ≈ 8,000 chars).
_MAX_CHARS = 7_500


def _init_vertexai(project: str) -> TextEmbeddingModel:
    logger.info(
        "→ _init_vertexai called",
        extra={"json_fields": {"project": project, "location": _LOCATION, "model": _EMBED_MODEL}},
    )
    vertexai.init(project=project, location=_LOCATION)
    model = TextEmbeddingModel.from_pretrained(_EMBED_MODEL)
    logger.info("← _init_vertexai done: model %s loaded", _EMBED_MODEL)
    return model


def _embed_batch(model: TextEmbeddingModel, texts: list[str], task: str) -> list[list[float]]:
    """Embed a batch of texts with the given task type."""
    logger.debug(
        "→ _embed_batch called",
        extra={"json_fields": {"batch_size": len(texts), "task": task}},
    )
    t0 = time.monotonic()
    inputs = [TextEmbeddingInput(text, task) for text in texts]
    results = model.get_embeddings(inputs)
    embeddings = [list(r.values) for r in results]
    latency_ms = int((time.monotonic() - t0) * 1000)
    logger.debug(
        "← _embed_batch done",
        extra={"json_fields": {"batch_size": len(texts), "embed_dim": len(embeddings[0]) if embeddings else 0, "latency_ms": latency_ms}},
    )
    return embeddings


def embed_and_store(
    chunks: list[ChunkDoc],
    project: str,
    truncate: bool = False,
) -> None:
    """
    Embed all chunks and upsert into BigQuery.

    Args:
        chunks:   Output of chunker.build_chunks()
        project:  GCP project ID
        truncate: If True, truncate BQ tables before inserting (full refresh)
    """
    logger.info(
        "→ embed_and_store called",
        extra={"json_fields": {"chunk_count": len(chunks), "project": project, "truncate": truncate}},
    )
    model = _init_vertexai(project)
    bq = bigquery.Client(project=project)

    chunks_table = f"{project}.risklens_embeddings.chunks"
    vectors_table = f"{project}.risklens_embeddings.vectors"

    if truncate:
        logger.info("Truncating embedding tables before full refresh")
        bq.query(f"TRUNCATE TABLE `{chunks_table}`").result()
        logger.info("Truncated: %s", chunks_table)
        bq.query(f"TRUNCATE TABLE `{vectors_table}`").result()
        logger.info("Truncated: %s", vectors_table)

    now = datetime.now(timezone.utc).isoformat()
    # Truncate any chunk that exceeds the per-text character limit
    oversized = [c for c in chunks if len(c.text) > _MAX_CHARS]
    if oversized:
        logger.warning(
            "Truncating %d oversized chunks to %d chars",
            len(oversized), _MAX_CHARS,
            extra={"json_fields": {"oversized_chunk_ids": [c.chunk_id for c in oversized[:5]]}},
        )
    texts = [c.text[:_MAX_CHARS] if len(c.text) > _MAX_CHARS else c.text for c in chunks]

    logger.info(
        "Embedding %d chunks with %s (batch_size=%d)",
        len(chunks), _EMBED_MODEL, _BATCH_SIZE,
        extra={"json_fields": {"chunk_count": len(chunks), "model": _EMBED_MODEL, "batch_size": _BATCH_SIZE}},
    )
    all_embeddings: list[list[float]] = []
    total_batches = (len(texts) + _BATCH_SIZE - 1) // _BATCH_SIZE
    t_embed_start = time.monotonic()

    for i in range(0, len(texts), _BATCH_SIZE):
        batch = texts[i : i + _BATCH_SIZE]
        batch_num = i // _BATCH_SIZE + 1
        logger.debug("Embedding batch %d/%d (%d texts)", batch_num, total_batches, len(batch))
        t_batch = time.monotonic()
        embeddings = _embed_batch(model, batch, "RETRIEVAL_DOCUMENT")
        batch_ms = int((time.monotonic() - t_batch) * 1000)
        all_embeddings.extend(embeddings)
        logger.info(
            "Embedded batch %d/%d: %d/%d done (%dms)",
            batch_num, total_batches, min(i + _BATCH_SIZE, len(texts)), len(texts), batch_ms,
            extra={"json_fields": {"batch": batch_num, "total_batches": total_batches, "latency_ms": batch_ms}},
        )

    total_embed_ms = int((time.monotonic() - t_embed_start) * 1000)
    logger.info(
        "All batches embedded",
        extra={"json_fields": {"total_embeddings": len(all_embeddings), "total_embed_ms": total_embed_ms}},
    )

    chunk_rows = [
        {
            "chunk_id": c.chunk_id,
            "asset_id": c.asset_id,
            "text": c.text,
            "source_type": c.source_type,
            "domain": c.domain,
            "created_at": now,
        }
        for c in chunks
    ]
    vector_rows = [
        {"chunk_id": c.chunk_id, "embedding": emb}
        for c, emb in zip(chunks, all_embeddings)
    ]

    logger.info(
        "Writing chunk rows to BigQuery",
        extra={"json_fields": {"row_count": len(chunk_rows), "table": chunks_table}},
    )
    t_bq = time.monotonic()
    errors = bq.insert_rows_json(chunks_table, chunk_rows)
    if errors:
        logger.error(
            "BigQuery insert errors (chunks): %s",
            errors,
            extra={"json_fields": {"error_count": len(errors), "table": chunks_table}},
        )
        raise RuntimeError(f"BigQuery insert errors (chunks): {errors}")
    chunk_bq_ms = int((time.monotonic() - t_bq) * 1000)
    logger.info("Chunks written in %dms", chunk_bq_ms)

    logger.info(
        "Writing vector rows to BigQuery",
        extra={"json_fields": {"row_count": len(vector_rows), "table": vectors_table}},
    )
    t_vec = time.monotonic()
    errors = bq.insert_rows_json(vectors_table, vector_rows)
    if errors:
        logger.error(
            "BigQuery insert errors (vectors): %s",
            errors,
            extra={"json_fields": {"error_count": len(errors), "table": vectors_table}},
        )
        raise RuntimeError(f"BigQuery insert errors (vectors): {errors}")
    vec_bq_ms = int((time.monotonic() - t_vec) * 1000)
    logger.info("Vectors written in %dms", vec_bq_ms)

    logger.info(
        "← embed_and_store done",
        extra={"json_fields": {
            "chunks_stored": len(chunk_rows),
            "vectors_stored": len(vector_rows),
            "total_embed_ms": total_embed_ms,
            "chunk_bq_ms": chunk_bq_ms,
            "vec_bq_ms": vec_bq_ms,
        }},
    )


def embed_query(query: str, project: str) -> list[float]:
    """
    Embed a single search query (RETRIEVAL_QUERY task type).
    Used at query time by the retriever.
    """
    logger.debug(
        "→ embed_query called",
        extra={"json_fields": {"query_preview": query[:80], "project": project}},
    )
    t0 = time.monotonic()
    model = _init_vertexai(project)
    results = _embed_batch(model, [query], "RETRIEVAL_QUERY")
    latency_ms = int((time.monotonic() - t0) * 1000)
    embedding = results[0]
    logger.debug(
        "← embed_query done",
        extra={"json_fields": {"embed_dim": len(embedding), "latency_ms": latency_ms}},
    )
    return embedding
