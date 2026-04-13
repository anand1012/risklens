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
from datetime import datetime, timezone

import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from google.cloud import bigquery

from indexing.chunker import ChunkDoc

logger = logging.getLogger(__name__)

_EMBED_MODEL = "text-embedding-004"
_LOCATION = os.environ.get("GCP_REGION", "us-central1")
# Vertex AI max batch size for text-embedding-004
_BATCH_SIZE = 250


def _init_vertexai(project: str) -> TextEmbeddingModel:
    vertexai.init(project=project, location=_LOCATION)
    return TextEmbeddingModel.from_pretrained(_EMBED_MODEL)


def _embed_batch(model: TextEmbeddingModel, texts: list[str], task: str) -> list[list[float]]:
    """Embed a batch of texts with the given task type."""
    inputs = [TextEmbeddingInput(text, task) for text in texts]
    results = model.get_embeddings(inputs)
    return [list(r.values) for r in results]


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
    model = _init_vertexai(project)
    bq = bigquery.Client(project=project)

    chunks_table = f"{project}.risklens_embeddings.chunks"
    vectors_table = f"{project}.risklens_embeddings.vectors"

    if truncate:
        logger.info("Truncating embedding tables…")
        bq.query(f"TRUNCATE TABLE `{chunks_table}`").result()
        bq.query(f"TRUNCATE TABLE `{vectors_table}`").result()

    now = datetime.now(timezone.utc).isoformat()
    texts = [c.text for c in chunks]

    logger.info("Embedding %d chunks with %s (batches of %d)…", len(chunks), _EMBED_MODEL, _BATCH_SIZE)
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), _BATCH_SIZE):
        batch = texts[i : i + _BATCH_SIZE]
        embeddings = _embed_batch(model, batch, "RETRIEVAL_DOCUMENT")
        all_embeddings.extend(embeddings)
        logger.info("  embedded %d / %d", min(i + _BATCH_SIZE, len(texts)), len(texts))

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

    logger.info("Writing %d rows → %s", len(chunk_rows), chunks_table)
    errors = bq.insert_rows_json(chunks_table, chunk_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors (chunks): {errors}")

    logger.info("Writing %d rows → %s", len(vector_rows), vectors_table)
    errors = bq.insert_rows_json(vectors_table, vector_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors (vectors): {errors}")

    logger.info("Embedding complete — %d chunks stored.", len(chunks))


def embed_query(query: str, project: str) -> list[float]:
    """
    Embed a single search query (RETRIEVAL_QUERY task type).
    Used at query time by the retriever.
    """
    model = _init_vertexai(project)
    results = _embed_batch(model, [query], "RETRIEVAL_QUERY")
    return results[0]
