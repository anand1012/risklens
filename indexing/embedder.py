"""
RiskLens — Cohere Embedder
Embeds ChunkDocs using Cohere embed-english-v3.0 and writes results to BigQuery:
  - risklens_embeddings.chunks  — text + metadata
  - risklens_embeddings.vectors — chunk_id + embedding (FLOAT64 REPEATED)

Usage:
    from indexing.embedder import embed_and_store
    embed_and_store(chunks, project="risklens-frtb-2026", cohere_api_key="...")
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import cohere
from google.cloud import bigquery

from indexing.chunker import ChunkDoc

logger = logging.getLogger(__name__)

# Cohere limits: 96 texts per batch for embed-v3 with input_type=search_document
_BATCH_SIZE = 96
_EMBED_MODEL = "embed-english-v3.0"


def _embed_batch(co: cohere.Client, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts with retry on rate limit."""
    for attempt in range(3):
        try:
            response = co.embed(
                texts=texts,
                model=_EMBED_MODEL,
                input_type="search_document",
            )
            return [list(e) for e in response.embeddings]
        except cohere.TooManyRequestsError:
            wait = 2 ** attempt
            logger.warning("Cohere rate limit, waiting %ds (attempt %d/3)", wait, attempt + 1)
            time.sleep(wait)
    raise RuntimeError("Cohere embedding failed after 3 retries")


def embed_and_store(
    chunks: list[ChunkDoc],
    project: str,
    cohere_api_key: Optional[str] = None,
    truncate: bool = False,
) -> None:
    """
    Embed all chunks and upsert into BigQuery.

    Args:
        chunks:         Output of chunker.build_chunks()
        project:        GCP project ID
        cohere_api_key: Cohere API key (falls back to COHERE_API_KEY env var)
        truncate:       If True, truncate BQ tables before inserting (full refresh)
    """
    api_key = cohere_api_key or os.environ.get("COHERE_API_KEY")
    if not api_key:
        raise ValueError("Cohere API key required: pass cohere_api_key= or set COHERE_API_KEY")

    co = cohere.Client(api_key)
    bq = bigquery.Client(project=project)

    chunks_table = f"{project}.risklens_embeddings.chunks"
    vectors_table = f"{project}.risklens_embeddings.vectors"

    if truncate:
        logger.info("Truncating embedding tables…")
        bq.query(f"TRUNCATE TABLE `{chunks_table}`").result()
        bq.query(f"TRUNCATE TABLE `{vectors_table}`").result()

    now = datetime.now(timezone.utc).isoformat()

    # Embed in batches
    all_embeddings: list[list[float]] = []
    texts = [c.text for c in chunks]

    logger.info("Embedding %d chunks in batches of %d…", len(chunks), _BATCH_SIZE)
    for i in range(0, len(texts), _BATCH_SIZE):
        batch = texts[i : i + _BATCH_SIZE]
        embeddings = _embed_batch(co, batch)
        all_embeddings.extend(embeddings)
        logger.info("  embedded %d / %d", min(i + _BATCH_SIZE, len(texts)), len(texts))

    # Build BQ rows
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

    # Insert chunks
    logger.info("Writing %d rows → %s", len(chunk_rows), chunks_table)
    errors = bq.insert_rows_json(chunks_table, chunk_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors (chunks): {errors}")

    # Insert vectors
    logger.info("Writing %d rows → %s", len(vector_rows), vectors_table)
    errors = bq.insert_rows_json(vectors_table, vector_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors (vectors): {errors}")

    logger.info("Embedding complete — %d chunks stored.", len(chunks))


def embed_query(query: str, cohere_api_key: Optional[str] = None) -> list[float]:
    """
    Embed a single search query (input_type=search_query).
    Used at query time by the retriever, not during indexing.
    """
    api_key = cohere_api_key or os.environ.get("COHERE_API_KEY")
    if not api_key:
        raise ValueError("Cohere API key required")

    co = cohere.Client(api_key)
    response = co.embed(
        texts=[query],
        model=_EMBED_MODEL,
        input_type="search_query",
    )
    return list(response.embeddings[0])
