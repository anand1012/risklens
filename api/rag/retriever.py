"""
RiskLens — Hybrid Retriever
Combines BM25 (lexical) + BigQuery ANN vector search (semantic) via score fusion.

At query time:
  1. BM25 scores from the in-memory index (loaded at startup)
  2. Vector scores from BQ VECTOR_SEARCH over risklens_embeddings.vectors_s
  3. Reciprocal Rank Fusion merges both ranked lists

The retriever returns a list of RetrievedDoc (text + metadata + score),
which the LangGraph chain uses to build context for Claude.
"""

import logging
from dataclasses import dataclass
from google.cloud import bigquery

from indexing.bm25_index import BM25Okapi, ChunkDoc, search as bm25_search
from indexing.embedder import embed_query

logger = logging.getLogger(__name__)

_DEFAULT_TOP_K = 8
_RRF_K = 60  # standard RRF constant


@dataclass
class RetrievedDoc:
    chunk_id: str
    asset_id: str
    text: str
    source_type: str
    domain: str
    score: float
    metadata: dict


# ---------------------------------------------------------------------------
# Vector search via BigQuery VECTOR_SEARCH
# ---------------------------------------------------------------------------

def _bq_vector_search(
    query_embedding: list[float],
    project: str,
    top_k: int,
    bq_client: bigquery.Client,
) -> list[tuple[str, float]]:
    """
    Returns list of (chunk_id, distance) sorted by ascending distance.
    Uses BigQuery VECTOR_SEARCH with cosine distance.
    """
    # Flatten embedding to a SQL ARRAY literal
    vec_literal = "[" + ", ".join(str(v) for v in query_embedding) + "]"

    sql = f"""
        SELECT
            base.chunk_id,
            distance
        FROM VECTOR_SEARCH(
            TABLE `{project}.risklens_embeddings.vectors_s`,
            'embedding',
            (SELECT {vec_literal} AS embedding),
            top_k => {top_k},
            distance_type => 'COSINE'
        )
        ORDER BY distance ASC
    """
    results = []
    try:
        for row in bq_client.query(sql).result():
            results.append((row.chunk_id, float(row.distance)))
    except Exception as e:
        logger.warning("BQ vector search failed: %s", e)
    return results


# ---------------------------------------------------------------------------
# Reciprocal Rank Fusion
# ---------------------------------------------------------------------------

def _rrf_merge(
    bm25_results: list[tuple[ChunkDoc, float]],
    vector_chunk_ids: list[tuple[str, float]],
    corpus_by_id: dict[str, ChunkDoc],
    top_k: int,
) -> list[tuple[ChunkDoc, float]]:
    """
    Merge two ranked lists using RRF.
    bm25_results:      [(ChunkDoc, bm25_score), ...]  ranked 1..N
    vector_chunk_ids:  [(chunk_id, distance), ...]    ranked 1..M (lower distance = better)
    """
    scores: dict[str, float] = {}

    for rank, (doc, _) in enumerate(bm25_results, start=1):
        scores[doc.chunk_id] = scores.get(doc.chunk_id, 0.0) + 1.0 / (_RRF_K + rank)

    for rank, (chunk_id, _) in enumerate(vector_chunk_ids, start=1):
        scores[chunk_id] = scores.get(chunk_id, 0.0) + 1.0 / (_RRF_K + rank)

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    results = []
    for chunk_id, rrf_score in ranked:
        doc = corpus_by_id.get(chunk_id)
        if doc:
            results.append((doc, rrf_score))
    return results


# ---------------------------------------------------------------------------
# Public retriever
# ---------------------------------------------------------------------------

def retrieve(
    query: str,
    bm25_index: BM25Okapi,
    corpus: list[ChunkDoc],
    bq_client: bigquery.Client,
    project: str,
    top_k: int = _DEFAULT_TOP_K,
) -> list[RetrievedDoc]:
    """
    Hybrid retrieve: BM25 + BQ vector search → RRF merge.

    Args:
        query:      Natural language question from the user
        bm25_index: Loaded BM25Okapi (from app.state)
        corpus:     List of ChunkDocs matching the BM25 index (from app.state)
        bq_client:  BigQuery client (from app.state)
        project:    GCP project ID
        top_k:      Number of docs to return

    Returns:
        List of RetrievedDoc sorted by RRF score descending
    """
    fetch_k = top_k * 3  # fetch more candidates before merging

    # BM25 retrieval
    bm25_results = bm25_search(bm25_index, corpus, query, top_k=fetch_k)

    # Vector retrieval (Vertex AI embed_query uses Workload Identity — no key needed)
    query_emb = embed_query(query, project=project)
    vector_results = _bq_vector_search(query_emb, project, fetch_k, bq_client)

    # Build corpus lookup
    corpus_by_id = {c.chunk_id: c for c in corpus}

    # Merge
    merged = _rrf_merge(bm25_results, vector_results, corpus_by_id, top_k=top_k)

    return [
        RetrievedDoc(
            chunk_id=doc.chunk_id,
            asset_id=doc.asset_id,
            text=doc.text,
            source_type=doc.source_type,
            domain=doc.domain,
            score=score,
            metadata=doc.metadata,
        )
        for doc, score in merged
    ]
