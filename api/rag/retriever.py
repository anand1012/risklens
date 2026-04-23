"""
RiskLens — Hybrid Retriever
Combines BM25 (lexical) + BigQuery ANN vector search (semantic) via score fusion.

At query time:
  1. BM25 scores from the in-memory index (loaded at startup)
  2. Vector scores from BQ VECTOR_SEARCH over risklens_embeddings.vectors
  3. Reciprocal Rank Fusion merges both ranked lists

The retriever returns a list of RetrievedDoc (text + metadata + score),
which the LangGraph chain uses to build context for Claude.
"""

import logging
import time
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
    logger.info(
        "[rag] Vector search starting | table=risklens_embeddings.vectors | top_k=%d | embedding_dim=%d",
        top_k, len(query_embedding),
        extra={"json_fields": {"top_k": top_k, "embedding_dim": len(query_embedding)}},
    )
    # Flatten embedding to a SQL ARRAY literal
    vec_literal = "[" + ", ".join(str(v) for v in query_embedding) + "]"

    sql = f"""
        SELECT
            base.chunk_id,
            distance
        FROM VECTOR_SEARCH(
            TABLE `{project}.risklens_embeddings.vectors`,
            'embedding',
            (SELECT {vec_literal} AS embedding),
            top_k => {top_k},
            distance_type => 'COSINE'
        )
        ORDER BY distance ASC
    """
    t0 = time.monotonic()
    results = []
    try:
        for row in bq_client.query(sql).result():
            results.append((row.chunk_id, float(row.distance)))
        latency_ms = int((time.monotonic() - t0) * 1000)
        top_dist = round(results[0][1], 4) if results else None
        logger.info(
            "[rag] ✓ Vector search complete | table=risklens_embeddings.vectors | hits=%d | top_distance=%.4f | latency_ms=%d",
            len(results), top_dist or 0.0, latency_ms,
            extra={"json_fields": {"hit_count": len(results), "top_distance": top_dist, "latency_ms": latency_ms}},
        )
        if not results:
            logger.warning("[rag] Vector search returned 0 hits | table=risklens_embeddings.vectors | top_k=%d", top_k)
    except Exception as e:
        latency_ms = int((time.monotonic() - t0) * 1000)
        logger.warning(
            "[rag] FAILED: Vector search error | table=risklens_embeddings.vectors | latency_ms=%d | error=%s",
            latency_ms, e,
            extra={"json_fields": {"latency_ms": latency_ms}},
        )
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
    logger.info(
        "[rag] RRF merge starting | bm25_hits=%d | vector_hits=%d | top_k=%d | rrf_k=%d",
        len(bm25_results), len(vector_chunk_ids), top_k, _RRF_K,
        extra={"json_fields": {
            "bm25_count": len(bm25_results),
            "vector_count": len(vector_chunk_ids),
            "top_k": top_k,
        }},
    )
    scores: dict[str, float] = {}

    for rank, (doc, _) in enumerate(bm25_results, start=1):
        scores[doc.chunk_id] = scores.get(doc.chunk_id, 0.0) + 1.0 / (_RRF_K + rank)

    for rank, (chunk_id, _) in enumerate(vector_chunk_ids, start=1):
        scores[chunk_id] = scores.get(chunk_id, 0.0) + 1.0 / (_RRF_K + rank)

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    results = []
    missing = 0
    for chunk_id, rrf_score in ranked:
        doc = corpus_by_id.get(chunk_id)
        if doc:
            results.append((doc, rrf_score))
        else:
            missing += 1
            logger.debug("RRF: chunk_id %s not found in corpus_by_id", chunk_id)

    if missing:
        logger.warning("[rag] RRF merge: %d chunk_ids not found in corpus", missing)
    top_rrf = round(ranked[0][1], 4) if ranked else 0.0
    logger.info(
        "[rag] ✓ RRF merge complete | unique_scored=%d | merged_returned=%d | top_rrf_score=%.4f | missing_chunks=%d",
        len(scores), len(results), top_rrf, missing,
        extra={"json_fields": {"merged_count": len(results), "missing_chunks": missing, "top_rrf_score": top_rrf}},
    )
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
    logger.info(
        "[rag] Hybrid retrieval starting | query=%s | top_k=%d | corpus_size=%d | method=BM25+vector→RRF",
        query[:80], top_k, len(corpus),
        extra={"json_fields": {
            "query_preview": query[:80],
            "top_k": top_k,
            "corpus_size": len(corpus),
        }},
    )
    fetch_k = top_k * 3  # fetch more candidates before merging

    # BM25 retrieval
    t_bm25 = time.monotonic()
    bm25_results = bm25_search(bm25_index, corpus, query, top_k=fetch_k)
    bm25_ms = int((time.monotonic() - t_bm25) * 1000)
    bm25_top_score = round(bm25_results[0][1], 4) if bm25_results else 0.0
    logger.info(
        "[rag] BM25 retrieval complete | hits=%d | top_score=%.4f | latency_ms=%d | query=%s",
        len(bm25_results), bm25_top_score, bm25_ms, query[:80],
        extra={"json_fields": {"bm25_hits": len(bm25_results), "bm25_top_score": bm25_top_score, "latency_ms": bm25_ms}},
    )
    if not bm25_results:
        logger.warning("[rag] BM25 returned 0 hits | query=%s", query[:80])

    # Vector retrieval (Vertex AI embed_query uses Workload Identity — no key needed)
    t_embed = time.monotonic()
    query_emb = embed_query(query, project=project)
    embed_ms = int((time.monotonic() - t_embed) * 1000)
    logger.info("[rag] Query embedding complete | embed_dim=%d | latency_ms=%d", len(query_emb), embed_ms,
                extra={"json_fields": {"embed_dim": len(query_emb), "latency_ms": embed_ms}})

    vector_results = _bq_vector_search(query_emb, project, fetch_k, bq_client)

    # Build corpus lookup
    corpus_by_id = {c.chunk_id: c for c in corpus}

    # Merge
    merged = _rrf_merge(bm25_results, vector_results, corpus_by_id, top_k=top_k)

    docs = [
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
    top_score = round(docs[0].score, 4) if docs else None
    logger.info(
        "[rag] ✓ Hybrid retrieval complete | returned=%d | top_rrf_score=%.4f | bm25_hits=%d | vector_hits=%d | query=%s",
        len(docs), top_score or 0.0, len(bm25_results), len(vector_results), query[:80],
        extra={"json_fields": {
            "returned_count": len(docs),
            "top_score": top_score,
        }},
    )
    if not docs:
        logger.warning("[rag] retrieve returned 0 docs | query=%s | bm25_hits=%d | vector_hits=%d", query[:80], len(bm25_results), len(vector_results))
    return docs
