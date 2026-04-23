"""
Search router — fast BM25 keyword search over the indexed corpus.

GET /search?q=<query>&top_k=10

Uses the BM25 index loaded into app.state at startup (no BQ call needed).
Fast enough to power a live search-as-you-type experience.
"""

import logging

from fastapi import APIRouter, Query, Request

from indexing.bm25_index import search as bm25_search

router = APIRouter(prefix="/search", tags=["search"])
logger = logging.getLogger(__name__)


@router.get("")
def keyword_search(
    request: Request,
    q: str = Query(..., min_length=2, description="Search query"),
    top_k: int = Query(10, ge=1, le=50),
    source_type: str | None = Query(None, description="asset_desc | schema_doc | pipeline_doc"),
    domain: str | None = Query(None),
):
    logger.info(
        "[api] GET /search | source=bm25_index | q=%s | top_k=%d | source_type=%s | domain=%s",
        q[:80], top_k, source_type or "all", domain or "all",
        extra={"json_fields": {"q_preview": q[:80], "top_k": top_k, "source_type": source_type, "domain": domain}},
    )
    bm25 = request.app.state.bm25_index
    corpus = request.app.state.bm25_corpus
    corpus_size = len(corpus) if corpus else 0

    # Over-fetch before filtering
    raw_results = bm25_search(bm25, corpus, q, top_k=top_k * 3)
    pre_filter_count = len(raw_results)

    # Optional post-filters
    if source_type:
        raw_results = [(d, s) for d, s in raw_results if d.source_type == source_type]
    if domain:
        raw_results = [(d, s) for d, s in raw_results if d.domain == domain]

    raw_results = raw_results[:top_k]

    if not raw_results:
        logger.warning(
            "[api] GET /search returned 0 results | source=bm25_index | corpus_size=%d | q=%s | source_type=%s | domain=%s",
            corpus_size, q[:80], source_type, domain,
            extra={"json_fields": {"q_preview": q[:80], "source_type": source_type, "domain": domain}},
        )

    results = [
        {
            "chunk_id": doc.chunk_id,
            "asset_id": doc.asset_id,
            "source_type": doc.source_type,
            "domain": doc.domain,
            "score": round(score, 4),
            "name": doc.metadata.get("name") or doc.metadata.get("asset_name", doc.asset_id),
            "snippet": doc.text[:300],
        }
        for doc, score in raw_results
    ]
    top_score = round(raw_results[0][1], 4) if raw_results else 0.0
    logger.info(
        "[api] ✓ GET /search | source=bm25_index | corpus_size=%d | pre_filter=%d | results=%d | top_score=%.4f | q=%s",
        corpus_size, pre_filter_count, len(results), top_score, q[:80],
        extra={"json_fields": {"result_count": len(results), "q_preview": q[:80], "top_score": top_score}},
    )
    return results
