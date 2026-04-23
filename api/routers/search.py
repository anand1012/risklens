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
        "→ keyword_search called",
        extra={"json_fields": {"q_preview": q[:80], "top_k": top_k, "source_type": source_type, "domain": domain}},
    )
    bm25 = request.app.state.bm25_index
    corpus = request.app.state.bm25_corpus
    logger.debug("keyword_search: corpus_size=%d", len(corpus) if corpus else 0)

    # Over-fetch before filtering
    raw_results = bm25_search(bm25, corpus, q, top_k=top_k * 3)
    logger.debug("keyword_search: raw BM25 results before filtering: %d", len(raw_results))

    # Optional post-filters
    if source_type:
        before = len(raw_results)
        raw_results = [(d, s) for d, s in raw_results if d.source_type == source_type]
        logger.debug("keyword_search: source_type filter '%s' reduced %d→%d", source_type, before, len(raw_results))
    if domain:
        before = len(raw_results)
        raw_results = [(d, s) for d, s in raw_results if d.domain == domain]
        logger.debug("keyword_search: domain filter '%s' reduced %d→%d", domain, before, len(raw_results))

    raw_results = raw_results[:top_k]

    if not raw_results:
        logger.warning(
            "keyword_search returned 0 results",
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
    logger.info(
        "← keyword_search done",
        extra={"json_fields": {"result_count": len(results), "q_preview": q[:80]}},
    )
    return results
