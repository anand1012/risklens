"""
Search router — fast BM25 keyword search over the indexed corpus.

GET /search?q=<query>&top_k=10

Uses the BM25 index loaded into app.state at startup (no BQ call needed).
Fast enough to power a live search-as-you-type experience.
"""

from fastapi import APIRouter, Query, Request

from indexing.bm25_index import search as bm25_search

router = APIRouter(prefix="/search", tags=["search"])


@router.get("")
def keyword_search(
    request: Request,
    q: str = Query(..., min_length=2, description="Search query"),
    top_k: int = Query(10, ge=1, le=50),
    source_type: str | None = Query(None, description="asset_desc | schema_doc | pipeline_doc"),
    domain: str | None = Query(None),
):
    bm25 = request.app.state.bm25_index
    corpus = request.app.state.bm25_corpus

    results = bm25_search(bm25, corpus, q, top_k=top_k * 3)  # over-fetch before filtering

    # Optional post-filters
    if source_type:
        results = [(d, s) for d, s in results if d.source_type == source_type]
    if domain:
        results = [(d, s) for d, s in results if d.domain == domain]

    results = results[:top_k]

    return [
        {
            "chunk_id": doc.chunk_id,
            "asset_id": doc.asset_id,
            "source_type": doc.source_type,
            "domain": doc.domain,
            "score": round(score, 4),
            "name": doc.metadata.get("name") or doc.metadata.get("asset_name", doc.asset_id),
            "snippet": doc.text[:300],
        }
        for doc, score in results
    ]
