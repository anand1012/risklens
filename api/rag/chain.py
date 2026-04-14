"""
RiskLens — LangGraph RAG Agent
A single-turn ReAct-style agent that:
  1. Retrieves relevant context via the hybrid retriever
  2. Answers using Claude (claude-sonnet-4-6) with grounded context
  3. Streams tokens back via an async generator

The graph has two nodes:
  retrieve → generate

For the current scope (single-turn Q&A) this is intentionally simple.
Multi-turn memory can be layered on later via LangGraph checkpointing.
"""

import json
import logging
import os
from typing import AsyncGenerator, Optional, TypedDict

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

from api.rag.retriever import RetrievedDoc, retrieve
from indexing.bm25_index import BM25Okapi, ChunkDoc
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_MODEL = "claude-sonnet-4-6"

_SYSTEM_PROMPT = """You are RiskLens AI, an expert assistant for a financial data catalog \
built for FRTB (Fundamental Review of the Trading Book) compliance.

You help risk managers, data stewards, and quants understand:
- What data assets exist (tables, feeds, models, reports)
- How data flows through the pipeline (lineage)
- Data quality, ownership, and SLA status
- FRTB-specific concepts (VaR, ES, P&L Attribution, NMRF, BCBS 239)

Always ground your answers in the provided context. If the context does not contain \
enough information to answer, say so clearly rather than guessing.
Be concise and precise — your audience are quantitative finance professionals."""

_RELEVANCE_PROMPT = """You are a query classifier for RiskLens, a financial data catalog \
for FRTB (Fundamental Review of the Trading Book) compliance.

Classify the following question as RELEVANT or IRRELEVANT.

RELEVANT topics include:
- FRTB, Basel III/IV, regulatory capital, VaR, Expected Shortfall (ES)
- P&L Attribution Test (PLAT), back-testing, NMRF, risk factor eligibility (RFET)
- Data tables, schemas, lineage, data quality, SLA, data catalog
- Risk desks, trading books, capital charges, market risk
- The RiskLens pipeline (bronze/silver/gold layers, Spark jobs, BigQuery)
- Any question about the data, tables, metrics, or system in this catalog

IRRELEVANT topics include:
- General knowledge (history, science, geography, cooking, sports, weather)
- Programming unrelated to this system, entertainment, personal advice
- Anything clearly outside financial risk data management

Reply with ONLY the single word: RELEVANT or IRRELEVANT"""

_DECLINE_MESSAGE = (
    "I'm RiskLens AI, specialized in FRTB compliance and financial data catalog topics. "
    "Your question appears to be outside my area of expertise. "
    "I can help you with questions about:\n"
    "- FRTB regulatory metrics (VaR, ES, capital charges, back-testing)\n"
    "- Data tables, schemas, and lineage in the RiskLens catalog\n"
    "- Data quality, SLA status, and pipeline details\n\n"
    "Please ask a question related to the RiskLens system or FRTB risk data."
)


async def _is_relevant(query: str, ant_key: str) -> bool:
    """Fast relevance classifier — uses a single-token response for minimal latency."""
    llm = ChatAnthropic(
        model=_MODEL,
        anthropic_api_key=ant_key,
        max_tokens=5,
    )
    messages = [
        SystemMessage(content=_RELEVANCE_PROMPT),
        HumanMessage(content=f"Question: {query}"),
    ]
    try:
        response = await llm.ainvoke(messages)
        return "IRRELEVANT" not in response.content.upper()
    except Exception as e:
        logger.warning("Relevance check failed (%s) — defaulting to relevant", e)
        return True  # fail open: if classifier errors, let it through


# ---------------------------------------------------------------------------
# LangGraph state
# ---------------------------------------------------------------------------

class AgentState(TypedDict):
    query: str
    context: list[RetrievedDoc]
    answer: str


# ---------------------------------------------------------------------------
# Node functions
# ---------------------------------------------------------------------------

def _build_context_block(docs: list[RetrievedDoc]) -> str:
    parts = []
    for i, doc in enumerate(docs, start=1):
        parts.append(
            f"[{i}] ({doc.source_type} | {doc.domain})\n{doc.text}"
        )
    return "\n\n---\n\n".join(parts)


# ---------------------------------------------------------------------------
# Streaming entry point (used by chat router)
# ---------------------------------------------------------------------------

async def stream_answer(
    query: str,
    bm25_index: BM25Okapi,
    corpus: list[ChunkDoc],
    bq_client: bigquery.Client,
    project: str,
    top_k: int = 8,
    anthropic_api_key: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    """
    Retrieve context then stream Claude's answer token by token.
    Yields plain text chunks suitable for an SSE response.
    """
    ant_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not ant_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    # ── Off-topic guard ───────────────────────────────────────────────────────
    if not await _is_relevant(query, ant_key):
        logger.info("Off-topic query rejected: %.80s", query)
        yield f"data: __sources__{json.dumps([])}\n\n"
        yield f"data: {json.dumps(_DECLINE_MESSAGE)}\n\n"
        yield "data: __done__\n\n"
        return

    # Retrieve (Vertex AI embeddings use Workload Identity — no extra key)
    docs = retrieve(
        query=query,
        bm25_index=bm25_index,
        corpus=corpus,
        bq_client=bq_client,
        project=project,
        top_k=top_k,
    )
    logger.info("Retrieved %d docs for query: %.80s", len(docs), query)

    context_block = _build_context_block(docs)
    user_message = (
        f"Context from the RiskLens data catalog:\n\n{context_block}\n\n"
        f"Question: {query}"
    )

    llm = ChatAnthropic(
        model=_MODEL,
        anthropic_api_key=ant_key,
        streaming=True,
        max_tokens=1024,
    )

    messages = [
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=user_message),
    ]

    # Yield source metadata first as a special SSE event (prefixed so the
    # frontend can parse it separately from the text stream)
    sources_json = _sources_payload(docs)
    yield f"data: __sources__{sources_json}\n\n"

    async for chunk in llm.astream(
        messages,
        config={
            "run_name": "risklens_chat",
            "metadata": {
                "query": query,
                "top_k": top_k,
                "docs_retrieved": len(docs),
                "source_types": list({d.source_type for d in docs}),
            },
        },
    ):
        token = chunk.content
        if token:
            yield f"data: {json.dumps(token)}\n\n"

    yield "data: __done__\n\n"


def _sources_payload(docs: list[RetrievedDoc]) -> str:
    import json
    sources = [
        {
            "chunk_id": d.chunk_id,
            "asset_id": d.asset_id,
            "source_type": d.source_type,
            "domain": d.domain,
            "score": round(d.score, 4),
            "name": d.metadata.get("name") or d.metadata.get("asset_name", d.asset_id),
        }
        for d in docs
    ]
    return json.dumps(sources)


# ---------------------------------------------------------------------------
# Non-streaming entry point (for testing / batch use)
# ---------------------------------------------------------------------------

async def answer(
    query: str,
    bm25_index: BM25Okapi,
    corpus: list[ChunkDoc],
    bq_client: bigquery.Client,
    project: str,
    top_k: int = 8,
    anthropic_api_key: Optional[str] = None,
) -> dict:
    """Returns {"answer": str, "sources": list[dict]}"""
    ant_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not ant_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    # ── Off-topic guard ───────────────────────────────────────────────────────
    if not await _is_relevant(query, ant_key):
        logger.info("Off-topic query rejected: %.80s", query)
        return {"answer": _DECLINE_MESSAGE, "sources": [], "relevant": False}

    docs = retrieve(
        query=query,
        bm25_index=bm25_index,
        corpus=corpus,
        bq_client=bq_client,
        project=project,
        top_k=top_k,
    )

    context_block = _build_context_block(docs)
    user_message = (
        f"Context from the RiskLens data catalog:\n\n{context_block}\n\n"
        f"Question: {query}"
    )

    llm = ChatAnthropic(
        model=_MODEL,
        anthropic_api_key=ant_key,
        max_tokens=1024,
    )

    messages = [
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=user_message),
    ]

    response = await llm.ainvoke(
        messages,
        config={
            "run_name": "risklens_chat",
            "metadata": {
                "query": query,
                "top_k": top_k,
                "docs_retrieved": len(docs),
                "source_types": list({d.source_type for d in docs}),
            },
        },
    )
    return {
        "answer": response.content,
        "sources": [
            {
                "chunk_id": d.chunk_id,
                "asset_id": d.asset_id,
                "source_type": d.source_type,
                "domain": d.domain,
                "score": round(d.score, 4),
                "name": d.metadata.get("name") or d.metadata.get("asset_name", d.asset_id),
            }
            for d in docs
        ],
    }
