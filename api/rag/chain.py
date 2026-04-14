"""
RiskLens — RAG Agent with BigQuery Tool
Single-turn agent that:
  1. Checks relevance (Haiku, fast) IN PARALLEL with retrieval — zero overhead for on-topic queries
  2. Retrieves context via hybrid retriever (Vertex AI + BM25)
  3. Streams answer via Claude, with access to a query_bigquery tool for live data

OFF_TOPIC flow:
  - _is_relevant() runs concurrently with retrieve() via asyncio.gather()
  - If off-topic, decline is returned immediately; retrieval result is discarded
  - On-topic queries pay no extra latency for the classifier

BigQuery tool:
  - query_bigquery(sql) — Claude calls this for live data questions
  - Phase 1: stream with tool; Phase 2: if tool called, execute + stream continuation
"""

import asyncio
import json
import logging
import os
from functools import reduce
from operator import add
from typing import AsyncGenerator, Optional

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

from api.rag.retriever import RetrievedDoc, retrieve
from indexing.bm25_index import BM25Okapi, ChunkDoc
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_MODEL       = "claude-sonnet-4-6"
_HAIKU_MODEL = "claude-haiku-4-5-20251001"

_SYSTEM_PROMPT = """You are RiskLens AI, an expert assistant for a financial data catalog \
built for FRTB (Fundamental Review of the Trading Book) compliance.

You help risk managers, data stewards, and quants with:
- What data assets exist (tables, feeds, models, reports)
- How data flows through the pipeline (lineage)
- Data quality, ownership, and SLA status
- FRTB-specific concepts (VaR, ES, P&L Attribution, NMRF, BCBS 239, RFET, PLAT)
- How to use the RiskLens application and its UI features

RiskLens Application UI:
The app has five tabs: Catalog, Lineage, Governance, Assets, and FRTB Risk.
- Catalog tab: browse all data assets (tables, feeds, reports). Click any asset to see its \
schema, ownership, description, and quality score.
- Lineage tab: interactive graph showing how data flows between assets. \
  • Click any asset in the catalog to open its lineage graph. \
  • Hops control (top-right, buttons 1–4): sets graph traversal depth — how many \
    upstream and downstream steps to expand from the selected node. \
    Hop 1 = direct parents/children only. Hop 4 = full pipeline ancestry. \
  • Nodes are colour-coded by layer: bronze (raw), silver (cleaned), gold (aggregated), \
    reference (lookup), source (external API), pipeline (Spark job).
- Governance tab: data quality scores, SLA status, and ownership by desk/team.
- Assets tab: full asset inventory with filtering by domain and layer.
- FRTB Risk tab: live risk metrics — Risk Summary, Capital Charge, Back-Testing \
  (traffic light zones), PLAT results, and RFET eligibility.
- AI Chat panel (bottom-right): this assistant. Ask about data, lineage, FRTB concepts, \
  or how to navigate the app.

You have access to a query_bigquery tool. Use it ONLY when the question explicitly asks \
for live/current data: which desks are breaching, actual numeric values, row counts, \
SLA status, pass/fail results, or desk-level metrics from today's run. \
Do NOT use the tool for: regulatory definitions, FRTB methodology, threshold explanations, \
schema descriptions, lineage questions, or UI/navigation questions.

BigQuery project: risklens-frtb-2026
Key datasets and tables:
  risklens_gold     — backtesting, capital_charge, es_outputs, plat_results, rfet_results, risk_summary, trade_positions
  risklens_silver   — risk_enriched, risk_outputs, rates, prices, trades
  risklens_catalog  — assets, quality_scores, sla_status, ownership
  risklens_lineage  — nodes, edges

Always be concise and precise — your audience are quantitative finance professionals."""

_RELEVANCE_PROMPT = (
    "You are a classifier for RiskLens — a financial data catalog and FRTB compliance app. "
    "Reply YES if the query is about any of: FRTB, financial risk, trading data, "
    "data catalogs, data pipelines, data lineage, database tables, BigQuery datasets, "
    "regulatory compliance, data quality, data governance, the RiskLens application itself "
    "(its tabs, UI features, navigation, controls like hops/filters/charts), "
    "or related financial/quantitative/data-engineering topics in a banking or risk context. "
    "Reply NO only for clearly unrelated topics (cooking, sports, general geography, "
    "unrelated coding questions, etc.).\n"
    "Query: "
)

_DECLINE_MESSAGE = (
    "I'm RiskLens AI — I specialise in FRTB compliance and financial data catalog topics. "
    "Please ask about risk data, tables, lineage, or FRTB regulatory concepts."
)

_BQ_TOOL_DEF = {
    "name": "query_bigquery",
    "description": (
        "Execute a read-only SELECT SQL query against the RiskLens BigQuery data warehouse. "
        "Use for live data: current traffic light zones, breach flags, capital charges, "
        "RFET pass/fail status, SLA breaches, row counts, or any quantitative lookup. "
        "Use fully qualified table names, e.g. "
        "`risklens-frtb-2026.risklens_gold.backtesting`."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": (
                    "A BigQuery SELECT or WITH SQL query. "
                    "Include LIMIT ≤ 30. Only read queries allowed."
                ),
            }
        },
        "required": ["sql"],
    },
}


# ---------------------------------------------------------------------------
# Relevance classifier (Haiku — fast and cheap)
# ---------------------------------------------------------------------------

async def _is_relevant(query: str, api_key: str) -> bool:
    """
    Binary relevance check using Haiku.
    Runs concurrently with retrieve() so on-topic queries pay zero extra latency.
    """
    llm = ChatAnthropic(
        model=_HAIKU_MODEL,
        anthropic_api_key=api_key,
        max_tokens=5,
    )
    response = await llm.ainvoke([HumanMessage(content=_RELEVANCE_PROMPT + query)])
    text = _extract_text(response.content).strip().upper()
    return text.startswith("Y")


# ---------------------------------------------------------------------------
# BigQuery query executor
# ---------------------------------------------------------------------------

def _run_bq_query(bq_client: bigquery.Client, project: str, sql: str,
                  max_rows: int = 30) -> str:
    """Execute a read-only BQ query, return results as a plain-text table."""
    sql_upper = sql.strip().upper()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return "Error: only SELECT/WITH queries are permitted."
    if "LIMIT" not in sql_upper:
        sql = sql.rstrip("; ") + f" LIMIT {max_rows}"

    try:
        rows = list(bq_client.query(sql).result())
    except Exception as exc:
        logger.warning("BQ query failed: %s", exc)
        return f"Query error: {exc}"

    if not rows:
        return "Query returned no rows."

    fields = list(rows[0].keys())
    header = " | ".join(fields)
    sep = "-" * min(120, len(header) + 4)
    body = "\n".join(
        " | ".join("NULL" if row[f] is None else str(row[f]) for f in fields)
        for row in rows[:max_rows]
    )
    result = f"{header}\n{sep}\n{body}"
    if len(rows) >= max_rows:
        result += f"\n({max_rows} rows shown)"
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_context_block(docs: list[RetrievedDoc]) -> str:
    parts = []
    for i, doc in enumerate(docs, start=1):
        parts.append(f"[{i}] ({doc.source_type} | {doc.domain})\n{doc.text}")
    return "\n\n---\n\n".join(parts)


def _sources_payload(docs: list[RetrievedDoc]) -> str:
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


def _extract_text(content) -> str:
    """Normalise AIMessage.content — handles both str and list of content blocks."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            b.get("text", "") for b in content
            if isinstance(b, dict) and b.get("type") == "text"
        )
    return ""


# ---------------------------------------------------------------------------
# Fallback: non-streaming answer when streaming/reduce fails
# ---------------------------------------------------------------------------

async def _fallback_answer(messages, ant_key: str, run_cfg: dict):
    """
    Async generator: yields a single SSE token from a plain (no-tool) ainvoke.
    Used when Phase 1 streaming or chunk-merge fails and no text was yielded yet.
    """
    llm = ChatAnthropic(model=_MODEL, anthropic_api_key=ant_key, max_tokens=1024)
    try:
        resp = await llm.ainvoke(messages, config=run_cfg)
        text = _extract_text(resp.content)
        if text:
            yield f"data: {json.dumps(text)}\n\n"
    except Exception as exc:
        logger.error("Fallback invocation failed: %s", exc)


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
    Yields SSE-formatted strings: 'data: ...\n\n'

    Relevance check runs in parallel with retrieval — no latency overhead
    for on-topic queries.
    """
    ant_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not ant_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    # Run relevance classifier and retrieval concurrently
    relevant, docs = await asyncio.gather(
        _is_relevant(query, ant_key),
        asyncio.to_thread(
            retrieve,
            query=query,
            bm25_index=bm25_index,
            corpus=corpus,
            bq_client=bq_client,
            project=project,
            top_k=top_k,
        ),
    )

    if not relevant:
        yield f"data: __sources__{json.dumps([])}\n\n"
        yield f"data: {json.dumps(_DECLINE_MESSAGE)}\n\n"
        yield "data: __done__\n\n"
        return

    logger.info("Retrieved %d docs for query: %.80s", len(docs), query)

    context_block = _build_context_block(docs)
    user_message = (
        f"Context from the RiskLens data catalog:\n\n{context_block}\n\n"
        f"Question: {query}"
    )
    messages = [SystemMessage(content=_SYSTEM_PROMPT), HumanMessage(content=user_message)]

    llm = ChatAnthropic(
        model=_MODEL, anthropic_api_key=ant_key, streaming=True, max_tokens=1024,
    )
    llm_with_tools = llm.bind_tools([_BQ_TOOL_DEF])

    yield f"data: __sources__{_sources_payload(docs)}\n\n"

    run_cfg = {
        "run_name": "risklens_chat",
        "metadata": {"query": query, "top_k": top_k, "docs_retrieved": len(docs)},
    }

    plain_llm = ChatAnthropic(
        model=_MODEL, anthropic_api_key=ant_key, streaming=True, max_tokens=1024,
    )

    # ── Phase 1: stream with tools ────────────────────────────────────────────
    chunks: list = []
    text_yielded = False

    try:
        async for chunk in llm_with_tools.astream(messages, config=run_cfg):
            chunks.append(chunk)
            content = chunk.content
            if isinstance(content, str):
                token = content
            elif isinstance(content, list):
                token = "".join(
                    b.get("text", "") for b in content
                    if isinstance(b, dict) and b.get("type") == "text"
                )
            else:
                token = ""
            if token:
                text_yielded = True
                yield f"data: {json.dumps(token)}\n\n"
    except Exception as exc:
        logger.error("Phase 1 stream error: %s", exc, exc_info=True)
        if not text_yielded:
            async for tok in _fallback_answer(messages, ant_key, run_cfg):
                yield tok
        yield "data: __done__\n\n"
        return

    # ── Phase 2: execute tool call if present ─────────────────────────────────
    if chunks:
        try:
            full_msg = reduce(add, chunks)
        except Exception as exc:
            logger.warning("Could not merge stream chunks (%s) — using fallback", exc)
            if not text_yielded:
                async for tok in _fallback_answer(messages, ant_key, run_cfg):
                    yield tok
            yield "data: __done__\n\n"
            return

        tool_calls = getattr(full_msg, "tool_calls", None)
        if tool_calls:
            tc = tool_calls[0]
            sql = tc.get("args", {}).get("sql", "")
            logger.info("BQ tool call — sql: %.300s", sql)

            tool_result = _run_bq_query(bq_client, project, sql)
            logger.info("BQ result preview: %.200s", tool_result[:200])

            follow_up = messages + [
                full_msg,
                ToolMessage(content=tool_result, tool_call_id=tc["id"]),
            ]
            phase2_yielded = False
            try:
                async for chunk in plain_llm.astream(follow_up, config=run_cfg):
                    content = chunk.content
                    if isinstance(content, str):
                        token = content
                    elif isinstance(content, list):
                        token = "".join(
                            b.get("text", "") for b in content
                            if isinstance(b, dict) and b.get("type") == "text"
                        )
                    else:
                        token = ""
                    if token:
                        phase2_yielded = True
                        yield f"data: {json.dumps(token)}\n\n"
            except Exception as exc:
                logger.error("Phase 2 stream error: %s", exc, exc_info=True)

            # Phase 2 produced nothing — build a plain answer from tool result
            if not phase2_yielded:
                logger.warning("Phase 2 yielded nothing — falling back to plain answer")
                fallback_msgs = messages + [
                    HumanMessage(content=(
                        f"Here is the query result:\n{tool_result}\n\n"
                        f"Please answer the original question using this data."
                    ))
                ]
                async for tok in _fallback_answer(fallback_msgs, ant_key, run_cfg):
                    yield tok

    yield "data: __done__\n\n"


# ---------------------------------------------------------------------------
# Non-streaming entry point (testing / batch)
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
    """Returns {"answer": str, "sources": list[dict], "relevant": bool}"""
    ant_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not ant_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    # Run relevance classifier and retrieval concurrently
    relevant, docs = await asyncio.gather(
        _is_relevant(query, ant_key),
        asyncio.to_thread(
            retrieve,
            query=query,
            bm25_index=bm25_index,
            corpus=corpus,
            bq_client=bq_client,
            project=project,
            top_k=top_k,
        ),
    )

    if not relevant:
        return {"answer": _DECLINE_MESSAGE, "sources": [], "relevant": False}

    context_block = _build_context_block(docs)
    user_message = (
        f"Context from the RiskLens data catalog:\n\n{context_block}\n\n"
        f"Question: {query}"
    )
    messages = [SystemMessage(content=_SYSTEM_PROMPT), HumanMessage(content=user_message)]

    llm = ChatAnthropic(model=_MODEL, anthropic_api_key=ant_key, max_tokens=1024)
    llm_with_tools = llm.bind_tools([_BQ_TOOL_DEF])

    response = await llm_with_tools.ainvoke(messages)
    content = _extract_text(response.content)

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

    tool_calls = getattr(response, "tool_calls", None)
    if tool_calls:
        tc = tool_calls[0]
        sql = tc.get("args", {}).get("sql", "")
        tool_result = _run_bq_query(bq_client, project, sql)
        follow_up = messages + [
            response,
            ToolMessage(content=tool_result, tool_call_id=tc["id"]),
        ]
        final = await llm.ainvoke(follow_up)
        return {"answer": _extract_text(final.content), "sources": sources, "relevant": True}

    return {"answer": content, "sources": sources, "relevant": True}
