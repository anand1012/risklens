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
Key datasets and tables (partition columns noted after the arrow — use these \
in WHERE clauses for recent data, NEVER invent column names):
  risklens_gold
    backtesting, capital_charge, es_outputs, pnl_vectors, risk_summary → calc_date (DATE)
    trade_positions → trade_date (STRING)
    plat_results    → calc_date (DATE)
    rfet_results    → rfet_date (DATE)
  risklens_silver
    positions, risk_enriched, risk_outputs → calc_date or trade_date; all have processed_at
    trades, prices, rates                   → trade_date (STRING); all have processed_at
  risklens_catalog
    assets            — asset_id, name, type, domain, layer, description, tags, row_count, updated_at
    schema_registry   — asset_id, column_name, data_type, nullable, description, sample_value
    sla_status        — asset_id, expected_refresh, actual_refresh, breach_flag, breach_duration_mins, checked_at
    quality_scores    — asset_id, null_rate, schema_drift, freshness_status, duplicate_rate, last_checked
    ownership         — asset_id, owner_name, team, steward, email, assigned_date
    desk_registry, access_log
  risklens_lineage
    nodes — node_id, name, type, domain, layer, metadata
    edges — edge_id, from_node_id, to_node_id, relationship, pipeline_job

When asked "when was X last loaded" or "latest refresh" prefer \
risklens_catalog.sla_status.actual_refresh or the table's own processed_at / \
calc_date — never fabricate columns like processed_at on tables that only have \
calc_date. When in doubt, query risklens_catalog.schema_registry for the asset_id \
before writing the final SELECT.

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
    logger.info("[rag] Relevance classifier starting | model=%s | query=%s", _HAIKU_MODEL, query[:80], extra={"json_fields": {"query_preview": query[:80], "model": _HAIKU_MODEL}})
    llm = ChatAnthropic(
        model=_HAIKU_MODEL,
        anthropic_api_key=api_key,
        max_tokens=5,
    )
    response = await llm.ainvoke([HumanMessage(content=_RELEVANCE_PROMPT + query)])
    text = _extract_text(response.content).strip().upper()
    result = text.startswith("Y")
    logger.info("[rag] Relevance classifier done | relevant=%s | response=%s | model=%s | query=%s", result, text[:20], _HAIKU_MODEL, query[:80], extra={"json_fields": {"relevant": result, "classifier_response": text[:20]}})
    return result


# ---------------------------------------------------------------------------
# BigQuery query executor
# ---------------------------------------------------------------------------

def _run_bq_query(bq_client: bigquery.Client, project: str, sql: str,
                  max_rows: int = 30) -> str:
    """Execute a read-only BQ query, return results as a plain-text table."""
    import time as _time
    sql_preview = sql.strip()[:200]
    logger.info(
        "[rag] BQ tool call executing | sql=%s | max_rows=%d",
        sql_preview, max_rows,
        extra={"json_fields": {"sql_preview": sql_preview, "max_rows": max_rows}},
    )
    sql_upper = sql.strip().upper()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        logger.warning("[rag] BQ tool rejected non-SELECT query | sql=%s", sql.strip()[:100])
        return "Error: only SELECT/WITH queries are permitted."
    if "LIMIT" not in sql_upper:
        sql = sql.rstrip("; ") + f" LIMIT {max_rows}"

    t0 = _time.monotonic()
    try:
        rows = list(bq_client.query(sql).result())
    except Exception as exc:
        latency_ms = int((_time.monotonic() - t0) * 1000)
        logger.error(
            "[rag] FAILED: BQ tool query error | latency_ms=%d | sql=%s | error=%s",
            latency_ms, sql_preview, exc,
            exc_info=True,
            extra={"json_fields": {"sql_preview": sql_preview, "latency_ms": latency_ms}},
        )
        return f"Query error: {exc}"

    latency_ms = int((_time.monotonic() - t0) * 1000)

    if not rows:
        logger.warning(
            "[rag] BQ tool returned 0 rows | latency_ms=%d | sql=%s",
            latency_ms, sql_preview,
            extra={"json_fields": {"latency_ms": latency_ms, "sql_preview": sql_preview}},
        )
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
    logger.info(
        "[rag] ✓ BQ tool query complete | rows=%d | cols=%d | latency_ms=%d | sql=%s",
        len(rows), len(fields), latency_ms, sql_preview,
        extra={"json_fields": {"row_count": len(rows), "latency_ms": latency_ms, "col_count": len(fields)}},
    )
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_context_block(docs: list[RetrievedDoc]) -> str:
    logger.debug("→ _build_context_block called", extra={"json_fields": {"doc_count": len(docs)}})
    parts = []
    for i, doc in enumerate(docs, start=1):
        logger.debug("  doc[%d]: chunk_id=%s source_type=%s domain=%s score=%.4f", i, doc.chunk_id, doc.source_type, doc.domain, doc.score)
        parts.append(f"[{i}] ({doc.source_type} | {doc.domain})\n{doc.text}")
    result = "\n\n---\n\n".join(parts)
    logger.debug("← _build_context_block done", extra={"json_fields": {"context_length": len(result)}})
    return result


def _sources_payload(docs: list[RetrievedDoc]) -> str:
    logger.debug("→ _sources_payload called", extra={"json_fields": {"doc_count": len(docs)}})
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
    payload = json.dumps(sources)
    logger.debug("← _sources_payload done", extra={"json_fields": {"source_count": len(sources)}})
    return payload


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
    logger.info("→ _fallback_answer called", extra={"json_fields": {"message_count": len(messages)}})
    llm = ChatAnthropic(model=_MODEL, anthropic_api_key=ant_key, max_tokens=1024)
    try:
        resp = await llm.ainvoke(messages, config=run_cfg)
        text = _extract_text(resp.content)
        if text:
            logger.info("← _fallback_answer yielding text", extra={"json_fields": {"text_length": len(text)}})
            yield f"data: {json.dumps(text)}\n\n"
        else:
            logger.warning("_fallback_answer produced empty text")
    except Exception as exc:
        logger.error("Fallback invocation failed: %s", exc, exc_info=True)


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
    logger.info(
        "[rag] stream_answer starting | query=%s | top_k=%d | model=%s",
        query[:80], top_k, _MODEL,
        extra={"json_fields": {"query_preview": query[:80], "top_k": top_k}},
    )
    ant_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not ant_key:
        logger.error("[rag] ANTHROPIC_API_KEY not set — cannot stream answer")
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
    logger.info(
        "[rag] Parallel classify+retrieve complete | relevant=%s | docs_retrieved=%d | query=%s",
        relevant, len(docs), query[:80],
        extra={"json_fields": {"relevant": relevant, "retrieved_doc_count": len(docs)}},
    )

    if not relevant:
        logger.info("[rag] Query classified off-topic | returning decline | query=%s", query[:80])
        yield f"data: __sources__{json.dumps([])}\n\n"
        yield f"data: {json.dumps(_DECLINE_MESSAGE)}\n\n"
        yield "data: __done__\n\n"
        return

    logger.info("[rag] Query on-topic | docs_retrieved=%d | query=%s", len(docs), query[:80])

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

    logger.info("[rag] Emitting sources payload | docs=%d | query=%s", len(docs), query[:80])
    yield f"data: __sources__{_sources_payload(docs)}\n\n"

    run_cfg = {
        "run_name": "risklens_chat",
        "metadata": {"query": query, "top_k": top_k, "docs_retrieved": len(docs)},
    }

    plain_llm = ChatAnthropic(
        model=_MODEL, anthropic_api_key=ant_key, streaming=True, max_tokens=1024,
    )

    # ── Phase 1: stream with tools ────────────────────────────────────────────
    logger.info("[rag] Phase 1 streaming starting | model=%s | tools=query_bigquery | query=%s", _MODEL, query[:80], extra={"json_fields": {"model": _MODEL}})
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
        logger.info(
            "[rag] Phase 1 stream complete | chunks=%d | text_yielded=%s | query=%s",
            len(chunks), text_yielded, query[:80],
            extra={"json_fields": {"chunk_count": len(chunks), "text_yielded": text_yielded}},
        )
    except Exception as exc:
        logger.error("[rag] FAILED: Phase 1 stream error | query=%s | error=%s", query[:80], exc, exc_info=True)
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
            logger.info(
                "[rag] BQ tool call detected | sql=%s | tool_call_id=%s | query=%s",
                sql[:200], tc.get("id"), query[:80],
                extra={"json_fields": {"sql_preview": sql[:300], "tool_call_id": tc.get("id")}},
            )

            tool_result = _run_bq_query(bq_client, project, sql)
            logger.info(
                "[rag] BQ tool result ready | result_length=%d | query=%s",
                len(tool_result), query[:80],
                extra={"json_fields": {"result_preview": tool_result[:200]}},
            )

            follow_up = messages + [
                full_msg,
                ToolMessage(content=tool_result, tool_call_id=tc["id"]),
            ]
            logger.info("[rag] Phase 2 streaming starting (continuation after BQ tool) | model=%s | query=%s", _MODEL, query[:80])
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
                logger.info("[rag] Phase 2 stream complete | yielded=%s | query=%s", phase2_yielded, query[:80])
            except Exception as exc:
                logger.error("[rag] FAILED: Phase 2 stream error | query=%s | error=%s", query[:80], exc, exc_info=True)

            # Phase 2 produced nothing — build a plain answer from tool result
            if not phase2_yielded:
                logger.warning("[rag] Phase 2 yielded nothing — falling back | query=%s", query[:80])
                fallback_msgs = messages + [
                    HumanMessage(content=(
                        f"Here is the query result:\n{tool_result}\n\n"
                        f"Please answer the original question using this data."
                    ))
                ]
                async for tok in _fallback_answer(fallback_msgs, ant_key, run_cfg):
                    yield tok
        else:
            logger.info("[rag] No BQ tool calls in Phase 1 response | query=%s", query[:80])

    logger.info("[rag] ✓ stream_answer complete | query=%s", query[:80])
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
    logger.info(
        "→ answer called",
        extra={"json_fields": {"query_preview": query[:80], "top_k": top_k}},
    )
    ant_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not ant_key:
        logger.error("ANTHROPIC_API_KEY not set — cannot answer")
        raise ValueError("ANTHROPIC_API_KEY not set")

    logger.debug("Launching parallel relevance check + retrieval for non-streaming answer")
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
    logger.info(
        "Parallel gather complete (non-streaming)",
        extra={"json_fields": {"relevant": relevant, "retrieved_doc_count": len(docs)}},
    )

    if not relevant:
        logger.info("Query classified off-topic — returning decline")
        return {"answer": _DECLINE_MESSAGE, "sources": [], "relevant": False}

    context_block = _build_context_block(docs)
    user_message = (
        f"Context from the RiskLens data catalog:\n\n{context_block}\n\n"
        f"Question: {query}"
    )
    messages = [SystemMessage(content=_SYSTEM_PROMPT), HumanMessage(content=user_message)]

    llm = ChatAnthropic(model=_MODEL, anthropic_api_key=ant_key, max_tokens=1024)
    llm_with_tools = llm.bind_tools([_BQ_TOOL_DEF])

    logger.debug("Invoking LLM (non-streaming) with %d messages", len(messages))
    response = await llm_with_tools.ainvoke(messages)
    content = _extract_text(response.content)
    logger.debug("LLM response length: %d chars", len(content))

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
        logger.info(
            "BQ tool call in non-streaming answer",
            extra={"json_fields": {"sql_preview": sql[:300]}},
        )
        tool_result = _run_bq_query(bq_client, project, sql)
        follow_up = messages + [
            response,
            ToolMessage(content=tool_result, tool_call_id=tc["id"]),
        ]
        logger.debug("Invoking LLM for Phase 2 continuation (non-streaming)")
        final = await llm.ainvoke(follow_up)
        final_text = _extract_text(final.content)
        logger.info(
            "← answer done (with tool call)",
            extra={"json_fields": {"answer_length": len(final_text), "source_count": len(sources)}},
        )
        return {"answer": final_text, "sources": sources, "relevant": True}

    logger.info(
        "← answer done (no tool call)",
        extra={"json_fields": {"answer_length": len(content), "source_count": len(sources)}},
    )
    return {"answer": content, "sources": sources, "relevant": True}
