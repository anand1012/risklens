"""
RiskLens — RAG Agent with BigQuery Tool
Single-turn agent that:
  1. Retrieves context via hybrid retriever (Vertex AI + BM25)
  2. Streams answer via Claude, with access to a query_bigquery tool
  3. OFF_TOPIC guard is embedded in _SYSTEM_PROMPT — no separate classifier call

OFF_TOPIC flow:
  - Claude outputs "OFF_TOPIC" as its entire response for irrelevant queries
  - stream_answer() detects this in the first ≤20 chars and yields _DECLINE_MESSAGE
  - Saves one full LLM call vs the old pre-classifier approach

BigQuery tool:
  - query_bigquery(sql) — Claude calls this for live data questions
  - Phase 1: stream with tool; Phase 2: if tool called, execute + stream continuation
"""

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

_MODEL = "claude-sonnet-4-6"

_SYSTEM_PROMPT = """You are RiskLens AI, an expert assistant for a financial data catalog \
built for FRTB (Fundamental Review of the Trading Book) compliance.

You help risk managers, data stewards, and quants with:
- What data assets exist (tables, feeds, models, reports)
- How data flows through the pipeline (lineage)
- Data quality, ownership, and SLA status
- FRTB-specific concepts (VaR, ES, P&L Attribution, NMRF, BCBS 239, RFET, PLAT)

You have access to a query_bigquery tool. Use it when the question requires live data \
(e.g. current breaches, actual risk values, row counts, SLA status, desk-level metrics). \
For conceptual or schema questions, use the provided context.

BigQuery project: risklens-frtb-2026
Key datasets and tables:
  risklens_gold     — backtesting, capital_charge, es_outputs, plat_results, rfet_results, risk_summary, trade_positions
  risklens_silver   — risk_enriched, risk_outputs, rates, prices, trades
  risklens_catalog  — assets, quality_scores, sla_status, ownership
  risklens_lineage  — nodes, edges

Always be concise and precise — your audience are quantitative finance professionals.

If the question is entirely unrelated to FRTB, financial data, risk management, or this \
system, respond with exactly: OFF_TOPIC"""

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
    """
    ant_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not ant_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    docs = retrieve(
        query=query, bm25_index=bm25_index, corpus=corpus,
        bq_client=bq_client, project=project, top_k=top_k,
    )
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

    # ── Phase 1: stream with tools ────────────────────────────────────────────
    chunks: list = []
    off_topic_buf = ""
    off_topic_resolved = False

    run_cfg = {
        "run_name": "risklens_chat",
        "metadata": {"query": query, "top_k": top_k, "docs_retrieved": len(docs)},
    }

    async for chunk in llm_with_tools.astream(messages, config=run_cfg):
        chunks.append(chunk)
        token = chunk.content if isinstance(chunk.content, str) else ""
        if not token:
            continue  # tool_call_chunks have no string content

        if not off_topic_resolved:
            off_topic_buf += token
            # Wait until we have enough chars to make the call
            if len(off_topic_buf) >= 20 or "\n" in off_topic_buf:
                off_topic_resolved = True
                if off_topic_buf.strip().upper().startswith("OFF_TOPIC"):
                    yield f"data: {json.dumps(_DECLINE_MESSAGE)}\n\n"
                    yield "data: __done__\n\n"
                    return
                yield f"data: {json.dumps(off_topic_buf)}\n\n"
                off_topic_buf = ""
        else:
            yield f"data: {json.dumps(token)}\n\n"

    # Flush buffer if stream ended before we accumulated 20 chars
    if not off_topic_resolved and off_topic_buf:
        if off_topic_buf.strip().upper().startswith("OFF_TOPIC"):
            yield f"data: {json.dumps(_DECLINE_MESSAGE)}\n\n"
            yield "data: __done__\n\n"
            return
        yield f"data: {json.dumps(off_topic_buf)}\n\n"

    # ── Phase 2: execute tool call if present ─────────────────────────────────
    if chunks:
        try:
            full_msg = reduce(add, chunks)
        except Exception as exc:
            logger.warning("Could not merge stream chunks: %s", exc)
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
            plain_llm = ChatAnthropic(
                model=_MODEL, anthropic_api_key=ant_key, streaming=True, max_tokens=1024,
            )
            async for chunk in plain_llm.astream(follow_up, config=run_cfg):
                token = chunk.content if isinstance(chunk.content, str) else ""
                if token:
                    yield f"data: {json.dumps(token)}\n\n"

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

    docs = retrieve(
        query=query, bm25_index=bm25_index, corpus=corpus,
        bq_client=bq_client, project=project, top_k=top_k,
    )
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

    if content.strip().upper().startswith("OFF_TOPIC"):
        return {"answer": _DECLINE_MESSAGE, "sources": [], "relevant": False}

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
