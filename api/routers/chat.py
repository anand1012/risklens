"""
Chat router — streaming RAG chat endpoint.

POST /chat
  Body: {"query": "What tables contain VaR data?", "top_k": 8}
  Response: text/event-stream

SSE event format:
  data: __sources__[{...}, ...]   — JSON array of source docs (first event)
  data: <token>                   — streamed answer tokens
  data: __done__                  — end of stream

Also logs each chat query to risklens_catalog.access_log for analytics.
"""

import logging
import os
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.db.bigquery import get_client, project
from api.rag.chain import stream_answer

router = APIRouter(prefix="/chat", tags=["chat"])
logger = logging.getLogger(__name__)


class ChatRequest(BaseModel):
    query: str
    top_k: int = 8


@router.post("")
async def chat(req: ChatRequest, request: Request):
    session_id = request.headers.get("x-session-id", "unknown")
    logger.info(
        "[api] POST /chat | session_id=%s | query=%s | top_k=%d",
        session_id, req.query[:80], req.top_k,
        extra={"json_fields": {
            "session_id": session_id,
            "query_preview": req.query[:80],
            "top_k": req.top_k,
        }},
    )
    bm25 = request.app.state.bm25_index
    corpus = request.app.state.bm25_corpus
    bq_client = request.app.state.bq_client
    corpus_size = len(corpus) if corpus else 0
    logger.info("[api] POST /chat | source=bm25_index+vertex_vector_search | corpus_size=%d | top_k=%d | session_id=%s", corpus_size, req.top_k, session_id)

    _log_query(req.query, request)

    async def event_stream():
        logger.info(
            "[api] POST /chat streaming started | session_id=%s | query=%s",
            session_id, req.query[:80],
            extra={"json_fields": {"session_id": session_id, "query_preview": req.query[:80]}},
        )
        token_count = 0
        try:
            async for chunk in stream_answer(
                query=req.query,
                bm25_index=bm25,
                corpus=corpus,
                bq_client=bq_client,
                project=project(),
                top_k=req.top_k,
                anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY"),
            ):
                token_count += 1
                yield chunk
        except Exception as exc:
            logger.error(
                "[api] FAILED: Chat stream error | session_id=%s | query=%s | error=%s",
                session_id, req.query[:80], exc,
                exc_info=True,
                extra={"json_fields": {"session_id": session_id}},
            )
            yield "data: __done__\n\n"
        logger.info(
            "[api] ✓ POST /chat stream complete | session_id=%s | sse_events=%d | query=%s",
            session_id, token_count, req.query[:80],
            extra={"json_fields": {"session_id": session_id, "sse_events_emitted": token_count}},
        )

    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _log_query(query: str, request: Request) -> None:
    """Fire-and-forget access log write to BigQuery."""
    session_id = request.headers.get("x-session-id")
    logger.info(
        "[api] Logging chat query | table=risklens_catalog.access_log | session_id=%s | query=%s",
        session_id, query[:80],
        extra={"json_fields": {
            "query_preview": query[:80],
            "session_id": session_id,
        }},
    )
    try:
        row = {
            "event_id": str(uuid.uuid4()),
            "page": "chat",
            "action": "chat_query",
            "detail": query[:500],
            "ip_address": request.client.host if request.client else None,
            "session_id": session_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        get_client().insert_rows_json(
            f"{project()}.risklens_catalog.access_log", [row]
        )
        logger.info("[api] ✓ Chat query logged | table=risklens_catalog.access_log | event_id=%s | session_id=%s", row["event_id"], session_id)
    except Exception as exc:
        logger.warning("[api] FAILED: Could not write chat query to access log | table=risklens_catalog.access_log | session_id=%s | error=%s", session_id, exc)
