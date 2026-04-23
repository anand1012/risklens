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
        "→ chat called",
        extra={"json_fields": {
            "session_id": session_id,
            "query_preview": req.query[:80],
            "top_k": req.top_k,
        }},
    )
    bm25 = request.app.state.bm25_index
    corpus = request.app.state.bm25_corpus
    bq_client = request.app.state.bq_client
    logger.debug(
        "chat: corpus_size=%d, top_k=%d",
        len(corpus) if corpus else 0, req.top_k,
    )

    _log_query(req.query, request)

    async def event_stream():
        logger.info(
            "chat: stream started",
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
                "Chat stream failed: %s",
                exc,
                exc_info=True,
                extra={"json_fields": {"session_id": session_id}},
            )
            yield "data: __done__\n\n"
        logger.info(
            "chat: stream ended",
            extra={"json_fields": {"session_id": session_id, "sse_events_emitted": token_count}},
        )

    logger.info("← chat: returning StreamingResponse")
    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _log_query(query: str, request: Request) -> None:
    """Fire-and-forget access log write to BigQuery."""
    logger.info(
        "→ _log_query called",
        extra={"json_fields": {
            "query_preview": query[:80],
            "session_id": request.headers.get("x-session-id"),
        }},
    )
    try:
        row = {
            "event_id": str(uuid.uuid4()),
            "page": "chat",
            "action": "chat_query",
            "detail": query[:500],
            "ip_address": request.client.host if request.client else None,
            "session_id": request.headers.get("x-session-id"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        get_client().insert_rows_json(
            f"{project()}.risklens_catalog.access_log", [row]
        )
        logger.debug("_log_query: access log row inserted, event_id=%s", row["event_id"])
    except Exception as exc:
        logger.warning("_log_query: failed to write access log: %s", exc)
