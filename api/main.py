"""
RiskLens — FastAPI Application Entry Point

Lifespan startup loads:
  - BigQuery client
  - BM25 index + corpus from GCS (built by indexing/run_indexing.py)

These are stored on app.state so all routers share a single instance.

Environment variables required:
  GCP_PROJECT            — GCP project ID (default: risklens-frtb-2026)
  GCS_BUCKET             — GCS bucket with BM25 index (default: risklens-frtb-2026-indexes)
  ANTHROPIC_API_KEY      — for Claude (chat endpoint)

LangSmith tracing (optional — omit to disable):
  LANGCHAIN_TRACING_V2   — set to "true" to enable
  LANGCHAIN_API_KEY      — LangSmith API key
  LANGCHAIN_PROJECT      — project name in LangSmith (default: risklens)
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from langsmith import Client as LangSmithClient

from api.db.bigquery import get_client
from api.middleware.logging_middleware import RequestLoggingMiddleware
from api.routers import catalog, chat, governance, lineage, risk, search
from indexing.bm25_index import load_from_gcs
from common.logging_setup import setup_cloud_logging

# Install Cloud Logging as the root handler (falls back to basicConfig locally)
setup_cloud_logging(labels={"service": "api"})
logger = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_BUCKET", "risklens-frtb-2026-indexes")


def _init_langsmith() -> None:
    """Enable LangSmith tracing if env vars are present."""
    if os.environ.get("LANGCHAIN_TRACING_V2", "").lower() != "true":
        logger.info("[api] LangSmith tracing disabled | set LANGCHAIN_TRACING_V2=true to enable")
        return
    if not os.environ.get("LANGCHAIN_API_KEY"):
        logger.warning("[api] LangSmith: LANGCHAIN_TRACING_V2=true but LANGCHAIN_API_KEY not set — tracing skipped")
        return
    # Default project name so traces are grouped under "risklens" in the UI
    os.environ.setdefault("LANGCHAIN_PROJECT", "risklens")
    try:
        LangSmithClient()  # validates the key at startup
        logger.info(
            "[api] LangSmith tracing enabled | project=%s",
            os.environ["LANGCHAIN_PROJECT"],
        )
    except Exception as e:
        logger.warning("[api] LangSmith init failed | error=%s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pod      = os.getenv("HOSTNAME",        "unknown")
    ns       = os.getenv("POD_NAMESPACE",   "unknown")
    node     = os.getenv("NODE_NAME",       "unknown")
    image    = os.getenv("IMAGE_TAG",       "unknown")
    project  = os.getenv("GCP_PROJECT",     "risklens-frtb-2026")
    logger.info(
        "[k8s] Pod starting | pod=%s | namespace=%s | node=%s | image=%s | project=%s | gcs_bucket=%s",
        pod, ns, node, image, project, _GCS_BUCKET,
        extra={"json_fields": {"pod": pod, "namespace": ns, "node": node, "image": image, "gcs_bucket": _GCS_BUCKET}},
    )
    _init_langsmith()

    app.state.bq_client = get_client()
    logger.info(
        "[api] ✓ BigQuery client initialized | project=%s",
        app.state.bq_client.project,
        extra={"json_fields": {"project": app.state.bq_client.project}},
    )

    logger.info(
        "[api] Loading BM25 index from GCS | bucket=%s | path=indexes/",
        _GCS_BUCKET,
        extra={"json_fields": {"bucket": _GCS_BUCKET, "path": "indexes/"}},
    )
    bm25, corpus = load_from_gcs(bucket=_GCS_BUCKET)
    app.state.bm25_index = bm25
    app.state.bm25_corpus = corpus
    logger.info(
        "[api] ✓ BM25 index loaded | chunks=%d | bucket=%s",
        len(corpus), _GCS_BUCKET,
        extra={"json_fields": {"doc_count": len(corpus)}},
    )

    logger.info("[api] ✓ RiskLens API startup complete | routers=catalog,lineage,governance,risk,search,chat | middleware=CORS,RequestLogging | pod=%s | bm25_chunks=%d", pod, len(corpus))

    yield

    logger.info("[api] RiskLens API shutdown | pod=%s", pod)


app = FastAPI(
    title="RiskLens API",
    description="FRTB Data Catalog & AI Lineage Explorer",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to frontend origin in production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(catalog.router,    prefix="/api")
app.include_router(lineage.router,    prefix="/api")
app.include_router(governance.router, prefix="/api")
app.include_router(risk.router,       prefix="/api")
app.include_router(search.router,     prefix="/api")
app.include_router(chat.router,       prefix="/api")


@app.get("/health", tags=["meta"])
def health():
    bm25_docs = len(app.state.bm25_corpus) if hasattr(app.state, "bm25_corpus") else 0
    result = {"status": "ok", "bm25_docs": bm25_docs}
    logger.info("[api] GET /health | status=ok | bm25_chunks=%d", bm25_docs, extra={"json_fields": result})
    return result
