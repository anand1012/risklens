"""
RiskLens — FastAPI Application Entry Point

Lifespan startup loads:
  - BigQuery client
  - BM25 index + corpus from GCS (built by indexing/run_indexing.py)

These are stored on app.state so all routers share a single instance.

Environment variables required:
  GCP_PROJECT       — GCP project ID (default: risklens-frtb-2026)
  GCS_BUCKET        — GCS bucket with BM25 index (default: risklens-frtb-2026-indexes)
  ANTHROPIC_API_KEY — for Claude (chat endpoint)
  COHERE_API_KEY    — for embeddings (chat endpoint)
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.db.bigquery import get_client
from api.routers import catalog, chat, governance, lineage, search
from indexing.bm25_index import load_from_gcs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

_GCS_BUCKET = os.environ.get("GCS_BUCKET", "risklens-frtb-2026-indexes")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up RiskLens API…")

    app.state.bq_client = get_client()
    logger.info("BigQuery client ready (project: %s)", app.state.bq_client.project)

    logger.info("Loading BM25 index from gs://%s/indexes/…", _GCS_BUCKET)
    bm25, corpus = load_from_gcs(bucket=_GCS_BUCKET)
    app.state.bm25_index = bm25
    app.state.bm25_corpus = corpus
    logger.info("BM25 index ready: %d documents", len(corpus))

    yield

    logger.info("Shutting down RiskLens API.")


app = FastAPI(
    title="RiskLens API",
    description="FRTB Data Catalog & AI Lineage Explorer",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to frontend origin in production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(catalog.router)
app.include_router(lineage.router)
app.include_router(governance.router)
app.include_router(search.router)
app.include_router(chat.router)


@app.get("/health", tags=["meta"])
def health():
    return {
        "status": "ok",
        "bm25_docs": len(app.state.bm25_corpus) if hasattr(app.state, "bm25_corpus") else 0,
    }
