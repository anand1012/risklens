"""
RiskLens — Indexing Pipeline Runner
Orchestrates: chunk → embed (Vertex AI) → bm25

Usage:
    python -m indexing.run_indexing \
        --project risklens-frtb-2026 \
        --bucket  risklens-frtb-2026-indexes \
        [--truncate]

Requires GCP Application Default Credentials (ADC) or Workload Identity.
No API keys needed — Vertex AI uses IAM authentication.
"""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="RiskLens indexing pipeline")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--bucket", required=True, help="GCS bucket for BM25 index artifacts")
    parser.add_argument(
        "--truncate",
        action="store_true",
        default=False,
        help="Truncate BQ embedding tables before inserting (full refresh)",
    )
    args = parser.parse_args()

    # ── Step 1: chunk ────────────────────────────────────────────────────────
    logger.info("=== Step 1/4: Chunking BigQuery metadata ===")
    from indexing.chunker import build_chunks
    chunks = build_chunks(project=args.project)
    if not chunks:
        logger.error("No chunks produced — is BigQuery populated? Run the ingestion pipeline first.")
        sys.exit(1)
    logger.info("BQ metadata chunks: %d", len(chunks))

    # ── Step 1b: UI docs ─────────────────────────────────────────────────────
    from indexing.ui_docs import build_ui_docs
    ui_chunks = build_ui_docs()
    chunks.extend(ui_chunks)
    logger.info("UI docs → %d chunks (total: %d)", len(ui_chunks), len(chunks))

    # ── Step 2: embed ────────────────────────────────────────────────────────
    logger.info("=== Step 2/4: Generating Vertex AI embeddings → BigQuery ===")
    from indexing.embedder import embed_and_store
    embed_and_store(chunks, project=args.project, truncate=args.truncate)

    # ── Step 3: bm25 ────────────────────────────────────────────────────────
    logger.info("=== Step 3/4: Building BM25 index → GCS ===")
    from indexing.bm25_index import build_and_save
    build_and_save(chunks, bucket=args.bucket)

    logger.info("=== Indexing complete ===")
    logger.info("  BQ chunks:  %s.risklens_embeddings.chunks", args.project)
    logger.info("  BQ vectors: %s.risklens_embeddings.vectors", args.project)
    logger.info("  GCS index:  gs://%s/indexes/", args.bucket)


if __name__ == "__main__":
    main()
