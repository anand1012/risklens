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
import time

from common.logging_setup import setup_cloud_logging

# Install Cloud Logging as the root handler (falls back to basicConfig locally)
setup_cloud_logging(labels={"service": "indexing"})
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("[indexing] Pipeline starting | program=run_indexing.py | steps=chunk→ui_docs→embed→bm25")
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
    logger.info(
        "[indexing] Args | project=%s | bucket=%s | truncate=%s",
        args.project, args.bucket, args.truncate,
        extra={"json_fields": {"project": args.project, "bucket": args.bucket, "truncate": args.truncate}},
    )

    t_pipeline_start = time.monotonic()

    # ── Step 1: chunk ────────────────────────────────────────────────────────
    logger.info("[indexing] Step 1/4: Chunking BigQuery metadata | sources=risklens_catalog.assets+schema_registry+ownership+quality_scores+sla_status+desk_registry | project=%s", args.project)
    t_step1 = time.monotonic()
    from indexing.chunker import build_chunks
    chunks = build_chunks(project=args.project)
    step1_ms = int((time.monotonic() - t_step1) * 1000)
    if not chunks:
        logger.error(
            "[indexing] FAILED: No chunks produced | project=%s | is BigQuery populated? Run the ingestion pipeline first.",
            args.project,
            extra={"json_fields": {"project": args.project}},
        )
        sys.exit(1)
    logger.info(
        "[indexing] ✓ Step 1 complete | bq_chunks=%d | latency_ms=%d",
        len(chunks), step1_ms,
        extra={"json_fields": {"bq_chunk_count": len(chunks), "step1_ms": step1_ms}},
    )

    # ── Step 1b: UI docs ─────────────────────────────────────────────────────
    logger.info("[indexing] Step 1b/4: Loading UI documentation chunks | source=indexing/ui_docs.py")
    t_step1b = time.monotonic()
    from indexing.ui_docs import build_ui_docs
    ui_chunks = build_ui_docs()
    step1b_ms = int((time.monotonic() - t_step1b) * 1000)
    chunks.extend(ui_chunks)
    logger.info(
        "[indexing] ✓ Step 1b complete | ui_chunks=%d | total_chunks=%d | latency_ms=%d",
        len(ui_chunks), len(chunks), step1b_ms,
        extra={"json_fields": {"ui_chunk_count": len(ui_chunks), "total_chunks": len(chunks), "step1b_ms": step1b_ms}},
    )

    # ── Step 2: embed ────────────────────────────────────────────────────────
    logger.info("[indexing] Step 2/4: Generating Vertex AI embeddings | chunks=%d | target=risklens_embeddings.vectors | truncate=%s", len(chunks), args.truncate)
    t_step2 = time.monotonic()
    from indexing.embedder import embed_and_store
    embed_and_store(chunks, project=args.project, truncate=args.truncate)
    step2_ms = int((time.monotonic() - t_step2) * 1000)
    logger.info(
        "[indexing] ✓ Step 2 complete | chunks_embedded=%d | latency_ms=%d | table=risklens_embeddings.vectors",
        len(chunks), step2_ms,
        extra={"json_fields": {"chunks_embedded": len(chunks), "step2_ms": step2_ms}},
    )

    # ── Step 3: bm25 ────────────────────────────────────────────────────────
    logger.info("[indexing] Step 3/4: Building BM25 index | chunks=%d | target=gs://%s/indexes/", len(chunks), args.bucket)
    t_step3 = time.monotonic()
    from indexing.bm25_index import build_and_save
    build_and_save(chunks, bucket=args.bucket)
    step3_ms = int((time.monotonic() - t_step3) * 1000)
    logger.info(
        "[indexing] ✓ Step 3 complete | bucket=%s | latency_ms=%d",
        args.bucket, step3_ms,
        extra={"json_fields": {"bucket": args.bucket, "step3_ms": step3_ms}},
    )

    total_ms = int((time.monotonic() - t_pipeline_start) * 1000)
    logger.info(
        "[indexing] ✓ Indexing pipeline complete | total_chunks=%d | total_ms=%d | step1_ms=%d | step1b_ms=%d | step2_ms=%d | step3_ms=%d",
        len(chunks), total_ms, step1_ms, step1b_ms, step2_ms, step3_ms,
        extra={"json_fields": {
            "total_ms": total_ms,
            "step1_ms": step1_ms,
            "step1b_ms": step1b_ms,
            "step2_ms": step2_ms,
            "step3_ms": step3_ms,
            "total_chunks": len(chunks),
        }},
    )
    logger.info("[indexing]   BQ chunks:  %s.risklens_embeddings.chunks", args.project)
    logger.info("[indexing]   BQ vectors: %s.risklens_embeddings.vectors", args.project)
    logger.info("[indexing]   GCS index:  gs://%s/indexes/", args.bucket)


if __name__ == "__main__":
    main()
