"""
RiskLens — BM25 Index Builder
Builds a BM25Okapi index over ChunkDocs and serializes it to GCS.
The FastAPI backend loads this at startup for hybrid retrieval.

GCS layout:
  gs://<bucket>/indexes/bm25_corpus.pkl   — list[ChunkDoc]  (the raw docs)
  gs://<bucket>/indexes/bm25_index.pkl    — BM25Okapi object

Usage (build & save):
    from indexing.bm25_index import build_and_save
    build_and_save(chunks, bucket="risklens-frtb-2026-indexes")

Usage (load at API startup):
    from indexing.bm25_index import load_from_gcs
    index, corpus = load_from_gcs(bucket="risklens-frtb-2026-indexes")
    results = search(index, corpus, query="VaR calculation table", top_k=5)
"""

import io
import logging
import pickle
import re
import time
from typing import Optional

from google.cloud import storage
from rank_bm25 import BM25Okapi

from indexing.chunker import ChunkDoc

logger = logging.getLogger(__name__)

_CORPUS_BLOB = "indexes/bm25_corpus.pkl"
_INDEX_BLOB = "indexes/bm25_index.pkl"


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> list[str]:
    """Lowercase, strip punctuation, split on whitespace."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9_\s]", " ", text)
    return text.split()


# ---------------------------------------------------------------------------
# Build & persist
# ---------------------------------------------------------------------------

def build_and_save(
    chunks: list[ChunkDoc],
    bucket: str,
    gcs_client: Optional[storage.Client] = None,
) -> BM25Okapi:
    """
    Build BM25Okapi from chunks and upload corpus + index to GCS.

    Args:
        chunks:     Output of chunker.build_chunks()
        bucket:     GCS bucket name (will be created if it doesn't exist)
        gcs_client: Optional pre-built GCS client (for testing/injection)

    Returns:
        The in-memory BM25Okapi index
    """
    logger.info(
        "[indexing] build_and_save starting | chunks=%d | bucket=%s | target=gs://%s/indexes/ | program=bm25_index.py",
        len(chunks), bucket, bucket,
        extra={"json_fields": {"chunk_count": len(chunks), "bucket": bucket}},
    )
    gcs = gcs_client or storage.Client()

    t0 = time.monotonic()
    tokenized = [_tokenize(c.text) for c in chunks]
    tokenize_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "[indexing] Tokenization complete | chunks=%d | tokenize_ms=%d",
        len(chunks), tokenize_ms,
        extra={"json_fields": {"chunk_count": len(chunks), "tokenize_ms": tokenize_ms}},
    )

    vocab: set[str] = set()
    for toks in tokenized:
        vocab.update(toks)
    logger.info(
        "[indexing] Building BM25Okapi index | doc_count=%d | vocab_size=%d | program=bm25_index.py",
        len(chunks), len(vocab),
        extra={"json_fields": {"doc_count": len(chunks), "vocab_size": len(vocab)}},
    )

    t_build = time.monotonic()
    bm25 = BM25Okapi(tokenized)
    build_ms = int((time.monotonic() - t_build) * 1000)
    logger.info(
        "[indexing] ✓ BM25Okapi built | doc_count=%d | vocab_size=%d | build_ms=%d",
        len(chunks), len(vocab), build_ms,
        extra={"json_fields": {"doc_count": len(chunks), "vocab_size": len(vocab), "build_ms": build_ms}},
    )

    logger.info("[indexing] Uploading corpus pickle | blob=gs://%s/%s", bucket, _CORPUS_BLOB)
    corpus_size = _upload_pickle(gcs, bucket, _CORPUS_BLOB, chunks)
    logger.info("[indexing] Uploading index pickle | blob=gs://%s/%s", bucket, _INDEX_BLOB)
    index_size = _upload_pickle(gcs, bucket, _INDEX_BLOB, bm25)

    logger.info(
        "[indexing] ✓ build_and_save complete | bucket=%s | corpus_bytes=%d | index_bytes=%d | doc_count=%d | vocab_size=%d | program=bm25_index.py",
        bucket, corpus_size, index_size, len(chunks), len(vocab),
        extra={"json_fields": {
            "bucket": bucket,
            "corpus_bytes": corpus_size,
            "index_bytes": index_size,
            "doc_count": len(chunks),
            "vocab_size": len(vocab),
        }},
    )
    return bm25


def _upload_pickle(
    gcs: storage.Client,
    bucket_name: str,
    blob_name: str,
    obj: object,
) -> int:
    """Upload pickled object to GCS. Returns uploaded byte count."""
    bucket = gcs.bucket(bucket_name)
    if not bucket.exists():
        logger.info("[indexing] GCS bucket does not exist — creating | bucket=%s", bucket_name)
        bucket.create(location="US")
        logger.info("[indexing] ✓ Created GCS bucket | bucket=%s", bucket_name)

    buf = io.BytesIO()
    pickle.dump(obj, buf)
    buf.seek(0)
    size = buf.getbuffer().nbytes

    blob = bucket.blob(blob_name)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    logger.info(
        "[indexing] ✓ Uploaded pickle | gcs_path=gs://%s/%s | size_bytes=%d",
        bucket_name, blob_name, size,
        extra={"json_fields": {"bucket": bucket_name, "blob_name": blob_name, "size_bytes": size}},
    )
    return size


# ---------------------------------------------------------------------------
# Load (API startup)
# ---------------------------------------------------------------------------

def load_from_gcs(
    bucket: str,
    gcs_client: Optional[storage.Client] = None,
) -> tuple[BM25Okapi, list[ChunkDoc]]:
    """
    Download and deserialize the BM25 index + corpus from GCS.
    Called once at FastAPI startup; results should be cached in app state.

    Returns:
        (bm25_index, corpus)
    """
    logger.info(
        "[indexing] load_from_gcs starting | bucket=%s | corpus=gs://%s/%s | index=gs://%s/%s | program=bm25_index.py",
        bucket, bucket, _CORPUS_BLOB, bucket, _INDEX_BLOB,
        extra={"json_fields": {"bucket": bucket}},
    )
    gcs = gcs_client or storage.Client()

    t0 = time.monotonic()
    corpus: list[ChunkDoc] = _download_pickle(gcs, bucket, _CORPUS_BLOB)
    corpus_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "[indexing] ✓ Corpus downloaded | docs=%d | gcs_path=gs://%s/%s | latency_ms=%d",
        len(corpus), bucket, _CORPUS_BLOB, corpus_ms,
        extra={"json_fields": {"doc_count": len(corpus), "latency_ms": corpus_ms}},
    )

    t1 = time.monotonic()
    bm25: BM25Okapi = _download_pickle(gcs, bucket, _INDEX_BLOB)
    index_ms = int((time.monotonic() - t1) * 1000)
    logger.info(
        "[indexing] ✓ BM25 index downloaded | gcs_path=gs://%s/%s | latency_ms=%d",
        bucket, _INDEX_BLOB, index_ms,
        extra={"json_fields": {"latency_ms": index_ms}},
    )

    logger.info(
        "[indexing] ✓ load_from_gcs complete | doc_count=%d | corpus_ms=%d | index_ms=%d | bucket=%s",
        len(corpus), corpus_ms, index_ms, bucket,
        extra={"json_fields": {"doc_count": len(corpus), "corpus_ms": corpus_ms, "index_ms": index_ms}},
    )
    return bm25, corpus


def _download_pickle(
    gcs: storage.Client,
    bucket_name: str,
    blob_name: str,
) -> object:
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    logger.debug("[indexing] Downloaded pickle | gcs_path=gs://%s/%s | bytes=%d", bucket_name, blob_name, len(data))
    return pickle.loads(data)


# ---------------------------------------------------------------------------
# Search helper (used by retriever at query time)
# ---------------------------------------------------------------------------

def search(
    bm25: BM25Okapi,
    corpus: list[ChunkDoc],
    query: str,
    top_k: int = 10,
) -> list[tuple[ChunkDoc, float]]:
    """
    BM25 retrieval for a query string.

    Returns:
        List of (ChunkDoc, score) sorted by descending score, length top_k.
    """
    tokens = _tokenize(query)
    scores = bm25.get_scores(tokens)
    top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
    results = [(corpus[i], float(scores[i])) for i in top_indices if scores[i] > 0]
    top_score = round(results[0][1], 4) if results else 0.0
    logger.debug(
        "[indexing] BM25 search | query=%s | tokens=%d | corpus=%d | hits=%d | top_score=%.4f | top_k=%d",
        query[:80], len(tokens), len(corpus), len(results), top_score, top_k,
        extra={"json_fields": {"hit_count": len(results), "top_score": top_score, "tokens": len(tokens)}},
    )
    if not results:
        logger.warning("[indexing] BM25 search returned 0 hits | query=%s | corpus_size=%d | tokens=%d", query[:80], len(corpus), len(tokens))
    return results
