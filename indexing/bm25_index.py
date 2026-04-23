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
        "→ build_and_save called",
        extra={"json_fields": {"chunk_count": len(chunks), "bucket": bucket}},
    )
    gcs = gcs_client or storage.Client()

    logger.debug("Tokenizing %d chunks for BM25 index", len(chunks))
    t0 = time.monotonic()
    tokenized = [_tokenize(c.text) for c in chunks]
    tokenize_ms = int((time.monotonic() - t0) * 1000)
    logger.debug("Tokenization done in %dms", tokenize_ms)

    vocab: set[str] = set()
    for toks in tokenized:
        vocab.update(toks)
    logger.info(
        "Building BM25 index",
        extra={"json_fields": {"doc_count": len(chunks), "vocab_size": len(vocab)}},
    )

    t_build = time.monotonic()
    bm25 = BM25Okapi(tokenized)
    build_ms = int((time.monotonic() - t_build) * 1000)
    logger.info("BM25Okapi built in %dms", build_ms)

    logger.info("Uploading corpus pickle to GCS: %s", _CORPUS_BLOB)
    corpus_size = _upload_pickle(gcs, bucket, _CORPUS_BLOB, chunks)
    logger.info("Uploading index pickle to GCS: %s", _INDEX_BLOB)
    index_size = _upload_pickle(gcs, bucket, _INDEX_BLOB, bm25)

    logger.info(
        "← build_and_save done",
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
    logger.debug("→ _upload_pickle: bucket=%s blob=%s", bucket_name, blob_name)
    bucket = gcs.bucket(bucket_name)
    if not bucket.exists():
        logger.info("GCS bucket %s does not exist — creating", bucket_name)
        bucket.create(location="US")
        logger.info("Created GCS bucket: %s", bucket_name)

    buf = io.BytesIO()
    pickle.dump(obj, buf)
    buf.seek(0)
    size = buf.getbuffer().nbytes

    blob = bucket.blob(blob_name)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    logger.info(
        "← _upload_pickle done: %s (%d bytes)",
        blob_name, size,
        extra={"json_fields": {"blob_name": blob_name, "size_bytes": size}},
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
        "→ load_from_gcs called",
        extra={"json_fields": {"bucket": bucket}},
    )
    gcs = gcs_client or storage.Client()

    logger.debug("Downloading corpus pickle from GCS: %s", _CORPUS_BLOB)
    t0 = time.monotonic()
    corpus: list[ChunkDoc] = _download_pickle(gcs, bucket, _CORPUS_BLOB)
    corpus_ms = int((time.monotonic() - t0) * 1000)
    logger.debug("Corpus downloaded in %dms: %d docs", corpus_ms, len(corpus))

    logger.debug("Downloading index pickle from GCS: %s", _INDEX_BLOB)
    t1 = time.monotonic()
    bm25: BM25Okapi = _download_pickle(gcs, bucket, _INDEX_BLOB)
    index_ms = int((time.monotonic() - t1) * 1000)
    logger.debug("Index downloaded in %dms", index_ms)

    logger.info(
        "← load_from_gcs done",
        extra={"json_fields": {"doc_count": len(corpus), "corpus_ms": corpus_ms, "index_ms": index_ms}},
    )
    return bm25, corpus


def _download_pickle(
    gcs: storage.Client,
    bucket_name: str,
    blob_name: str,
) -> object:
    logger.debug("→ _download_pickle: bucket=%s blob=%s", bucket_name, blob_name)
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    logger.debug("← _download_pickle: downloaded %d bytes for %s", len(data), blob_name)
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
    logger.debug(
        "→ search called",
        extra={"json_fields": {"query_preview": query[:80], "top_k": top_k, "corpus_size": len(corpus)}},
    )
    tokens = _tokenize(query)
    logger.debug("search: tokenized query → %d tokens: %s", len(tokens), tokens[:10])
    scores = bm25.get_scores(tokens)
    top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
    results = [(corpus[i], float(scores[i])) for i in top_indices if scores[i] > 0]
    logger.debug(
        "← search done",
        extra={"json_fields": {"hit_count": len(results), "top_score": round(results[0][1], 4) if results else None}},
    )
    if not results:
        logger.warning("BM25 search returned 0 hits for query: %.80s", query)
    return results
