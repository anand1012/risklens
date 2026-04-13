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
    gcs = gcs_client or storage.Client()

    tokenized = [_tokenize(c.text) for c in chunks]
    logger.info("Building BM25 index over %d documents…", len(chunks))
    bm25 = BM25Okapi(tokenized)

    _upload_pickle(gcs, bucket, _CORPUS_BLOB, chunks)
    _upload_pickle(gcs, bucket, _INDEX_BLOB, bm25)

    logger.info(
        "BM25 index saved → gs://%s/%s and gs://%s/%s",
        bucket, _CORPUS_BLOB,
        bucket, _INDEX_BLOB,
    )
    return bm25


def _upload_pickle(
    gcs: storage.Client,
    bucket_name: str,
    blob_name: str,
    obj: object,
) -> None:
    bucket = gcs.bucket(bucket_name)
    if not bucket.exists():
        bucket.create(location="US")
        logger.info("Created GCS bucket: %s", bucket_name)

    buf = io.BytesIO()
    pickle.dump(obj, buf)
    buf.seek(0)

    blob = bucket.blob(blob_name)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    logger.info("Uploaded %s (%d bytes)", blob_name, buf.tell())


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
    gcs = gcs_client or storage.Client()
    corpus: list[ChunkDoc] = _download_pickle(gcs, bucket, _CORPUS_BLOB)
    bm25: BM25Okapi = _download_pickle(gcs, bucket, _INDEX_BLOB)
    logger.info("BM25 index loaded: %d documents", len(corpus))
    return bm25, corpus


def _download_pickle(
    gcs: storage.Client,
    bucket_name: str,
    blob_name: str,
) -> object:
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
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
    return [(corpus[i], float(scores[i])) for i in top_indices if scores[i] > 0]
