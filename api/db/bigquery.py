"""
RiskLens — BigQuery client factory and shared query helpers.
All routers import from here so we never create duplicate BQ clients.
"""

import logging
import math
import os
import time
from functools import lru_cache

from google.cloud import bigquery

from common.logging_setup import setup_cloud_logging

setup_cloud_logging(labels={"service": "api"})
logger = logging.getLogger(__name__)

_PROJECT = os.environ.get("GCP_PROJECT", "risklens-frtb-2026")


@lru_cache(maxsize=1)
def get_client() -> bigquery.Client:
    logger.info("[api] Initializing BigQuery client | project=%s", _PROJECT,
                extra={"json_fields": {"project": _PROJECT}})
    client = bigquery.Client(project=_PROJECT)
    logger.info("[api] ✓ BigQuery client ready | project=%s", client.project,
                extra={"json_fields": {"project": client.project}})
    return client


def project() -> str:
    return _PROJECT


# ---------------------------------------------------------------------------
# Shared query helpers
# ---------------------------------------------------------------------------

def query_rows(sql: str, params: list | None = None) -> list[dict]:
    """Run a query and return results as a list of plain dicts."""
    sql_preview = sql.strip()[:200]
    logger.info(
        "[api] BigQuery query starting | sql=%s | params=%d | program=bigquery.py",
        sql_preview, len(params) if params else 0,
        extra={"json_fields": {"sql_preview": sql_preview, "param_count": len(params) if params else 0}},
    )
    client = get_client()
    t0 = time.monotonic()
    try:
        if params:
            job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        else:
            job = client.query(sql)
        rows = []
        for row in job.result():
            d = dict(row)
            # Replace NaN/Inf with None so JSON serialization doesn't fail
            for k, v in d.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    d[k] = None
            rows.append(d)
        latency_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            "[api] ✓ BigQuery query complete | rows=%d | latency_ms=%d | sql=%s",
            len(rows), latency_ms, sql_preview,
            extra={"json_fields": {"row_count": len(rows), "latency_ms": latency_ms, "sql_preview": sql_preview}},
        )
        if not rows:
            logger.warning("[api] BigQuery returned 0 rows | sql=%s", sql_preview,
                           extra={"json_fields": {"sql_preview": sql_preview}})
        return rows
    except Exception as exc:
        latency_ms = int((time.monotonic() - t0) * 1000)
        logger.error(
            "[api] FAILED: BigQuery query error | latency_ms=%d | sql=%s | error=%s",
            latency_ms, sql_preview, exc,
            exc_info=True,
            extra={"json_fields": {"sql_preview": sql_preview, "latency_ms": latency_ms}},
        )
        raise
