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
    logger.info("→ get_client called", extra={"json_fields": {"project": _PROJECT}})
    client = bigquery.Client(project=_PROJECT)
    logger.info("← get_client done", extra={"json_fields": {"project": client.project}})
    return client


def project() -> str:
    logger.debug("→ project called, returning %s", _PROJECT)
    return _PROJECT


# ---------------------------------------------------------------------------
# Shared query helpers
# ---------------------------------------------------------------------------

def query_rows(sql: str, params: list | None = None) -> list[dict]:
    """Run a query and return results as a list of plain dicts."""
    logger.info(
        "→ query_rows called",
        extra={"json_fields": {"sql_preview": sql.strip()[:500], "has_params": bool(params)}},
    )
    client = get_client()
    t0 = time.monotonic()
    try:
        if params:
            logger.debug("Running parameterized query with %d params", len(params))
            job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        else:
            logger.debug("Running unparameterized query")
            job = client.query(sql)
        rows = []
        for row in job.result():
            d = dict(row)
            # Replace NaN/Inf with None so JSON serialization doesn't fail
            for k, v in d.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    logger.debug("NaN/Inf replaced with None for field %s", k)
                    d[k] = None
            rows.append(d)
        latency_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            "← query_rows done",
            extra={"json_fields": {"row_count": len(rows), "latency_ms": latency_ms}},
        )
        if not rows:
            logger.warning("query_rows returned 0 rows", extra={"json_fields": {"sql_preview": sql.strip()[:200]}})
        return rows
    except Exception as exc:
        latency_ms = int((time.monotonic() - t0) * 1000)
        logger.error(
            "query_rows failed after %dms: %s",
            latency_ms, exc,
            exc_info=True,
            extra={"json_fields": {"sql_preview": sql.strip()[:500], "latency_ms": latency_ms}},
        )
        raise
