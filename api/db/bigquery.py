"""
RiskLens — BigQuery client factory and shared query helpers.
All routers import from here so we never create duplicate BQ clients.
"""

import os
from functools import lru_cache

from google.cloud import bigquery

_PROJECT = os.environ.get("GCP_PROJECT", "risklens-frtb-2026")


@lru_cache(maxsize=1)
def get_client() -> bigquery.Client:
    return bigquery.Client(project=_PROJECT)


def project() -> str:
    return _PROJECT


# ---------------------------------------------------------------------------
# Shared query helpers
# ---------------------------------------------------------------------------

def query_rows(sql: str, params: list | None = None) -> list[dict]:
    """Run a query and return results as a list of plain dicts."""
    client = get_client()
    if params:
        job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    else:
        job = client.query(sql)
    return [dict(row) for row in job.result()]
