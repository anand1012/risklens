"""
Governance router — ownership and SLA health endpoints.

GET /governance/sla          SLA breaches summary
GET /governance/ownership    All asset ownership records
GET /governance/quality      Quality scores across all assets
"""

from fastapi import APIRouter, Query

from api.db.bigquery import query_rows, project

router = APIRouter(prefix="/governance", tags=["governance"])


@router.get("/sla")
def get_sla_status(breaches_only: bool = Query(False)):
    where = "s.breach_flag = TRUE" if breaches_only else "1=1"
    sql = f"""
        SELECT
            a.asset_id,
            a.name,
            a.domain,
            a.layer,
            s.expected_refresh,
            s.actual_refresh,
            s.breach_flag,
            s.breach_duration_mins,
            s.checked_at
        FROM `{project()}.risklens_catalog.sla_status` s
        JOIN `{project()}.risklens_catalog.assets` a USING (asset_id)
        WHERE {where}
        ORDER BY s.breach_flag DESC, s.breach_duration_mins DESC
    """
    return query_rows(sql)


@router.get("/ownership")
def get_ownership(team: str | None = Query(None)):
    where = f"o.team = '{team}'" if team else "1=1"
    sql = f"""
        SELECT
            a.asset_id,
            a.name,
            a.domain,
            a.layer,
            o.owner_name,
            o.team,
            o.steward,
            o.email,
            o.assigned_date
        FROM `{project()}.risklens_catalog.ownership` o
        JOIN `{project()}.risklens_catalog.assets` a USING (asset_id)
        WHERE {where}
        ORDER BY o.team, o.owner_name
    """
    return query_rows(sql)


@router.get("/quality")
def get_quality_scores(
    freshness_status: str | None = Query(None, description="fresh | stale | critical"),
    schema_drift: bool | None = Query(None),
):
    filters = ["1=1"]
    if freshness_status:
        filters.append(f"q.freshness_status = '{freshness_status}'")
    if schema_drift is not None:
        filters.append(f"q.schema_drift = {str(schema_drift).upper()}")
    where = " AND ".join(filters)

    sql = f"""
        SELECT
            a.asset_id,
            a.name,
            a.domain,
            a.layer,
            q.null_rate,
            q.schema_drift,
            q.freshness_status,
            q.duplicate_rate,
            q.last_checked
        FROM `{project()}.risklens_catalog.quality_scores` q
        JOIN `{project()}.risklens_catalog.assets` a USING (asset_id)
        WHERE {where}
        ORDER BY q.freshness_status DESC, q.null_rate DESC
    """
    return query_rows(sql)
