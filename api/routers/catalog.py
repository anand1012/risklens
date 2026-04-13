"""
Catalog router — data asset discovery endpoints.

GET /assets                   list all assets (with optional ?domain= ?layer= filters)
GET /assets/{asset_id}        full asset detail (asset + ownership + quality + SLA)
GET /assets/{asset_id}/schema columns for an asset
"""

from fastapi import APIRouter, Query
from typing import Optional

from api.db.bigquery import query_rows, project

router = APIRouter(prefix="/assets", tags=["catalog"])


@router.get("")
def list_assets(
    domain: Optional[str] = Query(None, description="Filter by domain (risk, market_data, …)"),
    layer: Optional[str] = Query(None, description="Filter by layer (bronze, silver, gold)"),
    type: Optional[str] = Query(None, description="Filter by type (table, feed, report, model)"),
    limit: int = Query(100, le=500),
):
    filters = ["1=1"]
    if domain:
        filters.append(f"a.domain = '{domain}'")
    if layer:
        filters.append(f"a.layer = '{layer}'")
    if type:
        filters.append(f"a.type = '{type}'")
    where = " AND ".join(filters)

    sql = f"""
        SELECT
            a.asset_id,
            a.name,
            a.type,
            a.domain,
            a.layer,
            a.description,
            a.tags,
            a.row_count,
            a.updated_at,
            o.owner_name,
            o.team,
            q.freshness_status,
            q.null_rate,
            s.breach_flag
        FROM `{project()}.risklens_catalog.assets` a
        LEFT JOIN `{project()}.risklens_catalog.ownership` o USING (asset_id)
        LEFT JOIN `{project()}.risklens_catalog.quality_scores` q USING (asset_id)
        LEFT JOIN `{project()}.risklens_catalog.sla_status` s USING (asset_id)
        WHERE {where}
        ORDER BY a.domain, a.layer, a.name
        LIMIT {limit}
    """
    return query_rows(sql)


@router.get("/{asset_id}")
def get_asset(asset_id: str):
    sql = f"""
        SELECT
            a.*,
            o.owner_name,
            o.team,
            o.steward,
            o.email,
            q.null_rate,
            q.schema_drift,
            q.freshness_status,
            q.duplicate_rate,
            q.last_checked,
            s.expected_refresh,
            s.actual_refresh,
            s.breach_flag,
            s.breach_duration_mins
        FROM `{project()}.risklens_catalog.assets` a
        LEFT JOIN `{project()}.risklens_catalog.ownership` o USING (asset_id)
        LEFT JOIN `{project()}.risklens_catalog.quality_scores` q USING (asset_id)
        LEFT JOIN `{project()}.risklens_catalog.sla_status` s USING (asset_id)
        WHERE a.asset_id = '{asset_id}'
        LIMIT 1
    """
    rows = query_rows(sql)
    if not rows:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' not found")
    return rows[0]


@router.get("/{asset_id}/schema")
def get_asset_schema(asset_id: str):
    sql = f"""
        SELECT column_name, data_type, nullable, description, sample_value
        FROM `{project()}.risklens_catalog.schema_registry`
        WHERE asset_id = '{asset_id}'
        ORDER BY column_name
    """
    return query_rows(sql)
