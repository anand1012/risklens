"""
Lineage router — data flow graph endpoints.

GET /lineage/nodes                   all lineage nodes
GET /lineage/graph/{asset_id}        subgraph centered on an asset (N hops upstream+downstream)
"""

from fastapi import APIRouter, Query

from api.db.bigquery import query_rows, project

router = APIRouter(prefix="/lineage", tags=["lineage"])


@router.get("/nodes")
def list_nodes(
    domain: str | None = Query(None),
    layer: str | None = Query(None),
    limit: int = Query(200, le=1000),
):
    filters = ["1=1"]
    if domain:
        filters.append(f"domain = '{domain}'")
    if layer:
        filters.append(f"layer = '{layer}'")
    where = " AND ".join(filters)

    sql = f"""
        SELECT node_id, name, type, domain, layer, metadata
        FROM `{project()}.risklens_lineage.nodes`
        WHERE {where}
        ORDER BY domain, layer, name
        LIMIT {limit}
    """
    return query_rows(sql)


@router.get("/graph/{asset_id}")
def get_lineage_graph(asset_id: str, hops: int = Query(2, ge=1, le=4)):
    """
    Return nodes + edges for the subgraph within `hops` of asset_id.
    The frontend uses this to render the lineage DAG.
    """
    # Seed node IDs — start from the requested asset
    # We iteratively expand via edges up to `hops` levels
    seed_sql = f"""
        SELECT node_id FROM `{project()}.risklens_lineage.nodes`
        WHERE node_id = '{asset_id}'
    """
    seeds = [r["node_id"] for r in query_rows(seed_sql)]
    if not seeds:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Node '{asset_id}' not found")

    visited: set[str] = set(seeds)
    frontier: set[str] = set(seeds)

    for _ in range(hops):
        if not frontier:
            break
        ids = ", ".join(f"'{n}'" for n in frontier)
        expansion_sql = f"""
            SELECT DISTINCT
                CASE WHEN from_node_id IN ({ids}) THEN to_node_id ELSE from_node_id END AS neighbor
            FROM `{project()}.risklens_lineage.edges`
            WHERE from_node_id IN ({ids}) OR to_node_id IN ({ids})
        """
        neighbors = {r["neighbor"] for r in query_rows(expansion_sql)} - visited
        visited.update(neighbors)
        frontier = neighbors

    all_ids = ", ".join(f"'{n}'" for n in visited)

    nodes_sql = f"""
        SELECT node_id, name, type, domain, layer, metadata
        FROM `{project()}.risklens_lineage.nodes`
        WHERE node_id IN ({all_ids})
    """
    edges_sql = f"""
        SELECT edge_id, from_node_id, to_node_id, relationship, pipeline_job
        FROM `{project()}.risklens_lineage.edges`
        WHERE from_node_id IN ({all_ids}) AND to_node_id IN ({all_ids})
    """

    return {
        "root_id": asset_id,
        "hops": hops,
        "nodes": query_rows(nodes_sql),
        "edges": query_rows(edges_sql),
    }
