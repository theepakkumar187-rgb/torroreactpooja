from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import re
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import requests
import base64

router = APIRouter()

class ColumnLineage(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str  # "direct_match", "transformed", "aggregated"

class LineageNode(BaseModel):
    id: str
    name: str
    type: str
    catalog: str
    connector_id: str
    source_system: str
    columns: List[Dict[str, Any]]  # Include column details

class LineageEdge(BaseModel):
    source: str
    target: str
    relationship: str
    column_lineage: Optional[List[ColumnLineage]] = []

class LineageResponse(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    column_relationships: int  # Count of column-level relationships

def build_column_lineage_from_metadata(source_asset: Dict, target_asset: Dict) -> List[ColumnLineage]:
    """
    Build column-level lineage based on metadata (column names, types, descriptions).
    This is REAL lineage based on actual discovered metadata!
    """
    column_relationships = []
    
    source_columns = source_asset.get('columns', [])
    target_columns = target_asset.get('columns', [])
    
    if not source_columns or not target_columns:
        return []
    
    # Strategy 1: Exact column name match
    source_col_map = {col['name'].lower(): col for col in source_columns}
    
    for target_col in target_columns:
        target_col_name = target_col['name'].lower()
        
        # Direct match by name
        if target_col_name in source_col_map:
            source_col = source_col_map[target_col_name]
            
            # Check if types are compatible
            relationship_type = "direct_match"
            if source_col['type'] != target_col['type']:
                relationship_type = "transformed"
            
            column_relationships.append(ColumnLineage(
                source_table=source_asset['id'],
                source_column=source_col['name'],
                target_table=target_asset['id'],
                target_column=target_col['name'],
                relationship_type=relationship_type
            ))
        
        # Strategy 2: Fuzzy match (e.g., customer_id vs customerId)
        else:
            # Remove underscores and compare
            target_normalized = target_col_name.replace('_', '')
            
            for source_col_name, source_col in source_col_map.items():
                source_normalized = source_col_name.replace('_', '')
                
                if target_normalized == source_normalized:
                    column_relationships.append(ColumnLineage(
                        source_table=source_asset['id'],
                        source_column=source_col['name'],
                        target_table=target_asset['id'],
                        target_column=target_col['name'],
                        relationship_type="direct_match"
                    ))
                    break
                
                # Check if target column name contains source column name (aggregation pattern)
                elif source_col_name in target_col_name or target_col_name in source_col_name:
                    column_relationships.append(ColumnLineage(
                        source_table=source_asset['id'],
                        source_column=source_col['name'],
                        target_table=target_asset['id'],
                        target_column=target_col['name'],
                        relationship_type="transformed"
                    ))
                    break
    
    return column_relationships

def extract_table_references_from_sql(sql: str, project_id: str = None) -> List[str]:
    """
    Extract table references from SQL query.
    Returns fully qualified table names.
    """
    if not sql:
        return []
    
    # Pattern to match BigQuery table references
    patterns = [
        r'`([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`',
        r'([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'`([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`',
        r'\bFROM\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\b',
        r'\bJOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\b',
    ]
    
    tables = set()
    
    for pattern in patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE)
        for match in matches:
            groups = match.groups()
            if len(groups) == 3:
                tables.add(f"{groups[0]}.{groups[1]}.{groups[2]}")
            elif len(groups) == 2:
                if project_id:
                    tables.add(f"{project_id}.{groups[0]}.{groups[1]}")
                else:
                    tables.add(f"{groups[0]}.{groups[1]}")
    
    return list(tables)

def get_bigquery_view_lineage(asset: Dict[str, Any], connector_config: Dict[str, Any]) -> List[str]:
    """
    Get lineage for a BigQuery view by analyzing its SQL definition.
    Returns list of upstream table IDs.
    """
    if asset.get('type') != 'View':
        return []
    
    try:
        asset_id = asset.get('id', '')
        parts = asset_id.split('.')
        if len(parts) < 3:
            return []
        
        project_id = parts[0]
        dataset_id = parts[1]
        table_id = parts[2]
        
        if 'service_account_json' in connector_config:
            service_account_info = json.loads(connector_config['service_account_json'])
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
            )
            client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            client = bigquery.Client(project=project_id)
        
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        
        if table.view_query:
            upstream_tables = extract_table_references_from_sql(table.view_query, project_id)
            print(f"DEBUG: View {asset_id} references tables: {upstream_tables}")
            return upstream_tables
        
        return []
        
    except Exception as e:
        print(f"Error getting BigQuery view lineage for {asset.get('id')}: {str(e)}")
        return []

@router.get("/lineage", response_model=LineageResponse)
async def get_data_lineage():
    """
    Get comprehensive COLUMN-LEVEL data lineage from discovered assets.
    Uses metadata (column names, types, descriptions) to build relationships.
    """
    try:
        from main import discovered_assets, active_connectors
        
        print(f"DEBUG: Analyzing lineage for {len(discovered_assets)} assets")
        
        nodes = []
        edges = []
        column_relationship_count = 0
        
        connector_map = {conn['id']: conn for conn in active_connectors}
        
        # Build asset map
        asset_map = {}
        for asset in discovered_assets:
            if asset.get('type') in ['Table', 'View']:
                asset_id = asset.get('id')
                connector_id = asset.get('connector_id', '')
                
                source_system = 'Unknown'
                if connector_id.startswith('bq_'):
                    source_system = 'BigQuery'
                elif connector_id.startswith('starburst_'):
                    source_system = 'Starburst Galaxy'
                
                node = LineageNode(
                    id=asset_id,
                    name=asset.get('name', 'Unknown'),
                    type=asset.get('type', 'Table'),
                    catalog=asset.get('catalog', 'Unknown'),
                    connector_id=connector_id,
                    source_system=source_system,
                    columns=asset.get('columns', [])
                )
                nodes.append(node)
                asset_map[asset_id] = asset
        
        print(f"DEBUG: Found {len(nodes)} table/view nodes")
        
        # Strategy 1: Analyze Views for SQL-based lineage
        for asset in discovered_assets:
            if asset.get('type') == 'View':
                asset_id = asset.get('id')
                connector_id = asset.get('connector_id', '')
                connector_config = connector_map.get(connector_id, {})
                
                upstream_tables = []
                if connector_id.startswith('bq_'):
                    upstream_tables = get_bigquery_view_lineage(asset, connector_config)
                
                for upstream_table_id in upstream_tables:
                    if upstream_table_id in asset_map:
                        # Build column-level lineage
                        source_asset = asset_map[upstream_table_id]
                        target_asset = asset
                        column_lineage = build_column_lineage_from_metadata(source_asset, target_asset)
                        column_relationship_count += len(column_lineage)
                        
                        edge = LineageEdge(
                            source=upstream_table_id,
                            target=asset_id,
                            relationship='feeds_into',
                            column_lineage=column_lineage
                        )
                        edges.append(edge)
                        print(f"DEBUG: Created edge with {len(column_lineage)} column relationships: {upstream_table_id} -> {asset_id}")
        
        # Strategy 2: Metadata-based lineage (column name matching across all tables)
        # This finds relationships even without views!
        if len(edges) < 5:  # If we don't have many edges, infer from metadata
            print("DEBUG: Building metadata-based lineage...")
            
            # Group by catalog to avoid cross-catalog false positives
            tables_by_catalog = {}
            for asset in discovered_assets:
                if asset.get('type') in ['Table', 'View']:
                    catalog = asset.get('catalog', '')
                    if catalog not in tables_by_catalog:
                        tables_by_catalog[catalog] = []
                    tables_by_catalog[catalog].append(asset)
            
            # Within each catalog, find tables with matching columns
            for catalog, assets_in_catalog in tables_by_catalog.items():
                for i, asset1 in enumerate(assets_in_catalog):
                    for asset2 in assets_in_catalog[i+1:]:
                        # Check if they share common columns
                        column_lineage = build_column_lineage_from_metadata(asset1, asset2)
                        
                        # If they share 2+ columns, likely there's a relationship
                        if len(column_lineage) >= 2:
                            # Determine direction based on naming patterns
                            asset1_name = asset1.get('name', '').lower()
                            asset2_name = asset2.get('name', '').lower()
                            
                            # Convention: raw/source -> staging -> prod/analytics
                            source_id = asset1['id']
                            target_id = asset2['id']
                            
                            # Reverse if needed based on naming
                            if ('raw' in asset2_name or 'source' in asset2_name) and ('prod' in asset1_name or 'analytics' in asset1_name):
                                source_id, target_id = target_id, source_id
                            elif asset1.get('type') == 'View' and asset2.get('type') == 'Table':
                                # Views typically depend on tables
                                source_id = asset2['id']
                                target_id = asset1['id']
                            
                            # Check if edge already exists
                            edge_exists = any(e.source == source_id and e.target == target_id for e in edges)
                            
                            if not edge_exists:
                                column_relationship_count += len(column_lineage)
                                edge = LineageEdge(
                                    source=source_id,
                                    target=target_id,
                                    relationship='inferred_from_metadata',
                                    column_lineage=column_lineage
                                )
                                edges.append(edge)
                                print(f"DEBUG: Inferred edge with {len(column_lineage)} column relationships: {source_id} -> {target_id}")
        
        print(f"DEBUG: Found {len(edges)} edges with {column_relationship_count} column relationships")
        
        return LineageResponse(
            nodes=nodes, 
            edges=edges,
            column_relationships=column_relationship_count
        )
        
    except Exception as e:
        print(f"Error getting data lineage: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to get data lineage: {str(e)}")

@router.get("/lineage/{asset_id:path}")
async def get_asset_lineage(asset_id: str):
    """
    Get column-level lineage for a specific asset (upstream and downstream).
    """
    try:
        from main import discovered_assets
        
        asset = next((a for a in discovered_assets if a['id'] == asset_id), None)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        full_lineage = await get_data_lineage()
        
        related_node_ids = {asset_id}
        
        upstream_edges = [e for e in full_lineage.edges if e.target == asset_id]
        for edge in upstream_edges:
            related_node_ids.add(edge.source)
        
        downstream_edges = [e for e in full_lineage.edges if e.source == asset_id]
        for edge in downstream_edges:
            related_node_ids.add(edge.target)
        
        filtered_nodes = [n for n in full_lineage.nodes if n.id in related_node_ids]
        filtered_edges = [e for e in full_lineage.edges if e.source in related_node_ids and e.target in related_node_ids]
        
        column_count = sum(len(e.column_lineage or []) for e in filtered_edges)
        
        return LineageResponse(
            nodes=filtered_nodes, 
            edges=filtered_edges,
            column_relationships=column_count
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting asset lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get asset lineage: {str(e)}")
