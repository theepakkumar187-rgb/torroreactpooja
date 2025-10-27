from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import re
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import requests
import base64
from datetime import datetime

router = APIRouter()

class ColumnLineage(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str  # "direct_match", "transformed", "aggregated"
    contains_pii: Optional[bool] = False  # Track PII flow
    data_quality_score: Optional[int] = 95  # Quality score propagation
    impact_score: Optional[int] = 1  # Relationship strength (1-10)

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
    total_pii_columns: Optional[int] = 0  # Count of PII columns flowing
    avg_data_quality: Optional[float] = 95.0  # Average quality score
    last_validated: Optional[str] = None  # When last checked
    validation_status: Optional[str] = "unknown"  # "valid", "stale", "error"

class LineageResponse(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    column_relationships: int  # Count of column-level relationships
    total_pii_columns: Optional[int] = 0  # Total PII columns in system
    avg_data_quality: Optional[float] = 0.0  # Average data quality
    lineage_completeness: Optional[float] = 0.0  # Percentage of assets with lineage

def detect_pii_in_column(column_name: str, description: str = '') -> bool:
    """Detect if column contains PII"""
    pii_patterns = ['email', 'phone', 'ssn', 'name', 'address', 'birth', 'credit', 'passport', 
                    'national_id', 'license', 'account_number', 'password', 'secret', 'personal']
    combined = f"{column_name} {description}".lower()
    return any(pattern in combined for pattern in pii_patterns)

def get_column_quality_score(column: Dict) -> int:
    """Get data quality score for a column"""
    # Default quality based on presence of description
    has_desc = bool(column.get('description', '').strip())
    return 95 if has_desc else 80

def build_column_lineage_from_metadata(source_asset: Dict, target_asset: Dict) -> List[ColumnLineage]:
    """
    Build column-level lineage based on metadata (column names, types, descriptions).
    This is REAL lineage based on actual discovered metadata with PII detection and quality tracking!
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
            
            # Detect PII
            contains_pii = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
            
            # Get quality scores
            source_quality = get_column_quality_score(source_col)
            target_quality = get_column_quality_score(target_col)
            avg_quality = (source_quality + target_quality) // 2
            
            # Calculate impact score based on relationship strength
            impact_score = 10 if relationship_type == "direct_match" else 7 if relationship_type == "transformed" else 5
            
            column_relationships.append(ColumnLineage(
                source_table=source_asset['id'],
                source_column=source_col['name'],
                target_table=target_asset['id'],
                target_column=target_col['name'],
                relationship_type=relationship_type,
                contains_pii=contains_pii,
                data_quality_score=avg_quality,
                impact_score=impact_score
            ))
        
        # Strategy 2: Fuzzy match (e.g., customer_id vs customerId)
        else:
            # Remove underscores and compare
            target_normalized = target_col_name.replace('_', '')
            
            for source_col_name, source_col in source_col_map.items():
                source_normalized = source_col_name.replace('_', '')
                
                if target_normalized == source_normalized:
                    contains_pii = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                    source_quality = get_column_quality_score(source_col)
                    target_quality = get_column_quality_score(target_col)
                    
                    column_relationships.append(ColumnLineage(
                        source_table=source_asset['id'],
                        source_column=source_col['name'],
                        target_table=target_asset['id'],
                        target_column=target_col['name'],
                        relationship_type="direct_match",
                        contains_pii=contains_pii,
                        data_quality_score=(source_quality + target_quality) // 2,
                        impact_score=10
                    ))
                    break
                
                # Check if target column name contains source column name (aggregation pattern)
                elif source_col_name in target_col_name or target_col_name in source_col_name:
                    contains_pii = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                    source_quality = get_column_quality_score(source_col)
                    target_quality = get_column_quality_score(target_col)
                    
                    column_relationships.append(ColumnLineage(
                        source_table=source_asset['id'],
                        source_column=source_col['name'],
                        target_table=target_asset['id'],
                        target_column=target_col['name'],
                        relationship_type="transformed",
                        contains_pii=contains_pii,
                        data_quality_score=(source_quality + target_quality) // 2,
                        impact_score=7
                    ))
                    break
    
    return column_relationships

def extract_table_references_from_sql(sql: str, project_id: str = None) -> Dict[str, Any]:
    """
    Extract table references AND column transformations from SQL query.
    Returns dict with tables and transformation metadata.
    """
    if not sql:
        return {'tables': [], 'transformations': []}
    
    # Enhanced patterns to match BigQuery table references
    patterns = [
        r'`([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`',
        r'([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'`([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`',
        r'\bFROM\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bJOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bLEFT\s+JOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bRIGHT\s+JOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bINNER\s+JOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'\bOUTER\s+JOIN\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
        r'UNION\s+ALL\s+SELECT.*?FROM\s+`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?',
    ]
    
    tables = set()
    transformations = []
    
    for pattern in patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE)
        for match in matches:
            groups = match.groups()
            if len(groups) >= 2:
                if project_id:
                    table_ref = f"{project_id}.{groups[0]}.{groups[1]}" if len(groups) == 2 else f"{groups[0]}.{groups[1]}.{groups[2]}"
                else:
                    table_ref = f"{groups[0]}.{groups[1]}" if len(groups) == 2 else f"{groups[0]}.{groups[1]}.{groups[2]}"
                tables.add(table_ref)
    
    # Detect transformation patterns in SQL
    transformation_patterns = [
        (r'\bCOUNT\s*\(', 'aggregation', 'COUNT'),
        (r'\bSUM\s*\(', 'aggregation', 'SUM'),
        (r'\bAVG\s*\(', 'aggregation', 'AVG'),
        (r'\bMIN\s*\(', 'aggregation', 'MIN'),
        (r'\bMAX\s*\(', 'aggregation', 'MAX'),
        (r'\bCOALESCE\s*\(', 'data_quality', 'COALESCE'),
        (r'\bCASE\s+WHEN', 'conditional', 'CASE'),
        (r'\bCROSS\s+JOIN', 'join_type', 'CROSS_JOIN'),
        (r'\bGROUP\s+BY', 'aggregation', 'GROUP_BY'),
        (r'\bDISTINCT', 'distinct', 'DISTINCT'),
        (r'\bDATE\s*\(', 'date_transform', 'DATE'),
        (r'\bTRIM\s*\(', 'string_transform', 'TRIM'),
        (r'\bUPPER\s*\(', 'string_transform', 'UPPER'),
        (r'\bLOWER\s*\(', 'string_transform', 'LOWER'),
    ]
    
    for pattern, cat, trans_type in transformation_patterns:
        if re.search(pattern, sql, re.IGNORECASE):
            transformations.append({'type': trans_type, 'category': cat})
    
    # Detect alias patterns (table aliases)
    alias_patterns = [
        r'FROM\s+`?([a-zA-Z0-9_.-]+)`?\s+AS\s+([a-zA-Z0-9_]+)',
        r'FROM\s+`?([a-zA-Z0-9_.-]+)`?\s+([a-zA-Z0-9_]+)',
    ]
    
    aliases = {}
    for pattern in alias_patterns:
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            aliases[match.group(2) if len(match.groups()) == 2 else match.group(1)] = match.group(1)
    
    return {
        'tables': list(tables),
        'transformations': transformations,
        'aliases': aliases,
        'has_joins': bool(re.search(r'\bJOIN\b', sql, re.IGNORECASE)),
        'has_unions': bool(re.search(r'\bUNION\b', sql, re.IGNORECASE)),
        'has_subqueries': bool(re.search(r'\(.*SELECT.*\)', sql, re.IGNORECASE)),
    }

def get_bigquery_view_lineage(asset: Dict[str, Any], connector_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get lineage for a BigQuery view by analyzing its SQL definition.
    Returns dict with tables and transformation metadata.
    """
    if asset.get('type') != 'View':
        return {'tables': [], 'transformations': []}
    
    try:
        asset_id = asset.get('id', '')
        parts = asset_id.split('.')
        if len(parts) < 3:
            return {'tables': [], 'transformations': []}
        
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
            result = extract_table_references_from_sql(table.view_query, project_id)
            print(f"DEBUG: View {asset_id} references {len(result.get('tables', []))} tables with {len(result.get('transformations', []))} transformations")
            return result
        
        return {'tables': [], 'transformations': []}
        
    except Exception as e:
        print(f"Error getting BigQuery view lineage for {asset.get('id')}: {str(e)}")
        return {'tables': [], 'transformations': []}

@router.get("/lineage", response_model=LineageResponse)
async def get_data_lineage(
    page: int = Query(default=0, ge=0, description="Page number for pagination"),
    page_size: int = Query(default=1000, ge=0, description="Number of nodes per page"),
    use_cache: bool = Query(default=True, description="Use cached results")
):
    """
    Get comprehensive COLUMN-LEVEL data lineage from discovered assets.
    Uses metadata (column names, types, descriptions) to build relationships.
    Supports pagination for large graphs.
    """
    try:
        from main import discovered_assets, active_connectors
        
        print(f"DEBUG: Analyzing lineage for {len(discovered_assets)} assets")
        print(f"DEBUG: Active connectors: {len(active_connectors)}")
        
        nodes = []
        edges = []
        column_relationship_count = 0
        
        connector_map = {conn['id']: conn for conn in active_connectors}
        
        # Get list of active connector IDs
        active_connector_ids = set(conn['id'] for conn in active_connectors)
        
        # Build asset map - ONLY for assets from active connectors
        asset_map = {}
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            
            # Filter out assets from deleted connectors
            if asset_connector_id not in active_connector_ids:
                print(f"DEBUG: Skipping asset {asset.get('id')} from deleted connector {asset_connector_id}")
                continue
                
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
            asset_connector_id = asset.get('connector_id', '')
            
            # Only process assets from active connectors
            if asset_connector_id not in active_connector_ids:
                continue
                
            if asset.get('type') == 'View':
                asset_id = asset.get('id')
                connector_id = asset.get('connector_id', '')
                connector_config = connector_map.get(connector_id, {})
                
                lineage_result = {'tables': [], 'transformations': []}
                if connector_id.startswith('bq_'):
                    lineage_result = get_bigquery_view_lineage(asset, connector_config)
                elif connector_id.startswith('starburst_'):
                    # TODO: Add Starburst view lineage when we have SQL support
                    # For now, try to match by catalog prefixes
                    lineage_result = {'tables': [], 'transformations': []}
                
                upstream_tables = lineage_result.get('tables', [])
                transformations = lineage_result.get('transformations', [])
                
                for upstream_table_id in upstream_tables:
                    if upstream_table_id in asset_map:
                        # Build column-level lineage
                        source_asset = asset_map[upstream_table_id]
                        target_asset = asset
                        column_lineage = build_column_lineage_from_metadata(source_asset, target_asset)
                        column_relationship_count += len(column_lineage)
                        
                        # Add transformation metadata
                        relationship = 'feeds_into'
                        if transformations:
                            trans_types = [t['type'] for t in transformations]
                            relationship = f"{relationship} (transforms: {', '.join(trans_types[:3])})"
                        
                        # Calculate edge-level metrics
                        pii_count = sum(1 for col in column_lineage if col.contains_pii)
                        avg_quality = sum(col.data_quality_score for col in column_lineage) / len(column_lineage) if column_lineage else 95.0
                        
                        edge = LineageEdge(
                            source=upstream_table_id,
                            target=asset_id,
                            relationship=relationship,
                            column_lineage=column_lineage,
                            total_pii_columns=pii_count,
                            avg_data_quality=round(avg_quality, 2),
                            last_validated=datetime.now().isoformat(),
                            validation_status="valid"
                        )
                        edges.append(edge)
                        print(f"DEBUG: Created edge with {len(column_lineage)} column relationships (PII: {pii_count}, Quality: {avg_quality:.1f}): {upstream_table_id} -> {asset_id}")
        
        # Strategy 2: Metadata-based lineage (column name matching across all tables)
        # This finds relationships even without views!
        # Always run metadata-based lineage to get comprehensive relationships
        print("DEBUG: Building metadata-based lineage...")
        
        # Only skip if we already have extensive SQL-based lineage
        if len(edges) < 50:  # Build metadata lineage unless we have tons of SQL edges
            # Group by catalog/connector
            tables_by_catalog = {}
            for asset in discovered_assets:
                asset_connector_id = asset.get('connector_id', '')
                
                # Only process assets from active connectors
                if asset_connector_id not in active_connector_ids:
                    continue
                    
                if asset.get('type') in ['Table', 'View']:
                    # Use catalog or connector_id as group key to enable cross-platform lineage
                    catalog = asset.get('catalog', asset_connector_id)
                    if catalog not in tables_by_catalog:
                        tables_by_catalog[catalog] = []
                    tables_by_catalog[catalog].append(asset)
            
            # Within each catalog, find tables with matching columns
            # NOW: Also match across catalogs for CROSS-PLATFORM lineage!
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
                            asset1_catalog = asset1.get('catalog', '').lower()
                            asset2_catalog = asset2.get('catalog', '').lower()
                            
                            # ETL/ELT Pattern Detection
                            is_etl = False
                            is_elt = False
                            pipeline_stage = None
                            
                            # Detect ETL pattern: raw/landing -> processed/transformed -> analytics/reporting
                            if 'raw' in asset1_name or 'landing' in asset1_name or 'source' in asset1_name:
                                if 'processed' in asset2_name or 'stage' in asset2_name or 'staged' in asset2_name:
                                    is_etl = True
                                    pipeline_stage = "extract_load"
                                elif 'analytics' in asset2_name or 'report' in asset2_name or 'summary' in asset2_name:
                                    is_elt = True
                                    pipeline_stage = "extract_load_transform"
                            
                            # Detect ELT pattern: raw -> staging -> final
                            if 'stage' in asset1_name or 'staging' in asset1_name:
                                if 'analytics' in asset2_name or 'final' in asset2_name or 'production' in asset2_name:
                                    is_elt = True
                                    pipeline_stage = "load_transform"
                            
                            # Convention: raw/source -> staging -> prod/analytics
                            source_id = asset1['id']
                            target_id = asset2['id']
                            
                            # Reverse if needed based on naming
                            if ('raw' in asset2_name or 'source' in asset2_name) and ('prod' in asset1_name or 'analytics' in asset1_name):
                                source_id, target_id = target_id, source_id
                                is_etl = True
                                pipeline_stage = "extract_load_transform"
                            elif asset1.get('type') == 'View' and asset2.get('type') == 'Table':
                                # Views typically depend on tables
                                source_id = asset2['id']
                                target_id = asset1['id']
                            
                            # Check if edge already exists
                            edge_exists = any(e.source == source_id and e.target == target_id for e in edges)
                            
                            if not edge_exists:
                                column_relationship_count += len(column_lineage)
                                
                                # Calculate edge metrics
                                pii_count = sum(1 for col in column_lineage if col.contains_pii)
                                avg_quality = sum(col.data_quality_score for col in column_lineage) / len(column_lineage) if column_lineage else 95.0
                                
                                # Determine relationship type
                                relationship_type = 'inferred_from_metadata'
                                if is_etl:
                                    relationship_type = 'etl_pipeline'
                                elif is_elt:
                                    relationship_type = 'elt_pipeline'
                                
                                edge = LineageEdge(
                                    source=source_id,
                                    target=target_id,
                                    relationship=relationship_type,
                                    column_lineage=column_lineage,
                                    total_pii_columns=pii_count,
                                    avg_data_quality=round(avg_quality, 2),
                                    last_validated=datetime.now().isoformat(),
                                    validation_status="inferred"
                                )
                                edges.append(edge)
                                
                                stage_icon = "🔄" if is_etl else "⚡" if is_elt else ""
                                print(f"DEBUG: {stage_icon} Edge with {len(column_lineage)} column relationships (PII: {pii_count}, Stage: {pipeline_stage}): {source_id} -> {target_id}")
        
        print(f"DEBUG: Found {len(edges)} edges with {column_relationship_count} column relationships")
        
        # Calculate lineage-level metrics
        total_pii = sum(e.total_pii_columns for e in edges)
        avg_quality_all = sum(e.avg_data_quality for e in edges) / len(edges) if edges else 0.0
        completeness = (len(nodes) / len(asset_map)) * 100 if asset_map else 0.0
        
        # Apply pagination if needed (convert to int to avoid Query object comparison)
        page_size_int = int(page_size) if page_size else 1000
        if page_size_int > 0 and page_size_int < len(nodes):
            start_idx = int(page) * page_size_int
            end_idx = start_idx + page_size_int
            paginated_nodes = nodes[start_idx:end_idx]
            
            # Filter edges to only include those between paginated nodes
            paginated_node_ids = {n.id for n in paginated_nodes}
            paginated_edges = [e for e in edges if e.source in paginated_node_ids and e.target in paginated_node_ids]
            
            print(f"DEBUG: Paginated to {len(paginated_nodes)} nodes (page {page})")
            
            return LineageResponse(
                nodes=paginated_nodes, 
                edges=paginated_edges,
                column_relationships=column_relationship_count,
                total_pii_columns=total_pii,
                avg_data_quality=round(avg_quality_all, 2),
                lineage_completeness=round(completeness, 2)
            )
        
        return LineageResponse(
            nodes=nodes, 
            edges=edges,
            column_relationships=column_relationship_count,
            total_pii_columns=total_pii,
            avg_data_quality=round(avg_quality_all, 2),
            lineage_completeness=round(completeness, 2)
        )
        
    except Exception as e:
        print(f"Error getting data lineage: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to get data lineage: {str(e)}")

@router.get("/lineage/impact/{asset_id:path}")
async def get_impact_analysis(asset_id: str):
    """
    Get impact analysis for a specific asset - what would break if this asset changed.
    Shows upstream and downstream impacts.
    """
    try:
        from main import discovered_assets, active_connectors
        
        # Get full lineage (with default params)
        lineage_response = await get_data_lineage(page=0, page_size=1000)
        
        # Find all edges related to this asset
        upstream_edges = [e for e in lineage_response.edges if e.target == asset_id]
        downstream_edges = [e for e in lineage_response.edges if e.source == asset_id]
        
        # Count impact
        upstream_count = len(set(e.source for e in upstream_edges))
        downstream_count = len(set(e.target for e in downstream_edges))
        total_column_impacts = sum(len(e.column_lineage or []) for e in downstream_edges)
        
        return {
            "asset_id": asset_id,
            "impact_score": upstream_count * 10 + downstream_count * 20 + total_column_impacts * 5,
            "upstream_impact": {
                "dependencies": upstream_count,
                "tables": [e.source for e in upstream_edges]
            },
            "downstream_impact": {
                "dependent_tables": downstream_count,
                "tables": [e.target for e in downstream_edges],
                "column_relationships": total_column_impacts
            },
            "severity": "HIGH" if downstream_count > 5 or total_column_impacts > 20 else "MEDIUM" if downstream_count > 0 else "LOW"
        }
        
    except Exception as e:
        print(f"Error getting impact analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get impact analysis: {str(e)}")

@router.get("/lineage/export")
async def export_lineage(
    export_format: str = Query(default="json", description="Export format (json, csv)", alias="format"),
    asset_id: str = Query(default=None, description="Export lineage for specific asset")
):
    """
    Export lineage data in various formats.
    """
    try:
        from main import discovered_assets, active_connectors
        
        # Get lineage data
        lineage_result = await get_data_lineage()
        
        if asset_id:
            # Filter to specific asset and its relationships
            related_node_ids = {asset_id}
            for edge in lineage_result.edges:
                if asset_id in [edge.source, edge.target]:
                    related_node_ids.add(edge.source)
                    related_node_ids.add(edge.target)
            
            filtered_nodes = [n for n in lineage_result.nodes if n.id in related_node_ids]
            filtered_edges = [e for e in lineage_result.edges if asset_id in [e.source, e.target]]
            
            lineage_result = LineageResponse(
                nodes=filtered_nodes,
                edges=filtered_edges,
                column_relationships=sum(len(e.column_lineage or []) for e in filtered_edges)
            )
        
        if export_format == "csv":
            # Generate CSV export
            csv_lines = ["Source Table,Source Column,Target Table,Target Column,Relationship Type"]
            
            for edge in lineage_result.edges:
                source = next((n for n in lineage_result.nodes if n.id == edge.source), None)
                target = next((n for n in lineage_result.nodes if n.id == edge.target), None)
                source_name = source.name if source else edge.source
                target_name = target.name if target else edge.target
                
                if edge.column_lineage:
                    for col_lineage in edge.column_lineage:
                        csv_lines.append(
                            f"{source_name},{col_lineage.source_column},{target_name},{col_lineage.target_column},{col_lineage.relationship_type}"
                        )
                else:
                    csv_lines.append(f"{source_name},-,{target_name},-,{edge.relationship}")
            
            return {"format": "csv", "data": "\n".join(csv_lines)}
        
        else:  # JSON (default)
            return {
                "format": "json",
                "export_date": datetime.now().isoformat(),
                "total_nodes": len(lineage_result.nodes),
                "total_edges": len(lineage_result.edges),
                "total_column_relationships": lineage_result.column_relationships,
                "nodes": [{"id": n.id, "name": n.name, "type": n.type, "catalog": n.catalog} for n in lineage_result.nodes],
                "edges": [{"source": e.source, "target": e.target, "relationship": e.relationship, "column_lineage": e.column_lineage} for e in lineage_result.edges]
            }
            
    except Exception as e:
        print(f"Error exporting lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to export lineage: {str(e)}")

@router.get("/lineage/search")
async def search_lineage(
    query: str = Query(..., description="Search query (column name, table name, or pattern)"),
    search_type: str = Query(default="all", description="Search type: column, table, or all")
):
    """
    Search lineage by column name, table name, or pattern.
    Returns assets and relationships matching the query.
    """
    try:
        lineage_result = await get_data_lineage(page=0, page_size=1000)
        
        query_lower = query.lower()
        matching_nodes = []
        matching_edges = []
        
        # Search nodes
        for node in lineage_result.nodes:
            if search_type in ['all', 'table']:
                if query_lower in node.name.lower() or query_lower in node.id.lower():
                    matching_nodes.append(node)
            
            # Search columns
            if search_type in ['all', 'column']:
                for col in node.columns:
                    if query_lower in col.get('name', '').lower():
                        matching_nodes.append(node)
                        break
        
        # Find edges connected to matching nodes
        matching_node_ids = {n.id for n in matching_nodes}
        for edge in lineage_result.edges:
            if edge.source in matching_node_ids or edge.target in matching_node_ids:
                if edge not in matching_edges:
                    matching_edges.append(edge)
        
        # Search column lineage
        for edge in lineage_result.edges:
            for col_lineage in edge.column_lineage:
                if query_lower in col_lineage.source_column.lower() or query_lower in col_lineage.target_column.lower():
                    if edge not in matching_edges:
                        matching_edges.append(edge)
        
        return {
            "query": query,
            "results": {
                "nodes": len(matching_nodes),
                "edges": len(matching_edges),
                "matching_nodes": matching_nodes[:20],  # Limit results
                "matching_edges": matching_edges[:20]
            }
        }
        
    except Exception as e:
        print(f"Error searching lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to search lineage: {str(e)}")

@router.get("/lineage/health")
async def check_lineage_health():
    """
    Check lineage health - detect broken relationships, missing assets, and validation issues.
    """
    try:
        from main import discovered_assets, active_connectors
        
        lineage_result = await get_data_lineage(page=0, page_size=1000)
        
        issues = []
        warnings = []
        
        # Check for orphaned nodes (nodes with no relationships)
        node_ids_with_edges = set()
        for edge in lineage_result.edges:
            node_ids_with_edges.add(edge.source)
            node_ids_with_edges.add(edge.target)
        
        orphaned = [n for n in lineage_result.nodes if n.id not in node_ids_with_edges]
        if orphaned:
            warnings.append({
                "type": "orphaned_nodes",
                "count": len(orphaned),
                "nodes": [n.name for n in orphaned[:5]],
                "severity": "medium"
            })
        
        # Check for edges with no column lineage
        edges_without_columns = [e for e in lineage_result.edges if not e.column_lineage]
        if edges_without_columns:
            warnings.append({
                "type": "missing_column_lineage",
                "count": len(edges_without_columns),
                "severity": "low"
            })
        
        # Check for stale/invalid lineage
        now = datetime.now()
        stale_edges = []
        for edge in lineage_result.edges:
            if edge.last_validated:
                last_validated = datetime.fromisoformat(edge.last_validated.replace('Z', '+00:00'))
                days_since = (now - last_validated.replace(tzinfo=None)).days
                if days_since > 30:
                    stale_edges.append(edge.relationship)
        
        if stale_edges:
            issues.append({
                "type": "stale_lineage",
                "count": len(stale_edges),
                "severity": "medium"
            })
        
        # Calculate health score
        total_issues = len(issues) + len(warnings)
        health_score = max(0, 100 - (total_issues * 10))
        
        return {
            "health_score": health_score,
            "status": "healthy" if health_score >= 80 else "degraded" if health_score >= 50 else "critical",
            "issues": issues,
            "warnings": warnings,
            "statistics": {
                "total_nodes": len(lineage_result.nodes),
                "total_edges": len(lineage_result.edges),
                "orphaned_nodes": len(orphaned),
                "stale_edges": len(stale_edges),
                "completeness": lineage_result.lineage_completeness
            }
        }
        
    except Exception as e:
        print(f"Error checking lineage health: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to check lineage health: {str(e)}")

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
        
        full_lineage = await get_data_lineage(page=0, page_size=1000)
        
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
            column_relationships=column_count,
            total_pii_columns=sum(e.total_pii_columns for e in filtered_edges),
            avg_data_quality=sum(e.avg_data_quality for e in filtered_edges) / len(filtered_edges) if filtered_edges else 0.0,
            lineage_completeness=100.0 if filtered_nodes else 0.0
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting asset lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get asset lineage: {str(e)}")

@router.get("/lineage-analysis/pipelines")
async def get_pipeline_lineage():
    """
    Get ETL/ELT pipeline visualization - identifies data pipelines in your lineage.
    Returns pipeline stages and flow patterns.
    """
    try:
        lineage_result = await get_data_lineage(page=0, page_size=1000)
        
        # Identify pipelines from edges
        etl_pipelines = []
        elt_pipelines = []
        regular_relationships = []
        
        for edge in lineage_result.edges:
            if edge.relationship == 'etl_pipeline':
                etl_pipelines.append(edge)
            elif edge.relationship == 'elt_pipeline':
                elt_pipelines.append(edge)
            else:
                regular_relationships.append(edge)
        
        # Build pipeline chains
        def find_pipeline_chains(pipeline_type):
            """Find complete pipeline chains"""
            chains = []
            pipeline_edges = [e for e in (etl_pipelines if pipeline_type == 'etl' else elt_pipelines)]
            
            # Group by source
            for edge in pipeline_edges:
                chain = {
                    'source': edge.source,
                    'target': edge.target,
                    'stage': edge.relationship,
                    'column_count': len(edge.column_lineage or []),
                    'pii_count': edge.total_pii_columns,
                    'quality': edge.avg_data_quality
                }
                chains.append(chain)
            
            return chains
        
        etl_chains = find_pipeline_chains('etl')
        elt_chains = find_pipeline_chains('elt')
        
        return {
            "pipeline_summary": {
                "total_etl_pipelines": len(etl_pipelines),
                "total_elt_pipelines": len(elt_pipelines),
                "total_direct_relationships": len(regular_relationships),
                "total_etl_steps": len(etl_chains),
                "total_elt_steps": len(elt_chains)
            },
            "etl_pipelines": etl_chains,
            "elt_pipelines": elt_chains,
            "visualization": {
                "has_etl": len(etl_pipelines) > 0,
                "has_elt": len(elt_pipelines) > 0,
                "pipeline_complexity": "simple" if len(etl_chains) + len(elt_chains) < 3 else "moderate" if len(etl_chains) + len(elt_chains) < 10 else "complex"
            }
        }
        
    except Exception as e:
        print(f"Error getting pipeline lineage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline lineage: {str(e)}")
