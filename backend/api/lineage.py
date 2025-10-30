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
import os
from typing import Tuple
import hmac
import hashlib
from .graph_store import GraphStore

# Optional SQL AST parser
try:
    import sqlglot
    from sqlglot import parse_one
    HAS_SQLGLOT = True
except Exception:
    HAS_SQLGLOT = False

router = APIRouter()

CURATION_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'curation_edits.json')
GRAPH_STORE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'lineage_store.json')
QUERYLOG_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'query_logs.json')

def _load_curation() -> Dict[str, Any]:
    try:
        if os.path.exists(CURATION_FILE):
            with open(CURATION_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"WARN: Failed to load curation file: {e}")
    return {"proposals": []}

def _save_curation(payload: Dict[str, Any]):
    try:
        with open(CURATION_FILE, 'w') as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"WARN: Failed to save curation file: {e}")

def _load_json_file(path: str, default: Any) -> Any:
    try:
        if os.path.exists(path):
            with open(path, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"WARN: Failed to load {path}: {e}")
    return default

def _save_json_file(path: str, payload: Any):
    try:
        with open(path, 'w') as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"WARN: Failed to save {path}: {e}")

def save_lineage_snapshot(snapshot: Dict[str, Any]):
    store = _load_json_file(GRAPH_STORE_FILE, {"snapshots": []})
    payload = {
        "created_at": datetime.now().isoformat(),
        "data": snapshot
    }
    # Optional HMAC signature for provenance
    signing_key = os.getenv('TORRO_LINEAGE_SIGNING_KEY')
    if signing_key:
        signature = hmac.new(signing_key.encode('utf-8'), json.dumps(snapshot, sort_keys=True).encode('utf-8'), hashlib.sha256).hexdigest()
        payload["signature"] = signature
        payload["signature_alg"] = "HMAC-SHA256"
    store.setdefault("snapshots", []).append(payload)
    _save_json_file(GRAPH_STORE_FILE, store)

def sign_edge(edge: 'LineageEdge') -> Optional[str]:
    try:
        signing_key = os.getenv('TORRO_LINEAGE_SIGNING_KEY')
        if not signing_key:
            return None
        payload = json.dumps({
            'source': edge.source,
            'target': edge.target,
            'relationship': edge.relationship,
            'created_at': edge.created_at,
        }, sort_keys=True).encode('utf-8')
        return hmac.new(signing_key.encode('utf-8'), payload, hashlib.sha256).hexdigest()
    except Exception:
        return None

def _reconcile_openlineage_to_edges(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    events = store.get('openlineage', [])
    for entry in events:
        evt = entry.get('event', {})
        inputs = (evt.get('inputs') or [])
        outputs = (evt.get('outputs') or [])
        # Create edges from inputs -> outputs
        for inp in inputs:
            s = inp.get('name') or inp.get('namespace')
            if not s:
                continue
            for out in outputs:
                t = out.get('name') or out.get('namespace')
                if not t:
                    continue
                now_iso = datetime.now().isoformat()
                edge = LineageEdge(
                    source=s,
                    target=t,
                    relationship='openlineage_job',
                    column_lineage=[],
                    total_pii_columns=0,
                    avg_data_quality=95.0,
                    last_validated=now_iso,
                    validation_status='valid',
                    confidence_score=0.8,
                    evidence=['openlineage'],
                    sources=['openlineage'],
                    created_at=now_iso,
                    updated_at=now_iso,
                )
                edge.edge_signature = sign_edge(edge)
                edges.append(edge.model_dump())
    return edges

def _reconcile_dbt_to_edges(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    batches = store.get('dbt', [])
    for batch in batches:
        for node in batch.get('nodes', []):
            deps = node.get('depends_on') or []
            target = node.get('name')
            if not target:
                continue
            for dep in deps:
                if not dep:
                    continue
                now_iso = datetime.now().isoformat()
                edge = LineageEdge(
                    source=dep,
                    target=target,
                    relationship='dbt_dependency',
                    column_lineage=[],
                    total_pii_columns=0,
                    avg_data_quality=95.0,
                    last_validated=now_iso,
                    validation_status='valid',
                    confidence_score=0.75,
                    evidence=['dbt'],
                    sources=['dbt'],
                    created_at=now_iso,
                    updated_at=now_iso,
                )
                edge.edge_signature = sign_edge(edge)
                edges.append(edge.model_dump())
    return edges

def _reconcile_airflow_to_edges(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    batches = store.get('airflow', [])
    for batch in batches:
        for task in batch.get('tasks', []):
            target = task.get('task_id')
            for upstream in (task.get('upstream') or []):
                if not upstream or not target:
                    continue
                now_iso = datetime.now().isoformat()
                edge = LineageEdge(
                    source=upstream,
                    target=target,
                    relationship='airflow_upstream',
                    column_lineage=[],
                    total_pii_columns=0,
                    avg_data_quality=95.0,
                    last_validated=now_iso,
                    validation_status='valid',
                    confidence_score=0.6,
                    evidence=['airflow'],
                    sources=['airflow'],
                    created_at=now_iso,
                    updated_at=now_iso,
                )
                edge.edge_signature = sign_edge(edge)
                edges.append(edge.model_dump())
    return edges

def _reconcile_metadata_to_edges(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Placeholder for OpenMetadata/Atlas relationships if present in payload
    edges: List[Dict[str, Any]] = []
    batches = store.get('metadata', [])
    for batch in batches:
        payload = batch.get('payload') or {}
        rels = payload.get('relationships') or []
        for r in rels:
            s = r.get('source')
            t = r.get('target')
            rel = r.get('type', 'metadata_relationship')
            if not s or not t:
                continue
            now_iso = datetime.now().isoformat()
            edge = LineageEdge(
                source=s,
                target=t,
                relationship=rel,
                column_lineage=[],
                total_pii_columns=0,
                avg_data_quality=95.0,
                last_validated=now_iso,
                validation_status='valid',
                confidence_score=0.7,
                evidence=['metadata'],
                sources=['metadata'],
                created_at=now_iso,
                updated_at=now_iso,
            )
            edge.edge_signature = sign_edge(edge)
            edges.append(edge.model_dump())
    return edges

def _logs_imply_relationship(source_id: str, target_id: str) -> bool:
    logs = _load_json_file(QUERYLOG_FILE, {"entries": []}).get("entries", [])
    s_short = source_id.split('.')[-1].lower()
    t_short = target_id.split('.')[-1].lower()
    for entry in logs:
        sql = (entry.get("sql") or "").lower()
        if s_short in sql and t_short in sql:
            return True
    return False

def _require_role(role: str, headers: Dict[str, Any]):
    required = role.lower()
    got = (headers.get('x-role') or headers.get('X-Role') or '').lower()
    if required and got != required:
        raise HTTPException(status_code=403, detail="Forbidden: insufficient role")

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
    confidence_score: Optional[float] = 0.0  # Confidence (0-1) in this edge
    evidence: Optional[List[str]] = []  # Signals that support this edge
    sources: Optional[List[str]] = []  # Systems/logs that contributed
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    edge_signature: Optional[str] = None

class LineageResponse(BaseModel):
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    column_relationships: int  # Count of column-level relationships
    total_pii_columns: Optional[int] = 0  # Total PII columns in system
    avg_data_quality: Optional[float] = 0.0  # Average data quality
    lineage_completeness: Optional[float] = 0.0  # Percentage of assets with lineage
    avg_confidence: Optional[float] = 0.0  # Average confidence across edges

def compute_edge_confidence(relationship: str, column_lineage: List[ColumnLineage], transformations: Optional[List[Dict[str, Any]]] = None) -> tuple:
    """Return (confidence_score, evidence) based on signals present."""
    base = 0.4
    evidence: List[str] = []
    if relationship in ["foreign_key", "etl_pipeline", "elt_pipeline", "id_relationship"]:
        base = 0.6
        evidence.append(f"relationship_type:{relationship}")
    if column_lineage:
        mappings = len(column_lineage)
        avg_impact = sum((cl.impact_score or 1) for cl in column_lineage) / max(1, mappings)
        base += min(0.3, mappings * 0.03)
        base += min(0.2, (avg_impact / 10.0) * 0.2)
        evidence.append(f"column_mappings:{mappings}")
    if transformations:
        trans_types = {t.get('type') for t in transformations}
        if any(t in trans_types for t in ["FOREIGN_KEY", "ETL_PIPELINE", "ELT_PIPELINE", "ID_RELATIONSHIP"]):
            base += 0.15
            evidence.append("transformations:strong")
        if any(t in trans_types for t in ["COUNT", "SUM", "JOIN", "DISTINCT"]):
            evidence.append("transformations:sql_ops")
    return (max(0.0, min(1.0, base)), evidence)

def detect_pii_in_column(column_name: str, description: str = '') -> tuple:
    """Detect if column contains PII and return sensitivity level"""
    # Enhanced PII patterns with sensitivity levels
    pii_patterns = {
        'HIGH': ['ssn', 'social_security', 'passport', 'national_id', 'license_number', 
                'credit_card', 'account_number', 'password', 'secret', 'private_key'],
        'MEDIUM': ['email', 'phone', 'mobile', 'address', 'zip', 'postal', 'birth_date', 
                  'birthday', 'age', 'gender', 'race', 'ethnicity'],
        'LOW': ['name', 'first_name', 'last_name', 'full_name', 'username', 'user_id']
    }
    
    combined = f"{column_name} {description}".lower()
    
    for sensitivity, patterns in pii_patterns.items():
        if any(pattern in combined for pattern in patterns):
            return True, sensitivity
    
    return False, 'NONE'

def get_enterprise_data_quality_score(column: Dict, table_metadata: Dict = None) -> int:
    """Calculate enterprise-level data quality score for a column"""
    score = 50  # Base score
    
    # Column completeness
    if column.get('nullable') == False:
        score += 10  # NOT NULL constraint
    if column.get('unique'):
        score += 5   # UNIQUE constraint
    if column.get('primary_key'):
        score += 15  # Primary key
    
    # Description quality
    description = column.get('description', '')
    if description and description.strip() and description != '-':
        score += 20
        if len(description) > 50:  # Detailed description
            score += 5
    
    # Data type appropriateness
    col_type = column.get('type', '').upper()
    col_name = column.get('name', '').lower()
    
    # Check for appropriate data types
    if 'email' in col_name and 'VARCHAR' in col_type:
        score += 5
    elif 'date' in col_name and any(dt in col_type for dt in ['DATE', 'TIMESTAMP']):
        score += 5
    elif 'id' in col_name and any(dt in col_type for dt in ['INTEGER', 'BIGINT']):
        score += 5
    
    # Table-level metadata
    if table_metadata:
        if table_metadata.get('row_count', 0) > 1000:
            score += 5  # Large table suggests importance
        if table_metadata.get('last_modified'):
            score += 5  # Recently modified suggests active use
    
    return min(100, max(0, score))  # Clamp between 0-100

def get_column_quality_score(column: Dict) -> int:
    """Get data quality score for a column"""
    # Default quality based on presence of description
    has_desc = bool(column.get('description', '').strip())
    return 95 if has_desc else 80

def extract_column_usage_from_sql(sql: str, table_name: str) -> Dict[str, List[str]]:
    """
    Extract which columns are actually used in SQL queries.
    Returns dict mapping table_name -> list of columns used
    """
    if not sql:
        return {}
    
    column_usage = {}
    
    # Pattern to match column references in SELECT, WHERE, GROUP BY, ORDER BY, etc.
    column_patterns = [
        r'\bSELECT\s+([^,]+?)(?:\s+FROM|\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bWHERE\s+([^,]+?)(?:\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bGROUP\s+BY\s+([^,]+?)(?:\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bORDER\s+BY\s+([^,]+?)(?:\s+UNION|\s*$)',
        r'\bHAVING\s+([^,]+?)(?:\s+UNION|\s*$)',
    ]
    
    for pattern in column_patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)
        for match in matches:
            columns_text = match.group(1)
            # Split by comma and clean up column names
            columns = [col.strip().split('.')[-1].strip('`"\'') for col in columns_text.split(',')]
            columns = [col for col in columns if col and not col.upper() in ['*', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT']]
            
            if table_name not in column_usage:
                column_usage[table_name] = []
            column_usage[table_name].extend(columns)
    
    # Also look for JOIN conditions that reference columns
    join_patterns = [
        r'\bJOIN\s+[^\s]+\s+ON\s+([^,]+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bLEFT\s+JOIN\s+[^\s]+\s+ON\s+([^,]+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bRIGHT\s+JOIN\s+[^\s]+\s+ON\s+([^,]+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
        r'\bINNER\s+JOIN\s+[^\s]+\s+ON\s+([^,]+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s*$)',
    ]
    
    for pattern in join_patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)
        for match in matches:
            join_condition = match.group(1)
            # Extract column names from join conditions
            columns = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', join_condition)
            columns = [col for col in columns if col.upper() not in ['ON', 'AND', 'OR', '=', '!=', '<', '>', '<=', '>=']]
            
            if table_name not in column_usage:
                column_usage[table_name] = []
            column_usage[table_name].extend(columns)
    
    # Remove duplicates and return
    for table in column_usage:
        column_usage[table] = list(set(column_usage[table]))
    
    return column_usage

def analyze_cross_table_sql_relationships(source_asset: Dict, target_asset: Dict, discovered_assets: List[Dict]) -> List[ColumnLineage]:
    """
    Analyze SQL queries across different tables to find actual column relationships.
    This looks for JOINs, subqueries, and other cross-table operations.
    """
    column_relationships = []
    
    source_columns = source_asset.get('columns', [])
    target_columns = target_asset.get('columns', [])
    
    if not source_columns or not target_columns:
        return []
    
    # Get SQL definitions
    source_sql = source_asset.get('sql', '') or source_asset.get('definition', '')
    target_sql = target_asset.get('sql', '') or target_asset.get('definition', '')
    
    # Look for cross-table references in SQL
    source_table_name = source_asset.get('name', '').lower()
    target_table_name = target_asset.get('name', '').lower()
    
    # Check if source table is referenced in target SQL
    if target_sql and source_table_name in target_sql.lower():
        # Extract column usage from target SQL
        target_usage = extract_column_usage_from_sql(target_sql, target_table_name)
        
        # Find matching columns
        for target_col in target_columns:
            target_col_name = target_col['name'].lower()
            
            # Check if this target column uses source columns
            for source_col in source_columns:
                source_col_name = source_col['name'].lower()
                
                # Check for direct column references
                if (f"{source_table_name}.{source_col_name}" in target_sql.lower() or 
                    f"`{source_table_name}`.`{source_col_name}`" in target_sql.lower() or
                    f"{source_table_name}.{source_col_name}" in target_sql.lower()):
                    
                    # Detect PII
                    contains_pii = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                    
                    # Get quality scores
                    source_quality = get_column_quality_score(source_col)
                    target_quality = get_column_quality_score(target_col)
                    avg_quality = (source_quality + target_quality) // 2
                    
                    column_relationships.append(ColumnLineage(
                        source_table=source_asset['id'],
                        source_column=source_col['name'],
                        target_table=target_asset['id'],
                        target_column=target_col['name'],
                        relationship_type="cross_table_sql",
                        contains_pii=contains_pii,
                        data_quality_score=avg_quality,
                        impact_score=9
                    ))
    
    # Check if target table is referenced in source SQL
    elif source_sql and target_table_name in source_sql.lower():
        # Extract column usage from source SQL
        source_usage = extract_column_usage_from_sql(source_sql, source_table_name)
        
        # Find matching columns
        for source_col in source_columns:
            source_col_name = source_col['name'].lower()
            
            # Check if this source column uses target columns
            for target_col in target_columns:
                target_col_name = target_col['name'].lower()
                
                # Check for direct column references
                if (f"{target_table_name}.{target_col_name}" in source_sql.lower() or 
                    f"`{target_table_name}`.`{target_col_name}`" in source_sql.lower() or
                    f"{target_table_name}.{target_col_name}" in source_sql.lower()):
                    
                    # Detect PII
                    contains_pii = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                    
                    # Get quality scores
                    source_quality = get_column_quality_score(source_col)
                    target_quality = get_column_quality_score(target_col)
                    avg_quality = (source_quality + target_quality) // 2
                    
                    column_relationships.append(ColumnLineage(
                        source_table=source_asset['id'],
                        source_column=source_col['name'],
                        target_table=target_asset['id'],
                        target_column=target_col['name'],
                        relationship_type="cross_table_sql",
                        contains_pii=contains_pii,
                        data_quality_score=avg_quality,
                        impact_score=9
                    ))
    
    return column_relationships

def build_column_lineage_from_usage(source_asset: Dict, target_asset: Dict, discovered_assets: List[Dict]) -> List[ColumnLineage]:
    """
    Build column-level lineage based on ACTUAL USAGE in SQL queries, views, and transformations.
    This analyzes real SQL to find which columns are actually used together.
    """
    column_relationships = []
    
    source_columns = source_asset.get('columns', [])
    target_columns = target_asset.get('columns', [])
    
    if not source_columns or not target_columns:
        return []
    
    # Get SQL definitions for views and queries
    source_sql = source_asset.get('sql', '') or source_asset.get('definition', '')
    target_sql = target_asset.get('sql', '') or target_asset.get('definition', '')
    
    # Extract column usage from SQL
    source_usage = extract_column_usage_from_sql(source_sql, source_asset.get('name', ''))
    target_usage = extract_column_usage_from_sql(target_sql, target_asset.get('name', ''))
    
    # Strategy 1: Cross-table SQL analysis
    cross_table_relationships = analyze_cross_table_sql_relationships(source_asset, target_asset, discovered_assets)
    column_relationships.extend(cross_table_relationships)
    
    # Strategy 2: Direct usage-based matching
    # Check if target columns are actually used in source SQL
    source_col_map = {col['name'].lower(): col for col in source_columns}
    target_col_map = {col['name'].lower(): col for col in target_columns}
    
    # Find columns that are actually used in transformations
    for target_col in target_columns:
        target_col_name = target_col['name'].lower()
        
        # Check if this target column is derived from source columns
        for source_col in source_columns:
            source_col_name = source_col['name'].lower()
            
            # Check for direct usage in SQL
            is_used = False
            relationship_type = "inferred"
            
            # Check if source column is used in target SQL
            if target_sql and source_col_name in target_sql.lower():
                is_used = True
                relationship_type = "sql_reference"
            
            # Check if target column is derived from source column in source SQL
            elif source_sql and target_col_name in source_sql.lower():
                is_used = True
                relationship_type = "sql_derived"
            
            # Check for transformation patterns
            elif any(pattern in target_sql.lower() for pattern in [
                f"select {source_col_name}",
                f"from {source_col_name}",
                f"join {source_col_name}",
                f"where {source_col_name}",
                f"group by {source_col_name}",
                f"order by {source_col_name}"
            ]):
                is_used = True
                relationship_type = "sql_transformation"
            
            # Check for aggregation patterns (e.g., COUNT(source_col) -> target_col)
            elif any(pattern in target_sql.lower() for pattern in [
                f"count({source_col_name})",
                f"sum({source_col_name})",
                f"avg({source_col_name})",
                f"min({source_col_name})",
                f"max({source_col_name})"
            ]):
                is_used = True
                relationship_type = "aggregation"
            
            # Check for string transformation patterns
            elif any(pattern in target_sql.lower() for pattern in [
                f"upper({source_col_name})",
                f"lower({source_col_name})",
                f"trim({source_col_name})",
                f"substring({source_col_name}",
                f"concat({source_col_name}"
            ]):
                is_used = True
                relationship_type = "string_transform"
            
            # Check for date transformation patterns
            elif any(pattern in target_sql.lower() for pattern in [
                f"date({source_col_name})",
                f"extract({source_col_name}",
                f"format_date({source_col_name}"
            ]):
                is_used = True
                relationship_type = "date_transform"
            
            if is_used:
                # Detect PII
                contains_pii = detect_pii_in_column(source_col['name'], source_col.get('description', ''))
                
                # Get quality scores
                source_quality = get_column_quality_score(source_col)
                target_quality = get_column_quality_score(target_col)
                avg_quality = (source_quality + target_quality) // 2
                
                # Calculate impact score based on relationship strength
                impact_score = 10 if relationship_type == "sql_reference" else 8 if relationship_type == "sql_derived" else 6
                
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
    
    # No fallback - only return relationships found through actual SQL usage
    return column_relationships


def build_column_lineage_from_metadata(source_asset: Dict, target_asset: Dict) -> List[ColumnLineage]:
    """
    Build column-level lineage based on ACTUAL USAGE in SQL queries and views.
    This is REAL lineage based on actual SQL usage patterns with PII detection and quality tracking!
    """
    # Only use usage-based matching - no fallback to name matching
    return build_column_lineage_from_usage(source_asset, target_asset, [])

def extract_table_references_from_sql(sql: str, project_id: str = None) -> Dict[str, Any]:
    """
    Extract table references AND column transformations from SQL query.
    Returns dict with tables and transformation metadata.
    """
    if not sql:
        return {'tables': [], 'transformations': [], 'column_usage': {}}

    # Prefer AST-based parsing when available
    if HAS_SQLGLOT:
        try:
            node = parse_one(sql)
            tables = {".".join([p for p in map(str, t.parts) if p]) for t in node.find_all(sqlglot.expressions.Table)}
            # Basic transformation detection via function calls
            functions = {f.name.upper() for f in node.find_all(sqlglot.expressions.Func)}
            transformations = []
            for fn in ["COUNT", "SUM", "AVG", "MIN", "MAX", "COALESCE", "CASE", "DISTINCT"]:
                if fn in functions:
                    cat = 'aggregation' if fn in ["COUNT","SUM","AVG","MIN","MAX"] else 'data_quality' if fn=="COALESCE" else 'conditional' if fn=="CASE" else 'distinct'
                    transformations.append({'type': fn, 'category': cat})
            return {
                'tables': list(tables),
                'transformations': transformations,
                'aliases': {},
                'column_usage': extract_column_usage_from_sql(sql, "query"),
                'has_joins': any(j for j in ["JOIN"] if j in sql.upper()),
                'has_unions': "UNION" in sql.upper(),
                'has_subqueries': "SELECT" in sql.upper() and "(" in sql and ")" in sql
            }
        except Exception:
            pass  # Fallback to regex below
    
    # Enhanced patterns to match BigQuery and Starburst table references
    patterns = [
        # BigQuery patterns
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
        # Starburst patterns (catalog.schema.table format)
        r'"([a-zA-Z0-9_-]+)"\."([a-zA-Z0-9_-]+)"\."([a-zA-Z0-9_-]+)"',
        r'([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'"([a-zA-Z0-9_-]+)"\."([a-zA-Z0-9_-]+)"',
        r'\bFROM\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bJOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bLEFT\s+JOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bRIGHT\s+JOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bINNER\s+JOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'\bOUTER\s+JOIN\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        r'UNION\s+ALL\s+SELECT.*?FROM\s+"?([a-zA-Z0-9_-]+)"?\."([a-zA-Z0-9_-]+)"?',
        # Additional Starburst patterns
        r'\bFROM\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bJOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bLEFT\s+JOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bRIGHT\s+JOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bINNER\s+JOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
        r'\bOUTER\s+JOIN\s+([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)',
    ]
    
    tables = set()
    transformations = []
    column_usage = {}
    
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
    
    # Extract column usage from SQL
    column_usage = extract_column_usage_from_sql(sql, "query")
    
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
        'column_usage': column_usage,
        'has_joins': bool(re.search(r'\bJOIN\b', sql, re.IGNORECASE)),
        'has_unions': bool(re.search(r'\bUNION\b', sql, re.IGNORECASE)),
        'has_subqueries': bool(re.search(r'\(.*SELECT.*\)', sql, re.IGNORECASE)),
    }

def get_starburst_view_lineage(asset: Dict[str, Any], connector_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get lineage for a Starburst view by analyzing its SQL definition.
    Returns dict with tables and transformation metadata.
    """
    if asset.get('type') != 'View':
        return {'tables': [], 'transformations': []}
    
    try:
        asset_id = asset.get('id', '')
        catalog = asset.get('catalog', '')
        schema = asset.get('schema', '')
        table_name = asset.get('name', '')
        
        # Get SQL definition from asset metadata
        sql_definition = asset.get('sql', '') or asset.get('definition', '') or asset.get('view_definition', '')
        
        if not sql_definition:
            print(f"DEBUG: No SQL definition found for Starburst view {asset_id}")
            return {'tables': [], 'transformations': []}
        
        # Extract table references and transformations from SQL
        result = extract_table_references_from_sql(sql_definition, catalog)
        print(f"DEBUG: Starburst view {asset_id} references {len(result.get('tables', []))} tables with {len(result.get('transformations', []))} transformations")
        return result
        
    except Exception as e:
        print(f"Error getting Starburst view lineage for {asset.get('id')}: {str(e)}")
        return {'tables': [], 'transformations': []}

def get_starburst_table_lineage(asset: Dict[str, Any], discovered_assets: List[Dict]) -> Dict[str, Any]:
    """
    Get lineage for a Starburst table by analyzing foreign keys, constraints, and metadata.
    This provides enterprise-level lineage even without Views.
    """
    if asset.get('type') != 'Table':
        return {'tables': [], 'transformations': []}
    
    try:
        asset_id = asset.get('id', '')
        catalog = asset.get('catalog', '')
        schema = asset.get('schema', '')
        table_name = asset.get('name', '')
        
        related_tables = []
        transformations = []
        
        # Get table constraints and foreign keys
        constraints = asset.get('constraints', [])
        foreign_keys = asset.get('foreign_keys', [])
        indexes = asset.get('indexes', [])
        
        # Analyze foreign key relationships
        for fk in foreign_keys:
            if 'referenced_table' in fk:
                ref_table = fk['referenced_table']
                # Find the referenced table in discovered assets
                for other_asset in discovered_assets:
                    if (other_asset.get('catalog') == catalog and 
                        other_asset.get('schema') == schema and 
                        other_asset.get('name') == ref_table):
                        related_tables.append(f"{catalog}.{schema}.{ref_table}")
                        transformations.append({
                            'type': 'FOREIGN_KEY',
                            'category': 'constraint',
                            'source_table': f"{catalog}.{schema}.{ref_table}",
                            'target_table': asset_id,
                            'columns': fk.get('columns', [])
                        })
                        break
        
        # Analyze column relationships based on naming patterns and types
        columns = asset.get('columns', [])
        for col in columns:
            col_name = col.get('name', '').lower()
            col_type = col.get('type', '')
            
            # Look for ID columns that might reference other tables
            if any(pattern in col_name for pattern in ['_id', '_key', 'id_', 'key_']):
                # Find tables with similar ID columns
                for other_asset in discovered_assets:
                    if (other_asset.get('id') != asset_id and 
                        other_asset.get('type') == 'Table' and
                        other_asset.get('catalog') == catalog):
                        
                        other_columns = other_asset.get('columns', [])
                        for other_col in other_columns:
                            other_col_name = other_col.get('name', '').lower()
                            other_col_type = other_col.get('type', '')
                            
                            # Check for matching ID patterns
                            if (col_type == other_col_type and 
                                any(pattern in other_col_name for pattern in ['_id', '_key', 'id_', 'key_']) and
                                col_name != other_col_name):
                                
                                related_tables.append(other_asset.get('id'))
                                transformations.append({
                                    'type': 'ID_RELATIONSHIP',
                                    'category': 'metadata',
                                    'source_table': other_asset.get('id'),
                                    'target_table': asset_id,
                                    'columns': [col_name, other_col_name]
                                })
        
        # Analyze table naming patterns for ETL/ELT pipelines
        table_name_lower = table_name.lower()
        for other_asset in discovered_assets:
            if (other_asset.get('id') != asset_id and 
                other_asset.get('type') == 'Table' and
                other_asset.get('catalog') == catalog):
                
                other_name = other_asset.get('name', '').lower()
                
                # Detect ETL patterns
                if (('raw' in table_name_lower or 'source' in table_name_lower) and 
                    ('processed' in other_name or 'stage' in other_name)):
                    related_tables.append(other_asset.get('id'))
                    transformations.append({
                        'type': 'ETL_PIPELINE',
                        'category': 'pipeline',
                        'source_table': asset_id,
                        'target_table': other_asset.get('id'),
                        'stage': 'extract_load'
                    })
                elif (('stage' in table_name_lower or 'staging' in table_name_lower) and 
                      ('analytics' in other_name or 'final' in other_name or 'prod' in other_name)):
                    related_tables.append(other_asset.get('id'))
                    transformations.append({
                        'type': 'ELT_PIPELINE',
                        'category': 'pipeline',
                        'source_table': asset_id,
                        'target_table': other_asset.get('id'),
                        'stage': 'load_transform'
                    })
        
        print(f"DEBUG: Starburst table {asset_id} has {len(related_tables)} related tables with {len(transformations)} transformations")
        return {
            'tables': related_tables,
            'transformations': transformations,
            'constraints': constraints,
            'foreign_keys': foreign_keys
        }
        
    except Exception as e:
        print(f"Error getting Starburst table lineage for {asset.get('id')}: {str(e)}")
        return {'tables': [], 'transformations': []}

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
    use_cache: bool = Query(default=True, description="Use cached results"),
    as_of: Optional[str] = Query(default=None, description="Return lineage as of this ISO timestamp"),
    snapshot: bool = Query(default=False, description="Save a snapshot of the lineage result")
):
    """
    Get comprehensive COLUMN-LEVEL data lineage from discovered assets.
    Uses metadata (column names, types, descriptions) to build relationships.
    Supports pagination for large graphs.
    """
    try:
        from main import active_connectors, load_assets
        
        # Reload assets from file to get latest data
        discovered_assets = load_assets()
        
        print(f"DEBUG: Analyzing lineage for {len(discovered_assets)} assets")
        print(f"DEBUG: Active connectors: {len(active_connectors)}")
        print(f"DEBUG: Active connector IDs: {[conn['id'] for conn in active_connectors]}")
        
        nodes = []
        edges = []
        column_relationship_count = 0
        
        connector_map = {conn['id']: conn for conn in active_connectors}
        
        # Get list of active connector IDs
        active_connector_ids = set(conn['id'] for conn in active_connectors)
        
        # Build asset map - Include assets from active connectors ONLY
        asset_map = {}
        filtered_count = 0
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            
            # Only include assets from active connectors
            if asset_connector_id in active_connector_ids:
                pass  # Include this asset
            else:
                print(f"DEBUG: Skipping asset {asset.get('id')} from deleted connector {asset_connector_id}")
                filtered_count += 1
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
        print(f"DEBUG: Filtered out {filtered_count} assets from deleted connectors")
        
        # Count Starburst assets specifically
        starburst_assets = [a for a in discovered_assets if a.get('connector_id', '').startswith('starburst_')]
        starburst_nodes = [n for n in nodes if n.connector_id.startswith('starburst_')]
        print(f"DEBUG: Starburst assets: {len(starburst_assets)} total, {len(starburst_nodes)} included in lineage")
        
        # Strategy 1: Analyze Views for SQL-based lineage
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            
            # Process assets from active connectors ONLY
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
                    lineage_result = get_starburst_view_lineage(asset, connector_config)
                
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
                        
                        # Enrich confidence with query-log evidence
                        trans_plus = list(transformations) if transformations else []
                        if _logs_imply_relationship(upstream_table_id, asset_id):
                            trans_plus.append({'type': 'QUERY_LOG'})
                        confidence, evidence = compute_edge_confidence(relationship='feeds_into', column_lineage=column_lineage, transformations=trans_plus)
                        now_iso = datetime.now().isoformat()
                        edge = LineageEdge(
                            source=upstream_table_id,
                            target=asset_id,
                            relationship=relationship,
                            column_lineage=column_lineage,
                            total_pii_columns=pii_count,
                            avg_data_quality=round(avg_quality, 2),
                            last_validated=now_iso,
                            validation_status="valid",
                            confidence_score=round(confidence, 3),
                            evidence=evidence,
                            sources=["view_sql"],
                            created_at=now_iso,
                            updated_at=now_iso
                        )
                        edge.edge_signature = sign_edge(edge)
                        edges.append(edge)
                        print(f"DEBUG: Created edge with {len(column_lineage)} column relationships (PII: {pii_count}, Quality: {avg_quality:.1f}): {upstream_table_id} -> {asset_id}")
        
        # Strategy 1.5: Starburst Table-level lineage analysis (foreign keys, constraints, ETL patterns)
        print("DEBUG: Building Starburst table-level lineage...")
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            
            # Process assets from active connectors ONLY
            if asset_connector_id not in active_connector_ids:
                continue
                
            if asset.get('type') == 'Table' and asset_connector_id.startswith('starburst_'):
                asset_id = asset.get('id')
                connector_id = asset.get('connector_id', '')
                
                # Get table-level lineage (foreign keys, constraints, ETL patterns)
                table_lineage = get_starburst_table_lineage(asset, discovered_assets)
                upstream_tables = table_lineage.get('tables', [])
                transformations = table_lineage.get('transformations', [])
                
                for upstream_table_id in upstream_tables:
                    if upstream_table_id in asset_map:
                        # Build column-level lineage based on table relationships
                        source_asset = asset_map[upstream_table_id]
                        target_asset = asset
                        
                        # Create column lineage based on foreign key or ID relationships
                        column_lineage = []
                        for transformation in transformations:
                            if transformation.get('target_table') == asset_id and transformation.get('source_table') == upstream_table_id:
                                # Create column relationships based on transformation metadata
                                columns = transformation.get('columns', [])
                                if len(columns) >= 2:
                                    # Create direct column mapping
                                    for i in range(min(len(columns), 2)):
                                        source_col = columns[0] if i == 0 else f"{columns[0]}_ref"
                                        target_col = columns[1] if i == 0 else f"{columns[1]}_ref"
                                        
                                        # Enhanced PII detection
                                        contains_pii, pii_sensitivity = detect_pii_in_column(source_col, '')
                                        
                                        # Enterprise data quality scoring
                                        source_quality = get_enterprise_data_quality_score({'name': source_col, 'type': 'VARCHAR'}, source_asset)
                                        target_quality = get_enterprise_data_quality_score({'name': target_col, 'type': 'VARCHAR'}, target_asset)
                                        avg_quality = (source_quality + target_quality) // 2
                                        
                                        column_lineage.append(ColumnLineage(
                                            source_table=upstream_table_id,
                                            source_column=source_col,
                                            target_table=asset_id,
                                            target_column=target_col,
                                            relationship_type=transformation.get('type', 'foreign_key').lower(),
                                            contains_pii=contains_pii,
                                            data_quality_score=avg_quality,
                                            impact_score=10  # High impact for foreign key relationships
                                        ))
                        # Fallback: infer ID-based mappings when none provided explicitly
                        if not column_lineage:
                            source_cols = {c.get('name',''): c for c in (source_asset.get('columns', []) or [])}
                            target_cols = {c.get('name',''): c for c in (target_asset.get('columns', []) or [])}
                            inferred_pairs = []
                            for name in source_cols.keys():
                                lname = name.lower()
                                if lname == 'id' or lname.endswith('_id') or 'id_' in lname:
                                    if name in target_cols:
                                        inferred_pairs.append((name, name))
                            for s_name, t_name in inferred_pairs[:3]:  # limit noise
                                contains_pii, _ = detect_pii_in_column(s_name, source_cols[s_name].get('description',''))
                                source_quality = get_enterprise_data_quality_score({'name': s_name, 'type': source_cols[s_name].get('type','')}, source_asset)
                                target_quality = get_enterprise_data_quality_score({'name': t_name, 'type': target_cols[t_name].get('type','')}, target_asset)
                                avg_quality = (source_quality + target_quality) // 2
                                column_lineage.append(ColumnLineage(
                                    source_table=upstream_table_id,
                                    source_column=s_name,
                                    target_table=asset_id,
                                    target_column=t_name,
                                    relationship_type='id_inference',
                                    contains_pii=contains_pii,
                                    data_quality_score=avg_quality,
                                    impact_score=4
                                ))
                        
                        if column_lineage:
                            column_relationship_count += len(column_lineage)
                            
                            # Calculate edge metrics
                            pii_count = sum(1 for col in column_lineage if col.contains_pii)
                            avg_quality = sum(col.data_quality_score for col in column_lineage) / len(column_lineage)
                            
                            # Determine relationship type
                            relationship_type = 'foreign_key'
                            if any(t.get('type') == 'ETL_PIPELINE' for t in transformations):
                                relationship_type = 'etl_pipeline'
                            elif any(t.get('type') == 'ELT_PIPELINE' for t in transformations):
                                relationship_type = 'elt_pipeline'
                            elif any(t.get('type') == 'ID_RELATIONSHIP' for t in transformations):
                                relationship_type = 'id_relationship'
                            
                            trans_plus = list(transformations) if transformations else []
                            if _logs_imply_relationship(upstream_table_id, asset_id):
                                trans_plus.append({'type': 'QUERY_LOG'})
                            confidence, evidence = compute_edge_confidence(relationship=relationship_type, column_lineage=column_lineage, transformations=trans_plus)
                            now_iso = datetime.now().isoformat()
                            edge = LineageEdge(
                                source=upstream_table_id,
                                target=asset_id,
                                relationship=relationship_type,
                                column_lineage=column_lineage,
                                total_pii_columns=pii_count,
                                avg_data_quality=round(avg_quality, 2),
                                last_validated=now_iso,
                                validation_status="valid",
                                confidence_score=round(confidence, 3),
                                evidence=evidence,
                                sources=["starburst_metadata"],
                                created_at=now_iso,
                                updated_at=now_iso
                            )
                            edge.edge_signature = sign_edge(edge)
                            edges.append(edge)
                            print(f"DEBUG: Created Starburst table edge with {len(column_lineage)} column relationships (PII: {pii_count}, Type: {relationship_type}): {upstream_table_id} -> {asset_id}")
        
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
                
                # Process assets from active connectors ONLY
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
                        # Check if they share common columns using actual usage analysis
                        column_lineage = build_column_lineage_from_usage(asset1, asset2, discovered_assets)
                        
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
                                
                                trans_plus = []
                                if _logs_imply_relationship(source_id, target_id):
                                    trans_plus.append({'type': 'QUERY_LOG'})
                                confidence, evidence = compute_edge_confidence(relationship=relationship_type, column_lineage=column_lineage, transformations=trans_plus)
                                now_iso = datetime.now().isoformat()
                                edge = LineageEdge(
                                    source=source_id,
                                    target=target_id,
                                    relationship=relationship_type,
                                    column_lineage=column_lineage,
                                    total_pii_columns=pii_count,
                                    avg_data_quality=round(avg_quality, 2),
                                    last_validated=now_iso,
                                    validation_status="inferred",
                                    confidence_score=round(confidence, 3),
                                    evidence=evidence,
                                    sources=["metadata_inference"],
                                    created_at=now_iso,
                                    updated_at=now_iso
                                )
                                edge.edge_signature = sign_edge(edge)
                                edges.append(edge)
                                
                                stage_icon = "🔄" if is_etl else "⚡" if is_elt else ""
                                print(f"DEBUG: {stage_icon} Edge with {len(column_lineage)} column relationships (PII: {pii_count}, Stage: {pipeline_stage}): {source_id} -> {target_id}")
        
        # Optional temporal filtering
        if as_of:
            try:
                as_of_dt = datetime.fromisoformat(as_of.replace('Z', '+00:00'))
                pre_filter_edge_count = len(edges)
                edges = [e for e in edges if e.created_at and datetime.fromisoformat(e.created_at.replace('Z', '+00:00')) <= as_of_dt]
                # Keep only nodes referenced by filtered edges
                node_ids_kept = {e.source for e in edges} | {e.target for e in edges}
                nodes = [n for n in nodes if n.id in node_ids_kept]
                print(f"DEBUG: Temporal filter as_of={as_of} kept {len(edges)}/{pre_filter_edge_count} edges and {len(nodes)} nodes")
            except Exception as _:
                print("WARN: Invalid as_of; skipping temporal filter")

        print(f"DEBUG: Found {len(edges)} edges with {column_relationship_count} column relationships")
        
        # Calculate lineage-level metrics
        total_pii = sum(e.total_pii_columns for e in edges)
        avg_quality_all = sum(e.avg_data_quality for e in edges) / len(edges) if edges else 0.0
        completeness = (len(nodes) / len(asset_map)) * 100 if asset_map else 0.0
        avg_confidence_all = sum((e.confidence_score or 0.0) for e in edges) / len(edges) if edges else 0.0

        # Upsert to graph store if configured
        try:
            store = GraphStore()
            store.upsert_nodes([n.model_dump() for n in nodes])
            store.upsert_edges([e.model_dump() for e in edges])
            store.close()
        except Exception as _:
            pass
        
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
            
            avg_confidence_page = sum((e.confidence_score or 0.0) for e in paginated_edges) / len(paginated_edges) if paginated_edges else 0.0
            response = LineageResponse(
                nodes=paginated_nodes, 
                edges=paginated_edges,
                column_relationships=column_relationship_count,
                total_pii_columns=total_pii,
                avg_data_quality=round(avg_quality_all, 2),
                lineage_completeness=round(completeness, 2),
                avg_confidence=round(avg_confidence_page, 3)
            )
            if snapshot:
                save_lineage_snapshot(json.loads(response.model_dump_json()))
            return response
        
        response = LineageResponse(
            nodes=nodes, 
            edges=edges,
            column_relationships=column_relationship_count,
            total_pii_columns=total_pii,
            avg_data_quality=round(avg_quality_all, 2),
            lineage_completeness=round(completeness, 2),
            avg_confidence=round(avg_confidence_all, 3)
        )
        if snapshot:
            save_lineage_snapshot(json.loads(response.model_dump_json()))
        return response
        
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
        from main import active_connectors, load_assets
        
        # Reload assets from file to get latest data
        discovered_assets = load_assets()
        
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
        from main import active_connectors, load_assets
        
        # Reload assets from file to get latest data
        discovered_assets = load_assets()
        
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
        from main import active_connectors, load_assets
        
        # Reload assets from file to get latest data
        discovered_assets = load_assets()
        
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
        
        # SLIs we still keep: completeness and avg_data_quality only
        avg_quality = lineage_result.avg_data_quality or 0.0
        completeness = lineage_result.lineage_completeness or 0.0

        # Simplified response without health_score, avg_confidence, or freshness metrics
        status = "healthy" if not issues and not warnings else "degraded"
        return {
            "status": status,
            "issues": issues,
            "warnings": warnings,
            "statistics": {
                "total_nodes": len(lineage_result.nodes),
                "total_edges": len(lineage_result.edges),
                "orphaned_nodes": len(orphaned),
                "stale_edges": len(stale_edges),
                "completeness": completeness,
                "avg_data_quality": round(avg_quality, 2)
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
        from main import load_assets
        
        # Reload assets from file to get latest data
        discovered_assets = load_assets()
        
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

@router.post("/lineage/validate/keys")
async def validate_keys(sample_size: int = Query(default=0, ge=0), x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """Validate PK/FK relationships heuristically (names/types) and return findings.
    Note: Stats sampling is stubbed; integrate warehouse connections to compute real stats.
    """
    try:
        _require_role('admin', {'x-role': x_role})
        lineage = await get_data_lineage(page=0, page_size=1000)
        findings = []
        for edge in lineage.edges:
            for cl in (edge.column_lineage or []):
                src = cl.source_column.lower()
                tgt = cl.target_column.lower()
                pk_like = src == 'id' or src.endswith('_id')
                fk_like = tgt == 'id' or tgt.endswith('_id')
                if pk_like or fk_like:
                    findings.append({
                        'source': edge.source,
                        'target': edge.target,
                        'source_column': cl.source_column,
                        'target_column': cl.target_column,
                        'type_match': True,
                        'name_pattern': 'pkfk_like',
                        'confidence_hint': 0.7
                    })
        # Optional BigQuery stats validation when sample_size > 0
        if sample_size and sample_size > 0:
            from main import active_connectors
            bq_connectors = [c for c in active_connectors if c['id'].startswith('bq_')]
            try:
                from google.cloud import bigquery
                from google.oauth2 import service_account
                for bc in bq_connectors:
                    sa_json = bc.get('service_account_json')
                    if not sa_json:
                        continue
                    creds = service_account.Credentials.from_service_account_info(json.loads(sa_json))
                    client = bigquery.Client(credentials=creds, project=creds.project_id)
                    # Simple sampling check example (distinct ratio proxy)
                    for f in findings[:5]:
                        table_id = f['source']
                        col = f['source_column']
                        q = f"SELECT COUNT(*) AS n, COUNT(DISTINCT {col}) AS nd FROM `{table_id}` LIMIT {sample_size}"
                        try:
                            res = list(client.query(q).result())
                            if res:
                                n = res[0].get('n') or 0
                                nd = res[0].get('nd') or 0
                                f['distinct_ratio_sample'] = float(nd) / float(n) if n else 0.0
                                f['confidence_hint'] = max(f['confidence_hint'], 0.85 if f['distinct_ratio_sample'] > 0.9 else 0.7)
                        except Exception:
                            continue
            except Exception:
                pass
        return { 'findings': findings, 'count': len(findings) }
    except Exception as e:
        print(f"Error validating keys: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to validate keys: {str(e)}")

@router.post("/lineage/curation/propose")
async def propose_lineage_edit(payload: Dict[str, Any], x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """
    Propose a manual lineage edge. Payload: {source, target, relationship, columns?}
    Stored for review. Not immediately merged.
    """
    try:
        # RBAC: require 'admin' role for curation
        _require_role('admin', {'x-role': x_role})
        proposals = _load_curation()
        now_iso = datetime.now().isoformat()
        entry = {
            "source": payload.get("source"),
            "target": payload.get("target"),
            "relationship": payload.get("relationship", "manual"),
            "column_lineage": payload.get("column_lineage", []),
            "proposed_at": now_iso,
            "status": "proposed",
            "notes": payload.get("notes", "")
        }
        proposals.setdefault("proposals", []).append(entry)
        _save_curation(proposals)
        return {"status": "ok", "saved": entry}
    except Exception as e:
        print(f"Error proposing lineage edit: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to propose lineage edit: {str(e)}")

@router.post("/lineage/curation/approve")
async def approve_lineage_edit(source: str, target: str, x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """
    Approve a proposed lineage edge. On approval, it's materialized as a high-confidence manual edge.
    """
    try:
        # RBAC: require 'admin'
        _require_role('admin', {'x-role': x_role})
        proposals = _load_curation()
        match = None
        for p in proposals.get("proposals", []):
            if p.get("source") == source and p.get("target") == target and p.get("status") == "proposed":
                match = p
                break
        if not match:
            raise HTTPException(status_code=404, detail="Proposal not found")

        # Build an edge object to return (not persisted into assets; client can merge)
        now_iso = datetime.now().isoformat()
        edge = LineageEdge(
            source=source,
            target=target,
            relationship=match.get("relationship", "manual"),
            column_lineage=[ColumnLineage(**cl) for cl in match.get("column_lineage", [])] if match.get("column_lineage") else [],
            total_pii_columns=0,
            avg_data_quality=95.0,
            last_validated=now_iso,
            validation_status="valid",
            confidence_score=0.95,
            evidence=["manual_curation"],
            sources=["user"],
            created_at=now_iso,
            updated_at=now_iso
        )
        match["status"] = "approved"
        match["approved_at"] = now_iso
        _save_curation(proposals)
        return {"status": "ok", "edge": edge}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error approving lineage edit: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to approve lineage edit: {str(e)}")

@router.post("/lineage/ingest/querylog")
async def ingest_query_log(payload: Dict[str, Any], x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """Ingest a runtime query log: {system, sql, timestamp}. Stored for evidence and later reconciliation."""
    try:
        _require_role('admin', {'x-role': x_role})
        logs = _load_json_file(QUERYLOG_FILE, {"entries": []})
        logs.setdefault("entries", []).append({
            "system": payload.get("system", "unknown"),
            "sql": payload.get("sql", ""),
            "timestamp": payload.get("timestamp", datetime.now().isoformat()),
        })
        _save_json_file(QUERYLOG_FILE, logs)
        return {"status": "ok", "stored": True, "count": len(logs.get("entries", []))}
    except Exception as e:
        print(f"Error ingesting query log: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to ingest query log: {str(e)}")

@router.post("/lineage/ingest/dbt")
async def ingest_dbt(payload: Dict[str, Any], x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """Ingest dbt manifest/run-results subset: {nodes:[{name,depends_on,sql}]}. Stored in graph store evidence."""
    try:
        _require_role('admin', {'x-role': x_role})
        store = _load_json_file(GRAPH_STORE_FILE, {"snapshots": [], "dbt": []})
        store.setdefault("dbt", []).append({
            "received_at": datetime.now().isoformat(),
            "nodes": payload.get("nodes", [])
        })
        _save_json_file(GRAPH_STORE_FILE, store)
        return {"status": "ok", "stored": True, "dbt_batches": len(store.get("dbt", []))}
    except Exception as e:
        print(f"Error ingesting dbt: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to ingest dbt: {str(e)}")

@router.post("/lineage/ingest/airflow")
async def ingest_airflow(payload: Dict[str, Any], x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """Ingest Airflow DAG lineage hints: {dag_id, tasks:[{task_id, upstream, sql?}]}"""
    try:
        _require_role('admin', {'x-role': x_role})
        store = _load_json_file(GRAPH_STORE_FILE, {"snapshots": [], "airflow": []})
        store.setdefault("airflow", []).append({
            "received_at": datetime.now().isoformat(),
            "dag_id": payload.get("dag_id"),
            "tasks": payload.get("tasks", [])
        })
        _save_json_file(GRAPH_STORE_FILE, store)
        return {"status": "ok", "stored": True, "airflow_batches": len(store.get("airflow", []))}
    except Exception as e:
        print(f"Error ingesting Airflow: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to ingest Airflow: {str(e)}")

@router.post("/lineage/ingest/openlineage")
async def ingest_openlineage(payload: Dict[str, Any], x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """Ingest OpenLineage events. Stored raw for later reconciliation."""
    try:
        _require_role('admin', {'x-role': x_role})
        store = _load_json_file(GRAPH_STORE_FILE, {"snapshots": [], "openlineage": []})
        store.setdefault("openlineage", []).append({
            "received_at": datetime.now().isoformat(),
            "event": payload
        })
        _save_json_file(GRAPH_STORE_FILE, store)
        return {"status": "ok", "stored": True, "openlineage_events": len(store.get("openlineage", []))}
    except Exception as e:
        print(f"Error ingesting OpenLineage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to ingest OpenLineage: {str(e)}")

@router.post("/lineage/ingest/metadata")
async def ingest_metadata(payload: Dict[str, Any], x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """Ingest metadata snapshots from OpenMetadata/Atlas-like sources."""
    try:
        _require_role('admin', {'x-role': x_role})
        store = _load_json_file(GRAPH_STORE_FILE, {"snapshots": [], "metadata": []})
        store.setdefault("metadata", []).append({
            "received_at": datetime.now().isoformat(),
            "payload": payload
        })
        _save_json_file(GRAPH_STORE_FILE, store)
        return {"status": "ok", "stored": True, "metadata_batches": len(store.get("metadata", []))}
    except Exception as e:
        print(f"Error ingesting metadata: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to ingest metadata: {str(e)}")

@router.post("/lineage/reconcile")
async def reconcile_artifacts(x_role: Optional[str] = Query(default=None, alias="X-Role")):
    """Convert ingested artifacts (OpenLineage/dbt/Airflow/Metadata) into edges and upsert to graph store."""
    try:
        _require_role('admin', {'x-role': x_role})
        store_json = _load_json_file(GRAPH_STORE_FILE, {"snapshots": []})
        new_edges: List[Dict[str, Any]] = []
        new_edges += _reconcile_openlineage_to_edges(store_json)
        new_edges += _reconcile_dbt_to_edges(store_json)
        new_edges += _reconcile_airflow_to_edges(store_json)
        new_edges += _reconcile_metadata_to_edges(store_json)
        # Upsert
        gs = GraphStore()
        try:
            gs.upsert_edges(new_edges)
        finally:
            gs.close()
        return {"status": "ok", "created_edges": len(new_edges)}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error reconciling artifacts: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to reconcile artifacts: {str(e)}")
        store.setdefault("airflow", []).append({
            "received_at": datetime.now().isoformat(),
            "dag_id": payload.get("dag_id"),
            "tasks": payload.get("tasks", [])
        })
        _save_json_file(GRAPH_STORE_FILE, store)
        return {"status": "ok", "stored": True, "airflow_batches": len(store.get("airflow", []))}
    except Exception as e:
        print(f"Error ingesting Airflow: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to ingest Airflow: {str(e)}")
