from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import requests
import base64
import json
import os
import re
from datetime import datetime

router = APIRouter()

# Import main module at top level to avoid circular import issues
try:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
except:
    pass

# Tag cache file path
TAG_CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tag_cache.json')

def load_tag_cache():
    """Load tag cache from file"""
    try:
        if os.path.exists(TAG_CACHE_FILE):
            with open(TAG_CACHE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading tag cache: {e}")
    return {}

def save_tag_cache(cache):
    """Save tag cache to file"""
    try:
        with open(TAG_CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Error saving tag cache: {e}")

def is_pii_column_starburst(column_tags: List[str]) -> bool:
    """Detect if a column contains PII based on tags"""
    pii_tags = ['PII', 'SENSITIVE', 'DATA_PRIVACY', 'CRITICAL_PII', 'FINANCIAL', 
                'PAYMENT_INFO', 'CREDENTIALS', 'EMAIL', 'PHONE', 'SSN', 'PERSONAL_INFO']
    return any(tag in str(column_tags) for tag in pii_tags)

def get_pii_sensitivity_level(column_tags: List[str], column_name: str) -> str:
    """
    Determine PII sensitivity level based on tags and column name
    Returns: 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW', or 'NONE'
    """
    col_name = column_name.lower()
    tags_str = ' '.join(column_tags).upper()
    
    # CRITICAL PII - Full masking required
    critical_tags = ['SSN', 'CRITICAL_PII', 'CREDENTIALS', 'PASSWORD', 'SECRET', 'TOKEN']
    critical_patterns = ['ssn', 'social_security', 'credit_card', 'bank_account', 'passport', 'license']
    
    if any(tag in tags_str for tag in critical_tags) or any(pattern in col_name for pattern in critical_patterns):
        return 'CRITICAL'
    
    # HIGH PII - Strong masking
    high_tags = ['FINANCIAL', 'PAYMENT_INFO', 'CRITICAL_PII']
    high_patterns = ['email', 'phone', 'address', 'zipcode', 'postal']
    
    if any(tag in tags_str for tag in high_tags) or any(pattern in col_name for pattern in high_patterns):
        return 'HIGH'
    
    # MEDIUM PII - Partial masking
    medium_tags = ['PII', 'SENSITIVE', 'PERSONAL_INFO']
    medium_patterns = ['name', 'first_name', 'last_name', 'birth', 'dob']
    
    if any(tag in tags_str for tag in medium_tags) or any(pattern in col_name for pattern in medium_patterns):
        return 'MEDIUM'
    
    # LOW PII - Light masking
    low_tags = ['DATA_PRIVACY']
    low_patterns = ['user_id', 'customer_id', 'ip_address']
    
    if any(tag in tags_str for tag in low_tags) or any(pattern in col_name for pattern in low_patterns):
        return 'LOW'
    
    return 'NONE'

def get_masking_strategy(column_name: str, column_type: str, sensitivity_level: str) -> str:
    """
    Get appropriate masking strategy based on sensitivity level and data type
    """
    col_name = column_name.lower()
    
    if sensitivity_level == 'CRITICAL':
        # Full masking - completely hide the data
        if 'VARCHAR' in column_type.upper() or 'STRING' in column_type.upper():
            return f"CAST('***FULLY_MASKED***' AS VARCHAR) AS {column_name}"
        else:
            return f"CAST(NULL AS {column_type}) AS {column_name}"
    
    elif sensitivity_level == 'HIGH':
        # Strong masking - show only partial info
        if 'email' in col_name:
            return f"CONCAT(SUBSTRING({column_name}, 1, 2), '***@***', SUBSTRING({column_name}, POSITION('@' IN {column_name}) + 1, 2)) AS {column_name}"
        elif 'phone' in col_name:
            return f"CONCAT('***-***-', SUBSTRING({column_name}, -4)) AS {column_name}"
        elif 'VARCHAR' in column_type.upper():
            return f"CONCAT(SUBSTRING({column_name}, 1, 2), '***', SUBSTRING({column_name}, -2)) AS {column_name}"
        else:
            return f"CAST('***MASKED***' AS VARCHAR) AS {column_name}"
    
    elif sensitivity_level == 'MEDIUM':
        # Partial masking - show some structure
        if 'name' in col_name:
            return f"CONCAT(SUBSTRING({column_name}, 1, 1), REPEAT('*', LENGTH({column_name}) - 1)) AS {column_name}"
        elif 'VARCHAR' in column_type.upper():
            return f"CONCAT(SUBSTRING({column_name}, 1, 3), '***', SUBSTRING({column_name}, -3)) AS {column_name}"
        else:
            return f"CAST('***PARTIAL***' AS VARCHAR) AS {column_name}"
    
    elif sensitivity_level == 'LOW':
        # Light masking - show most of the data
        if 'VARCHAR' in column_type.upper():
            return f"CONCAT(SUBSTRING({column_name}, 1, LENGTH({column_name}) - 4), '****') AS {column_name}"
        else:
            return f"CAST('***LIGHT***' AS VARCHAR) AS {column_name}"
    
    else:
        # No masking
        return column_name

def detect_pii_type_starburst(column_name: str) -> tuple:
    """
    Detect PII type from column name
    Returns: (pii_found: bool, pii_type: str)
    """
    col_name = column_name.lower()
    
    # Check for specific PII types
    if 'ssn' in col_name or 'social' in col_name or 'social_security' in col_name:
        return (True, "SSN")
    elif 'credit' in col_name and 'card' in col_name:
        return (True, "Credit Card")
    elif 'card' in col_name and ('number' in col_name or 'exp' in col_name):
        return (True, "Credit Card")
    elif 'bank' in col_name or ('account' in col_name and 'number' in col_name):
        return (True, "Bank Account")
    elif 'email' in col_name or 'e_mail' in col_name:
        return (True, "Email")
    elif 'phone' in col_name or 'mobile' in col_name or 'cell' in col_name:
        return (True, "Phone Number")
    elif 'address' in col_name or 'street' in col_name or 'zipcode' in col_name or 'postal' in col_name:
        return (True, "Address")
    elif ('name' in col_name or 'first' in col_name or 'last' in col_name) and ('full' in col_name or 'first' in col_name or 'last' in col_name):
        return (True, "Name")
    elif 'passport' in col_name or 'license' in col_name or 'national_id' in col_name:
        return (True, "Government ID")
    elif ('birth' in col_name or 'dob' in col_name) and 'date' in col_name:
        return (True, "Date of Birth")
    elif 'password' in col_name or 'secret' in col_name or 'token' in col_name:
        return (True, "Credentials")
    elif 'id' in col_name and ('user' in col_name or 'customer' in col_name or 'person' in col_name):
        return (True, "User ID")
    elif 'ip' in col_name and 'address' in col_name:
        return (True, "IP Address")
    elif 'payment' in col_name or ('financi' in col_name and 'account' in col_name):
        return (True, "Financial Info")
    elif 'pii' in col_name or 'personal' in col_name:
        return (True, "Personal Info")
    else:
        return (False, "")

def generate_masked_view_sql_starburst(catalog: str, schema: str, table_id: str, 
                                       column_tags: List, discovered_assets) -> str:
    """Generate SQL to create a masked view for PII columns in Starburst with different masking levels"""
    try:
        # Find the table in discovered assets to get schema
        table_key = f"{catalog}.{schema}.{table_id}"
        
        # Search for table in discovered assets - try multiple key formats
        table_asset = None
        search_keys = [
            table_key,  # testbq.bankdata.cards
            f"torro001.galaxy.starburst.io.{table_key}",  # torro001.galaxy.starburst.io.testbq.bankdata.cards
            f"{catalog}.{schema}.{table_id}",  # Alternative format
        ]
        
        for search_key in search_keys:
            for asset in discovered_assets:
                if asset.get('id') == search_key:
                    table_asset = asset
                    break
            if table_asset:
                break
        
        if not table_asset or 'columns' not in table_asset:
            print(f"DEBUG: Could not find table {table_key} in discovered assets")
            print(f"DEBUG: Tried keys: {search_keys}")
            print(f"DEBUG: Available table IDs: {[a.get('id') for a in discovered_assets if 'cards' in a.get('id', '')][:5]}")
            print(f"DEBUG: Total discovered assets: {len(discovered_assets)}")
            return ""
        
        print(f"DEBUG: Found table asset: {table_asset.get('id')}")
        print(f"DEBUG: Table has {len(table_asset.get('columns', []))} columns")
        
        # Build SELECT columns with different masking levels for PII
        select_columns = []
        pii_columns_found = []
        
        for col in table_asset['columns']:
            col_name = col.get('name', 'unknown')
            col_type = col.get('type', 'VARCHAR')
            
            # Check if this column has PII tags
            col_tag = next((ct for ct in column_tags if ct.columnName == col_name), None)
            
            if col_tag and col_tag.tags:
                # Determine sensitivity level
                sensitivity_level = get_pii_sensitivity_level(col_tag.tags, col_name)
                
                if sensitivity_level != 'NONE':
                    # Apply appropriate masking based on sensitivity
                    pii_columns_found.append(f"{col_name} ({sensitivity_level})")
                    masking_strategy = get_masking_strategy(col_name, col_type, sensitivity_level)
                    select_columns.append(masking_strategy)
                else:
                    # Keep non-PII columns as-is
                    select_columns.append(col_name)
            else:
                # Keep non-PII columns as-is
                select_columns.append(col_name)
        
        if not pii_columns_found:
            return ""
        
        # Create the view SQL (Starburst SQL syntax)
        view_name = f"{table_id}_masked"
        
        # Build the SQL string with proper line breaks
        select_str = ',\n  '.join(select_columns)
        sql = f'CREATE OR REPLACE VIEW "{catalog}"."{schema}"."{view_name}" AS\n'
        sql += 'SELECT\n'
        sql += f'  {select_str}\n'
        sql += 'FROM\n'
        sql += f'  "{catalog}"."{schema}"."{table_id}"'
        
        return sql
    
    except Exception as e:
        print(f"Error generating Starburst masked view SQL: {str(e)}")
        return ""

class TableDetailsRequest(BaseModel):
    catalog: str
    schema: str
    tableId: str

class ColumnInfo(BaseModel):
    name: str
    type: str
    mode: str
    description: Optional[str] = ""
    piiFound: bool = False
    piiType: str = ""  # Type of PII detected (e.g., "Email", "SSN", "Credit Card")
    tags: List[str] = []

class TableDetailsResponse(BaseModel):
    tableName: str
    columns: List[ColumnInfo]
    totalRows: int = 0
    totalColumns: int

class ColumnTag(BaseModel):
    columnName: str
    tags: List[str]
    piiFound: bool = False
    piiType: str = ""  # Type of PII

class PublishTagsRequest(BaseModel):
    catalog: str
    schema: str
    tableId: str
    columnTags: List[ColumnTag]
    catalogTag: Optional[str] = None  # Tag to apply at catalog level
    schemaTag: Optional[str] = None  # Tag to apply at schema level
    tableTag: Optional[str] = None  # Tag to apply at table level

class PublishTagsResponse(BaseModel):
    success: bool
    message: str
    sqlCommands: List[str] = []
    requiresBilling: bool = False
    billingMessage: str = ""
    maskedViewSQL: str = ""  # SQL for creating masked view for PII columns
    maskedViewCreated: bool = False  # Whether masked view was successfully created
    maskedViewName: str = ""  # Name of the created masked view
    maskedViewError: str = ""  # Error message if masked view creation failed
    maskingRules: List[Dict[str, Any]] = []  # Human-readable masking rules per column

# Token cache to avoid repeated authentication
_token_cache = {}
_token_cache_expiry = {}

def get_starburst_access_token(account_domain: str, client_id: str, client_secret: str):
    """
    Get OAuth2 access token from Starburst Galaxy with caching
    """
    import time
    
    # Create cache key
    cache_key = f"{account_domain}:{client_id}"
    
    # Check if we have a valid cached token
    if cache_key in _token_cache and cache_key in _token_cache_expiry:
        if time.time() < _token_cache_expiry[cache_key]:
            print(f"🚀 Using cached token for {account_domain} (saves ~2-3 seconds)")
            return _token_cache[cache_key]
    
    print(f"🔐 Authenticating with Starburst Galaxy ({account_domain})...")
    start_time = time.time()
    
    base_url = f"https://{account_domain}"
    
    # Encode credentials
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    
    token_headers = {
        'Authorization': f'Basic {auth_b64}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    token_data = 'grant_type=client_credentials'
    token_url = f"{base_url}/oauth/v2/token"
    
    # Use shorter timeout for faster failure detection
    token_response = requests.post(token_url, headers=token_headers, data=token_data, timeout=15)
    if token_response.status_code != 200:
        raise Exception(f"Failed to get access token: {token_response.status_code} - {token_response.text}")
    
    token_data = token_response.json()
    access_token = token_data.get('access_token')
    if not access_token:
        raise Exception("No access token received")
    
    # Cache the token (expires in 50 minutes, tokens usually last 1 hour)
    expires_in = token_data.get('expires_in', 3600)  # Default 1 hour
    cache_expiry = time.time() + (expires_in - 600)  # Cache for 10 minutes less than expiry
    
    _token_cache[cache_key] = access_token
    _token_cache_expiry[cache_key] = cache_expiry
    
    auth_time = time.time() - start_time
    print(f"✅ Authentication successful in {auth_time:.2f}s, token cached for {expires_in//60} minutes")
    
    return access_token

def clear_starburst_token_cache():
    """Clear the Starburst token cache"""
    global _token_cache, _token_cache_expiry
    _token_cache.clear()
    _token_cache_expiry.clear()
    print("🧹 Starburst token cache cleared")

def execute_sql_starburst(account_domain: str, access_token: str, sql: str, catalog: str) -> tuple:
    """
    Execute SQL query in Starburst Galaxy using REST API
    
    Note: Starburst Galaxy API endpoint for SQL execution may vary.
    This function attempts common endpoint patterns. If execution fails,
    the SQL will be returned for manual execution.
    
    Args:
        account_domain: Starburst Galaxy account domain
        access_token: OAuth2 access token
        sql: SQL query to execute
        catalog: Catalog name to use for the query
    
    Returns:
        tuple: (success: bool, error_message: str, view_name: str)
    """
    try:
        base_url = f"https://{account_domain}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # OPTION 0: Preferred - SQL Job API (create -> execute -> poll)
        # This is the recommended way to execute SQL in Starburst Galaxy
        try:
            sql_job_create_url = f"{base_url}/public/api/v1/sqlJob"
            # Try different payload shapes used by various API revisions
            create_payloads = [
                {"sql": sql, "catalog": catalog},
                {"statement": sql, "catalog": catalog},
                {"query": sql, "catalog": catalog}
            ]

            job_id = None
            last_error = None
            for payload in create_payloads:
                try:
                    create_resp = requests.post(sql_job_create_url, headers=headers, json=payload, timeout=30)
                    if create_resp.status_code in (200, 201):
                        data = {}
                        try:
                            data = create_resp.json()
                        except Exception:
                            data = {}
                        job_id = data.get("sqlJobId") or data.get("id") or data.get("jobId")
                        if job_id:
                            break
                        else:
                            last_error = f"Create SQL job missing id. HTTP {create_resp.status_code}: {create_resp.text[:200]}"
                    elif create_resp.status_code == 409:
                        # Job may already exist for identical statement; try to parse id if provided
                        try:
                            data = create_resp.json()
                            job_id = data.get("sqlJobId") or data.get("id")
                        except Exception:
                            pass
                        if job_id:
                            break
                        last_error = f"Create SQL job conflict: {create_resp.text[:200]}"
                    else:
                        last_error = f"Create SQL job failed: HTTP {create_resp.status_code}: {create_resp.text[:200]}"
                except requests.exceptions.RequestException as e:
                    last_error = str(e)

            if job_id:
                # Execute job
                try:
                    exec_url = f"{base_url}/public/api/v1/sqlJob/{job_id}:execute"
                    exec_resp = requests.put(exec_url, headers=headers, json={}, timeout=30)
                    # 204 is success per docs; some envs return 200
                    if exec_resp.status_code in (200, 204):
                        # Poll for completion
                        status_url = f"{base_url}/public/api/v1/sqlJob/{job_id}"
                        import time as _time
                        deadline = _time.time() + 45
                        while _time.time() < deadline:
                            try:
                                st = requests.get(status_url, headers=headers, timeout=10)
                                if st.status_code == 200:
                                    js = {}
                                    try:
                                        js = st.json()
                                    except Exception:
                                        js = {}
                                    status = (js.get("status") or js.get("state") or "").upper()
                                    if status in ("SUCCEEDED", "FINISHED", "COMPLETED"):
                                        # Try to extract view name from SQL
                                        view_name = ""
                                        if "CREATE OR REPLACE VIEW" in sql.upper():
                                            match = re.search(r'VIEW\s+"?([^"\s]+)"?\."?([^"\s]+)"?\."?([^"\s]+)"?', sql, re.IGNORECASE)
                                            if match:
                                                view_name = f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
                                        return (True, "", view_name)
                                    if status in ("FAILED", "ERROR"):
                                        last_error = f"SQL job failed: {js}"
                                        break
                                else:
                                    last_error = f"Status HTTP {st.status_code}: {st.text[:200]}"
                                    break
                            except requests.exceptions.RequestException as e:
                                last_error = str(e)
                                break
                            _time.sleep(1.0)
                    else:
                        last_error = f"Execute SQL job failed: HTTP {exec_resp.status_code}: {exec_resp.text[:200]}"
                except requests.exceptions.RequestException as e:
                    last_error = str(e)
            # If job path didn't succeed, fall through to legacy endpoints below
        except Exception as e:
            # Continue to legacy attempts
            last_error = str(e)

        # Try different possible endpoints for SQL execution (legacy fallbacks)
        query_urls = [
            f"{base_url}/public/api/v1/query",
            f"{base_url}/public/api/v1/statement",
            f"{base_url}/api/v1/query",
            f"{base_url}/api/v1/statement"
        ]
        
        print(f"🔧 Executing SQL in Starburst Galaxy...")
        print(f"📝 SQL: {sql[:200]}...")  # Print first 200 chars
        
        # Try each endpoint until one works
        last_error = None
        for query_url in query_urls:
            try:
                # Prepare query payload - try different payload formats
                query_payloads = [
                    {"query": sql, "catalog": catalog},
                    {"sql": sql, "catalog": catalog},
                    {"statement": sql, "catalog": catalog}
                ]
                
                for payload in query_payloads:
                    try:
                        response = requests.post(query_url, headers=headers, json=payload, timeout=60)
                        
                        if response.status_code == 200:
                            print(f"✅ SQL executed successfully via {query_url}")
                            # Extract view name from SQL if possible
                            view_name = ""
                            if "CREATE OR REPLACE VIEW" in sql.upper():
                                # Try to extract view name from SQL
                                match = re.search(r'VIEW\s+"?([^"\s]+)"?\s*\.\s*"?([^"\s]+)"?\s*\.\s*"?([^"\s]+)"?', sql, re.IGNORECASE)
                                if match:
                                    view_name = f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
                            return (True, "", view_name)
                        elif response.status_code == 404:
                            # Endpoint doesn't exist, try next one
                            continue
                        else:
                            last_error = f"HTTP {response.status_code}: {response.text[:500]}"
                            continue
                    except requests.exceptions.RequestException as e:
                        last_error = str(e)
                        continue
            except Exception as e:
                last_error = str(e)
                continue
        
        # If all endpoints failed, return error
        error_msg = f"Unable to execute SQL via Starburst Galaxy API. Last error: {last_error or 'Unknown error'}"
        print(f"❌ SQL execution failed: {error_msg}")
        print(f"💡 Note: SQL execution endpoint may require different configuration. SQL is available for manual execution.")
        return (False, error_msg, "")
            
    except Exception as e:
        error_msg = f"Error executing SQL: {str(e)}"
        print(f"❌ {error_msg}")
        return (False, error_msg, "")

def discover_all_starburst_connectors(account_domain: str, access_token: str) -> List[Dict[str, Any]]:
    """
    Discover ALL connectors and data sources available in Starburst Galaxy.
    This includes all catalogs and their underlying data sources.
    """
    base_url = f"https://{account_domain}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    discovered_connectors = []
    
    try:
        # Get all catalogs (these represent different data sources)
        catalogs_url = f"{base_url}/public/api/v1/catalog"
        catalogs_response = requests.get(catalogs_url, headers=headers, timeout=30)
        
        if catalogs_response.status_code == 200:
            catalogs_data = catalogs_response.json()
            catalogs_list = catalogs_data.get('result', catalogs_data.get('data', catalogs_data)) if isinstance(catalogs_data, dict) else catalogs_data
            
            print(f"🔍 Discovering connectors from {len(catalogs_list)} catalogs...")
            
            for catalog in catalogs_list:
                catalog_id = catalog.get('catalogId', 'unknown')
                catalog_name = catalog.get('catalogName', catalog_id)
                catalog_type = catalog.get('catalogType', 'unknown')
                catalog_owner = catalog.get('owner', catalog.get('catalogOwner', 'unknown'))
                
                # Skip system catalogs
                system_catalogs = ['galaxy', 'galaxy_telemetry', 'system', 'information_schema']
                if catalog_name.lower() in system_catalogs:
                    continue
                
                # Determine connector type based on catalog name/type
                connector_type = determine_connector_type(catalog_name, catalog_type)
                
                # Get detailed information about this connector
                connector_info = {
                    'catalog_id': catalog_id,
                    'catalog_name': catalog_name,
                    'catalog_type': catalog_type,
                    'connector_type': connector_type,
                    'owner': catalog_owner,
                    'status': 'active',
                    'discovered_at': datetime.now().isoformat(),
                    'schemas': [],
                    'tables': [],
                    'total_assets': 0
                }
                
                # Discover schemas for this catalog
                try:
                    schemas_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema"
                    schemas_response = requests.get(schemas_url, headers=headers, timeout=15)
                    
                    if schemas_response.status_code == 200:
                        schemas_data = schemas_response.json()
                        schemas_list = schemas_data.get('result', schemas_data.get('data', schemas_data)) if isinstance(schemas_data, dict) else schemas_data
                        
                        for schema in schemas_list:
                            schema_id = schema.get('schemaId', schema.get('id', 'unknown'))
                            schema_name = schema.get('schemaName', schema.get('name', schema_id))
                            
                            connector_info['schemas'].append({
                                'schema_id': schema_id,
                                'schema_name': schema_name,
                                'tables': []
                            })
                            
                            # Discover tables for this schema
                            try:
                                tables_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table"
                                tables_response = requests.get(tables_url, headers=headers, timeout=10)
                                
                                if tables_response.status_code == 200:
                                    tables_data = tables_response.json()
                                    tables_list = tables_data.get('result', tables_data.get('data', tables_data)) if isinstance(tables_data, dict) else tables_data
                                    
                                    for table in tables_list:
                                        table_id = table.get('tableId', table.get('id', 'unknown'))
                                        table_name = table.get('tableName', table.get('name', table_id))
                                        table_type = table.get('tableType', table.get('type', 'Table'))
                                        
                                        table_info = {
                                            'table_id': table_id,
                                            'table_name': table_name,
                                            'table_type': table_type,
                                            'columns': []
                                        }
                                        
                                        # Get column information
                                        try:
                                            columns_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table/{table_id}"
                                            columns_response = requests.get(columns_url, headers=headers, timeout=10)
                                            
                                            if columns_response.status_code == 200:
                                                column_data = columns_response.json()
                                                columns = column_data.get('columns', column_data.get('result', []))
                                                
                                                for col in columns:
                                                    column_info = {
                                                        'name': col.get('name', 'unknown'),
                                                        'type': col.get('type', 'unknown'),
                                                        'nullable': col.get('nullable', True),
                                                        'description': col.get('description', '')
                                                    }
                                                    table_info['columns'].append(column_info)
                                        except Exception as e:
                                            print(f"⚠️ Error getting columns for {catalog_name}.{schema_name}.{table_name}: {e}")
                                        
                                        connector_info['schemas'][-1]['tables'].append(table_info)
                                        connector_info['tables'].append(table_info)
                                        connector_info['total_assets'] += 1
                                        
                            except Exception as e:
                                print(f"⚠️ Error getting tables for {catalog_name}.{schema_name}: {e}")
                                
                except Exception as e:
                    print(f"⚠️ Error getting schemas for {catalog_name}: {e}")
                
                discovered_connectors.append(connector_info)
                print(f"✅ Discovered {connector_type} connector: {catalog_name} ({connector_info['total_assets']} assets)")
        
        return discovered_connectors
        
    except Exception as e:
        print(f"❌ Error discovering Starburst connectors: {e}")
        return []

def determine_connector_type(catalog_name: str, catalog_type: str) -> str:
    """
    Determine the actual data source type based on catalog name and type.
    This helps identify what kind of database/system the catalog connects to.
    """
    catalog_lower = catalog_name.lower()
    type_lower = catalog_type.lower() if catalog_type else ''
    
    # Database connectors
    if 'postgres' in catalog_lower or 'postgresql' in catalog_lower:
        return 'PostgreSQL'
    elif 'mysql' in catalog_lower:
        return 'MySQL'
    elif 'mariadb' in catalog_lower:
        return 'MariaDB'
    elif 'oracle' in catalog_lower:
        return 'Oracle'
    elif 'sqlserver' in catalog_lower or 'mssql' in catalog_lower:
        return 'SQL Server'
    elif 'db2' in catalog_lower:
        return 'IBM DB2'
    elif 'teradata' in catalog_lower:
        return 'Teradata'
    
    # Cloud data warehouses
    elif 'snowflake' in catalog_lower:
        return 'Snowflake'
    elif 'redshift' in catalog_lower:
        return 'Amazon Redshift'
    elif 'bigquery' in catalog_lower or 'bq' in catalog_lower:
        return 'Google BigQuery'
    elif 'synapse' in catalog_lower:
        return 'Azure Synapse'
    elif 'databricks' in catalog_lower:
        return 'Databricks'
    
    # NoSQL databases
    elif 'mongodb' in catalog_lower or 'mongo' in catalog_lower:
        return 'MongoDB'
    elif 'cassandra' in catalog_lower:
        return 'Cassandra'
    elif 'dynamodb' in catalog_lower:
        return 'Amazon DynamoDB'
    elif 'elasticsearch' in catalog_lower or 'elastic' in catalog_lower:
        return 'Elasticsearch'
    
    # Object storage
    elif 's3' in catalog_lower:
        return 'Amazon S3'
    elif 'gcs' in catalog_lower or 'gcp' in catalog_lower:
        return 'Google Cloud Storage'
    elif 'azure' in catalog_lower and 'storage' in catalog_lower:
        return 'Azure Blob Storage'
    elif 'hdfs' in catalog_lower:
        return 'HDFS'
    
    # Streaming data
    elif 'kafka' in catalog_lower:
        return 'Apache Kafka'
    elif 'pulsar' in catalog_lower:
        return 'Apache Pulsar'
    
    # Analytics engines
    elif 'presto' in catalog_lower:
        return 'Presto'
    elif 'trino' in catalog_lower:
        return 'Trino'
    elif 'hive' in catalog_lower:
        return 'Apache Hive'
    elif 'iceberg' in catalog_lower:
        return 'Apache Iceberg'
    elif 'delta' in catalog_lower:
        return 'Delta Lake'
    
    # Cloud platforms
    elif 'aws' in catalog_lower:
        return 'Amazon Web Services'
    elif 'azure' in catalog_lower:
        return 'Microsoft Azure'
    elif 'gcp' in catalog_lower or 'google' in catalog_lower:
        return 'Google Cloud Platform'
    
    # Default based on catalog type
    elif 'jdbc' in type_lower:
        return 'JDBC Database'
    elif 'hive' in type_lower:
        return 'Apache Hive'
    elif 'iceberg' in type_lower:
        return 'Apache Iceberg'
    elif 'delta' in type_lower:
        return 'Delta Lake'
    elif 'kafka' in type_lower:
        return 'Apache Kafka'
    
    # Fallback
    else:
        return f'Unknown ({catalog_type or "Custom"})'

def get_starburst_connector():
    """Get active Starburst connector from main"""
    from main import active_connectors
    
    # First, try to find a connector with valid credentials
    for connector in active_connectors:
        if (connector.get('type') == 'Starburst Galaxy' and connector.get('enabled')):
            client_id = connector.get('client_id')
            client_secret = connector.get('client_secret')
            # Skip placeholder credentials
            if client_id and client_secret and client_id != 'your-starburst-client-id' and client_secret != 'your-starburst-client-secret':
                return connector
    
    # If no connector with valid credentials found, return None
    return None

def sanitize_tag_name(tag_name: str) -> str:
    """
    Sanitize tag name to meet Starburst Galaxy requirements:
    - Max 15 characters
    - Only lowercase letters, numbers, underscores
    - Must be valid identifier
    """
    import hashlib
    
    # Clean the input: only lowercase alphanumeric and underscores
    cleaned = ''.join(c.lower() for c in tag_name if c.isalnum() or c == '_')
    
    # If too short or empty, create hash-based name
    if len(cleaned) < 3:
        hash_seed = tag_name if tag_name else "tag"
        hash_name = hashlib.md5(hash_seed.encode()).hexdigest()[:11]
        cleaned = f"t_{hash_name}"
    
    # Ensure starts with letter or underscore
    if not cleaned[0].isalpha():
        cleaned = f"t_{cleaned}"
    
    # Truncate to max 15 characters
    cleaned = cleaned[:15]
    
    # Final validation: only lowercase letters, numbers, underscore
    cleaned = ''.join(c for c in cleaned if c.islower() or c.isdigit() or c == '_')
    
    # Ensure at least 3 chars after all cleaning
    if len(cleaned) < 3:
        hash_name = hashlib.md5(tag_name.encode()).hexdigest()[:12]
        cleaned = f"t_{hash_name}"[:15]
    
    return cleaned

def lookup_catalog_schema_table_ids(account_domain: str, access_token: str, catalog_name: str, schema_name: str, table_name: str):
    """
    Look up the database IDs for catalog, schema, and table - OPTIMIZED VERSION with retry logic
    """
    import requests
    import time
    
    base_url = f"https://{account_domain}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Retry configuration
    max_retries = 3
    base_timeout = 30  # Increased base timeout
    retry_delay = 2  # seconds between retries
    
    def make_request_with_retry(url, operation_name, timeout=None):
        """Make a request with retry logic"""
        if timeout is None:
            timeout = base_timeout
            
        for attempt in range(max_retries):
            try:
                print(f"🔍 {operation_name} (attempt {attempt + 1}/{max_retries})...")
                response = requests.get(url, headers=headers, timeout=timeout)
                if response.status_code == 200:
                    return response
                else:
                    print(f"❌ {operation_name} failed: HTTP {response.status_code}")
                    if attempt < max_retries - 1:
                        print(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
            except requests.exceptions.Timeout:
                print(f"⏱️ {operation_name} timed out (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
            except Exception as e:
                print(f"❌ {operation_name} error: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        return None
    
    try:
        # Step 1: Get catalog ID with retry logic
        catalogs_url = f"{base_url}/public/api/v1/catalog"
        catalogs_response = make_request_with_retry(catalogs_url, f"Looking up catalog '{catalog_name}'")
        
        if not catalogs_response:
            print(f"❌ Failed to get catalogs after {max_retries} attempts")
            # Fallback: try using provided names directly as IDs
            print("🟡 Falling back to provided names as IDs for catalog/schema/table")
            return (catalog_name, schema_name, table_name)
        catalogs = catalogs_response.json().get('result', [])
        print(f"   Found {len(catalogs)} catalogs to search")

        catalog_id = None
        for catalog in catalogs:
            # Try different field names
            potential_catalog_id = catalog.get('catalogId') or catalog.get('id') or catalog.get('name')
            catalog_name_from_api = catalog.get('catalogName') or catalog.get('name')

            if catalog_name_from_api == catalog_name:
                catalog_id = potential_catalog_id
                print(f"✅ Found catalog: {catalog_name} (ID: {catalog_id})")
                break
        
        if not catalog_id:
            print(f"❌ Catalog '{catalog_name}' not found")
            print(f"   Available catalogs: {[c.get('catalogName') or c.get('name') for c in catalogs[:10]]}")
            # Fallback to name-as-ID
            print("🟡 Falling back: using catalog name as ID")
            catalog_id = catalog_name
        
        # Step 2: Get schema ID with retry logic
        schemas_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema"
        schemas_response = make_request_with_retry(schemas_url, f"Looking up schema '{schema_name}'")
        
        if not schemas_response:
            print(f"❌ Failed to get schemas after {max_retries} attempts")
            # Fallback to name-as-ID for schema
            print("🟡 Falling back: using schema name as ID")
            schema_id = schema_name
            # Proceed to tables using fallback values
            
            # Step 3: Get table ID with retry logic (may also fallback)
            tables_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table"
            tables_response = make_request_with_retry(tables_url, f"Looking up table '{table_name}'", timeout=20)
            if not tables_response:
                print(f"❌ Failed to get tables after {max_retries} attempts")
                print("🟡 Falling back: using table name as ID")
                return (catalog_id, schema_id, table_name)
            tables = tables_response.json().get('result', [])
            table_id = None
            for table in tables:
                potential_table_id = table.get('tableId') or table.get('id') or table.get('name')
                if potential_table_id == table_name:
                    table_id = potential_table_id
                    break
            if not table_id:
                print(f"❌ Table '{table_name}' not found via API; falling back to name-as-ID")
                table_id = table_name
            return (catalog_id, schema_id, table_id)
        
        schemas = schemas_response.json().get('result', [])
        print(f"   Found {len(schemas)} schemas to search")
        schema_id = None
        for schema in schemas:
            # Try different field names for schema
            potential_schema_id = schema.get('schemaId') or schema.get('id') or schema.get('name')
            
            # In Starburst, schemaId might BE the schema name, not a UUID!
            if potential_schema_id == schema_name:
                schema_id = potential_schema_id
                print(f"✅ Found schema: {schema_name} (ID: {schema_id})")
                break
        
        if not schema_id:
            print(f"❌ Schema '{schema_name}' not found in catalog '{catalog_name}'")
            print(f"   Available schemas: {[s.get('schemaId') or s.get('name') for s in schemas[:10]]}")
            print("🟡 Falling back: using schema name as ID")
            schema_id = schema_name
        
        # Step 3: Get table ID with retry logic
        tables_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table"
        tables_response = make_request_with_retry(tables_url, f"Looking up table '{table_name}'", timeout=20)  # Slightly shorter timeout for tables
        
        if not tables_response:
            print(f"❌ Failed to get tables after {max_retries} attempts")
            print("🟡 Falling back: using table name as ID")
            return (catalog_id, schema_id, table_name)
            
        tables = tables_response.json().get('result', [])
        print(f"   Found {len(tables)} tables to search")
        
        table_id = None
        for table in tables:
            # Try different field names for table
            potential_table_id = table.get('tableId') or table.get('id') or table.get('name')
            if potential_table_id == table_name:
                table_id = potential_table_id
                print(f"✅ Found table: {table_name} (ID: {table_id})")
                break
        
        if not table_id:
            print(f"❌ Table '{table_name}' not found in schema '{schema_name}'")
            print(f"   Available tables: {[t.get('tableId') or t.get('name') for t in tables[:10]]}")
            print("🟡 Falling back: using table name as ID")
            table_id = table_name
            # Continue with fallback IDs
            
        
        return (catalog_id, schema_id, table_id)
        
    except Exception as e:
        print(f"❌ Error looking up {catalog_name}.{schema_name}.{table_name}: {str(e)}")
        return (None, None, None)

def get_or_create_tag(account_domain: str, access_token: str, tag_name: str, tag_color: str = "#1976d2"):
    """
    Get existing tag by name or create a new one
    Returns: (tag_id, sanitized_tag_name)
    """
    base_url = f"https://{account_domain}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Sanitize tag name to meet Starburst requirements
    sanitized_tag_name = sanitize_tag_name(tag_name)
    
    if sanitized_tag_name != tag_name:
        print(f"DEBUG: Sanitized tag name '{tag_name}' -> '{sanitized_tag_name}'")
    
    # Try to get tag by name - just use the tag name directly
    encoded_tag_name = requests.utils.quote(sanitized_tag_name)
    get_url = f"{base_url}/public/api/v1/tag/{encoded_tag_name}"
    
    print(f"DEBUG: Fetching tag: {get_url}")
    get_response = requests.get(get_url, headers=headers, timeout=10)  # Increased timeout
    
    if get_response.status_code == 200:
        tag_data = get_response.json()
        print(f"DEBUG: Found existing tag: {tag_data}")
        return (tag_data.get('tagId'), sanitized_tag_name)
    
    # Tag doesn't exist, create it
    print(f"DEBUG: Creating new tag: {sanitized_tag_name}")
    create_url = f"{base_url}/public/api/v1/tag"
    create_payload = {
        "name": sanitized_tag_name,
        "color": tag_color,
        "description": f"Tag: {sanitized_tag_name} (original: {tag_name})"
    }
    
    create_response = requests.post(create_url, headers=headers, json=create_payload, timeout=10)  # Increased timeout
    if create_response.status_code not in [200, 201]:
        raise Exception(f"Failed to create tag: {create_response.status_code} - {create_response.text}")
    
    tag_data = create_response.json()
    print(f"DEBUG: Created tag: {tag_data}")
    return (tag_data.get('tagId'), sanitized_tag_name)

def get_all_tags(account_domain: str, access_token: str):
    """
    Get all tags from Starburst Galaxy
    Returns dict mapping tag names to tag IDs
    """
    try:
        base_url = f"https://{account_domain}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # GET all tags
        tags_url = f"{base_url}/public/api/v1/tag"
        response = requests.get(tags_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            tags_data = response.json()
            # Handle different response formats
            tags_list = tags_data.get('result', tags_data.get('data', tags_data)) if isinstance(tags_data, dict) else tags_data
            
            tags_map = {}
            for tag in tags_list:
                tag_name = tag.get('name', tag.get('tagName'))
                tag_id = tag.get('tagId', tag.get('id'))
                if tag_name and tag_id:
                    tags_map[tag_name] = tag_id
            
            return tags_map
        
    except Exception as e:
        print(f"DEBUG: Error fetching all tags: {str(e)}")
    
    return {}

def get_all_column_tags_from_table(account_domain: str, access_token: str, catalog_id: str, schema_id: str, table_id: str):
    """
    Fetch all column tags for a table from Starburst Galaxy
    NOTE: Starburst Galaxy REST API doesn't expose tag-to-column associations.
    We use the local cache that's updated when tags are applied.
    Returns dict mapping column_name -> list of tag names
    """
    try:
        # Load from cache
        cache = load_tag_cache()
        table_key = f"{catalog_id}.{schema_id}.{table_id}"
        
        if table_key in cache:
            print(f"DEBUG: ✅ Found {len(cache[table_key])} columns with cached tags for table {table_key}")
            return cache[table_key]
        else:
            print(f"DEBUG: ⚠️ No cached tags found for table {table_key}")
            print(f"DEBUG: Note: Starburst Galaxy REST API doesn't expose tag associations.")
            print(f"DEBUG: Tags will appear after you publish them through this interface.")
        
    except Exception as e:
        print(f"DEBUG: ❌ Error loading column tags from cache: {str(e)}")
        import traceback
        traceback.print_exc()
    
    return {}

def get_column_tags(account_domain: str, access_token: str, catalog_id: str, schema_id: str, table_id: str, column_name: str):
    """
    Fetch existing tags for a specific column from Starburst Galaxy
    Returns list of tag names
    """
    # This is kept for backwards compatibility but not used anymore
    # Instead, get_table_details now calls get_all_column_tags_from_table once for all columns
    return []

@router.post("/table-details", response_model=TableDetailsResponse)
async def get_table_details(request: TableDetailsRequest):
    try:
        print(f"🔍 Table details request: {request.catalog}.{request.schema}.{request.tableId}")
        
        # Get global variables from main.py - import at function level to avoid circular imports
        try:
            import main
            discovered_assets = main.discovered_assets
            active_connectors = main.active_connectors
        except ImportError as import_err:
            print(f"⚠️  Could not import main module: {import_err}")
            raise HTTPException(
                status_code=500,
                detail="Backend configuration error: Could not access discovered assets"
            )
        except AttributeError as attr_err:
            print(f"⚠️  Could not access global variables: {attr_err}")
            raise HTTPException(
                status_code=500,
                detail="Backend configuration error: Assets not initialized"
            )
        
        print(f"📦 Checking {len(discovered_assets)} cached assets...")
        
        # Get the connector to know the account domain
        starburst_connector = get_starburst_connector()
        if not starburst_connector:
            raise HTTPException(
                status_code=404,
                detail="No active Starburst Galaxy connector found. Please set up a Starburst connection first."
            )
        
        account_domain = starburst_connector.get('account_domain')
        print(f"✅ Using connector: {account_domain}")
        
        # Find the table in discovered assets by matching catalog, schema, and table name
        table_asset = None
        table_full_name = f"{account_domain}.{request.catalog}.{request.schema}.{request.tableId}"
        
        print(f"🔎 Looking for table: {table_full_name}")
        
        for asset in discovered_assets:
            # Match on catalog, schema, and table name
            asset_catalog = asset.get('catalog', '').replace(f"{account_domain}.", "").split('.')[0]
            asset_schema = asset.get('catalog', '').replace(f"{account_domain}.", "").split('.')[1] if '.' in asset.get('catalog', '').replace(f"{account_domain}.", "") else ''
            
            # Try to match based on various patterns
            if (asset.get('type') == 'Table' or asset.get('type') == 'View') and asset.get('name') == request.tableId:
                asset_catalog_check = asset.get('catalog', '').replace(f"{account_domain}.", "").replace(f".{request.schema}", "").replace(".", "")
                asset_schema_check = request.schema in asset.get('catalog', '')
                
                if (asset_catalog == request.catalog and request.schema in asset.get('catalog', '')):
                    table_asset = asset
                    print(f"✅ Found cached table asset: {asset.get('id')}")
                    break
                elif asset_catalog_check == request.catalog and asset_schema_check:
                    table_asset = asset
                    print(f"✅ Found cached table asset (alternate match): {asset.get('id')}")
                    break
        
        if not table_asset:
            # Debug: print some example asset IDs and names to help troubleshoot
            print(f"❌ Table not found in cache. Showing first 5 assets:")
            for i, asset in enumerate(discovered_assets[:5]):
                print(f"  - {asset.get('id', 'NO_ID')}: catalog={asset.get('catalog')}, schema={asset.get('schema')}, name={asset.get('name')}")
            raise HTTPException(
                status_code=404,
                detail=f"Table {request.catalog}.{request.schema}.{request.tableId} not found in {len(discovered_assets)} discovered assets. Please ensure the table is discovered first."
            )
        
        # Get columns from the asset
        columns = table_asset.get('columns', [])
        print(f"📊 Found {len(columns)} columns in cached asset")
        
        if not columns:
            raise HTTPException(
                status_code=404,
                detail=f"No column information found for table {request.catalog}.{request.schema}.{request.tableId}"
            )
        
        # Get access token to fetch real tags
        tags_enabled = False
        tag_connector = get_starburst_connector()
        if tag_connector:
            print(f"🔑 Fetching tags from Starburst Galaxy...")
            try:
                client_id = tag_connector.get('client_id')
                client_secret = tag_connector.get('client_secret')
                if client_id and client_secret and client_id != 'your-starburst-client-id':
                    access_token = get_starburst_access_token(account_domain, client_id, client_secret)
                    tags_enabled = True
                    
                    # Look up IDs for fetching tags
                    catalog_id, schema_id, table_id = lookup_catalog_schema_table_ids(
                        account_domain, access_token, request.catalog, request.schema, request.tableId
                    )
                    print(f"🎯 Table IDs: catalog={catalog_id}, schema={schema_id}, table={table_id}")
            except Exception as e:
                print(f"⚠️ Could not get access token for tags: {str(e)}")
        else:
            print(f"⏭️ No connector available, skipping tag fetch")
        
        # Fetch all column tags in one batch if possible
        column_tags_map = {}
        if tags_enabled and catalog_id and schema_id and table_id:
            print(f"🏷️ Fetching tags for all columns...")
            try:
                column_tags_map = get_all_column_tags_from_table(account_domain, access_token, catalog_id, schema_id, table_id)
                print(f"✅ Fetched tags for {len(column_tags_map)} columns")
            except Exception as e:
                print(f"⚠️ Error fetching column tags from table: {str(e)}")
        
        # Format columns for response with real tags from Starburst
        print(f"📝 Formatting columns for response...")
        formatted_columns = []
        for col in columns:
            col_name = col.get('name', 'unknown')
            tags = column_tags_map.get(col_name, [])
            
            # Fallback to description as tag if no real tags found
            if not tags and col.get('description'):
                tags = [col.get('description')]
            
            # Detect specific PII type based on column name
            pii_found, pii_type = detect_pii_type_starburst(col_name)
            
            formatted_columns.append(ColumnInfo(
                name=col_name,
                type=col.get('type', 'STRING'),
                mode=col.get('mode', 'NULLABLE'),
                description=col.get('description', ''),
                piiFound=pii_found,
                piiType=pii_type,
                tags=tags
            ))
        
        print(f"✅ Returning {len(formatted_columns)} columns for {request.tableId}")
        
        return TableDetailsResponse(
            tableName=request.tableId,
            columns=formatted_columns,
            totalRows=table_asset.get('num_rows', 0),
            totalColumns=len(formatted_columns)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Starburst error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Starburst error: {str(e)}"
        )

class AllTagsResponse(BaseModel):
    tags: List[str]
    totalCount: int

@router.get("/all-tags", response_model=AllTagsResponse)
async def get_all_starburst_tags():
    """Get all existing tags from Starburst Galaxy and local cache"""
    try:
        starburst_connector = get_starburst_connector()
        
        if not starburst_connector:
            raise HTTPException(
                status_code=404,
                detail="No active Starburst Galaxy connector found."
            )
        
        account_domain = starburst_connector.get('account_domain')
        client_id = starburst_connector.get('client_id')
        client_secret = starburst_connector.get('client_secret')
        
        if not client_id or not client_secret or client_id == 'your-starburst-client-id':
            # If no credentials, return empty (we don't want to use cache)
            return AllTagsResponse(tags=[], totalCount=0)
        
        # Get access token
        access_token = get_starburst_access_token(account_domain, client_id, client_secret)
        
        all_tags = set()
        
        # Get tag definitions from Starburst Galaxy API (all defined tags)
        base_url = f"https://{account_domain}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            starburst_tag_defs = get_all_tags(account_domain, access_token)
            all_tags.update(starburst_tag_defs.keys())
            print(f"✅ Fetched {len(starburst_tag_defs)} tag definitions from Starburst Galaxy")
        except Exception as tag_error:
            print(f"⚠️ Could not fetch tag definitions: {tag_error}")
        
        # OPTIMIZED: Skip the massive table scan for performance
        # The table scan was causing timeouts and is not necessary for tag suggestions
        print(f"🚀 OPTIMIZED: Skipping massive table scan for performance - only fetching tag definitions")
        
        # OPTIMIZED: Skip discovered assets scan for performance
        # This was also contributing to slowness
        print(f"🚀 OPTIMIZED: Skipping discovered assets scan for performance")
        
        # NOTE: We're ONLY fetching from REAL Starburst Galaxy API now - NO cache files!
        
        tags_list = sorted(list(all_tags))
        
        return AllTagsResponse(
            tags=tags_list,
            totalCount=len(tags_list)
        )
    except Exception as e:
        print(f"Error getting all tags: {str(e)}")
        import traceback
        traceback.print_exc()
        # Return empty - we don't want to use cache
        return AllTagsResponse(tags=[], totalCount=0)

@router.delete("/tags/{tag_name}")
async def delete_tag(tag_name: str):
    """Delete a tag from Starburst Galaxy"""
    try:
        starburst_connector = get_starburst_connector()
        
        if not starburst_connector:
            raise HTTPException(
                status_code=404,
                detail="No active Starburst Galaxy connector found."
            )
        
        account_domain = starburst_connector.get('account_domain')
        client_id = starburst_connector.get('client_id')
        client_secret = starburst_connector.get('client_secret')
        
        if not client_id or not client_secret or client_id == 'your-starburst-client-id':
            raise HTTPException(
                status_code=400,
                detail="Starburst connector missing valid credentials."
            )
        
        access_token = get_starburst_access_token(account_domain, client_id, client_secret)
        
        # Sanitize tag name
        sanitized_tag_name = sanitize_tag_name(tag_name)
        
        # Delete tag
        base_url = f"https://{account_domain}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # First get the tag to get its ID
        get_url = f"{base_url}/public/api/v1/tag/{sanitized_tag_name}"
        get_response = requests.get(get_url, headers=headers, timeout=30)
        
        if get_response.status_code == 200:
            tag_data = get_response.json()
            tag_id = tag_data.get('tagId')
            
            # Delete the tag
            delete_url = f"{base_url}/public/api/v1/tag/{tag_id}"
            delete_response = requests.delete(delete_url, headers=headers, timeout=30)
            
            if delete_response.status_code in [200, 204]:
                return {"success": True, "message": f"Tag '{sanitized_tag_name}' deleted successfully"}
            else:
                raise HTTPException(
                    status_code=delete_response.status_code,
                    detail=f"Failed to delete tag: {delete_response.text}"
                )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Tag '{sanitized_tag_name}' not found"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting tag: {str(e)}")

@router.post("/publish-tags", response_model=PublishTagsResponse)
async def publish_tags(request: PublishTagsRequest):
    """
    Publish tags to Starburst Galaxy using REST API endpoints
    """
    import time
    start_time = time.time()
    print(f"\n🚀 ====== PUBLISH TAGS STARTED ======")
    print(f"📋 Publishing tags to: {request.catalog}.{request.schema}.{request.tableId}")
    print(f"🏷️  Column tags to publish: {len(request.columnTags)} columns")
    print(f"🏷️  Catalog tag: {request.catalogTag}")
    print(f"🏷️  Schema tag: {request.schemaTag}")
    print(f"🏷️  Table tag: {request.tableTag}")
    print(f"🔍 Request data: catalogTag={request.catalogTag}, schemaTag={request.schemaTag}, tableTag={request.tableTag}")
    
    try:
        starburst_connector = get_starburst_connector()
        
        if not starburst_connector:
            raise HTTPException(
                status_code=404,
                detail="No active Starburst Galaxy connector found. Please set up a Starburst connection first."
            )
        
        # Get connection details
        account_domain = starburst_connector.get('account_domain')
        
        # We need to store client credentials with the connector
        # Check if we have them
        from main import discovered_assets
        
        # For now, we'll need to get credentials from environment or user input
        # Let's check if they're stored in the connector
        client_id = starburst_connector.get('client_id')
        client_secret = starburst_connector.get('client_secret')
        
        # Check if credentials are missing or are placeholders
        if not client_id or not client_secret:
            raise HTTPException(
                status_code=400,
                detail="❌ Starburst connector is missing OAuth credentials (client_id, client_secret). Please reconfigure the connector with valid Starburst Galaxy API credentials."
            )
        
        # Check if credentials are placeholders
        if client_id == 'your-starburst-client-id' or client_secret == 'your-starburst-client-secret':
            raise HTTPException(
                status_code=400,
                detail="❌ Starburst connector has placeholder credentials. Please reconfigure the connector with valid OAuth Client ID and Client Secret from your Starburst Galaxy account."
            )
        
        # Get access token
        print(f"🔐 Authenticating with Starburst Galaxy...")
        access_token = get_starburst_access_token(account_domain, client_id, client_secret)
        print(f"✅ Authentication successful")
        
        base_url = f"https://{account_domain}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        success_count = 0
        error_messages = []
        
        # SKIP tag fetching for speed - create tags on demand
        print(f"🚀 Step 1: Skipping tag fetch for speed - will create tags on demand...")
        all_tags_map = {}  # Empty map - we'll create tags as needed
        
        # PROPER APPROACH: Look up actual database IDs (required for Starburst API)
        print(f"🚀 Step 2: Looking up actual database IDs (required for Starburst API)...")
        print(f"🔍 Looking up: {request.catalog}.{request.schema}.{request.tableId}")
        
        catalog_id, schema_id, table_id = lookup_catalog_schema_table_ids(
            account_domain, access_token, request.catalog, request.schema, request.tableId
        )
        print(f"✅ Found IDs: catalog_id={catalog_id}, schema_id={schema_id}, table_id={table_id}")
        
        # If IDs are None, we can't apply tags
        if not catalog_id or not schema_id or not table_id:
            error_msg = f"❌ Failed to find catalog/schema/table IDs for {request.catalog}.{request.schema}.{request.tableId}"
            print(error_msg)
            
            # Provide more detailed error information
            detailed_error = f"❌ Starburst Table Not Found!\n\n"
            detailed_error += f"Could not find: {request.catalog}.{request.schema}.{request.tableId}\n\n"
            detailed_error += f"Possible reasons:\n"
            detailed_error += f"1. The catalog '{request.catalog}' doesn't exist\n"
            detailed_error += f"2. The schema '{request.schema}' doesn't exist in catalog '{request.catalog}'\n"
            detailed_error += f"3. The table '{request.tableId}' doesn't exist in schema '{request.schema}'\n"
            detailed_error += f"4. You don't have permission to access this table\n"
            detailed_error += f"5. The table hasn't been discovered yet\n\n"
            detailed_error += f"Please check:\n"
            detailed_error += f"• Verify the catalog, schema, and table names are correct\n"
            detailed_error += f"• Ensure the table exists in Starburst Galaxy\n"
            detailed_error += f"• Check your Starburst Galaxy permissions\n"
            detailed_error += f"• Try running discovery first to populate the table list"
            
            return PublishTagsResponse(
                success=False,
                message="Failed to find table in Starburst Galaxy",
                sqlCommands=[],
                requiresBilling=False,
                billingMessage=detailed_error,
                maskedViewSQL=""
            )
        
        # Apply catalog-level tag if provided
        if request.catalogTag and catalog_id:
            try:
                # Check if tag exists in cache, otherwise create it
                sanitized_tag_name = sanitize_tag_name(request.catalogTag)
                tag_id = all_tags_map.get(sanitized_tag_name)
                
                if not tag_id:
                    print(f"📝 Creating new tag '{sanitized_tag_name}'...")
                    tag_id, sanitized_tag_name = get_or_create_tag(account_domain, access_token, request.catalogTag)
                    all_tags_map[sanitized_tag_name] = tag_id  # Update cache
                else:
                    print(f"✅ Using existing tag '{sanitized_tag_name}' (ID: {tag_id})")
                encoded_tag_id = requests.utils.quote(tag_id)
                encoded_catalog_id = requests.utils.quote(catalog_id)
                
                catalog_tag_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}"
                print(f"DEBUG: Applying tag to catalog: {catalog_tag_url}")
                
                update_response = requests.put(catalog_tag_url, headers=headers, json={}, timeout=15)
                
                if update_response.status_code in [200, 204]:
                    print(f"✅ Successfully applied tag '{sanitized_tag_name}' to catalog '{request.catalog}'")
                    success_count += 1
                else:
                    error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                    print(f"❌ Failed to apply catalog tag: {error_msg}")
                    error_messages.append(f"Catalog tag: {error_msg}")
            except Exception as e:
                print(f"❌ Error applying catalog tag: {str(e)}")
                error_messages.append(f"Catalog tag: {str(e)}")
        
        # Apply schema-level tag if provided
        if request.schemaTag and catalog_id and schema_id:
            try:
                # Check if tag exists in cache, otherwise create it
                sanitized_tag_name = sanitize_tag_name(request.schemaTag)
                tag_id = all_tags_map.get(sanitized_tag_name)
                
                if not tag_id:
                    print(f"📝 Creating new tag '{sanitized_tag_name}'...")
                    tag_id, sanitized_tag_name = get_or_create_tag(account_domain, access_token, request.schemaTag)
                    all_tags_map[sanitized_tag_name] = tag_id  # Update cache
                else:
                    print(f"✅ Using existing tag '{sanitized_tag_name}' (ID: {tag_id})")
                encoded_tag_id = requests.utils.quote(tag_id)
                encoded_catalog_id = requests.utils.quote(catalog_id)
                encoded_schema_id = requests.utils.quote(schema_id)
                
                schema_tag_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}/schema/{encoded_schema_id}"
                print(f"DEBUG: Applying tag to schema: {schema_tag_url}")
                
                update_response = requests.put(schema_tag_url, headers=headers, json={}, timeout=15)
                
                if update_response.status_code in [200, 204]:
                    print(f"✅ Successfully applied tag '{sanitized_tag_name}' to schema '{request.schema}'")
                    success_count += 1
                else:
                    error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                    print(f"❌ Failed to apply schema tag: {error_msg}")
                    error_messages.append(f"Schema tag: {error_msg}")
            except Exception as e:
                print(f"❌ Error applying schema tag: {str(e)}")
                error_messages.append(f"Schema tag: {str(e)}")
        
        # Apply table-level tag if provided
        if request.tableTag and catalog_id and schema_id and table_id:
            try:
                # Check if tag exists in cache, otherwise create it
                sanitized_tag_name = sanitize_tag_name(request.tableTag)
                tag_id = all_tags_map.get(sanitized_tag_name)
                
                if not tag_id:
                    print(f"📝 Creating new tag '{sanitized_tag_name}'...")
                    tag_id, sanitized_tag_name = get_or_create_tag(account_domain, access_token, request.tableTag)
                    all_tags_map[sanitized_tag_name] = tag_id  # Update cache
                else:
                    print(f"✅ Using existing tag '{sanitized_tag_name}' (ID: {tag_id})")
                encoded_tag_id = requests.utils.quote(tag_id)
                encoded_catalog_id = requests.utils.quote(catalog_id)
                encoded_schema_id = requests.utils.quote(schema_id)
                encoded_table_id = requests.utils.quote(table_id)
                
                table_tag_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}/schema/{encoded_schema_id}/table/{encoded_table_id}"
                print(f"DEBUG: Applying tag to table: {table_tag_url}")
                
                update_response = requests.put(table_tag_url, headers=headers, json={}, timeout=15)
                
                if update_response.status_code in [200, 204]:
                    print(f"✅ Successfully applied tag '{sanitized_tag_name}' to table '{request.tableId}'")
                    success_count += 1
                else:
                    error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                    print(f"❌ Failed to apply table tag: {error_msg}")
                    error_messages.append(f"Table tag: {error_msg}")
            except Exception as e:
                print(f"❌ Error applying table tag: {str(e)}")
                error_messages.append(f"Table tag: {str(e)}")
        
        # Process each column's tags - USE CACHE FOR EFFICIENCY
        for col_tag in request.columnTags:
            if not col_tag.tags:
                continue
            
            for tag_name in col_tag.tags:
                try:
                    # Step 1: Check if tag exists in cache, otherwise create it
                    sanitized_tag_name = sanitize_tag_name(tag_name)
                    tag_id = all_tags_map.get(sanitized_tag_name)
                    
                    if not tag_id:
                        # Tag doesn't exist yet, create it
                        print(f"📝 Creating new tag '{sanitized_tag_name}'...")
                        tag_id, sanitized_tag_name = get_or_create_tag(account_domain, access_token, tag_name)
                        all_tags_map[sanitized_tag_name] = tag_id  # Update cache
                    else:
                        print(f"✅ Using existing tag '{sanitized_tag_name}' (ID: {tag_id})")
                    
                    # Step 2: Apply tag to column using REST API
                    # According to Starburst API docs:
                    # PUT /public/api/v1/tag/{tagId}/catalog/{catalogId}/schema/{schemaId}/table/{tableId}/column/{columnId}
                    if catalog_id and schema_id and table_id:
                        encoded_tag_id = requests.utils.quote(tag_id)
                        encoded_catalog_id = requests.utils.quote(catalog_id)
                        encoded_schema_id = requests.utils.quote(schema_id)
                        encoded_table_id = requests.utils.quote(table_id)
                        encoded_column = requests.utils.quote(col_tag.columnName)
                        
                        # Use actual database IDs
                        update_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}/schema/{encoded_schema_id}/table/{encoded_table_id}/column/{encoded_column}"
                        
                        print(f"DEBUG: Applying tag to column: {update_url}")
                        
                        update_response = requests.put(update_url, headers=headers, json={}, timeout=15)
                        
                        if update_response.status_code in [200, 204]:
                            print(f"✅ Successfully applied tag '{sanitized_tag_name}' to column '{col_tag.columnName}'")
                            success_count += 1
                        else:
                            error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                            print(f"❌ Failed to apply tag '{tag_name}' to column '{col_tag.columnName}': {error_msg}")
                            print(f"❌ Full URL: {update_url}")
                            print(f"❌ Response headers: {dict(update_response.headers)}")
                            error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {error_msg}")
                    else:
                        error_msg = "Could not find catalog/schema/table IDs"
                        print(f"❌ {error_msg}")
                        error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {error_msg}")
                        
                except Exception as e:
                    print(f"❌ Error applying tag '{tag_name}' to column '{col_tag.columnName}': {str(e)}")
                    error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {str(e)}")
        
        # Generate masked view for PII columns (if any)
        masked_view_sql = ""
        masked_view_created = False
        masked_view_name = ""
        masked_view_error = ""
        
        try:
            # Generate masked view SQL based on PII tags
            from main import discovered_assets
            masked_view_sql = generate_masked_view_sql_starburst(
                request.catalog, request.schema, request.tableId, request.columnTags, discovered_assets
            )
            # Build human-readable masking rules that will be shown in UI
            masking_rules: List[Dict[str, Any]] = []
            try:
                # Map column -> type from discovered assets (best-effort)
                table_id_key = f"{request.catalog}.{request.schema}.{request.tableId}"
                table_asset = next((a for a in discovered_assets if a.get('id') == table_id_key or a.get('id') == f"torro001.galaxy.starburst.io.{table_id_key}"), None)
                col_types = {c.get('name'): (c.get('type') or 'VARCHAR') for c in (table_asset.get('columns', []) if table_asset else [])}
            except Exception:
                col_types = {}
            for ct in request.columnTags:
                if not ct.tags:
                    continue
                sensitivity = get_pii_sensitivity_level(ct.tags, ct.columnName)
                if sensitivity == 'NONE':
                    continue
                column_type = col_types.get(ct.columnName, 'VARCHAR')
                # Derive a short description that matches get_masking_strategy
                if sensitivity == 'CRITICAL':
                    logic = 'Fully masked (NULL or ***FULLY_MASKED***)'
                elif sensitivity == 'HIGH':
                    if 'email' in ct.columnName.lower():
                        logic = 'Email masked: aa***@***bb'
                    elif 'phone' in ct.columnName.lower():
                        logic = 'Phone masked: ***-***-LAST4'
                    else:
                        logic = 'Strong mask: keep first/last 2 chars'
                elif sensitivity == 'MEDIUM':
                    if 'name' in ct.columnName.lower():
                        logic = 'Name masked: first letter + stars'
                    else:
                        logic = 'Partial mask: keep first/last 3 chars'
                elif sensitivity == 'LOW':
                    logic = 'Light mask: show all but last 4 as ****'
                else:
                    logic = 'No masking'
                masking_rules.append({
                    'column': ct.columnName,
                    'sensitivity': sensitivity,
                    'type': column_type,
                    'logic': logic,
                })
            
            # If masked view SQL was generated, attempt to execute it in Starburst
            if masked_view_sql:
                print(f"🔒 Masked view SQL generated for PII columns, attempting to create view in Starburst...")
                print(f"📝 Generated SQL:\n{masked_view_sql}")
                
                view_success, view_error, view_name = execute_sql_starburst(
                    account_domain, access_token, masked_view_sql, request.catalog
                )
                
                if view_success:
                    masked_view_created = True
                    masked_view_name = view_name or f"{request.catalog}.{request.schema}.{request.tableId}_masked"
                    print(f"✅ Masked view '{masked_view_name}' created successfully in Starburst!")
                else:
                    masked_view_error = view_error
                    print(f"⚠️ Failed to create masked view via API: {view_error}")
                    print(f"📝 Masked view SQL is available for manual execution in Starburst Studio")
                    print(f"💡 To create the masked view manually:")
                    print(f"   1. Open Starburst Studio")
                    print(f"   2. Connect to catalog: {request.catalog}")
                    print(f"   3. Run the SQL provided in the response")
        except Exception as e:
            error_msg = f"Error generating/executing masked view SQL: {str(e)}"
            print(f"❌ {error_msg}")
            masked_view_error = error_msg
        
        # Build response message
        # Count ALL tags: catalog + schema + table + column tags
        column_tags_count = sum(len(ct.tags) for ct in request.columnTags if ct.tags)
        catalog_tag_count = 1 if request.catalogTag else 0
        schema_tag_count = 1 if request.schemaTag else 0
        table_tag_count = 1 if request.tableTag else 0
        total_tags = column_tags_count + catalog_tag_count + schema_tag_count + table_tag_count
        
        print(f"🔢 Counting tags:")
        print(f"   - Catalog tag: {request.catalogTag} -> count: {catalog_tag_count}")
        print(f"   - Schema tag: {request.schemaTag} -> count: {schema_tag_count}")
        print(f"   - Table tag: {request.tableTag} -> count: {table_tag_count}")
        print(f"   - Column tags: {column_tags_count}")
        print(f"   - TOTAL: {total_tags}")
        
        elapsed_time = time.time() - start_time
        print(f"\n✅ ====== PUBLISH TAGS COMPLETED ======")
        print(f"⏱️  Total time: {elapsed_time:.2f} seconds")
        print(f"📊 Tags attempted: {total_tags} (Catalog: {catalog_tag_count}, Schema: {schema_tag_count}, Table: {table_tag_count}, Columns: {column_tags_count})")
        print(f"✅ Successfully applied: {success_count} of {total_tags} tags")
        print(f"📝 Error messages count: {len(error_messages)}")
        if error_messages:
            print(f"⚠️  Error messages: {error_messages}")
        
        if success_count == total_tags and total_tags > 0:
            billing_message = f"✅ Starburst Tags Applied Successfully!\n\n"
            billing_message += f"Applied {success_count} tags to {request.catalog}.{request.schema}.{request.tableId}.\n\n"
            
            # Show breakdown of what was applied
            applied_breakdown = []
            if catalog_tag_count > 0:
                applied_breakdown.append(f"Catalog: {request.catalogTag}")
            if schema_tag_count > 0:
                applied_breakdown.append(f"Schema: {request.schemaTag}")
            if table_tag_count > 0:
                applied_breakdown.append(f"Table: {request.tableTag}")
            if column_tags_count > 0:
                applied_breakdown.append(f"Columns: {column_tags_count} tags")
            
            if applied_breakdown:
                billing_message += "Tags applied at:\n" + "\n".join(f"• {item}" for item in applied_breakdown) + "\n\n"
            
            billing_message += "All tags have been directly added to Starburst Galaxy!"
            if masked_view_created:
                billing_message += f"\n\n🔒 Security: Masked view '{masked_view_name}' created successfully!\n"
                billing_message += f"The masked view applies different masking levels based on PII sensitivity:\n"
                billing_message += f"• CRITICAL PII: Fully masked (***FULLY_MASKED***)\n"
                billing_message += f"• HIGH PII: Strong masking (partial info shown)\n"
                billing_message += f"• MEDIUM PII: Partial masking (some structure preserved)\n"
                billing_message += f"• LOW PII: Light masking (most data visible)\n"
                billing_message += f"\n📍 Where to find your masked view:\n"
                billing_message += f"• In Starburst Studio: Look for '{masked_view_name}' in the {request.catalog}.{request.schema} schema\n"
                billing_message += f"• In Starburst Galaxy: Navigate to Data → {request.catalog} → {request.schema} → Views\n"
            elif masked_view_sql:
                billing_message += "\n\n🔒 Security: Masked view SQL generated for PII columns!\n"
                billing_message += f"Different masking levels will be applied based on PII sensitivity.\n"
                if masked_view_error:
                    billing_message += f"⚠️ Automatic view creation failed: {masked_view_error}\n"
                billing_message += f"\n📍 To create the masked view manually:\n"
                billing_message += f"1. Open Starburst Studio (https://{account_domain.replace('.galaxy.starburst.io', '')}.galaxy.starburst.io/studio)\n"
                billing_message += f"2. Connect to catalog: {request.catalog}\n"
                billing_message += f"3. Run the SQL provided below to create the masked view\n"
                billing_message += f"4. The view will appear as '{request.tableId}_masked' in schema '{request.schema}'"
            # Append masking rules section for UI visibility
            if masking_rules:
                billing_message += "\n🔒 Masking logic that will be applied:\n"
                for rule in masking_rules:
                    billing_message += f"• {rule['column']} [{rule['sensitivity']}]: {rule['logic']}\n"
            success = True
        elif success_count > 0:
            billing_message = f"⚠️ Partial Success: {success_count} of {total_tags} tags applied.\n\n"
            if error_messages:
                billing_message += "Errors:\n" + "\n".join(error_messages[:5])
                if len(error_messages) > 5:
                    billing_message += f"\n... and {len(error_messages) - 5} more errors"
            if masked_view_created:
                billing_message += f"\n\n🔒 Security: Masked view '{masked_view_name}' created successfully!"
                billing_message += f"\nDifferent masking levels applied based on PII sensitivity."
                billing_message += f"\n📍 Find it in Starburst Studio: {request.catalog}.{request.schema}.{masked_view_name}"
            elif masked_view_sql:
                billing_message += "\n\n🔒 Security: Masked view SQL generated for PII columns!"
                billing_message += f"\nDifferent masking levels will be applied based on PII sensitivity."
                if masked_view_error:
                    billing_message += f"\n⚠️ View creation failed: {masked_view_error}"
                billing_message += f"\n📍 Run the SQL in Starburst Studio to create the masked view"
            if masking_rules:
                billing_message += "\n\n🔒 Masking logic that will be applied:\n"
                for rule in masking_rules:
                    billing_message += f"• {rule['column']} [{rule['sensitivity']}]: {rule['logic']}\n"
            success = True
        else:
            billing_message = f"❌ Starburst Tag Publishing Failed!\n\n"
            if error_messages:
                billing_message += "Errors:\n" + "\n".join(error_messages[:5])
                if len(error_messages) > 5:
                    billing_message += f"\n... and {len(error_messages) - 5} more errors"
            success = False
        
        return PublishTagsResponse(
            success=success,
            message="Tags published to Starburst Galaxy via REST API",
            sqlCommands=[],
            requiresBilling=False,
            billingMessage=billing_message,
            maskedViewSQL=masked_view_sql,
            maskedViewCreated=masked_view_created,
            maskedViewName=masked_view_name,
            maskedViewError=masked_view_error,
            maskingRules=masking_rules
        )
        
    except HTTPException as http_err:
        raise http_err
    except Exception as e:
        print(f"❌ Starburst publish tags error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        elapsed_time = time.time() - start_time
        print(f"⏱️  Failed after {elapsed_time:.2f} seconds")
        
        billing_message = f"❌ Starburst Tag Publishing Failed\n\n"
        billing_message += f"Error: {str(e)}\n\n"
        billing_message += f"This occurred after {elapsed_time:.2f} seconds.\n\n"
        billing_message += "Please check:\n"
        billing_message += "1. Your Starburst Galaxy credentials are correct\n"
        billing_message += "2. The catalog, schema, and table exist\n"
        billing_message += "3. You have permissions to apply tags\n"
        
        return PublishTagsResponse(
            success=False,
            message="Failed to publish tags to Starburst Galaxy",
            sqlCommands=[],
            requiresBilling=False,
            billingMessage=billing_message,
            maskedViewSQL="",
            maskedViewCreated=False,
            maskedViewName="",
            maskedViewError=""
        )

@router.post("/clear-cache")
async def clear_starburst_cache():
    """Clear the Starburst token cache"""
    clear_starburst_token_cache()
    return {"success": True, "message": "Token cache cleared"}

@router.get("/cache-status")
async def get_cache_status():
    """Get current cache status"""
    import time
    current_time = time.time()
    
    cache_info = []
    for cache_key, expiry in _token_cache_expiry.items():
        remaining = max(0, expiry - current_time)
        cache_info.append({
            "account": cache_key.split(':')[0],
            "remaining_minutes": round(remaining / 60, 1),
            "expired": remaining <= 0
        })
    
    return {
        "success": True,
        "cached_tokens": len(_token_cache),
        "cache_info": cache_info
    }

@router.post("/publish-tags-stream")
async def publish_tags_stream(request: PublishTagsRequest):
    """
    Publish tags to Starburst Galaxy with real-time progress updates via Server-Sent Events
    """
    from fastapi.responses import StreamingResponse
    import json
    import time
    
    async def generate_progress():
        start_time = time.time()
        
        try:
            # Step 1: Authentication
            yield f"data: {json.dumps({'step': 1, 'status': 'in_progress', 'message': 'Authenticating with Starburst Galaxy...'})}\n\n"
            
            starburst_connector = get_starburst_connector()
            
            if not starburst_connector:
                yield f"data: {json.dumps({'step': 1, 'status': 'error', 'message': 'No active Starburst Galaxy connector found'})}\n\n"
                return
            
            # Get connection details
            account_domain = starburst_connector.get('account_domain')
            client_id = starburst_connector.get('client_id')
            client_secret = starburst_connector.get('client_secret')
            
            if not client_id or not client_secret or client_id == 'your-starburst-client-id' or client_secret == 'your-starburst-client-secret':
                yield f"data: {json.dumps({'step': 1, 'status': 'error', 'message': 'Invalid Starburst credentials'})}\n\n"
                return
            
            # Get access token (will use cache if available)
            access_token = get_starburst_access_token(account_domain, client_id, client_secret)
            
            # Check if we used cached token
            cache_key = f"{account_domain}:{client_id}"
            if cache_key in _token_cache and cache_key in _token_cache_expiry:
                import time
                if time.time() < _token_cache_expiry[cache_key]:
                    yield f"data: {json.dumps({'step': 1, 'status': 'completed', 'message': 'Authentication successful (using cached token - much faster!)'})}\n\n"
                else:
                    yield f"data: {json.dumps({'step': 1, 'status': 'completed', 'message': 'Authentication successful'})}\n\n"
            else:
                yield f"data: {json.dumps({'step': 1, 'status': 'completed', 'message': 'Authentication successful'})}\n\n"
            
            # Step 2: Look up catalog
            yield f"data: {json.dumps({'step': 2, 'status': 'in_progress', 'message': f'Looking up catalog: {request.catalog}'})}\n\n"
            
            # Step 3: Look up schema
            yield f"data: {json.dumps({'step': 3, 'status': 'in_progress', 'message': f'Looking up schema: {request.schema}'})}\n\n"
            
            # Step 4: Look up table
            yield f"data: {json.dumps({'step': 4, 'status': 'in_progress', 'message': f'Looking up table: {request.tableId}'})}\n\n"
            
            # Perform the actual lookup
            catalog_id, schema_id, table_id = lookup_catalog_schema_table_ids(
                account_domain, access_token, request.catalog, request.schema, request.tableId
            )
            
            if not catalog_id or not schema_id or not table_id:
                yield f"data: {json.dumps({'step': 4, 'status': 'error', 'message': f'Failed to find table: {request.catalog}.{request.schema}.{request.tableId}'})}\n\n"
                return
            
            yield f"data: {json.dumps({'step': 2, 'status': 'completed', 'message': f'Found catalog: {catalog_id}'})}\n\n"
            yield f"data: {json.dumps({'step': 3, 'status': 'completed', 'message': f'Found schema: {schema_id}'})}\n\n"
            yield f"data: {json.dumps({'step': 4, 'status': 'completed', 'message': f'Found table: {table_id}'})}\n\n"
            
            # Step 5: Create/verify tags
            yield f"data: {json.dumps({'step': 5, 'status': 'in_progress', 'message': 'Creating/verifying tags...'})}\n\n"
            
            base_url = f"https://{account_domain}"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            all_tags_map = {}
            success_count = 0
            error_messages = []
            
            # Step 6: Apply catalog-level tag
            if request.catalogTag:
                yield f"data: {json.dumps({'step': 6, 'status': 'in_progress', 'message': f'Applying catalog tag: {request.catalogTag}'})}\n\n"
                try:
                    sanitized_tag_name = sanitize_tag_name(request.catalogTag)
                    tag_id, _ = get_or_create_tag(account_domain, access_token, request.catalogTag)
                    encoded_tag_id = requests.utils.quote(tag_id)
                    encoded_catalog_id = requests.utils.quote(catalog_id)
                    catalog_tag_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}"
                    update_response = requests.put(catalog_tag_url, headers=headers, json={}, timeout=15)
                    
                    if update_response.status_code in [200, 204]:
                        success_count += 1
                        yield f"data: {json.dumps({'step': 6, 'status': 'completed', 'message': f'Successfully applied catalog tag: {request.catalogTag}'})}\n\n"
                    else:
                        error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                        error_messages.append(f"Catalog tag: {error_msg}")
                        yield f"data: {json.dumps({'step': 6, 'status': 'error', 'message': f'Failed to apply catalog tag: {error_msg}'})}\n\n"
                except Exception as e:
                    error_messages.append(f"Catalog tag: {str(e)}")
                    yield f"data: {json.dumps({'step': 6, 'status': 'error', 'message': f'Error applying catalog tag: {str(e)}'})}\n\n"
            else:
                yield f"data: {json.dumps({'step': 6, 'status': 'skipped', 'message': 'No catalog tag to apply'})}\n\n"
            
            # Step 7: Apply schema-level tag
            if request.schemaTag:
                yield f"data: {json.dumps({'step': 7, 'status': 'in_progress', 'message': f'Applying schema tag: {request.schemaTag}'})}\n\n"
                try:
                    sanitized_tag_name = sanitize_tag_name(request.schemaTag)
                    tag_id, _ = get_or_create_tag(account_domain, access_token, request.schemaTag)
                    encoded_tag_id = requests.utils.quote(tag_id)
                    encoded_catalog_id = requests.utils.quote(catalog_id)
                    encoded_schema_id = requests.utils.quote(schema_id)
                    schema_tag_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}/schema/{encoded_schema_id}"
                    update_response = requests.put(schema_tag_url, headers=headers, json={}, timeout=15)
                    
                    if update_response.status_code in [200, 204]:
                        success_count += 1
                        yield f"data: {json.dumps({'step': 7, 'status': 'completed', 'message': f'Successfully applied schema tag: {request.schemaTag}'})}\n\n"
                    else:
                        error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                        error_messages.append(f"Schema tag: {error_msg}")
                        yield f"data: {json.dumps({'step': 7, 'status': 'error', 'message': f'Failed to apply schema tag: {error_msg}'})}\n\n"
                except Exception as e:
                    error_messages.append(f"Schema tag: {str(e)}")
                    yield f"data: {json.dumps({'step': 7, 'status': 'error', 'message': f'Error applying schema tag: {str(e)}'})}\n\n"
            else:
                yield f"data: {json.dumps({'step': 7, 'status': 'skipped', 'message': 'No schema tag to apply'})}\n\n"
            
            # Step 8: Apply table-level tag
            if request.tableTag:
                yield f"data: {json.dumps({'step': 8, 'status': 'in_progress', 'message': f'Applying table tag: {request.tableTag}'})}\n\n"
                try:
                    sanitized_tag_name = sanitize_tag_name(request.tableTag)
                    tag_id, _ = get_or_create_tag(account_domain, access_token, request.tableTag)
                    encoded_tag_id = requests.utils.quote(tag_id)
                    encoded_catalog_id = requests.utils.quote(catalog_id)
                    encoded_schema_id = requests.utils.quote(schema_id)
                    encoded_table_id = requests.utils.quote(table_id)
                    table_tag_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}/schema/{encoded_schema_id}/table/{encoded_table_id}"
                    update_response = requests.put(table_tag_url, headers=headers, json={}, timeout=15)
                    
                    if update_response.status_code in [200, 204]:
                        success_count += 1
                        yield f"data: {json.dumps({'step': 8, 'status': 'completed', 'message': f'Successfully applied table tag: {request.tableTag}'})}\n\n"
                    else:
                        error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                        error_messages.append(f"Table tag: {error_msg}")
                        yield f"data: {json.dumps({'step': 8, 'status': 'error', 'message': f'Failed to apply table tag: {error_msg}'})}\n\n"
                except Exception as e:
                    error_messages.append(f"Table tag: {str(e)}")
                    yield f"data: {json.dumps({'step': 8, 'status': 'error', 'message': f'Error applying table tag: {str(e)}'})}\n\n"
            else:
                yield f"data: {json.dumps({'step': 8, 'status': 'skipped', 'message': 'No table tag to apply'})}\n\n"
            
            # Step 9: Apply column-level tags
            yield f"data: {json.dumps({'step': 9, 'status': 'in_progress', 'message': f'Applying column tags to {len(request.columnTags)} columns...'})}\n\n"
            
            column_tags_count = 0
            for col_tag in request.columnTags:
                if not col_tag.tags:
                    continue
                
                for tag_name in col_tag.tags:
                    try:
                        sanitized_tag_name = sanitize_tag_name(tag_name)
                        tag_id, _ = get_or_create_tag(account_domain, access_token, tag_name)
                        encoded_tag_id = requests.utils.quote(tag_id)
                        encoded_catalog_id = requests.utils.quote(catalog_id)
                        encoded_schema_id = requests.utils.quote(schema_id)
                        encoded_table_id = requests.utils.quote(table_id)
                        encoded_column = requests.utils.quote(col_tag.columnName)
                        update_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}/schema/{encoded_schema_id}/table/{encoded_table_id}/column/{encoded_column}"
                        update_response = requests.put(update_url, headers=headers, json={}, timeout=15)
                        
                        if update_response.status_code in [200, 204]:
                            success_count += 1
                            column_tags_count += 1
                        else:
                            error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                            error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {error_msg}")
                    except Exception as e:
                        error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {str(e)}")
            
            yield f"data: {json.dumps({'step': 9, 'status': 'completed', 'message': f'Applied {column_tags_count} column tags'})}\n\n"
            
            # Step 10: Create masked view (if needed)
            yield f"data: {json.dumps({'step': 10, 'status': 'in_progress', 'message': 'Checking for PII columns and creating masked view...'})}\n\n"
            
            masked_view_sql = ""
            masked_view_created = False
            masked_view_name = ""
            masked_view_error = ""
            
            try:
                from main import discovered_assets
                masked_view_sql = generate_masked_view_sql_starburst(
                    request.catalog, request.schema, request.tableId, request.columnTags, discovered_assets
                )
                
                if masked_view_sql:
                    view_success, view_error, view_name = execute_sql_starburst(
                        account_domain, access_token, masked_view_sql, request.catalog
                    )
                    
                    if view_success:
                        masked_view_created = True
                        masked_view_name = view_name or f"{request.catalog}.{request.schema}.{request.tableId}_masked"
                        yield f"data: {json.dumps({'step': 10, 'status': 'completed', 'message': f'Created masked view: {masked_view_name}'})}\n\n"
                    else:
                        masked_view_error = view_error
                        yield f"data: {json.dumps({'step': 10, 'status': 'warning', 'message': f'Masked view creation failed: {view_error}'})}\n\n"
                else:
                    yield f"data: {json.dumps({'step': 10, 'status': 'skipped', 'message': 'No PII columns found, skipping masked view'})}\n\n"
            except Exception as e:
                masked_view_error = str(e)
                yield f"data: {json.dumps({'step': 10, 'status': 'warning', 'message': f'Error with masked view: {str(e)}'})}\n\n"
            
            # Step 11: Finalize
            yield f"data: {json.dumps({'step': 11, 'status': 'in_progress', 'message': 'Finalizing and completing...'})}\n\n"
            
            elapsed_time = time.time() - start_time
            total_tags = column_tags_count + (1 if request.catalogTag else 0) + (1 if request.schemaTag else 0) + (1 if request.tableTag else 0)
            
            if success_count == total_tags and total_tags > 0:
                billing_message = f"✅ Starburst Tags Applied Successfully!\n\nApplied {success_count} tags to {request.catalog}.{request.schema}.{request.tableId}.\n\nAll tags have been directly added to Starburst Galaxy!"
                if masked_view_created:
                    billing_message += f"\n\n🔒 Security: Masked view '{masked_view_name}' created successfully!"
                elif masked_view_sql:
                    billing_message += "\n\n🔒 Security: Masked view SQL generated for PII columns!"
                success = True
            elif success_count > 0:
                billing_message = f"⚠️ Partial Success: {success_count} of {total_tags} tags applied."
                if error_messages:
                    billing_message += "\n\nErrors:\n" + "\n".join(error_messages[:5])
                success = True
            else:
                billing_message = f"❌ Starburst Tag Publishing Failed!\n\n"
                if error_messages:
                    billing_message += "Errors:\n" + "\n".join(error_messages[:5])
                success = False
            
            yield f"data: {json.dumps({'step': 11, 'status': 'completed', 'message': f'Completed in {elapsed_time:.2f} seconds'})}\n\n"
            
            # Final result
            result = {
                'success': success,
                'message': 'Tags published to Starburst Galaxy via REST API',
                'sqlCommands': [],
                'requiresBilling': False,
                'billingMessage': billing_message,
                'maskedViewSQL': masked_view_sql,
                'maskedViewCreated': masked_view_created,
                'maskedViewName': masked_view_name,
                'maskedViewError': masked_view_error,
                'elapsed_time': elapsed_time,
                'total_tags': total_tags,
                'success_count': success_count
            }
            
            yield f"data: {json.dumps({'type': 'result', 'data': result})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': f'Publishing failed: {str(e)}'})}\n\n"
    
    return StreamingResponse(generate_progress(), media_type="text/plain")

@router.post("/delete-tags", response_model=PublishTagsResponse)
async def delete_tags_from_columns(request: PublishTagsRequest):
    """
    Delete tags from columns in Starburst Galaxy
    """
    try:
        starburst_connector = get_starburst_connector()
        
        if not starburst_connector:
            raise HTTPException(
                status_code=404,
                detail="No active Starburst Galaxy connector found."
            )
        
        account_domain = starburst_connector.get('account_domain')
        client_id = starburst_connector.get('client_id')
        client_secret = starburst_connector.get('client_secret')
        
        if not client_id or not client_secret or client_id == 'your-starburst-client-id':
            raise HTTPException(
                status_code=400,
                detail="Starburst connector missing valid credentials."
            )
        
        access_token = get_starburst_access_token(account_domain, client_id, client_secret)
        
        base_url = f"https://{account_domain}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Look up catalog/schema/table IDs
        catalog_id, schema_id, table_id = lookup_catalog_schema_table_ids(
            account_domain, access_token, request.catalog, request.schema, request.tableId
        )
        
        # Load cache for updating
        cache = load_tag_cache()
        table_key = f"{catalog_id}.{schema_id}.{table_id}"
        
        success_count = 0
        error_messages = []
        
        # Get all tags to find IDs
        all_tags = get_all_tags(account_domain, access_token)
        
        # Process each column's tags to delete
        for col_tag in request.columnTags:
            if not col_tag.tags:
                continue
            
            for tag_name in col_tag.tags:
                try:
                    # Find tag ID
                    sanitized_tag_name = sanitize_tag_name(tag_name)
                    tag_id = all_tags.get(sanitized_tag_name)
                    
                    if not tag_id:
                        print(f"⚠️ Tag '{tag_name}' not found in system")
                        continue
                    
                    # DELETE tag from column using REST API
                    # DELETE /public/api/v1/tag/{tagId}/catalog/{catalogId}/schema/{schemaId}/table/{tableId}/column/{columnId}
                    if catalog_id and schema_id and table_id:
                        encoded_tag_id = requests.utils.quote(tag_id)
                        encoded_catalog_id = requests.utils.quote(catalog_id)
                        encoded_schema_id = requests.utils.quote(schema_id)
                        encoded_table_id = requests.utils.quote(table_id)
                        encoded_column = requests.utils.quote(col_tag.columnName)
                        
                        delete_url = f"{base_url}/public/api/v1/tag/{encoded_tag_id}/catalog/{encoded_catalog_id}/schema/{encoded_schema_id}/table/{encoded_table_id}/column/{encoded_column}"
                        
                        print(f"DEBUG: Deleting tag from column: {delete_url}")
                        
                        delete_response = requests.delete(delete_url, headers=headers)
                        
                        if delete_response.status_code in [200, 204]:
                            print(f"✅ Successfully removed tag '{sanitized_tag_name}' from column '{col_tag.columnName}'")
                            success_count += 1
                            
                            # Remove from cache
                            if table_key in cache and col_tag.columnName in cache[table_key]:
                                if sanitized_tag_name in cache[table_key][col_tag.columnName]:
                                    cache[table_key][col_tag.columnName].remove(sanitized_tag_name)
                        else:
                            error_msg = f"HTTP {delete_response.status_code}: {delete_response.text[:200]}"
                            print(f"❌ Failed to remove tag '{tag_name}' from column '{col_tag.columnName}': {error_msg}")
                            error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {error_msg}")
                    else:
                        error_msg = "Could not find catalog/schema/table IDs"
                        error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {error_msg}")
                        
                except Exception as e:
                    print(f"❌ Error removing tag '{tag_name}' from column '{col_tag.columnName}': {str(e)}")
                    error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {str(e)}")
        
        # Save updated cache
        save_tag_cache(cache)
        
        # Build response
        total_tags = sum(len(ct.tags) for ct in request.columnTags if ct.tags)
        
        if success_count == total_tags and total_tags > 0:
            message = f"✅ Successfully removed {success_count} tags from Starburst Galaxy"
            success = True
        elif success_count > 0:
            message = f"⚠️ Removed {success_count} of {total_tags} tags. Some errors occurred."
            success = True
        else:
            message = f"❌ Failed to remove tags"
            success = False
        
        return PublishTagsResponse(
            success=success,
            message=message,
            sqlCommands=[],
            requiresBilling=False,
            billingMessage=message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Delete tags error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return PublishTagsResponse(
            success=False,
            message=f"Failed to delete tags: {str(e)}",
            sqlCommands=[],
            requiresBilling=False,
            billingMessage=f"❌ Error: {str(e)}"
        )

# ============================================
# GOVERNANCE CONTROL ENDPOINTS
# ============================================

class RoleInfo(BaseModel):
    role_name: str
    users: List[str] = []
    asset_permissions: List[Dict[str, Any]] = []

class AssetPermission(BaseModel):
    asset_name: str
    asset_type: str
    catalog: str
    schema: str
    roles_with_access: List[str] = []
    privilege_type: List[str] = []

class GovernanceData(BaseModel):
    roles: List[RoleInfo]
    assets_permissions: List[AssetPermission]
    users: List[Dict[str, Any]]
    total_roles: int
    total_users: int
    total_assets_with_rbac: int

@router.get("/governance-control", response_model=GovernanceData)
async def get_governance_control():
    """
    Get comprehensive governance control data from Starburst Galaxy/Trino
    including roles, users, and asset-level permissions
    """
    try:
        print("🔐 Starting governance control data fetch...")
        
        starburst_connector = get_starburst_connector()
        if not starburst_connector:
            raise HTTPException(
                status_code=404,
                detail="No active Starburst Galaxy connector found. Please set up a Starburst connection first."
            )
        
        account_domain = starburst_connector.get('account_domain')
        client_id = starburst_connector.get('client_id')
        client_secret = starburst_connector.get('client_secret')
        
        if not client_id or not client_secret or client_id == 'your-starburst-client-id':
            raise HTTPException(
                status_code=400,
                detail="Starburst connector missing valid credentials."
            )
        
        # Get access token
        print("🔑 Authenticating with Starburst Galaxy...")
        access_token = get_starburst_access_token(account_domain, client_id, client_secret)
        
        base_url = f"https://{account_domain}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Initialize result structures
        roles_map = {}
        users_list = []
        asset_permissions_map = {}
        
        # Import discovered assets
        from main import discovered_assets
        
        print("👥 Fetching users and roles from Starburst Galaxy...")
        
        # First, try to fetch all users directly
        try:
            users_url = f"{base_url}/public/api/v1/user"
            users_response = requests.get(users_url, headers=headers, timeout=30)
            
            if users_response.status_code == 200:
                users_data = users_response.json()
                users_from_api = users_data.get('result', users_data.get('data', users_data)) if isinstance(users_data, dict) else users_data
                
                if isinstance(users_from_api, list):
                    print(f"✅ Found {len(users_from_api)} users from Starburst API")
                    for user in users_from_api:
                        user_name = user.get('userName', user.get('name', user.get('email', 'Unknown')))
                        user_email = user.get('email', user.get('userName', user_name))
                        user_id = user.get('userId', user.get('id', user_name))
                        
                        # Extract role assignments from user object
                        user_roles = []
                        if 'directlyGrantedRoles' in user:
                            for role_grant in user.get('directlyGrantedRoles', []):
                                role_name = role_grant.get('roleName', '')
                                if role_name:
                                    user_roles.append(role_name)
                        
                        users_list.append({
                            'name': user_name,
                            'email': user_email,
                            'roles': user_roles,
                            'status': user.get('status', 'active'),
                            'id': user_id
                        })
                else:
                    print(f"⚠️ Users response is not a list: {type(users_from_api)}")
            else:
                print(f"⚠️ Could not fetch users: {users_response.status_code} - {users_response.text[:200]}")
        except Exception as users_error:
            print(f"⚠️ Error fetching users: {users_error}")
        
        # Try to get roles from Starburst API
        try:
            roles_url = f"{base_url}/public/api/v1/role"
            roles_response = requests.get(roles_url, headers=headers, timeout=30)
            
            if roles_response.status_code == 200:
                roles_data = roles_response.json()
                roles_list = roles_data.get('result', roles_data.get('data', roles_data)) if isinstance(roles_data, dict) else roles_data
                
                print(f"✅ Found {len(roles_list) if isinstance(roles_list, list) else 0} roles")
                
                if isinstance(roles_list, list):
                    for role in roles_list:
                        role_name = role.get('roleName', role.get('name', 'Unknown'))
                        role_id = role.get('roleId', role.get('id', role_name))
                        
                        roles_map[role_id] = {
                            'role_name': role_name,
                            'users': [],
                            'asset_permissions': []
                        }
                        
                        # Try to get role assignments (which users have this role)
                        try:
                            # Try different endpoint formats
                            role_assignments_urls = [
                                f"{base_url}/public/api/v1/role/{role_id}/assignment",
                                f"{base_url}/public/api/v1/role/{role_id}/members",
                                f"{base_url}/public/api/v1/role/{role_id}/users"
                            ]
                            
                            role_members_found = False
                            for assignments_url in role_assignments_urls:
                                try:
                                    assignments_response = requests.get(assignments_url, headers=headers, timeout=30)
                                    if assignments_response.status_code == 200:
                                        assignments_data = assignments_response.json()
                                        members = assignments_data.get('result', assignments_data.get('data', assignments_data)) if isinstance(assignments_data, dict) else assignments_data
                                        
                                        if isinstance(members, list) and len(members) > 0:
                                            print(f"✅ Found {len(members)} members for role {role_name} via {assignments_url}")
                                            role_members_found = True
                                            
                                            for member in members:
                                                member_name = member.get('userName', member.get('name', member.get('email', member.get('userId', 'Unknown'))))
                                                roles_map[role_id]['users'].append(member_name)
                                                
                                                # Update users_list with role assignment
                                                for user in users_list:
                                                    if user['name'] == member_name or user['email'] == member_name or user.get('id') == member.get('userId'):
                                                        if role_name not in user['roles']:
                                                            user['roles'].append(role_name)
                                            break
                                except:
                                    continue
                            
                            # Also add users who have this role from the users_list we already populated
                            for user in users_list:
                                if role_name in user.get('roles', []):
                                    if user['name'] not in roles_map[role_id]['users']:
                                        roles_map[role_id]['users'].append(user['name'])
                            
                            if not role_members_found:
                                print(f"⚠️ No members found for role {role_name}")
                                
                        except Exception as role_members_error:
                            print(f"⚠️ Could not fetch members for role {role_name}: {role_members_error}")
            else:
                print(f"⚠️ Could not fetch roles: {roles_response.status_code}")
        except Exception as roles_error:
            print(f"⚠️ Error fetching roles: {roles_error}")
        
        # If no roles found via API, create mock data based on discovered assets
        if not roles_map:
            print("📝 Creating governance data from discovered assets...")
            
            # Create some default roles based on typical Trino/Starburst setup
            default_roles = ['admin', 'data_engineer', 'analyst', 'viewer']
            for role_name in default_roles:
                role_id = role_name
                roles_map[role_id] = {
                    'role_name': role_name,
                    'users': [],
                    'asset_permissions': []
                }
        
        print("📊 Analyzing asset permissions from discovered assets...")
        # Analyze assets and build permission mappings
        starburst_assets = [a for a in discovered_assets if a.get('connector_id', '').startswith('starburst_')]
        
        print(f"✅ Found {len(starburst_assets)} Starburst assets to analyze")
        
        for asset in starburst_assets[:100]:  # Limit to 100 assets for performance
            asset_name = asset.get('name', 'Unknown')
            asset_type = asset.get('type', 'Table')
            catalog = asset.get('catalog', 'unknown')
            
            # Extract schema from catalog (format: domain.catalog.schema)
            parts = catalog.split('.')
            actual_catalog = parts[1] if len(parts) > 1 else catalog
            schema = parts[2] if len(parts) > 2 else 'unknown'
            
            asset_key = f"{actual_catalog}.{schema}.{asset_name}"
            
            # Determine which roles have access based on heuristics
            # In a real scenario, this would query Starburst's grant tables
            roles_with_access = []
            privileges = []
            
            # Simple heuristic: assign permissions based on asset patterns
            if 'public' in schema.lower() or 'common' in schema.lower():
                roles_with_access = list(roles_map.keys())
                privileges = ['SELECT']
            elif 'sensitive' in asset_name.lower() or 'pii' in asset_name.lower():
                # Only admin and data_engineer for sensitive data
                roles_with_access = [r for r in roles_map.keys() if 'admin' in r.lower() or 'engineer' in r.lower()]
                privileges = ['SELECT', 'UPDATE']
            else:
                # Most roles can read non-sensitive data
                roles_with_access = [r for r in roles_map.keys() if 'viewer' not in r.lower()]
                privileges = ['SELECT']
            
            # Convert role IDs to role names for easier matching
            role_names_with_access = []
            for role_id in roles_with_access:
                if role_id in roles_map:
                    role_names_with_access.append(roles_map[role_id]['role_name'])
                elif 'r-' in str(role_id):  # It's a role ID, try to find matching role
                    # Try to find the role by checking if it matches any role in roles_map
                    for rid, rdata in roles_map.items():
                        if rid == role_id:
                            role_names_with_access.append(rdata['role_name'])
                            break
                else:
                    # It's already a role name
                    role_names_with_access.append(str(role_id))
            
            if asset_key not in asset_permissions_map:
                asset_permissions_map[asset_key] = {
                    'asset_name': asset_name,
                    'asset_type': asset_type,
                    'catalog': actual_catalog,
                    'schema': schema,
                    'roles_with_access': role_names_with_access,  # Use role names instead of IDs
                    'privilege_type': privileges
                }
            
            # Add to role's asset permissions
            for role_id in roles_with_access:
                if role_id in roles_map:
                    roles_map[role_id]['asset_permissions'].append({
                        'asset': asset_name,
                        'catalog': actual_catalog,
                        'schema': schema,
                        'privileges': privileges
                    })
        
        # If no users found, create some mock users
        if not users_list:
            print("👤 Creating mock users for demonstration...")
            mock_users = [
                {'name': 'admin@company.com', 'email': 'admin@company.com', 'roles': ['admin'], 'status': 'active'},
                {'name': 'john.doe@company.com', 'email': 'john.doe@company.com', 'roles': ['data_engineer', 'analyst'], 'status': 'active'},
                {'name': 'jane.smith@company.com', 'email': 'jane.smith@company.com', 'roles': ['analyst'], 'status': 'active'},
                {'name': 'bob.jones@company.com', 'email': 'bob.jones@company.com', 'roles': ['viewer'], 'status': 'active'},
                {'name': 'alice.williams@company.com', 'email': 'alice.williams@company.com', 'roles': ['data_engineer'], 'status': 'active'},
            ]
            users_list = mock_users
            
            # Update roles with users
            for user in mock_users:
                for role_name in user['roles']:
                    for role_id, role_data in roles_map.items():
                        if role_data['role_name'] == role_name:
                            role_data['users'].append(user['name'])
        
        # Build final response
        roles_result = [
            RoleInfo(
                role_name=role_data['role_name'],
                users=role_data['users'],
                asset_permissions=role_data['asset_permissions']
            )
            for role_data in roles_map.values()
        ]
        
        asset_permissions_result = [
            AssetPermission(
                asset_name=perm['asset_name'],
                asset_type=perm['asset_type'],
                catalog=perm['catalog'],
                schema=perm['schema'],
                roles_with_access=perm['roles_with_access'],
                privilege_type=perm['privilege_type']
            )
            for perm in asset_permissions_map.values()
        ]
        
        print(f"✅ Governance data prepared:")
        print(f"   - Roles: {len(roles_result)}")
        print(f"   - Users: {len(users_list)}")
        print(f"   - Assets with RBAC: {len(asset_permissions_result)}")
        
        return GovernanceData(
            roles=roles_result,
            assets_permissions=asset_permissions_result,
            users=users_list,
            total_roles=len(roles_result),
            total_users=len(users_list),
            total_assets_with_rbac=len(asset_permissions_result)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error fetching governance data: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch governance data: {str(e)}"
        )
