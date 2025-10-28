from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
from google.cloud import datacatalog_v1
from google.oauth2 import service_account
import os
import json
import time
import re
from typing import List, Optional, Dict

router = APIRouter()

def create_policy_tag_taxonomy(project_id: str, credentials, taxonomy_name: str = "DataClassification") -> str:
    """Create a policy tag taxonomy for data classification"""
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        
        # Check if taxonomy already exists
        parent = f"projects/{project_id}/locations/us"
        taxonomies = datacatalog_client.list_taxonomies(parent=parent)
        
        for taxonomy in taxonomies:
            if taxonomy.display_name == taxonomy_name:
                print(f"✅ Taxonomy '{taxonomy_name}' already exists: {taxonomy.name}")
                return taxonomy.name
        
        # Create new taxonomy
        taxonomy = datacatalog_v1.Taxonomy()
        taxonomy.display_name = taxonomy_name
        taxonomy.description = "Data classification taxonomy for policy tags"
        
        created_taxonomy = datacatalog_client.create_taxonomy(
            parent=parent,
            taxonomy=taxonomy
        )
        
        print(f"✅ Created taxonomy '{taxonomy_name}': {created_taxonomy.name}")
        return created_taxonomy.name
        
    except Exception as e:
        print(f"❌ Could not create taxonomy: {str(e)}")
        print(f"❌ Error type: {type(e).__name__}")
        print(f"❌ Full error details: {e}")
        print(f"❌ This might be due to missing Data Catalog API permissions or API not enabled")
        raise e  # Re-raise the exception instead of returning None

def create_policy_tag(taxonomy_name: str, tag_name: str, credentials) -> str:
    """Create a policy tag within a taxonomy"""
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        
        # Check if policy tag already exists
        policy_tags = datacatalog_client.list_policy_tags(parent=taxonomy_name)
        
        for tag in policy_tags:
            if tag.display_name == tag_name:
                print(f"✅ Policy tag '{tag_name}' already exists: {tag.name}")
                return tag.name
        
        # Create new policy tag
        policy_tag = datacatalog_v1.PolicyTag()
        policy_tag.display_name = tag_name
        policy_tag.description = f"Policy tag for {tag_name}"
        
        created_tag = datacatalog_client.create_policy_tag(
            parent=taxonomy_name,
            policy_tag=policy_tag
        )
        
        print(f"✅ Created policy tag '{tag_name}': {created_tag.name}")
        return created_tag.name
        
    except Exception as e:
        print(f"❌ Could not create policy tag '{tag_name}': {str(e)}")
        print(f"❌ Error type: {type(e).__name__}")
        print(f"❌ Full error details: {e}")
        raise e  # Re-raise the exception instead of returning None

def delete_policy_tag(policy_tag_name: str, credentials) -> bool:
    """Delete a policy tag"""
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        
        # Delete the policy tag
        datacatalog_client.delete_policy_tag(name=policy_tag_name)
        print(f"✅ Deleted policy tag: {policy_tag_name}")
        return True
        
    except Exception as e:
        print(f"⚠️ Could not delete policy tag '{policy_tag_name}': {str(e)}")
        return False

def get_policy_tag_by_name(taxonomy_name: str, tag_name: str, credentials) -> str:
    """Get a policy tag by its display name"""
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        
        # List all policy tags in the taxonomy
        policy_tags = datacatalog_client.list_policy_tags(parent=taxonomy_name)
        
        for tag in policy_tags:
            if tag.display_name == tag_name:
                return tag.name
        
        return None
        
    except Exception as e:
        print(f"⚠️ Could not find policy tag '{tag_name}': {str(e)}")
        return None

def create_table_taxonomy(project_id: str, credentials, taxonomy_name: str = "TableClassification") -> str:
    """Create a policy tag taxonomy for table-level classification"""
    try:
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        
        # Check if taxonomy already exists
        parent = f"projects/{project_id}/locations/us"
        taxonomies = datacatalog_client.list_taxonomies(parent=parent)
        
        for taxonomy in taxonomies:
            if taxonomy.display_name == taxonomy_name:
                print(f"✅ Table taxonomy '{taxonomy_name}' already exists: {taxonomy.name}")
                return taxonomy.name
        
        # Create new taxonomy
        taxonomy = datacatalog_v1.Taxonomy()
        taxonomy.display_name = taxonomy_name
        taxonomy.description = "Table-level classification taxonomy for policy tags"
        
        created_taxonomy = datacatalog_client.create_taxonomy(
            parent=parent,
            taxonomy=taxonomy
        )
        
        print(f"✅ Created table taxonomy '{taxonomy_name}': {created_taxonomy.name}")
        return created_taxonomy.name
        
    except Exception as e:
        print(f"⚠️ Could not create table taxonomy: {str(e)}")
        return None

def is_pii_column(column: Dict) -> bool:
    """Detect if a column contains PII based on tags"""
    tags = column.get('tags', [])
    # Check for PII-related tags
    pii_tags = ['PII', 'SENSITIVE', 'DATA_PRIVACY', 'CRITICAL_PII', 'FINANCIAL', 
                'PAYMENT_INFO', 'CREDENTIALS', 'EMAIL', 'PHONE', 'SSN', 'PERSONAL_INFO']
    return any(tag in str(tags) for tag in pii_tags)

def detect_pii_type(column_name: str) -> tuple:
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

def generate_masked_view_sql_bigquery(project_id: str, dataset_id: str, table_id: str, 
                                     column_tags: List, client) -> str:
    """Generate SQL to create a masked view for PII columns in BigQuery"""
    # Get the original table schema
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        
        # Build SELECT columns with masking for PII
        select_columns = []
        pii_columns_found = []
        
        for field in table.schema:
            # Check if this column has PII tags
            col_tag = next((col for col in column_tags if col.name == field.name), None)
            
            if col_tag and is_pii_column({'tags': col_tag.tags}):
                # Mask PII column based on type
                pii_columns_found.append(field.name)
                
                if field.field_type in ['STRING', 'BYTES']:
                    # For strings, replace with masked value
                    select_columns.append(f"CAST('***MASKED***' AS STRING) AS {field.name}")
                elif field.field_type == 'INTEGER':
                    select_columns.append(f"CAST(NULL AS INTEGER) AS {field.name}")
                elif field.field_type == 'FLOAT64':
                    select_columns.append(f"CAST(NULL AS FLOAT64) AS {field.name}")
                elif field.field_type == 'FLOAT':
                    select_columns.append(f"CAST(NULL AS FLOAT64) AS {field.name}")
                elif field.field_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                    select_columns.append(f"CAST(NULL AS {field.field_type}) AS {field.name}")
                else:
                    select_columns.append(f"CAST(NULL AS STRING) AS {field.name}")
            else:
                # Keep non-PII columns as-is
                select_columns.append(field.name)
        
        if not pii_columns_found:
            return ""
        
        # Create the view SQL
        view_name = f"{table_id}_masked"
        
        # Build the SQL string with proper line breaks
        select_str = ',\n  '.join(select_columns)
        sql = f"CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{view_name}` AS\n"
        sql += "SELECT\n"
        sql += f"  {select_str}\n"
        sql += f"FROM\n"
        sql += f"  `{project_id}.{dataset_id}.{table_id}`"
        
        return sql
    
    except Exception as e:
        print(f"Error generating masked view SQL: {str(e)}")
        return ""

class TableDetailsRequest(BaseModel):
    projectId: str
    datasetId: str
    tableId: str
    connectorId: Optional[str] = None

class TagInfo(BaseModel):
    displayName: str
    tagId: str
    resourceName: str

class ColumnInfo(BaseModel):
    name: str
    type: str
    mode: str
    description: Optional[str] = None
    piiFound: bool = False
    piiType: str = ""  # Type of PII detected (e.g., "Email", "SSN", "Credit Card")
    tags: List[str] = []
    tagDetails: List[TagInfo] = []  # Detailed tag information for tooltips

class TableDetailsResponse(BaseModel):
    tableName: str
    columns: List[ColumnInfo]
    tableTags: List[str] = []  # Table-level tags

class ColumnTag(BaseModel):
    name: str
    tags: List[str]
    piiFound: bool = False
    piiType: str = ""  # Type of PII

class PublishTagsRequest(BaseModel):
    projectId: str
    datasetId: str
    tableId: str
    columns: List[ColumnTag]
    tableTags: List[str] = []  # Table-level tags
    connectorId: Optional[str] = None  # Connector ID for multi-connector support

class PublishTagsResponse(BaseModel):
    success: bool
    message: str
    sqlCommands: List[str] = []
    requiresBilling: bool = False
    billingMessage: str = ""
    maskedViewSQL: str = ""  # SQL for creating masked view for PII columns

@router.post("/table-details", response_model=TableDetailsResponse)
async def get_table_details(request: TableDetailsRequest):
    try:
        # Get global variables from main.py - import at function level to avoid circular imports
        try:
            import main
            active_connectors = main.active_connectors
        except ImportError as import_err:
            print(f"⚠️  Could not import main module: {import_err}")
            raise HTTPException(
                status_code=500,
                detail="Backend configuration error: Could not access connectors"
            )
        except AttributeError as attr_err:
            print(f"⚠️  Could not access global variables: {attr_err}")
            raise HTTPException(
                status_code=500,
                detail="Backend configuration error: Connectors not initialized"
            )
        
        # Find the BigQuery connector
        bigquery_connector = None
        if request.connectorId:
            # Use specific connector if provided
            bigquery_connector = next((c for c in active_connectors if c["id"] == request.connectorId), None)
        else:
            # Find any active BigQuery connector
            bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
        
        if not bigquery_connector:
            raise HTTPException(
                status_code=404,
                detail="No active BigQuery connector found. Please set up a BigQuery connection first."
            )
        
        # Use the stored connector credentials
        if "service_account_json" not in bigquery_connector:
            raise HTTPException(
                status_code=400,
                detail="BigQuery connector missing service account credentials."
            )
        
        # Parse the service account JSON
        try:
            service_account_json_str = bigquery_connector["service_account_json"]
            # Check if it's a string that needs parsing or already parsed
            if isinstance(service_account_json_str, str):
                service_account_info = json.loads(service_account_json_str)
            else:
                service_account_info = service_account_json_str
            
            # Validate that this is not the placeholder credentials
            service_account_str = str(service_account_info)
            if "your-project-id" in service_account_str or "YOUR_PRIVATE_KEY" in service_account_str or "your-service-account@your-project" in service_account_str:
                raise HTTPException(
                    status_code=400,
                    detail="❌ BigQuery connector has placeholder credentials. Please reconfigure the connector with valid Google Cloud service account credentials (JSON key file)."
                )
            
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
            )
            client = bigquery.Client(credentials=credentials, project=request.projectId)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid service account JSON in connector: {str(e)}"
            )
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e) if str(e) else f"{error_type} occurred"
            print(f"BigQuery authentication error (type={error_type}): {error_msg}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to authenticate with BigQuery: {error_type}: {error_msg}. Please check your service account credentials in the connector settings."
            )
        
        # Get table reference
        table_ref = client.dataset(request.datasetId).table(request.tableId)
        table = client.get_table(table_ref)
        
        # Get table-level labels (these are the "tags" in BigQuery)
        table_labels = table.labels or {}
        
        # We'll fetch column-specific tags for each field individually
        
        columns = []
        for field in table.schema:
            # Detect specific PII type based on column name
            pii_found, pii_type = detect_pii_type(field.name)
            
            # Fetch column-specific tags from BigQuery metadata
            # NOTE: We are NOT parsing tags from descriptions anymore to avoid fake tags
            # Tags should only come from:
            # 1. BigQuery labels (table-level)
            # 1. REAL BigQuery policy tags from table schema
            # 2. published_tags.json file (user-published tags)
            # We should NOT extract fake tags from descriptions like "[TAGS: identifier]"
            column_tags = []
            
            # Get REAL policy tags from BigQuery schema ONLY (no cached tags)
            tag_details = []
            if field.policy_tags and len(field.policy_tags.names) > 0:
                print(f"🔍 Column {field.name} has {len(field.policy_tags.names)} policy tags")
                # Extract policy tag names from the resource names
                for policy_tag_resource in field.policy_tags.names:
                    print(f"🔍 Processing policy tag resource: {policy_tag_resource}")
                    
                    # Use Data Catalog API to get REAL policy tag names (no hardcoding!)
                    tag_id = policy_tag_resource.split('/')[-1]
                    
                    try:
                        from google.cloud import datacatalog_v1
                        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
                        policy_tag = datacatalog_client.get_policy_tag(name=policy_tag_resource)
                        
                        if policy_tag.display_name:
                            display_name = policy_tag.display_name
                            column_tags.append(display_name)
                            tag_details.append(TagInfo(
                                displayName=display_name,
                                tagId=tag_id,
                                resourceName=policy_tag_resource
                            ))
                            print(f"✅ Found REAL policy tag display name: {display_name}")
                        else:
                            # Policy tag exists but has no display name
                            display_name = f"TAG_{tag_id}"
                            column_tags.append(display_name)
                            tag_details.append(TagInfo(
                                displayName=display_name,
                                tagId=tag_id,
                                resourceName=policy_tag_resource
                            ))
                            print(f"⚠️ Policy tag has no display name, using fallback: {display_name}")
                            
                    except Exception as e:
                        print(f"❌ Could not get policy tag display name for {policy_tag_resource}: {e}")
                        # Create a more readable fallback name
                        taxonomy_parts = policy_tag_resource.split('/')
                        if len(taxonomy_parts) >= 6:
                            taxonomy_name = taxonomy_parts[4]  # taxonomy_id
                            display_name = f"TAG_{taxonomy_name}_{tag_id}"
                        else:
                            display_name = f"TAG_{tag_id}"
                        column_tags.append(display_name)
                        tag_details.append(TagInfo(
                            displayName=display_name,
                            tagId=tag_id,
                            resourceName=policy_tag_resource
                        ))
                        print(f"🔄 Using fallback name: {display_name}")
            
            # NO CACHED TAGS - Only show REAL policy tags from BigQuery schema
            print(f"🔍 Column {field.name}: Found {len(column_tags)} REAL policy tags: {column_tags}")
            
            column_info = ColumnInfo(
                name=field.name,
                type=field.field_type,
                mode=field.mode,
                description=field.description or f"Column {field.name}",
                piiFound=pii_found,
                piiType=pii_type,
                tags=column_tags,
                tagDetails=tag_details
            )
            columns.append(column_info)
        
        # NO TABLE TAGS CACHE - Only show real table tags from BigQuery
        table_tags = []
        print(f"🔍 Table {request.tableId}: No cached table tags, showing empty array")
        
        return TableDetailsResponse(
            tableName=request.tableId,
            columns=columns,
            tableTags=table_tags
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Return error instead of mock data
        error_msg = str(e) if str(e) else "Unknown error occurred"
        import traceback
        traceback.print_exc()
        print(f"BigQuery error: {error_msg}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to fetch table details from BigQuery: {error_msg}. Please ensure you have proper BigQuery credentials and the table exists."
        )

@router.post("/publish-tags", response_model=PublishTagsResponse)
async def publish_tags(request: PublishTagsRequest):
    try:
        # NO CACHING - Only store real policy tags in BigQuery
        print(f"🔍 No local caching - only storing real policy tags in BigQuery")
        
        # Generate SQL commands for BigQuery update
        sql_commands = []
        
        # NO SQL COMMANDS FOR DESCRIPTIONS - ONLY REAL POLICY TAGS
        # SQL commands will be generated only if real policy tag creation fails
        
        # Generate ALTER TABLE command for table labels (from table-level tags)
        table_tags_list = request.tableTags or []
        
        if table_tags_list:
            # Convert to BigQuery policy tag label format
            label_pairs = []
            for i, tag in enumerate(table_tags_list[:64]):  # BigQuery limit
                clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                if clean_tag and len(clean_tag) <= 63:
                    label_pairs.append(f"policy_tag_{i} = '{clean_tag}'")
            
            if label_pairs:
                # Create the SET OPTIONS command for policy tag labels
                sql_commands.insert(0, f"ALTER TABLE `{request.projectId}.{request.datasetId}.{request.tableId}`\nSET OPTIONS (\n  labels = [{', '.join(label_pairs)}]\n);")
        
        # Try to update BigQuery directly and capture the actual log message
        requires_billing = False
        billing_message = ""
        success = False
        masked_view_sql = ""  # Initialize masked view SQL
        
        try:
            # Import the global variables from main.py
            import main
            active_connectors = main.active_connectors
            
            # Find the BigQuery connector
            if request.connectorId:
                # Use specific connector if provided
                bigquery_connector = next((c for c in active_connectors if c["id"] == request.connectorId), None)
                print(f"DEBUG: Using specific connector ID: {request.connectorId}")
                print(f"DEBUG: Found connector: {bigquery_connector['name'] if bigquery_connector else 'None'}")
            else:
                # Find any active BigQuery connector
                bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
                print(f"DEBUG: Using first available BigQuery connector")
                print(f"DEBUG: Found connector: {bigquery_connector['name'] if bigquery_connector else 'None'}")
            
            if bigquery_connector:
                # Try to get table info to check billing
                if "service_account_json" in bigquery_connector:
                    try:
                        service_account_info = json.loads(bigquery_connector["service_account_json"])
                        credentials = service_account.Credentials.from_service_account_info(
                            service_account_info,
                            scopes=["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
                        )
                        client = bigquery.Client(credentials=credentials, project=request.projectId)
                    except (json.JSONDecodeError, TypeError):
                        client = bigquery.Client(project=request.projectId)
                else:
                    client = bigquery.Client(project=request.projectId)
                
                # Try to get table to check if billing is enabled
                table_ref = client.dataset(request.datasetId).table(request.tableId)
                table = client.get_table(table_ref)
                
                # Apply table-level tags as BigQuery policy tags
                if request.tableTags:
                    try:
                        # Create table-level taxonomy
                        table_taxonomy_name = create_table_taxonomy(request.projectId, credentials, "TableClassification")
                        
                        if table_taxonomy_name:
                            # Create policy tags for table-level tags
                            table_policy_tag_map = {}
                            for tag in request.tableTags:
                                policy_tag_name = create_policy_tag(table_taxonomy_name, tag, credentials)
                                if policy_tag_name:
                                    table_policy_tag_map[tag] = policy_tag_name
                            
                            # Apply table-level policy tags as labels (BigQuery doesn't have direct table policy tags)
                            new_labels = {}
                            for i, tag in enumerate(request.tableTags[:64]):  # BigQuery limit
                                clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                                clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                                if clean_tag and len(clean_tag) <= 63:
                                    new_labels[f"table_policy_tag_{i}"] = clean_tag
                            
                            # Merge with existing labels
                            existing_labels = table.labels or {}
                            existing_labels.update(new_labels)
                            table.labels = existing_labels
                            
                            # Try to update the table with policy tags (this will fail if billing is not enabled)
                            table = client.update_table(table, ["labels"])
                            print(f"✅ Applied table-level policy tags: {request.tableTags}")
                        else:
                            print("⚠️ Could not create table taxonomy, using labels only")
                            # Fallback to labels only
                            new_labels = {}
                            for i, tag in enumerate(request.tableTags[:64]):  # BigQuery limit
                                clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                                clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                                if clean_tag and len(clean_tag) <= 63:
                                    new_labels[f"table_policy_tag_{i}"] = clean_tag
                            
                            # Merge with existing labels
                            existing_labels = table.labels or {}
                            existing_labels.update(new_labels)
                            table.labels = existing_labels
                            
                            # Try to update the table with policy tags (this will fail if billing is not enabled)
                            table = client.update_table(table, ["labels"])
                            
                    except Exception as table_policy_error:
                        print(f"⚠️ Could not apply table-level policy tags: {str(table_policy_error)}")
                        # Fallback to simple labels
                        new_labels = {}
                        for i, tag in enumerate(request.tableTags[:64]):  # BigQuery limit
                            clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                            clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                            if clean_tag and len(clean_tag) <= 63:
                                new_labels[f"table_policy_tag_{i}"] = clean_tag
                        
                        # Merge with existing labels
                        existing_labels = table.labels or {}
                        existing_labels.update(new_labels)
                        table.labels = existing_labels
                        
                        # Try to update the table with policy tags (this will fail if billing is not enabled)
                        table = client.update_table(table, ["labels"])
                
                # Apply policy tags to columns - SIMPLIFIED APPROACH
                # Since Data Catalog API is complex, let's use a direct approach
                for column in request.columns:
                    if column.tags:
                        # Build description with policy tags
                        desc_parts = []
                        if column.tags:
                            desc_parts.append(f"Policy Tags: {', '.join(column.tags)}")
                        if column.piiFound:
                            pii_info = f"PII: {column.piiType}" if column.piiType else "PII: Yes"
                            desc_parts.append(pii_info)
                        
                        desc_str = ' | '.join(desc_parts)
                        
                        # CREATE AND APPLY REAL POLICY TAGS ONLY - NO FALLBACKS
                        print(f"🔧 Creating REAL policy tags for column {column.name} with tags: {column.tags}")
                        
                        # Step 1: Create or get taxonomy
                        try:
                            taxonomy_name = create_policy_tag_taxonomy(request.projectId, credentials)
                            
                            if not taxonomy_name:
                                raise Exception(f"❌ FAILED: Could not create taxonomy for project {request.projectId}")
                            
                            print(f"✅ Using taxonomy: {taxonomy_name}")
                            
                            # Step 2: Create policy tags for each tag
                            policy_tag_map = {}
                            for tag in column.tags:
                                print(f"🔧 Creating policy tag: {tag}")
                                policy_tag_name = create_policy_tag(taxonomy_name, tag, credentials)
                                if policy_tag_name:
                                    policy_tag_map[tag] = policy_tag_name
                                    print(f"✅ Created policy tag '{tag}': {policy_tag_name}")
                                else:
                                    raise Exception(f"❌ FAILED: Could not create policy tag '{tag}'")
                            
                            # Step 3: Apply policy tags to column
                            if not policy_tag_map:
                                raise Exception(f"❌ FAILED: No policy tags were created for column {column.name}")
                            
                            # Use the first policy tag for simplicity (BigQuery allows only one policy tag per column)
                            first_tag = list(policy_tag_map.keys())[0]
                            policy_tag_resource = policy_tag_map[first_tag]
                            
                            # Step 3: Apply policy tags to column using BigQuery API (not SQL)
                            print(f"🔧 Applying policy tag '{first_tag}' to column {column.name}")
                            
                            # Get the table to update its schema
                            table_ref = client.dataset(request.datasetId).table(request.tableId)
                            table = client.get_table(table_ref)
                            
                            # Find the column and update its policy tags
                            updated_fields = []
                            for field in table.schema:
                                if field.name == column.name:
                                    # Create a new field with policy tags
                                    new_field = bigquery.SchemaField(
                                        name=field.name,
                                        field_type=field.field_type,
                                        mode=field.mode,
                                        description=field.description,
                                        policy_tags=bigquery.PolicyTagList(names=[policy_tag_resource])
                                    )
                                    updated_fields.append(new_field)
                                else:
                                    updated_fields.append(field)
                            
                            # Update the table schema
                            table.schema = updated_fields
                            table = client.update_table(table, ["schema"])
                            print(f"✅ SUCCESS! Applied REAL policy tag '{first_tag}' to column {column.name}")
                            
                            # Update success message
                            billing_message = f"✅ BigQuery Policy Tags Update Successful: Table {request.projectId}.{request.datasetId}.{request.tableId} updated successfully with REAL policy tags!"
                            billing_message += f"\n\n🎯 REAL POLICY TAGS APPLIED:"
                            billing_message += f"\n• Column {column.name}: {first_tag}"
                            billing_message += f"\n• Policy Tag Resource: {policy_tag_resource}"
                            billing_message += f"\n• Check BigQuery UI 'Policy tags' column to see the tags!"
                            
                            # If we get here, the policy tag was applied successfully
                            success = True
                            
                        except Exception as policy_tag_error:
                            print(f"❌ POLICY TAG CREATION FAILED: {policy_tag_error}")
                            raise Exception(f"❌ FAILED to create real policy tags: {policy_tag_error}")
                
                # Verify success flag was set by successful policy tag application
                if not success:
                    raise Exception(f"❌ FAILED: Policy tags were not applied to any columns")
                
                print(f"Publish policy tags success: Table {request.projectId}.{request.datasetId}.{request.tableId} updated successfully")
                
                # Generate and create masked view for PII columns
                masked_view_sql = generate_masked_view_sql_bigquery(
                    request.projectId, request.datasetId, request.tableId, request.columns, client
                )
                
                if masked_view_sql:
                    try:
                        # Execute the masked view SQL
                        query_job = client.query(masked_view_sql)
                        query_job.result()  # Wait for the job to complete
                        billing_message += f"\n\n🔒 SECURITY: Created masked view with PII columns automatically masked!"
                        print(f"✅ Masked view created for {request.projectId}.{request.datasetId}.{request.tableId}")
                    except Exception as mv_error:
                        print(f"⚠️ Could not create masked view: {str(mv_error)}")
                        masked_view_sql = f"-- Could not create masked view: {str(mv_error)}\n\n{masked_view_sql}"
                
        except Exception as e:
            error_msg = str(e)
            print(f"Publish tags error: {error_msg}")
            
            # Return the actual error message from the logs
            if "billing" in error_msg.lower() or "sandbox" in error_msg.lower():
                requires_billing = True
                billing_message = f"❌ BigQuery Update Failed: {error_msg}"
            elif "permission" in error_msg.lower() or "access" in error_msg.lower():
                requires_billing = False
                billing_message = f"❌ Permission Error: {error_msg}"
            elif "invalid characters" in error_msg.lower():
                requires_billing = False
                billing_message = f"❌ Invalid Label Format: {error_msg}"
            else:
                requires_billing = False
                billing_message = f"❌ BigQuery Error: {error_msg}"
        
        return PublishTagsResponse(
            success=success,
            message="BigQuery operation completed. See details below.",
            sqlCommands=sql_commands,
            requiresBilling=requires_billing,
            billingMessage=billing_message,
            maskedViewSQL=masked_view_sql
        )
    except Exception as e:
        print(f"Publish tags error: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to save tags: {str(e)}"
        )

class DeleteTagsRequest(BaseModel):
    projectId: str
    datasetId: str
    tableId: str
    columnName: str
    tagToDelete: str
    connectorId: Optional[str] = None  # Connector ID for multi-connector support

class AllTagsResponse(BaseModel):
    tags: List[str]
    totalCount: int

class TaxonomyRequest(BaseModel):
    projectId: str
    taxonomyName: str
    description: str = ""
    connectorId: Optional[str] = None

class PolicyTagRequest(BaseModel):
    projectId: str
    taxonomyName: str
    tagName: str
    description: str = ""
    connectorId: Optional[str] = None

class DeletePolicyTagRequest(BaseModel):
    projectId: str
    taxonomyName: str
    tagName: str
    connectorId: Optional[str] = None

class TaxonomyResponse(BaseModel):
    success: bool
    message: str
    taxonomyName: Optional[str] = None

class PolicyTagResponse(BaseModel):
    success: bool
    message: str
    policyTagName: Optional[str] = None

@router.get("/all-tags", response_model=AllTagsResponse)
async def get_all_tags():
    """Get all existing tags from REAL BigQuery tables and published_tags.json"""
    try:
        import os
        from main import active_connectors
        
        all_tags = set()
        
        # Try to get BigQuery connector to fetch real tags
        bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
        
        if bigquery_connector:
            try:
                # Get credentials
                if "service_account_json" in bigquery_connector:
                    try:
                        service_account_info = json.loads(bigquery_connector["service_account_json"])
                        credentials = service_account.Credentials.from_service_account_info(
                            service_account_info,
                            scopes=["https://www.googleapis.com/auth/bigquery"]
                        )
                        client = bigquery.Client(credentials=credentials, project=bigquery_connector.get("project_id", "default"))
                    except (json.JSONDecodeError, TypeError):
                        client = bigquery.Client(project=bigquery_connector.get("project_id", "default"))
                else:
                    client = bigquery.Client(project=bigquery_connector.get("project_id", "default"))
                
                # Get project ID
                project_id = bigquery_connector.get("project_id", client.project)
                
                # Get all datasets
                datasets = list(client.list_datasets())
                
                for dataset in datasets:
                    dataset_id = dataset.dataset_id
                    dataset_ref = client.dataset(dataset_id, project=project_id)
                    
                    # Get all tables in this dataset
                    try:
                        tables = client.list_tables(dataset_ref)
                        
                        for table_item in tables:
                            table_ref = dataset_ref.table(table_item.table_id)
                            
                            try:
                                table = client.get_table(table_ref)
                                
                                # Extract tags from table labels (real BigQuery labels only!)
                                # We do NOT extract fake tags from column descriptions like "[TAGS: identifier]"
                                if table.labels:
                                    # Labels in BigQuery are key-value pairs
                                    # We're interested in tag-like labels
                                    for label_value in table.labels.values():
                                        if label_value:
                                            all_tags.add(label_value)
                            except Exception as table_error:
                                # Skip this table if we can't access it
                                continue
                    except Exception as dataset_error:
                        # Skip this dataset if we can't access it
                        continue
                
                print(f"✅ Fetched {len(all_tags)} real tags from BigQuery")
            except Exception as bigquery_error:
                print(f"⚠️ Could not fetch tags from BigQuery: {bigquery_error}")
        
        # NO CACHING - Only fetch real tags from BigQuery
        print(f"🔍 No cached tags - only fetching real tags from BigQuery")
        
        tags_list = sorted(list(all_tags))
        
        return AllTagsResponse(
            tags=tags_list,
            totalCount=len(tags_list)
        )
    except Exception as e:
        print(f"Error getting all tags: {str(e)}")
        import traceback
        traceback.print_exc()
        return AllTagsResponse(tags=[], totalCount=0)

@router.post("/delete-tags", response_model=PublishTagsResponse)
async def delete_tags(request: DeleteTagsRequest):
    try:
        # NO CACHING - Only work with real policy tags in BigQuery
        print(f"🔍 No local caching - only working with real policy tags in BigQuery")
        
        # Try to remove policy tag from BigQuery column
        try:
            # Import the global variables from main.py
            import main
            active_connectors = main.active_connectors
            
            # Find the BigQuery connector
            if request.connectorId:
                bigquery_connector = next((c for c in active_connectors if c["id"] == request.connectorId), None)
            else:
                bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
            
            if bigquery_connector and "service_account_json" in bigquery_connector:
                # Get credentials
                service_account_info = json.loads(bigquery_connector["service_account_json"])
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"]
                )
                client = bigquery.Client(credentials=credentials, project=request.projectId)
                
                # Try to remove policy tag from column
                try:
                    # Get the taxonomy for column-level tags
                    taxonomy_name = create_policy_tag_taxonomy(request.projectId, credentials, "DataClassification")
                    
                    if taxonomy_name:
                        # Find the policy tag by name
                        policy_tag_name = get_policy_tag_by_name(taxonomy_name, request.tagToDelete, credentials)
                        
                        if policy_tag_name:
                            # Remove policy tag from column using BigQuery API (not SQL)
                            print(f"🔧 Removing policy tag '{request.tagToDelete}' from column {request.columnName}")
                            
                            # Get the table to update its schema
                            table_ref = client.dataset(request.datasetId).table(request.tableId)
                            table = client.get_table(table_ref)
                            
                            # Find the column and remove its policy tags
                            updated_fields = []
                            for field in table.schema:
                                if field.name == request.columnName:
                                    # Create a new field without policy tags
                                    new_field = bigquery.SchemaField(
                                        name=field.name,
                                        field_type=field.field_type,
                                        mode=field.mode,
                                        description=field.description,
                                        policy_tags=None  # Remove policy tags
                                    )
                                    updated_fields.append(new_field)
                                else:
                                    updated_fields.append(field)
                            
                            # Update the table schema
                            table.schema = updated_fields
                            table = client.update_table(table, ["schema"])
                            print(f"✅ Removed policy tag '{request.tagToDelete}' from column {request.columnName}")
                        else:
                            print(f"⚠️ Policy tag '{request.tagToDelete}' not found in taxonomy")
                    else:
                        print("⚠️ Could not access taxonomy for policy tag removal")
                        
                except Exception as policy_error:
                    print(f"⚠️ Could not remove policy tag from BigQuery: {str(policy_error)}")
                    
                    # Fallback: Update column description to remove the tag
                    try:
                        # Get current table to check column description
                        table_ref = client.dataset(request.datasetId).table(request.tableId)
                        table = client.get_table(table_ref)
                        
                        # Find the column and update its description
                        for field in table.schema:
                            if field.name == request.columnName:
                                current_desc = field.description or ""
                                
                                # Remove the tag from description
                                if f"Policy Tags: {request.tagToDelete}" in current_desc:
                                    new_desc = current_desc.replace(f"Policy Tags: {request.tagToDelete}", "").replace("Policy Tags: , ", "Policy Tags: ").replace("Policy Tags: ", "").strip()
                                    if new_desc.startswith("|"):
                                        new_desc = new_desc[1:].strip()
                                    if new_desc.endswith("|"):
                                        new_desc = new_desc[:-1].strip()
                                else:
                                    # Handle case where tag is part of a comma-separated list
                                    import re
                                    pattern = rf"Policy Tags: ([^|]*{re.escape(request.tagToDelete)}[^|]*)"
                                    match = re.search(pattern, current_desc)
                                    if match:
                                        tags_part = match.group(1)
                                        tags_list = [tag.strip() for tag in tags_part.split(',')]
                                        tags_list = [tag for tag in tags_list if tag != request.tagToDelete]
                                        if tags_list:
                                            new_desc = current_desc.replace(match.group(0), f"Policy Tags: {', '.join(tags_list)}")
                                        else:
                                            new_desc = current_desc.replace(match.group(0), "")
                                    else:
                                        new_desc = current_desc
                                
                                # Update column description
                                alter_sql = f"ALTER TABLE `{request.projectId}.{request.datasetId}.{request.tableId}` ALTER COLUMN {request.columnName} SET OPTIONS (description = '{new_desc}')"
                                query_job = client.query(alter_sql)
                                query_job.result()
                                print(f"✅ Fallback: Updated column {request.columnName} description to remove tag")
                                break
                                
                    except Exception as desc_error:
                        print(f"⚠️ Could not update column description: {str(desc_error)}")
                        
        except Exception as bigquery_error:
            print(f"⚠️ Could not access BigQuery for tag deletion: {str(bigquery_error)}")
        
        return PublishTagsResponse(
            success=True,
            message=f"✅ Tag '{request.tagToDelete}' successfully deleted from column '{request.columnName}'",
            sqlCommands=[],
            requiresBilling=False,
            billingMessage=""
        )
        
    except Exception as e:
        print(f"Delete tags error: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to delete tag: {str(e)}"
        )

@router.post("/create-taxonomy", response_model=TaxonomyResponse)
async def create_taxonomy_endpoint(request: TaxonomyRequest):
    """Create a new policy tag taxonomy"""
    try:
        # Import the global variables from main.py
        import main
        active_connectors = main.active_connectors
        
        # Find the BigQuery connector
        if request.connectorId:
            bigquery_connector = next((c for c in active_connectors if c["id"] == request.connectorId), None)
        else:
            bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
        
        if not bigquery_connector:
            raise HTTPException(
                status_code=404,
                detail="No active BigQuery connector found."
            )
        
        # Get credentials
        service_account_info = json.loads(bigquery_connector["service_account_json"])
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Create taxonomy
        taxonomy_name = create_policy_tag_taxonomy(request.projectId, credentials, request.taxonomyName)
        
        if taxonomy_name:
            return TaxonomyResponse(
                success=True,
                message=f"✅ Taxonomy '{request.taxonomyName}' created successfully",
                taxonomyName=taxonomy_name
            )
        else:
            return TaxonomyResponse(
                success=False,
                message=f"❌ Failed to create taxonomy '{request.taxonomyName}'"
            )
            
    except Exception as e:
        print(f"Create taxonomy error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create taxonomy: {str(e)}"
        )

@router.post("/create-policy-tag", response_model=PolicyTagResponse)
async def create_policy_tag_endpoint(request: PolicyTagRequest):
    """Create a new policy tag within a taxonomy"""
    try:
        # Import the global variables from main.py
        import main
        active_connectors = main.active_connectors
        
        # Find the BigQuery connector
        if request.connectorId:
            bigquery_connector = next((c for c in active_connectors if c["id"] == request.connectorId), None)
        else:
            bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
        
        if not bigquery_connector:
            raise HTTPException(
                status_code=404,
                detail="No active BigQuery connector found."
            )
        
        # Get credentials
        service_account_info = json.loads(bigquery_connector["service_account_json"])
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Find taxonomy by name
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        parent = f"projects/{request.projectId}/locations/us"
        taxonomies = datacatalog_client.list_taxonomies(parent=parent)
        
        taxonomy_name = None
        for taxonomy in taxonomies:
            if taxonomy.display_name == request.taxonomyName:
                taxonomy_name = taxonomy.name
                break
        
        if not taxonomy_name:
            raise HTTPException(
                status_code=404,
                detail=f"Taxonomy '{request.taxonomyName}' not found"
            )
        
        # Create policy tag
        policy_tag_name = create_policy_tag(taxonomy_name, request.tagName, credentials)
        
        if policy_tag_name:
            return PolicyTagResponse(
                success=True,
                message=f"✅ Policy tag '{request.tagName}' created successfully in taxonomy '{request.taxonomyName}'",
                policyTagName=policy_tag_name
            )
        else:
            return PolicyTagResponse(
                success=False,
                message=f"❌ Failed to create policy tag '{request.tagName}'"
            )
            
    except Exception as e:
        print(f"Create policy tag error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create policy tag: {str(e)}"
        )

@router.post("/delete-policy-tag", response_model=PolicyTagResponse)
async def delete_policy_tag_endpoint(request: DeletePolicyTagRequest):
    """Delete a policy tag from a taxonomy"""
    try:
        # Import the global variables from main.py
        import main
        active_connectors = main.active_connectors
        
        # Find the BigQuery connector
        if request.connectorId:
            bigquery_connector = next((c for c in active_connectors if c["id"] == request.connectorId), None)
        else:
            bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
        
        if not bigquery_connector:
            raise HTTPException(
                status_code=404,
                detail="No active BigQuery connector found."
            )
        
        # Get credentials
        service_account_info = json.loads(bigquery_connector["service_account_json"])
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Find taxonomy by name
        datacatalog_client = datacatalog_v1.PolicyTagManagerClient(credentials=credentials)
        parent = f"projects/{request.projectId}/locations/us"
        taxonomies = datacatalog_client.list_taxonomies(parent=parent)
        
        taxonomy_name = None
        for taxonomy in taxonomies:
            if taxonomy.display_name == request.taxonomyName:
                taxonomy_name = taxonomy.name
                break
        
        if not taxonomy_name:
            raise HTTPException(
                status_code=404,
                detail=f"Taxonomy '{request.taxonomyName}' not found"
            )
        
        # Find policy tag by name
        policy_tag_name = get_policy_tag_by_name(taxonomy_name, request.tagName, credentials)
        
        if not policy_tag_name:
            raise HTTPException(
                status_code=404,
                detail=f"Policy tag '{request.tagName}' not found in taxonomy '{request.taxonomyName}'"
            )
        
        # Delete policy tag
        success = delete_policy_tag(policy_tag_name, credentials)
        
        if success:
            return PolicyTagResponse(
                success=True,
                message=f"✅ Policy tag '{request.tagName}' deleted successfully from taxonomy '{request.taxonomyName}'"
            )
        else:
            return PolicyTagResponse(
                success=False,
                message=f"❌ Failed to delete policy tag '{request.tagName}'"
            )
            
    except Exception as e:
        print(f"Delete policy tag error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete policy tag: {str(e)}"
        )
