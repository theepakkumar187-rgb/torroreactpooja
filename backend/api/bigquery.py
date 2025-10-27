from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
import time
import re
from typing import List, Optional, Dict

router = APIRouter()

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
                elif field.field_type == 'FLOAT':
                    select_columns.append(f"CAST(NULL AS FLOAT) AS {field.name}")
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

class ColumnInfo(BaseModel):
    name: str
    type: str
    mode: str
    description: Optional[str] = None
    piiFound: bool = False
    piiType: str = ""  # Type of PII detected (e.g., "Email", "SSN", "Credit Card")
    tags: List[str] = []

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
            # 2. published_tags.json file (user-published tags)
            # We should NOT extract fake tags from descriptions like "[TAGS: identifier]"
            column_tags = []
            
            # Try to extract tags and PII info from published_tags.json if available
            try:
                import os
                tags_file = "published_tags.json"
                if os.path.exists(tags_file):
                    with open(tags_file, 'r') as f:
                        published_tags = json.load(f)
                    
                    table_key = f"{request.projectId}.{request.datasetId}.{request.tableId}"
                    if table_key in published_tags:
                        # Get tags and PII info for this specific column
                        for col_data in published_tags[table_key].get("columns", []):
                            if col_data.get("name") == field.name:
                                # Append any published tags that aren't already in column_tags
                                for tag in col_data.get("tags", []):
                                    if tag not in column_tags:
                                        column_tags.append(tag)
                                # Override PII info if published
                                if "piiFound" in col_data:
                                    pii_found = col_data.get("piiFound", False)
                                    pii_type = col_data.get("piiType", "")
                                break
            except Exception as e:
                print(f"Warning: Could not load published tags: {e}")
            
            column_info = ColumnInfo(
                name=field.name,
                type=field.field_type,
                mode=field.mode,
                description=field.description or f"Column {field.name}",
                piiFound=pii_found,
                piiType=pii_type,
                tags=column_tags
            )
            columns.append(column_info)
        
        # Fetch table-level tags
        table_tags = []
        try:
            # Load published table tags if available
            import os
            tags_file = "published_tags.json"
            if os.path.exists(tags_file):
                with open(tags_file, 'r') as f:
                    published_tags = json.load(f)
                
                table_key = f"{request.projectId}.{request.datasetId}.{request.tableId}"
                if table_key in published_tags:
                    table_tags = published_tags[table_key].get("tableTags", [])
        except Exception as e:
            print(f"Warning: Could not load table tags: {e}")
        
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
        # Store tags in a local file for reference
        import os
        tags_file = "published_tags.json"
        
        # Load existing tags
        existing_tags = {}
        if os.path.exists(tags_file):
            with open(tags_file, 'r') as f:
                existing_tags = json.load(f)
        
        # Update with new tags
        table_key = f"{request.projectId}.{request.datasetId}.{request.tableId}"
        existing_tags[table_key] = {
            "projectId": request.projectId,
            "datasetId": request.datasetId,
            "tableId": request.tableId,
            "columns": [{"name": col.name, "tags": col.tags, "piiFound": col.piiFound, "piiType": col.piiType} for col in request.columns],
            "tableTags": request.tableTags or [],  # Store table-level tags
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Save updated tags
        with open(tags_file, 'w') as f:
            json.dump(existing_tags, f, indent=2)
        
        # Generate SQL commands for BigQuery update
        sql_commands = []
        
        # Generate ALTER TABLE commands for column descriptions
        for column in request.columns:
            if column.tags or column.piiFound:
                # Build description with tags and PII info
                desc_parts = []
                if column.tags:
                    desc_parts.append(f"Tags: {', '.join(column.tags)}")
                if column.piiFound:
                    pii_info = f"PII: {column.piiType}" if column.piiType else "PII: Yes"
                    desc_parts.append(pii_info)
                
                desc_str = ' | '.join(desc_parts)
                sql_commands.append(f"ALTER TABLE `{request.projectId}.{request.datasetId}.{request.tableId}`\nALTER COLUMN {column.name} SET OPTIONS (description = '{desc_str}');")
        
        # Generate ALTER TABLE command for table labels (from table-level tags)
        table_tags_list = request.tableTags or []
        
        if table_tags_list:
            # Convert to BigQuery label format
            label_pairs = []
            for i, tag in enumerate(table_tags_list[:64]):  # BigQuery limit
                clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                if clean_tag and len(clean_tag) <= 63:
                    label_pairs.append(f"tag_{i} = '{clean_tag}'")
            
            if label_pairs:
                # Create the SET OPTIONS command for labels
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
            bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
            
            if bigquery_connector:
                # Try to get table info to check billing
                if "service_account_json" in bigquery_connector:
                    try:
                        service_account_info = json.loads(bigquery_connector["service_account_json"])
                        credentials = service_account.Credentials.from_service_account_info(
                            service_account_info,
                            scopes=["https://www.googleapis.com/auth/bigquery"]
                        )
                        client = bigquery.Client(credentials=credentials, project=request.projectId)
                    except (json.JSONDecodeError, TypeError):
                        client = bigquery.Client(project=request.projectId)
                else:
                    client = bigquery.Client(project=request.projectId)
                
                # Try to get table to check if billing is enabled
                table_ref = client.dataset(request.datasetId).table(request.tableId)
                table = client.get_table(table_ref)
                
                # Apply table-level tags as BigQuery labels
                if request.tableTags:
                    # Create labels from table tags
                    new_labels = {}
                    for i, tag in enumerate(request.tableTags[:64]):  # BigQuery limit
                        clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                        clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                        if clean_tag and len(clean_tag) <= 63:
                            new_labels[f"tag_{i}"] = clean_tag
                    
                    # Merge with existing labels
                    existing_labels = table.labels or {}
                    existing_labels.update(new_labels)
                    table.labels = existing_labels
                    
                    # Try to update the table with labels (this will fail if billing is not enabled)
                    table = client.update_table(table, ["labels"])
                
                # If we get here, it was successful
                success = True
                billing_message = f"✅ BigQuery Update Successful: Table {request.projectId}.{request.datasetId}.{request.tableId} updated successfully with tags."
                print(f"Publish tags success: Table {request.projectId}.{request.datasetId}.{request.tableId} updated successfully")
                
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

class AllTagsResponse(BaseModel):
    tags: List[str]
    totalCount: int

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
        
        # Also fetch tags from published_tags.json (user-published tags)
        try:
            import os
            tags_file = "published_tags.json"
            if os.path.exists(tags_file):
                with open(tags_file, 'r') as f:
                    published_tags = json.load(f)
                
                # Extract all tags from all tables
                for table_key, table_data in published_tags.items():
                    # Add table-level tags
                    for tag in table_data.get("tableTags", []):
                        all_tags.add(tag)
                    # Add column-level tags
                    for col in table_data.get("columns", []):
                        for tag in col.get("tags", []):
                            all_tags.add(tag)
                
                print(f"✅ Fetched additional {len([t for t in all_tags])} tags from published_tags.json")
        except Exception as published_tags_error:
            print(f"⚠️ Could not load published tags: {published_tags_error}")
        
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
        # Load existing tags
        import os
        tags_file = "published_tags.json"
        existing_tags = {}
        if os.path.exists(tags_file):
            with open(tags_file, 'r') as f:
                existing_tags = json.load(f)
        
        # Update tags by removing the specified tag
        table_key = f"{request.projectId}.{request.datasetId}.{request.tableId}"
        
        if table_key in existing_tags:
            # Update the specific column's tags
            for col_data in existing_tags[table_key].get("columns", []):
                if col_data.get("name") == request.columnName:
                    # Remove the tag if it exists
                    if "tags" in col_data:
                        tags_list = col_data["tags"]
                        if request.tagToDelete in tags_list:
                            tags_list.remove(request.tagToDelete)
                    break
            
            # Save updated tags
            with open(tags_file, 'w') as f:
                json.dump(existing_tags, f, indent=2)
        else:
            # If table doesn't exist in published tags, return success (nothing to delete)
            pass
        
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
