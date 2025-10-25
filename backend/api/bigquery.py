from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
import time
from typing import List, Optional, Dict

router = APIRouter()

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
    tags: List[str] = []

class TableDetailsResponse(BaseModel):
    tableName: str
    columns: List[ColumnInfo]

class ColumnTag(BaseModel):
    name: str
    tags: List[str]

class PublishTagsRequest(BaseModel):
    projectId: str
    datasetId: str
    tableId: str
    columns: List[ColumnTag]

class PublishTagsResponse(BaseModel):
    success: bool
    message: str
    sqlCommands: List[str] = []
    requiresBilling: bool = False
    billingMessage: str = ""

@router.post("/table-details", response_model=TableDetailsResponse)
async def get_table_details(request: TableDetailsRequest):
    try:
        # Import the global variables from main.py
        import main
        active_connectors = main.active_connectors
        
        # Find the BigQuery connector
        bigquery_connector = None
        if request.connectorId:
            # Use specific connector if provided
            bigquery_connector = next((c for c in active_connectors if c["id"] == request.connectorId), None)
        else:
            # Find any active BigQuery connector
            bigquery_connector = next((c for c in active_connectors if c["type"] == "BigQuery" and c["enabled"]), None)
        
        if not bigquery_connector:
            # If no connector found, try to use Application Default Credentials
            # This will work if you have gcloud auth set up or GOOGLE_APPLICATION_CREDENTIALS
            client = bigquery.Client(project=request.projectId)
        else:
            # Use the stored connector credentials
            if "service_account_json" in bigquery_connector:
                # Parse the service account JSON
                try:
                    service_account_info = json.loads(bigquery_connector["service_account_json"])
                    credentials = service_account.Credentials.from_service_account_info(
                        service_account_info,
                        scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
                    )
                    client = bigquery.Client(credentials=credentials, project=request.projectId)
                except (json.JSONDecodeError, TypeError):
                    # Fallback to ADC if stored credentials are invalid
                    client = bigquery.Client(project=request.projectId)
            else:
                # Fallback to ADC if no stored credentials
                client = bigquery.Client(project=request.projectId)
        
        # Get table reference
        table_ref = client.dataset(request.datasetId).table(request.tableId)
        table = client.get_table(table_ref)
        
        # We'll fetch column-specific tags for each field individually
        
        columns = []
        for field in table.schema:
            # Determine if field contains PII based on name patterns
            pii_keywords = ['email', 'phone', 'ssn', 'id', 'name', 'address', 'credit', 'card', 'user_id', 'customer_id', 'personal']
            pii_found = any(keyword in field.name.lower() for keyword in pii_keywords)
            
            # Fetch column-specific tags from BigQuery
            column_tags = []
            try:
                # Try to get column-level tags from INFORMATION_SCHEMA.COLUMNS
                query = f"""
                SELECT column_name, description
                FROM `{request.projectId}.{request.datasetId}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{request.tableId}' AND column_name = '{field.name}'
                """
                query_job = client.query(query)
                results = query_job.result()
                for row in results:
                    if row.description:
                        # If column has description, use it as a tag
                        column_tags.append(f"description:{row.description}")
            except Exception as e:
                print(f"Warning: Could not fetch column tags for {field.name}: {e}")
            
            # If no column-specific tags found, show empty array (nil)
            if not column_tags:
                column_tags = []
            
            column_info = ColumnInfo(
                name=field.name,
                type=field.field_type,
                mode=field.mode,
                description=field.description or f"Column {field.name}",
                piiFound=pii_found,
                tags=column_tags
            )
            columns.append(column_info)
        
        return TableDetailsResponse(
            tableName=request.tableId,
            columns=columns
        )
        
    except Exception as e:
        # Return error instead of mock data
        print(f"BigQuery error: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to fetch table details from BigQuery: {str(e)}. Please ensure you have proper BigQuery credentials and the table exists."
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
            "columns": [{"name": col.name, "tags": col.tags} for col in request.columns],
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Save updated tags
        with open(tags_file, 'w') as f:
            json.dump(existing_tags, f, indent=2)
        
        # Generate SQL commands for BigQuery update
        sql_commands = []
        
        # Generate ALTER TABLE commands for column descriptions
        for column in request.columns:
            if column.tags:
                tags_str = ', '.join(column.tags)
                sql_commands.append(f"ALTER TABLE `{request.projectId}.{request.datasetId}.{request.tableId}`\nALTER COLUMN {column.name} SET OPTIONS (description = 'Tags: {tags_str}');")
        
        # Generate ALTER TABLE command for table labels
        all_tags = []
        for column in request.columns:
            all_tags.extend(column.tags)
        
        if all_tags:
            # Create unique tags list
            unique_tags = list(set(all_tags))
            # Convert to BigQuery label format
            label_pairs = []
            for i, tag in enumerate(unique_tags[:64]):  # BigQuery limit
                clean_tag = tag.lower().replace(' ', '_').replace(':', '_').replace('-', '_')
                clean_tag = ''.join(c for c in clean_tag if c.isalnum() or c in '_-')
                if clean_tag and len(clean_tag) <= 63:
                    label_pairs.append(f"tag_{i} = '{clean_tag}'")
            
            if label_pairs:
                sql_commands.append(f"ALTER TABLE `{request.projectId}.{request.datasetId}.{request.tableId}`\nSET OPTIONS (\n  labels = [{', '.join(label_pairs)}]\n);")
        
        # Try to update BigQuery directly and capture the actual log message
        requires_billing = False
        billing_message = ""
        success = False
        
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
                
                # Try to update the table with labels (this will fail if billing is not enabled)
                table.labels = {"test": "value"}
                table = client.update_table(table, ["labels"])
                
                # If we get here, it was successful
                success = True
                billing_message = f"✅ BigQuery Update Successful: Table {request.projectId}.{request.datasetId}.{request.tableId} updated successfully with tags."
                print(f"Publish tags success: Table {request.projectId}.{request.datasetId}.{request.tableId} updated successfully")
                
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
            billingMessage=billing_message
        )
        
    except Exception as e:
        print(f"Publish tags error: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to save tags: {str(e)}"
        )
