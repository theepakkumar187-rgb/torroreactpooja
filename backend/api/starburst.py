from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import requests
import base64
import json

router = APIRouter()

class TableDetailsRequest(BaseModel):
    catalog: str
    schema: str
    tableId: str

class ColumnInfo(BaseModel):
    name: str
    type: str
    mode: str
    description: Optional[str] = ""
    tags: List[str] = []

class TableDetailsResponse(BaseModel):
    tableName: str
    columns: List[ColumnInfo]
    totalRows: int = 0
    totalColumns: int

class ColumnTag(BaseModel):
    columnName: str
    tags: List[str]

class PublishTagsRequest(BaseModel):
    catalog: str
    schema: str
    tableId: str
    columnTags: List[ColumnTag]

class PublishTagsResponse(BaseModel):
    success: bool
    message: str
    billingMessage: str = ""

def get_starburst_access_token(account_domain: str, client_id: str, client_secret: str):
    """
    Get OAuth2 access token from Starburst Galaxy
    """
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
    
    token_response = requests.post(token_url, headers=token_headers, data=token_data)
    if token_response.status_code != 200:
        raise Exception(f"Failed to get access token: {token_response.status_code} - {token_response.text}")
    
    access_token = token_response.json().get('access_token')
    if not access_token:
        raise Exception("No access token received")
    
    return access_token

def get_starburst_connector():
    """Get active Starburst connector from main"""
    from main import active_connectors
    
    for connector in active_connectors:
        if connector.get('type') == 'Starburst Galaxy' and connector.get('enabled'):
            return connector
    return None

def get_or_create_tag(account_domain: str, access_token: str, tag_name: str, tag_color: str = "#1976d2"):
    """
    Get existing tag by name or create a new one
    """
    base_url = f"https://{account_domain}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Try to get tag by name using name=value format
    encoded_tag_name = requests.utils.quote(f"name={tag_name}")
    get_url = f"{base_url}/public/api/v1/tag/{encoded_tag_name}"
    
    print(f"DEBUG: Fetching tag: {get_url}")
    get_response = requests.get(get_url, headers=headers)
    
    if get_response.status_code == 200:
        tag_data = get_response.json()
        print(f"DEBUG: Found existing tag: {tag_data}")
        return tag_data.get('tagId')
    
    # Tag doesn't exist, create it
    print(f"DEBUG: Creating new tag: {tag_name}")
    create_url = f"{base_url}/public/api/v1/tag"
    create_payload = {
        "name": tag_name,
        "color": tag_color,
        "description": f"Tag: {tag_name}"
    }
    
    create_response = requests.post(create_url, headers=headers, json=create_payload)
    if create_response.status_code not in [200, 201]:
        raise Exception(f"Failed to create tag: {create_response.status_code} - {create_response.text}")
    
    tag_data = create_response.json()
    print(f"DEBUG: Created tag: {tag_data}")
    return tag_data.get('tagId')

def get_asset_tags(account_domain: str, access_token: str, catalog: str, schema: str = None, table: str = None, column: str = None):
    """
    Fetch existing tags for catalog, schema, table, or column
    Returns list of tag names
    """
    # TODO: Implement fetching existing tags
    # This would require additional Starburst API endpoints to list tags for assets
    # For now, return empty list
    return []

@router.post("/table-details", response_model=TableDetailsResponse)
async def get_table_details(request: TableDetailsRequest):
    """
    Fetch Starburst Galaxy table details including columns and their metadata with existing tags
    """
    try:
        starburst_connector = get_starburst_connector()
        
        if not starburst_connector:
            raise HTTPException(
                status_code=404,
                detail="No active Starburst Galaxy connector found. Please set up a Starburst connection first."
            )
        
        # Get connection details
        account_domain = starburst_connector.get('account_domain')
        
        # Build the table ID
        table_id = f"{account_domain}.{request.catalog}.{request.schema}.{request.tableId}"
        
        print(f"DEBUG: Fetching Starburst table details for: {table_id}")
        
        # Find the table in discovered assets
        from main import discovered_assets
        
        table_asset = None
        for asset in discovered_assets:
            if asset.get('id') == table_id:
                table_asset = asset
                break
        
        if not table_asset:
            raise HTTPException(
                status_code=404,
                detail=f"Table {request.catalog}.{request.schema}.{request.tableId} not found in discovered assets. Please ensure the table is discovered first."
            )
        
        # Get columns from the asset
        columns = table_asset.get('columns', [])
        
        if not columns:
            raise HTTPException(
                status_code=404,
                detail=f"No column information found for table {request.catalog}.{request.schema}.{request.tableId}"
            )
        
        # Format columns for response with existing tags (descriptions as tags)
        formatted_columns = []
        for col in columns:
            # Use column description as tags if present
            tags = []
            if col.get('description'):
                tags = [col.get('description')]
            
            formatted_columns.append(ColumnInfo(
                name=col.get('name', 'unknown'),
                type=col.get('type', 'STRING'),
                mode=col.get('mode', 'NULLABLE'),
                description=col.get('description', ''),
                tags=tags
            ))
        
        return TableDetailsResponse(
            tableName=request.tableId,
            columns=formatted_columns,
            totalRows=table_asset.get('num_rows', 0),
            totalColumns=len(formatted_columns)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Starburst error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Starburst error: {str(e)}"
        )

@router.post("/publish-tags", response_model=PublishTagsResponse)
async def publish_tags(request: PublishTagsRequest):
    """
    Publish tags to Starburst Galaxy using REST API endpoints
    """
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
        
        if not client_id or not client_secret:
            raise HTTPException(
                status_code=400,
                detail="Starburst connector is missing OAuth credentials (client_id, client_secret). Please re-add the connector with API credentials."
            )
        
        # Get access token
        access_token = get_starburst_access_token(account_domain, client_id, client_secret)
        
        base_url = f"https://{account_domain}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        success_count = 0
        error_messages = []
        
        # Process each column's tags
        for col_tag in request.columnTags:
            if not col_tag.tags:
                continue
            
            for tag_name in col_tag.tags:
                try:
                    # Step 1: Get or create the tag
                    tag_id = get_or_create_tag(account_domain, access_token, tag_name)
                    
                    # Step 2: Apply tag to column using REST API
                    # Encode the parameters using name=value format
                    encoded_tag = requests.utils.quote(f"name={tag_name}")
                    encoded_catalog = requests.utils.quote(f"name={request.catalog}")
                    encoded_schema = requests.utils.quote(f"name={request.schema}")
                    encoded_table = requests.utils.quote(f"name={request.tableId}")
                    encoded_column = requests.utils.quote(f"name={col_tag.columnName}")
                    
                    # PUT endpoint to apply tag to column
                    update_url = f"{base_url}/public/api/v1/tag/{encoded_tag}/catalog/{encoded_catalog}/schema/{encoded_schema}/table/{encoded_table}/column/{encoded_column}"
                    
                    print(f"DEBUG: Applying tag to column: {update_url}")
                    
                    update_response = requests.put(update_url, headers=headers, json={})
                    
                    if update_response.status_code in [200, 204]:
                        print(f"✅ Successfully applied tag '{tag_name}' to column '{col_tag.columnName}'")
                        success_count += 1
                    else:
                        error_msg = f"HTTP {update_response.status_code}: {update_response.text[:200]}"
                        print(f"❌ Failed to apply tag '{tag_name}' to column '{col_tag.columnName}': {error_msg}")
                        error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {error_msg}")
                        
                except Exception as e:
                    print(f"❌ Error applying tag '{tag_name}' to column '{col_tag.columnName}': {str(e)}")
                    error_messages.append(f"Column '{col_tag.columnName}', Tag '{tag_name}': {str(e)}")
        
        # Build response message
        total_tags = sum(len(ct.tags) for ct in request.columnTags if ct.tags)
        
        if success_count == total_tags and total_tags > 0:
            billing_message = f"✅ Starburst Tags Applied Successfully!\n\n"
            billing_message += f"Applied {success_count} tags to table {request.catalog}.{request.schema}.{request.tableId}.\n\n"
            billing_message += "All tags have been directly added to Starburst Galaxy via REST API."
            success = True
        elif success_count > 0:
            billing_message = f"⚠️ Partial Success: {success_count} of {total_tags} tags applied.\n\n"
            if error_messages:
                billing_message += "Errors:\n" + "\n".join(error_messages[:5])
                if len(error_messages) > 5:
                    billing_message += f"\n... and {len(error_messages) - 5} more errors"
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
            billingMessage=billing_message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Starburst publish tags error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        billing_message = f"❌ Starburst Tag Publishing Failed: {str(e)}"
        
        return PublishTagsResponse(
            success=False,
            message="Failed to publish tags to Starburst Galaxy",
            billingMessage=billing_message
        )
