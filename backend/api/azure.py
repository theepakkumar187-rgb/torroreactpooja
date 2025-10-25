from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import asyncio
import json
from datetime import datetime, timedelta
import logging
from azure.purview.catalog import PurviewCatalogClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.core.exceptions import AzureError
import schedule
import threading
import time

router = APIRouter()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for Azure Purview
class AzurePurviewConnection(BaseModel):
    purview_account_name: str
    tenant_id: str
    client_id: str
    client_secret: str
    connection_name: str

class AzureDataAsset(BaseModel):
    id: str
    name: str
    type: str
    qualified_name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = []
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    properties: Dict[str, Any] = {}

class AzureDataCatalogue(BaseModel):
    assets: List[AzureDataAsset]
    total_count: int
    last_updated: datetime
    extraction_type: str  # "scheduled" or "on_demand"

class AzurePurviewConnectionTest(BaseModel):
    purview_account_name: str
    tenant_id: str
    client_id: str
    client_secret: str
    connection_name: str

class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
    discovered_assets: Optional[int] = 0
    connector_id: Optional[str] = None

class AzureExtractionRequest(BaseModel):
    extraction_type: str  # "scheduled" or "on_demand"
    schedule_interval: Optional[str] = None  # "hourly", "daily", "weekly"
    asset_types: Optional[List[str]] = None  # Filter by asset types

# Global variables for Azure connections and scheduler
azure_connections = {}
scheduler_running = False
scheduler_thread = None

def get_azure_client(connection_data: AzurePurviewConnection) -> PurviewCatalogClient:
    """Create and return an authenticated Azure Purview client"""
    try:
        credential = ClientSecretCredential(
            tenant_id=connection_data.tenant_id,
            client_id=connection_data.client_id,
            client_secret=connection_data.client_secret
        )
        
        client = PurviewCatalogClient(
            endpoint=f"https://{connection_data.purview_account_name}.purview.azure.com",
            credential=credential
        )
        
        return client
    except Exception as e:
        logger.error(f"Failed to create Azure Purview client: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to create Azure client: {str(e)}")

async def extract_azure_data_catalogue(connection_data: AzurePurviewConnection, asset_types: Optional[List[str]] = None) -> AzureDataCatalogue:
    """Extract data catalogue from Azure Purview"""
    try:
        client = get_azure_client(connection_data)
        
        # Search for all assets or filtered by type
        search_request = {
            "keywords": "*",
            "limit": 1000
        }
        
        if asset_types:
            search_request["filter"] = {
                "typeName": asset_types
            }
        
        # Perform the search
        search_response = client.search.query(search_request)
        
        assets = []
        for entity in search_response.get("value", []):
            asset = AzureDataAsset(
                id=entity.get("id", ""),
                name=entity.get("name", ""),
                type=entity.get("entityType", ""),
                qualified_name=entity.get("qualifiedName", ""),
                description=entity.get("description", ""),
                owner=entity.get("owner", ""),
                tags=entity.get("labels", []),
                created_time=entity.get("createTime"),
                modified_time=entity.get("lastModifiedTS"),
                properties=entity.get("attributes", {})
            )
            assets.append(asset)
        
        return AzureDataCatalogue(
            assets=assets,
            total_count=len(assets),
            last_updated=datetime.now(),
            extraction_type="on_demand"
        )
        
    except AzureError as e:
        logger.error(f"Azure API error: {e}")
        raise HTTPException(status_code=400, detail=f"Azure API error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during data extraction: {e}")
        raise HTTPException(status_code=500, detail=f"Data extraction failed: {str(e)}")

def scheduled_extraction_job():
    """Background job for scheduled data extraction"""
    global azure_connections
    
    logger.info("Running scheduled Azure data extraction...")
    
    for conn_id, conn_data in azure_connections.items():
        try:
            # Run extraction in a separate thread to avoid blocking
            asyncio.create_task(extract_azure_data_catalogue(conn_data))
            logger.info(f"Scheduled extraction completed for connection: {conn_id}")
        except Exception as e:
            logger.error(f"Scheduled extraction failed for {conn_id}: {e}")

def start_scheduler():
    """Start the background scheduler for periodic extractions"""
    global scheduler_running, scheduler_thread
    
    if scheduler_running:
        return
    
    scheduler_running = True
    
    def run_scheduler():
        while scheduler_running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("Azure data extraction scheduler started")

def stop_scheduler():
    """Stop the background scheduler"""
    global scheduler_running
    scheduler_running = False
    schedule.clear()
    logger.info("Azure data extraction scheduler stopped")

# API Endpoints
@router.post("/azure/test-connection", response_model=ConnectionTestResponse)
async def test_azure_connection(connection_data: AzurePurviewConnectionTest):
    """Test Azure Purview connection"""
    try:
        # Test the connection by creating a client
        client = get_azure_client(connection_data)
        
        # Try to perform a simple search to verify connection
        search_request = {"keywords": "*", "limit": 1}
        search_response = client.search.query(search_request)
        
        # Count total assets
        total_assets = search_response.get("@odata.count", 0)
        
        # Store the connection
        conn_id = f"azure_{connection_data.connection_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        azure_connections[conn_id] = connection_data
        
        return ConnectionTestResponse(
            success=True,
            message="Azure Purview connection successful",
            discovered_assets=total_assets,
            connector_id=conn_id
        )
        
    except Exception as e:
        logger.error(f"Azure connection test failed: {e}")
        return ConnectionTestResponse(
            success=False,
            message=f"Connection failed: {str(e)}"
        )

@router.post("/azure/extract-catalogue")
async def extract_data_catalogue(connection_data: AzurePurviewConnection, request: AzureExtractionRequest):
    """Extract data catalogue from Azure Purview on demand"""
    try:
        catalogue = await extract_azure_data_catalogue(connection_data, request.asset_types)
        catalogue.extraction_type = "on_demand"
        
        return catalogue
        
    except Exception as e:
        logger.error(f"Data catalogue extraction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")

@router.post("/azure/schedule-extraction")
async def schedule_data_extraction(connection_data: AzurePurviewConnection, request: AzureExtractionRequest):
    """Schedule periodic data extraction from Azure Purview"""
    try:
        if request.extraction_type != "scheduled":
            raise HTTPException(status_code=400, detail="Invalid extraction type for scheduling")
        
        if not request.schedule_interval:
            raise HTTPException(status_code=400, detail="Schedule interval is required for scheduled extraction")
        
        # Store the connection
        conn_id = f"azure_scheduled_{connection_data.connection_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        azure_connections[conn_id] = connection_data
        
        # Schedule the job
        if request.schedule_interval == "hourly":
            schedule.every().hour.do(scheduled_extraction_job)
        elif request.schedule_interval == "daily":
            schedule.every().day.at("02:00").do(scheduled_extraction_job)
        elif request.schedule_interval == "weekly":
            schedule.every().monday.at("02:00").do(scheduled_extraction_job)
        else:
            raise HTTPException(status_code=400, detail="Invalid schedule interval")
        
        # Start scheduler if not already running
        start_scheduler()
        
        return {
            "success": True,
            "message": f"Data extraction scheduled {request.schedule_interval}",
            "connector_id": conn_id,
            "next_run": schedule.next_run()
        }
        
    except Exception as e:
        logger.error(f"Failed to schedule extraction: {e}")
        raise HTTPException(status_code=500, detail=f"Scheduling failed: {str(e)}")

@router.get("/azure/connections")
async def get_azure_connections():
    """Get all Azure Purview connections"""
    return {
        "connections": [
            {
                "id": conn_id,
                "name": conn_data.connection_name,
                "purview_account": conn_data.purview_account_name,
                "created_at": datetime.now().isoformat()
            }
            for conn_id, conn_data in azure_connections.items()
        ]
    }

@router.delete("/azure/connections/{connector_id}")
async def delete_azure_connection(connector_id: str):
    """Delete an Azure Purview connection"""
    if connector_id not in azure_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    del azure_connections[connector_id]
    
    # If no more connections, stop scheduler
    if not azure_connections:
        stop_scheduler()
    
    return {"success": True, "message": "Connection deleted successfully"}

@router.get("/azure/scheduler/status")
async def get_scheduler_status():
    """Get scheduler status and next run times"""
    return {
        "scheduler_running": scheduler_running,
        "scheduled_jobs": len(schedule.jobs),
        "next_run": schedule.next_run().isoformat() if schedule.jobs else None,
        "active_connections": len(azure_connections)
    }

@router.post("/azure/scheduler/stop")
async def stop_scheduled_extraction():
    """Stop all scheduled extractions"""
    stop_scheduler()
    return {"success": True, "message": "Scheduled extractions stopped"}

# Initialize scheduler on module load
start_scheduler()
