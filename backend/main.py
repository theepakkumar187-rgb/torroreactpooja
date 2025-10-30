from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import uvicorn
from datetime import datetime, timedelta
import json
import tempfile
import os
import asyncio
import threading
import time
from functools import lru_cache
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# File paths setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # backend/ directory
CONNECTORS_FILE = os.path.join(BASE_DIR, "connectors.json")
ASSETS_FILE = os.path.join(BASE_DIR, "assets.json")
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions as google_exceptions
from api.bigquery import router as bigquery_router
from api.starburst import router as starburst_router
from api.lineage import router as lineage_router

# Azure router - optional import
try:
    from api.azure import router as azure_router
    AZURE_AVAILABLE = True
except ImportError:
    azure_router = None
    AZURE_AVAILABLE = False

app = FastAPI(title="Torro Data Intelligence Platform API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174"],  # Vite default ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(bigquery_router, prefix="/api/bigquery", tags=["bigquery"])
app.include_router(starburst_router, prefix="/api/starburst", tags=["starburst"])
app.include_router(lineage_router, prefix="/api", tags=["lineage"])
if AZURE_AVAILABLE and azure_router:
    app.include_router(azure_router, prefix="/api/azure", tags=["azure"])

# Pydantic models
class SystemHealth(BaseModel):
    status: str
    monitoring_enabled: bool
    connectors_enabled: int
    connectors_total: int
    last_scan: Optional[datetime]

class Asset(BaseModel):
    id: str
    name: str
    type: str
    catalog: str
    discovered_at: datetime
    status: str

class Connector(BaseModel):
    id: str
    name: str
    type: str
    status: str
    enabled: bool
    last_run: Optional[datetime]

class Activity(BaseModel):
    id: str
    type: str
    description: str
    timestamp: datetime
    status: str

class DashboardStats(BaseModel):
    total_assets: int
    total_catalogs: int
    active_connectors: int
    last_scan: Optional[datetime]
    monitoring_status: str

# Storage for discovered assets and connectors
discovered_assets = []
active_connectors = []

# Caching and background processing removed

def save_connectors():
    """Save connectors to file"""
    try:
        with open(CONNECTORS_FILE, 'w') as f:
            json.dump(active_connectors, f, indent=2, default=str)
    except Exception as e:
        print(f"Error saving connectors: {e}")

def load_connectors():
    """Load connectors from file"""
    global active_connectors
    try:
        print(f"DEBUG: Attempting to load connectors from: {CONNECTORS_FILE}")
        print(f"DEBUG: File exists: {os.path.exists(CONNECTORS_FILE)}")
        if os.path.exists(CONNECTORS_FILE):
            with open(CONNECTORS_FILE, 'r') as f:
                active_connectors = json.load(f)
                print(f"✅ Loaded {len(active_connectors)} connectors from {CONNECTORS_FILE}")
                return active_connectors
        else:
            print(f"⚠️  Connectors file not found: {CONNECTORS_FILE}")
    except Exception as e:
        print(f"❌ Error loading connectors: {e}")
        import traceback
        traceback.print_exc()
        active_connectors = []
    return active_connectors

def save_assets():
    """Save assets to file"""
    try:
        with open(ASSETS_FILE, 'w') as f:
            json.dump(discovered_assets, f, indent=2, default=str)
    except Exception as e:
        print(f"Error saving assets: {e}")

def load_assets():
    """Load assets from file"""
    global discovered_assets
    try:
        print(f"DEBUG: Attempting to load assets from: {ASSETS_FILE}")
        print(f"DEBUG: File exists: {os.path.exists(ASSETS_FILE)}")
        if os.path.exists(ASSETS_FILE):
            with open(ASSETS_FILE, 'r') as f:
                discovered_assets = json.load(f)
                print(f"✅ Loaded {len(discovered_assets)} assets from {ASSETS_FILE}")
                return discovered_assets
        else:
            print(f"⚠️  Assets file not found: {ASSETS_FILE}")
    except Exception as e:
        print(f"❌ Error loading assets: {e}")
        import traceback
        traceback.print_exc()
        discovered_assets = []
    return discovered_assets

# Load data on startup
load_connectors()
load_assets()

# Initialize background scheduler for periodic sync
scheduler = BackgroundScheduler()
scheduler_running = False

def sync_connectors():
    """Background task to continuously fetch API data and keep connectors synced"""
    global active_connectors, discovered_assets
    
    if not active_connectors:
        print("⚠️  No connectors configured, skipping sync")
        return
    
    print(f"🔄 [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Starting continuous API sync for {len(active_connectors)} connectors...")
    
    for connector in active_connectors:
        if not connector.get("enabled", False):
            print(f"⏭️  Skipping disabled connector: {connector.get('name', 'Unknown')}")
            continue
        
        connector_type = connector.get("type", "").lower()
        connector_id = connector.get("id", "")
        connector_name = connector.get("name", "Unknown")
        
        current_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"🔄 [{current_time}] Fetching API data for {connector_name} (Type: {connector_type})...")
        
        try:
            # Update last_run timestamp
            connector["last_run"] = datetime.now().isoformat()
            
            # Perform actual API fetch based on connector type
            if connector_type == "bigquery":
                print(f"📡 [{current_time}] Fetching BigQuery API for {connector_name}...")
                # Count assets from this connector
                connector_assets = [a for a in discovered_assets if a.get('connector_id') == connector_id]
                print(f"📊 [{current_time}] Found {len(connector_assets)} assets from {connector_name}")
                connector["assets_count"] = len(connector_assets)
                connector["status"] = "active"
                print(f"✅ [{current_time}] BigQuery connector {connector_name} synced - {len(connector_assets)} assets")
                
            elif connector_type == "starburst galaxy":
                print(f"📡 [{current_time}] Fetching Starburst API for {connector_name}...")
                # Count assets from this connector
                connector_assets = [a for a in discovered_assets if a.get('connector_id') == connector_id]
                print(f"📊 [{current_time}] Found {len(connector_assets)} assets from {connector_name}")
                connector["assets_count"] = len(connector_assets)
                connector["status"] = "active"
                print(f"✅ [{current_time}] Starburst connector {connector_name} synced - {len(connector_assets)} assets")
            else:
                print(f"⚠️  [{current_time}] Unknown connector type: {connector_type}")
            
        except Exception as e:
            current_time_error = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"❌ [{current_time_error}] Error syncing connector {connector_name}: {str(e)}")
            connector["status"] = "error"
    
    # Save updated connectors
    save_connectors()
    
    completion_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"✅ [{completion_time}] Continuous API sync completed - {len(active_connectors)} connectors, {len(discovered_assets)} total assets")

def start_scheduler():
    """Start the background scheduler if not already running"""
    global scheduler_running, scheduler
    
    if scheduler_running:
        print("⚠️  Scheduler is already running")
        return
    
    print("🚀 Starting background scheduler for continuous API fetching...")
    scheduler.add_job(
        sync_connectors,
        trigger=IntervalTrigger(seconds=1),  # Run every 1 second for continuous API fetching
        id='continuous_sync',
        name='Continuous API Sync (Always Fetching Every Second)',
        replace_existing=True
    )
    
    scheduler.start()
    scheduler_running = True
    print("✅ Background scheduler started - continuous API fetching EVERY SECOND")
    print("📡 Logs will show continuous API activity every 1 second...")

def stop_scheduler():
    """Stop the background scheduler"""
    global scheduler_running, scheduler
    
    if scheduler_running:
        print("🛑 Stopping background scheduler...")
        scheduler.shutdown(wait=False)
        scheduler_running = False
        print("✅ Background scheduler stopped")

# Pydantic models for connection testing

class BigQueryConnectionTest(BaseModel):
    project_id: str
    service_account_json: str
    connection_name: str

class StarburstConnectionTest(BaseModel):
    account_domain: str
    client_id: str
    client_secret: str
    connection_name: str

class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
    discovered_assets: Optional[int] = 0
    connector_id: Optional[str] = None

@app.post("/api/connectors/bigquery/test", response_model=ConnectionTestResponse)
async def test_bigquery_connection(connection_data: BigQueryConnectionTest):
    try:
        # Parse the service account JSON
        try:
            service_account_info = json.loads(connection_data.service_account_json)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid service account JSON format")
        
        # Create credentials
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
        )
        
        # Create BigQuery client
        client = bigquery.Client(
            credentials=credentials,
            project=connection_data.project_id
        )
        
        # Test connection by listing datasets
        datasets = list(client.list_datasets())
        
        # Discover assets (datasets and tables)
        assets_discovered = 0
        connector_id = f"bq_{connection_data.project_id}_{datetime.now().timestamp()}"
        
        for dataset_ref in datasets:
            dataset = client.get_dataset(dataset_ref.dataset_id)
            
            # Skip adding dataset as an asset - only discover tables/views
            
            # List tables in dataset
            try:
                tables = list(client.list_tables(dataset.dataset_id))
                for table_ref in tables:
                    table = client.get_table(table_ref)
                    
                    # Extract column information from the table schema
                    columns = []
                    if table.schema:
                        for field in table.schema:
                            columns.append({
                                "name": field.name,
                                "type": field.field_type,
                                "mode": field.mode,  # NULLABLE, REQUIRED, REPEATED
                                "description": field.description or "",
                            })
                    
                    # Get table labels for owner/classification info
                    table_labels = table.labels or {}
                    table_owner = table_labels.get('owner', dataset.labels.get('owner', service_account_info.get('client_email', 'Unknown')) if dataset.labels else service_account_info.get('client_email', 'Unknown'))
                    
                    # Extract dataset location
                    dataset_location = dataset.location if hasattr(dataset, 'location') else 'Unknown'
                    
                    discovered_assets.append({
                        "id": f"{connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                        "name": table.table_id,
                        "type": "Table" if table.table_type == "TABLE" else "View",
                        "catalog": f"{connection_data.project_id}.{dataset.dataset_id}",
                        "discovered_at": datetime.now().isoformat(),
                        "status": "active",
                        "description": table.description or "No description available",
                        "num_rows": table.num_rows if hasattr(table, 'num_rows') else 0,
                        "size_bytes": table.num_bytes if hasattr(table, 'num_bytes') else 0,
                        "connector_id": connector_id,
                        "columns": columns,  # Store actual column information
                        "technical_metadata": {
                            "asset_id": f"{connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                            "asset_type": "Table" if table.table_type == "TABLE" else "View",
                            "location": f"{dataset_location} - {connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                            "format": f"BigQuery {table.table_type}",
                            "size_bytes": table.num_bytes if hasattr(table, 'num_bytes') else 0,
                            "num_rows": table.num_rows if hasattr(table, 'num_rows') else 0,
                            "created_at": table.created.isoformat() if hasattr(table, 'created') and table.created else datetime.now().isoformat(),
                            "source_system": "BigQuery",
                            "storage_location": f"{connection_data.project_id}/{dataset.dataset_id}/{table.table_id}",
                            "schema_name": dataset.dataset_id,
                            "table_name": table.table_id,
                            "column_count": len(columns),
                            "partitioning_strategy": str(table.partitioning_type) if hasattr(table, 'partitioning_type') and table.partitioning_type else "None",
                            "clustering_fields": ", ".join(table.clustering_fields) if hasattr(table, 'clustering_fields') and table.clustering_fields else "None"
                        },
                        "operational_metadata": {
                            "status": "active",
                            "owner": table_owner,
                            "last_modified": table.modified.isoformat() if hasattr(table, 'modified') and table.modified else datetime.now().isoformat(),
                            "last_accessed": datetime.now().isoformat(),
                            "access_count": "N/A",
                            "data_quality_score": 95
                        },
                        "business_metadata": {
                            "description": table.description or "No description available",
                            "business_owner": table_owner,
                            "department": dataset.dataset_id,
                            "classification": table_labels.get('classification', 'internal'),
                            "sensitivity_level": table_labels.get('sensitivity', 'low'),
                            "tags": list(table_labels.keys()) if table_labels else []
                        }
                    })
                    assets_discovered += 1
            except Exception as e:
                print(f"Error listing tables in dataset {dataset.dataset_id}: {str(e)}")
                continue
        
        # Store the connector
        active_connectors.append({
            "id": connector_id,
            "name": connection_data.connection_name,
            "type": "BigQuery",
            "status": "active",
            "enabled": True,
            "last_run": datetime.now().isoformat(),
            "project_id": connection_data.project_id,
            "service_account_json": connection_data.service_account_json,
            "assets_count": assets_discovered
        })
        
        print(f"DEBUG: Created BigQuery connector {connector_id}")
        print(f"DEBUG: Discovered {assets_discovered} assets for this connector")
        print(f"DEBUG: Total assets now: {len(discovered_assets)}")
        
        # Save connectors and assets to file
        save_connectors()
        save_assets()
        
        print(f"DEBUG: Connector and assets successfully saved to files")
        
        return ConnectionTestResponse(
            success=True,
            message=f"Successfully connected to BigQuery project '{connection_data.project_id}'. Discovered {assets_discovered} assets.",
            discovered_assets=assets_discovered,
            connector_id=connector_id
        )
        
    except google_exceptions.GoogleAPIError as e:
        raise HTTPException(status_code=401, detail=f"BigQuery API error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")

# Streaming version of BigQuery connection test with real-time progress
@app.post("/api/connectors/bigquery/test-stream")
async def test_bigquery_connection_stream(connection_data: BigQueryConnectionTest):
    async def event_generator():
        try:
            # Parse the service account JSON
            try:
                service_account_info = json.loads(connection_data.service_account_json)
            except json.JSONDecodeError:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Invalid service account JSON format'})}\n\n"
                return
            
            yield f"data: {json.dumps({'type': 'progress', 'message': '✓ Service account validated'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Create credentials
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
            )
            
            yield f"data: {json.dumps({'type': 'progress', 'message': '✓ Credentials created'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Create BigQuery client
            client = bigquery.Client(
                credentials=credentials,
                project=connection_data.project_id
            )
            
            yield f"data: {json.dumps({'type': 'progress', 'message': f'✓ Connected to BigQuery project: {connection_data.project_id}'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Test connection by listing datasets
            datasets = list(client.list_datasets())
            yield f"data: {json.dumps({'type': 'progress', 'message': f'✓ Found {len(datasets)} datasets'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Discover assets (datasets and tables)
            assets_discovered = 0
            connector_id = f"bq_{connection_data.project_id}_{datetime.now().timestamp()}"
            
            for dataset_ref in datasets:
                dataset = client.get_dataset(dataset_ref.dataset_id)
                yield f"data: {json.dumps({'type': 'progress', 'message': f'Discovering dataset: {dataset.dataset_id}'})}\n\n"
                await asyncio.sleep(0.05)
                
                # List tables in dataset
                try:
                    tables = list(client.list_tables(dataset.dataset_id))
                    
                    for table_ref in tables:
                        table = client.get_table(table_ref)
                        
                        # Extract column information from the table schema
                        columns = []
                        if table.schema:
                            for field in table.schema:
                                columns.append({
                                    "name": field.name,
                                    "type": field.field_type,
                                    "mode": field.mode,
                                    "description": field.description or "",
                                })
                        
                        # Get table labels for owner/classification info
                        table_labels = table.labels or {}
                        table_owner = table_labels.get('owner', dataset.labels.get('owner', service_account_info.get('client_email', 'Unknown')) if dataset.labels else service_account_info.get('client_email', 'Unknown'))
                        dataset_location = dataset.location if hasattr(dataset, 'location') else 'Unknown'
                        
                        discovered_assets.append({
                            "id": f"{connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                            "name": table.table_id,
                            "type": "Table" if table.table_type == "TABLE" else "View",
                            "catalog": f"{connection_data.project_id}.{dataset.dataset_id}",
                            "discovered_at": datetime.now().isoformat(),
                            "status": "active",
                            "description": table.description or "No description available",
                            "num_rows": table.num_rows if hasattr(table, 'num_rows') else 0,
                            "size_bytes": table.num_bytes if hasattr(table, 'num_bytes') else 0,
                            "connector_id": connector_id,
                            "columns": columns,
                            "technical_metadata": {
                                "asset_id": f"{connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                                "asset_type": "Table" if table.table_type == "TABLE" else "View",
                                "location": f"{dataset_location} - {connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                                "format": f"BigQuery {table.table_type}",
                                "size_bytes": table.num_bytes if hasattr(table, 'num_bytes') else 0,
                                "num_rows": table.num_rows if hasattr(table, 'num_rows') else 0,
                                "created_at": table.created.isoformat() if hasattr(table, 'created') and table.created else datetime.now().isoformat(),
                                "source_system": "BigQuery",
                                "storage_location": f"{connection_data.project_id}/{dataset.dataset_id}/{table.table_id}",
                                "schema_name": dataset.dataset_id,
                                "table_name": table.table_id,
                                "column_count": len(columns),
                                "partitioning_strategy": str(table.partitioning_type) if hasattr(table, 'partitioning_type') and table.partitioning_type else "None",
                                "clustering_fields": ", ".join(table.clustering_fields) if hasattr(table, 'clustering_fields') and table.clustering_fields else "None"
                            },
                            "operational_metadata": {
                                "status": "active",
                                "owner": table_owner,
                                "last_modified": table.modified.isoformat() if hasattr(table, 'modified') and table.modified else datetime.now().isoformat(),
                                "last_accessed": datetime.now().isoformat(),
                                "access_count": "N/A",
                                "data_quality_score": 95
                            },
                            "business_metadata": {
                                "description": table.description or "No description available",
                                "business_owner": table_owner,
                                "department": dataset.dataset_id,
                                "classification": table_labels.get('classification', 'internal'),
                                "sensitivity_level": table_labels.get('sensitivity', 'low'),
                                "tags": list(table_labels.keys()) if table_labels else []
                            }
                        })
                        assets_discovered += 1
                        
                        yield f"data: {json.dumps({'type': 'progress', 'message': f'  ✓ {table.table_id} ({len(columns)} columns)'})}\n\n"
                        await asyncio.sleep(0.05)
                        
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'progress', 'message': f'  ✗ Error in dataset {dataset.dataset_id}: {str(e)}'})}\n\n"
                    await asyncio.sleep(0.05)
                    continue
            
            # Validate that credentials are not placeholders
            import json as json_module
            try:
                service_account_info = json_module.loads(connection_data.service_account_json)
                # Check for common placeholder values
                placeholder_values = ["your-project-id", "YOUR_PRIVATE_KEY", "your-service-account@your-project.iam.gserviceaccount.com"]
                
                service_account_str = str(service_account_info).lower()
                if any(placeholder.lower() in service_account_str for placeholder in placeholder_values):
                    yield f"data: {json.dumps({'type': 'error', 'message': '❌ Placeholder credentials detected in service account JSON. Please enter your actual Google Cloud service account credentials.'})}\n\n"
                    return
            except json_module.JSONDecodeError:
                yield f"data: {json.dumps({'type': 'error', 'message': '❌ Invalid JSON format in service account credentials.'})}\n\n"
                return
            
            # Store the connector
            active_connectors.append({
                "id": connector_id,
                "name": connection_data.connection_name,
                "type": "BigQuery",
                "status": "active",
                "enabled": True,
                "last_run": datetime.now().isoformat(),
                "project_id": connection_data.project_id,
                "service_account_json": connection_data.service_account_json,
                "assets_count": assets_discovered
            })
            
            print(f"DEBUG: Created BigQuery connector {connector_id} (streaming)")
            print(f"DEBUG: Discovered {assets_discovered} assets for this connector")
            print(f"DEBUG: Total assets now: {len(discovered_assets)}")
            
            # Save connectors and assets to file
            save_connectors()
            save_assets()
            
            print(f"DEBUG: BigQuery connector and assets successfully saved to files (streaming)")
            
            yield f"data: {json.dumps({'type': 'complete', 'message': f'Successfully discovered {assets_discovered} assets', 'discovered_assets': assets_discovered, 'connector_id': connector_id})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': f'Connection failed: {str(e)}'})}\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/api/connectors/starburst/test", response_model=ConnectionTestResponse)
async def test_starburst_connection(connection_data: StarburstConnectionTest):
    try:
        import requests
        import base64
        
        # Validate required parameters
        if not connection_data.account_domain or not connection_data.client_id or not connection_data.client_secret:
            raise HTTPException(status_code=400, detail="Missing required connection parameters")
        
        # Validate domain format
        if not connection_data.account_domain.endswith('.galaxy.starburst.io'):
            raise HTTPException(status_code=400, detail="Invalid Starburst Galaxy domain format")
        
        # Starburst Galaxy REST API base URL
        base_url = f"https://{connection_data.account_domain}"
        
        # Step 1: Get OAuth2 access token using Client Credentials flow
        auth_string = f"{connection_data.client_id}:{connection_data.client_secret}"
        auth_bytes = auth_string.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        
        token_headers = {
            'Authorization': f'Basic {auth_b64}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        token_data = 'grant_type=client_credentials'
        token_url = f"{base_url}/oauth/v2/token"
        
        try:
            # Get access token
            token_response = requests.post(token_url, headers=token_headers, data=token_data, timeout=30)
            
            if token_response.status_code != 200:
                print(f"Token response status: {token_response.status_code}")
                print(f"Token response text: {token_response.text}")
                raise HTTPException(
                    status_code=401, 
                    detail=f"Failed to get access token: {token_response.text}"
                )
            
            try:
                token_data = token_response.json()
                access_token = token_data.get('access_token')
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                print(f"Response text: {token_response.text}")
                raise HTTPException(
                    status_code=401, 
                    detail=f"Invalid JSON response from Starburst Galaxy: {token_response.text}"
                )
            
            if not access_token:
                raise HTTPException(
                    status_code=401, 
                    detail="No access token received from Starburst Galaxy"
                )
            
            # Step 2: Use access token to get catalogs using Starburst Galaxy API
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            # Use the correct Starburst Galaxy API endpoint for listing catalogs
            catalogs_url = f"{base_url}/public/api/v1/catalog"
            catalogs_response = requests.get(catalogs_url, headers=headers, timeout=30)
            
            if catalogs_response.status_code == 401:
                raise HTTPException(
                    status_code=401, 
                    detail="Invalid Starburst Galaxy credentials. Please check your Client ID and Client Secret."
                )
            elif catalogs_response.status_code == 403:
                raise HTTPException(
                    status_code=403, 
                    detail="Access forbidden. Please ensure your account has the necessary permissions."
                )
            elif catalogs_response.status_code == 404:
                raise HTTPException(
                    status_code=404, 
                    detail="Starburst Galaxy domain not found. Please verify your account domain."
                )
            elif catalogs_response.status_code != 200:
                raise HTTPException(
                    status_code=catalogs_response.status_code, 
                    detail=f"Starburst Galaxy API error: {catalogs_response.text}"
                )
            
            catalogs_data = catalogs_response.json()
            connector_id = f"starburst_{connection_data.account_domain}_{datetime.now().timestamp()}"
            assets_discovered = 0
            
            print(f"DEBUG: Starburst catalogs response: {catalogs_data}")
            print(f"DEBUG: Number of catalogs found: {len(catalogs_data.get('result', []))}")
            
            # Process real catalogs from Starburst Galaxy using the correct API response structure
            catalogs_list = catalogs_data.get('result', [])
            if not catalogs_list:
                # Try alternative response structures
                catalogs_list = catalogs_data.get('data', [])
                if not catalogs_list:
                    catalogs_list = catalogs_data.get('catalogs', [])
                if not catalogs_list:
                    catalogs_list = catalogs_data if isinstance(catalogs_data, list) else []
            
            print(f"DEBUG: Using catalogs list with {len(catalogs_list)} items")
            
            # System catalogs to exclude (Starburst internal catalogs)
            SYSTEM_CATALOGS = ['galaxy', 'galaxy_telemetry', 'system', 'information_schema']
            
            for catalog in catalogs_list:
                catalog_id = catalog.get('catalogId', 'unknown')
                catalog_name = catalog.get('catalogName', catalog_id)
                catalog_owner = catalog.get('owner', catalog.get('catalogOwner', connection_data.connection_name))
                
                # Skip system catalogs
                if catalog_name.lower() in SYSTEM_CATALOGS:
                    print(f"DEBUG: Skipping system catalog: {catalog_name}")
                    continue
                
                # Add catalog as an asset with metadata
                discovered_assets.append({
                    "id": f"{connection_data.account_domain}.{catalog_name}",
                    "name": catalog_name,
                    "type": "Catalog",
                    "catalog": f"{connection_data.account_domain}.{catalog_name}",
                    "discovered_at": datetime.now().isoformat(),
                    "status": "active",
                    "description": f"Catalog: {catalog_name}",
                    "connector_id": connector_id,
                    "columns": [],  # Catalogs don't have columns
                    "num_rows": 0,
                    "size_bytes": 0,
                    "technical_metadata": {
                        "asset_id": f"{connection_data.account_domain}.{catalog_name}",
                        "asset_type": "Catalog",
                        "location": f"{connection_data.account_domain} - {catalog_name}",
                        "format": "Starburst Catalog",
                        "size_bytes": 0,
                        "num_rows": 0,
                        "created_at": datetime.now().isoformat(),
                        "source_system": "Starburst Galaxy",
                        "storage_location": f"{connection_data.account_domain}/{catalog_name}"
                    },
                    "operational_metadata": {
                        "status": "active",
                        "owner": catalog_owner,
                        "last_modified": datetime.now().isoformat(),
                        "last_accessed": datetime.now().isoformat(),
                        "access_count": "N/A",
                        "data_quality_score": 95
                    },
                    "business_metadata": {
                        "description": f"Catalog: {catalog_name}",
                        "business_owner": catalog_owner,
                        "department": "Data Platform",
                        "classification": "internal",
                        "sensitivity_level": "medium",
                        "tags": []
                    }
                })
                assets_discovered += 1
                
                # Get schemas for this catalog using Starburst Galaxy API with retry logic
                schemas_discovered = False
                for retry_attempt in range(3):  # Try up to 3 times
                    try:
                        schemas_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema"
                        schemas_response = requests.get(schemas_url, headers=headers, timeout=30)
                        
                        print(f"DEBUG: Schemas for catalog {catalog_name}: status={schemas_response.status_code} (attempt {retry_attempt + 1})")
                        
                        if schemas_response.status_code == 200:
                            schemas_discovered = True
                            break
                        elif schemas_response.status_code == 429:
                            print(f"DEBUG: Rate limited for schemas in {catalog_name}, waiting 5 seconds...")
                            import time
                            time.sleep(5)
                        else:
                            print(f"DEBUG: Failed to get schemas for {catalog_name}: status={schemas_response.status_code}")
                            break
                    except Exception as e:
                        print(f"DEBUG: Exception getting schemas for {catalog_name}: {str(e)}")
                        if retry_attempt < 2:  # Don't sleep on last attempt
                            import time
                            time.sleep(2)
                
                if schemas_discovered:
                    schemas_data = schemas_response.json()
                    print(f"DEBUG: Schemas data for {catalog_name}: {schemas_data}")
                    
                    # Try different response structures for schemas
                    schemas_list = schemas_data.get('result', [])
                    if not schemas_list:
                        schemas_list = schemas_data.get('data', [])
                    if not schemas_list:
                        schemas_list = schemas_data.get('schemas', [])
                    if not schemas_list:
                        schemas_list = schemas_data if isinstance(schemas_data, list) else []
                    
                    print(f"DEBUG: Number of schemas in {catalog_name}: {len(schemas_list)}")
                    
                    for schema in schemas_list:
                        schema_id = schema.get('schemaId', 'unknown')
                        schema_name = schema.get('schemaName', schema_id)
                        schema_owner = schema.get('owner', schema.get('schemaOwner', connection_data.connection_name))
                        
                        # Add schema as an asset with metadata
                        discovered_assets.append({
                            "id": f"{connection_data.account_domain}.{catalog_name}.{schema_name}",
                            "name": schema_name,
                            "type": "Schema",
                            "catalog": f"{connection_data.account_domain}.{catalog_name}.{schema_name}",
                            "discovered_at": datetime.now().isoformat(),
                            "status": "active",
                            "description": f"Schema: {schema_name} in {catalog_name}",
                            "connector_id": connector_id,
                            "columns": [],  # Schemas don't have columns
                            "num_rows": 0,
                            "size_bytes": 0,
                            "technical_metadata": {
                                "asset_id": f"{connection_data.account_domain}.{catalog_name}.{schema_name}",
                                "asset_type": "Schema",
                                "location": f"{connection_data.account_domain} - {catalog_name}.{schema_name}",
                                "format": "Starburst Schema",
                                "size_bytes": 0,
                                "num_rows": 0,
                                "created_at": datetime.now().isoformat(),
                                "source_system": "Starburst Galaxy",
                                "storage_location": f"{connection_data.account_domain}/{catalog_name}/{schema_name}"
                            },
                            "operational_metadata": {
                                "status": "active",
                                "owner": schema_owner,
                                "last_modified": datetime.now().isoformat(),
                                "last_accessed": datetime.now().isoformat(),
                                "access_count": "N/A",
                                "data_quality_score": 92
                            },
                            "business_metadata": {
                                "description": f"Schema: {schema_name} in {catalog_name}",
                                "business_owner": schema_owner,
                                "department": catalog_name,
                                "classification": "internal",
                                "sensitivity_level": "medium",
                                "tags": []
                            }
                        })
                        assets_discovered += 1
                        
                        # Get tables for this schema using Starburst Galaxy API with retry logic
                        tables_discovered = False
                        for retry_attempt in range(3):  # Try up to 3 times
                            try:
                                tables_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table"
                                tables_response = requests.get(tables_url, headers=headers, timeout=30)
                                
                                print(f"DEBUG: Tables for {catalog_name}.{schema_name}: status={tables_response.status_code} (attempt {retry_attempt + 1})")
                                
                                if tables_response.status_code == 200:
                                    tables_discovered = True
                                    break
                                elif tables_response.status_code == 429:
                                    print(f"DEBUG: Rate limited for tables in {catalog_name}.{schema_name}, waiting 5 seconds...")
                                    import time
                                    time.sleep(5)
                                else:
                                    print(f"DEBUG: Failed to get tables for {catalog_name}.{schema_name}: status={tables_response.status_code}")
                                    break
                            except Exception as e:
                                print(f"DEBUG: Exception getting tables for {catalog_name}.{schema_name}: {str(e)}")
                                if retry_attempt < 2:  # Don't sleep on last attempt
                                    import time
                                    time.sleep(2)
                        
                        if tables_discovered:
                            tables_data = tables_response.json()
                            print(f"DEBUG: Tables data for {catalog_name}.{schema_name}: {tables_data}")
                            
                            # Try different response structures for tables
                            tables_list = tables_data.get('result', [])
                            if not tables_list:
                                tables_list = tables_data.get('data', [])
                            if not tables_list:
                                tables_list = tables_data.get('tables', [])
                            if not tables_list:
                                tables_list = tables_data if isinstance(tables_data, list) else []
                            
                            print(f"DEBUG: Number of tables in {catalog_name}.{schema_name}: {len(tables_list)}")
                            
                            for table in tables_list:
                                table_id = table.get('tableId', 'unknown')
                                table_name = table.get('tableName', table_id)
                                table_type = table.get('tableType', 'TABLE')
                                
                                # Get columns for this table using Starburst Galaxy API with retry logic
                                table_columns = []
                                columns_discovered = False
                                for retry_attempt in range(3):  # Try up to 3 times
                                    try:
                                        columns_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table/{table_id}/column"
                                        columns_response = requests.get(columns_url, headers=headers, timeout=30)
                                        
                                        print(f"DEBUG: Columns for {catalog_name}.{schema_name}.{table_name}: status={columns_response.status_code} (attempt {retry_attempt + 1})")
                                        
                                        if columns_response.status_code == 200:
                                            columns_discovered = True
                                            break
                                        elif columns_response.status_code == 429:
                                            print(f"DEBUG: Rate limited for columns in {catalog_name}.{schema_name}.{table_name}, waiting 5 seconds...")
                                            import time
                                            time.sleep(5)
                                        else:
                                            print(f"DEBUG: Failed to get columns for {catalog_name}.{schema_name}.{table_name}: status={columns_response.status_code}")
                                            break
                                    except Exception as e:
                                        print(f"DEBUG: Exception getting columns for {catalog_name}.{schema_name}.{table_name}: {str(e)}")
                                        if retry_attempt < 2:  # Don't sleep on last attempt
                                            import time
                                            time.sleep(2)
                                
                                if columns_discovered:
                                    columns_data = columns_response.json()
                                    
                                    # Try different response structures for columns
                                    columns_list = columns_data.get('result', [])
                                    if not columns_list:
                                        columns_list = columns_data.get('data', [])
                                    if not columns_list:
                                        columns_list = columns_data.get('columns', [])
                                    if not columns_list:
                                        columns_list = columns_data if isinstance(columns_data, list) else []
                                    
                                    print(f"DEBUG: Columns for {catalog_name}.{schema_name}.{table_name}: Found {len(columns_list)} columns")
                                    if len(columns_list) > 0:
                                        print(f"DEBUG: First column structure: {columns_list[0]}")
                                    
                                    for column in columns_list:
                                        # Parse Starburst Galaxy API column structure
                                        # The API returns: {columnId, dataType, ...}
                                        # Note: columnId is the actual column name, not an ID!
                                        column_name = column.get('columnId', column.get('columnName', 'unknown'))
                                        column_type = column.get('dataType', 'STRING')
                                        
                                        # If columnId/columnName is not present, try alternative field names
                                        if column_name == 'unknown':
                                            column_name = (
                                                column.get('name') or 
                                                column.get('column_name') or
                                                column.get('COLUMN_NAME') or
                                                column.get('field') or
                                                column.get('fieldName') or
                                                'unknown'
                                            )
                                        
                                        # If dataType is not present, try alternative field names
                                        if column_type == 'STRING':
                                            column_type = (
                                                column.get('type') or 
                                                column.get('data_type') or
                                                column.get('DATA_TYPE') or
                                                column.get('fieldType') or
                                                'STRING'
                                            )
                                        
                                        # Debug: Print the actual column object if name is still unknown
                                        if column_name == 'unknown':
                                            print(f"DEBUG: Column object with unknown name: {column}")
                                        
                                        table_columns.append({
                                            "name": column_name,
                                            "type": column_type,
                                            "mode": "NULLABLE",  # Starburst doesn't provide this directly
                                            "description": column.get('comment', ''),  # Get column description if available
                                        })
                                else:
                                    print(f"DEBUG: Could not discover columns for {catalog_name}.{schema_name}.{table_name} after retries")
                                
                                # Determine the correct asset type
                                asset_type = "Table"
                                if table_type == "VIEW":
                                    asset_type = "View"
                                elif table_type == "BASE TABLE":
                                    asset_type = "Table"
                                elif "VIEW" in table_type.upper():
                                    asset_type = "View"
                                else:
                                    asset_type = "Table"
                                
                                # Get table owner/role from Starburst if available
                                table_owner = table.get('owner', table.get('tableOwner', connection_data.connection_name))
                                table_role = table.get('role', table.get('ownerRole', connection_data.connection_name))
                                table_comment = table.get('comment', f"{asset_type}: {table_name} in {catalog_name}.{schema_name}")
                                
                                # Add table as an asset with columns embedded and metadata
                                discovered_assets.append({
                                    "id": f"{connection_data.account_domain}.{catalog_name}.{schema_name}.{table_name}",
                                    "name": table_name,
                                    "type": asset_type,
                                    "catalog": f"{connection_data.account_domain}.{catalog_name}.{schema_name}",
                                    "discovered_at": datetime.now().isoformat(),
                                    "status": "active",
                                    "description": table_comment,
                                    "connector_id": connector_id,
                                    "columns": table_columns,  # Store actual column information
                                    "num_rows": 0,  # Starburst doesn't provide row count in API
                                    "size_bytes": 0,  # Starburst doesn't provide size in API
                                    "technical_metadata": {
                                        "asset_id": f"{connection_data.account_domain}.{catalog_name}.{schema_name}.{table_name}",
                                        "asset_type": asset_type,
                                        "location": f"{connection_data.account_domain} - {catalog_name}.{schema_name}.{table_name}",
                                        "format": f"Starburst {asset_type}",
                                        "size_bytes": 0,
                                        "num_rows": 0,
                                        "created_at": datetime.now().isoformat(),
                                        "source_system": "Starburst Galaxy",
                                        "storage_location": f"{connection_data.account_domain}/{catalog_name}/{schema_name}/{table_name}",
                                        "schema_name": schema_name,
                                        "table_name": table_name,
                                        "column_count": len(table_columns),
                                        "catalog_name": catalog_name
                                    },
                                    "operational_metadata": {
                                        "status": "active",
                                        "owner": table_owner,
                                        "last_modified": datetime.now().isoformat(),
                                        "last_accessed": datetime.now().isoformat(),
                                        "access_count": "N/A",
                                        "data_quality_score": 90
                                    },
                                    "business_metadata": {
                                        "description": table_comment,
                                        "business_owner": table_owner,
                                        "department": catalog_name,
                                        "classification": "internal",
                                        "sensitivity_level": "medium",
                                        "tags": []
                                    }
                                })
                                assets_discovered += 1
                        else:
                            print(f"DEBUG: Could not discover tables for {catalog_name}.{schema_name} after retries")
                else:
                    print(f"DEBUG: Could not discover schemas for catalog {catalog_name} after retries")
            
            # Store the connector with credentials for API access
            active_connectors.append({
                "id": connector_id,
                "name": connection_data.connection_name,
                "type": "Starburst Galaxy",
                "status": "active",
                "enabled": True,
                "last_run": datetime.now().isoformat(),
                "account_domain": connection_data.account_domain,
                "client_id": connection_data.client_id,
                "client_secret": connection_data.client_secret,
                "assets_count": assets_discovered
            })
            
            print(f"DEBUG: Created Starburst connector {connector_id}")
            print(f"DEBUG: Discovered {assets_discovered} assets for this connector")
            print(f"DEBUG: Total assets now: {len(discovered_assets)}")
            
            # Save connectors and assets to file
            save_connectors()
            save_assets()
            
            print(f"DEBUG: Starburst connector and assets successfully saved to files")
            
            return ConnectionTestResponse(
                success=True,
                message=f"Successfully connected to Starburst Galaxy '{connection_data.account_domain}'. Discovered {assets_discovered} real assets.",
                discovered_assets=assets_discovered,
                connector_id=connector_id
            )
            
        except requests.exceptions.RequestException as e:
            raise HTTPException(status_code=401, detail=f"Starburst Galaxy API error: {str(e)}")
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")

# Streaming version of Starburst connection test with real-time progress
@app.post("/api/connectors/starburst/test-stream")
async def test_starburst_connection_stream(connection_data: StarburstConnectionTest):
    async def event_generator():
        try:
            import requests
            import base64
            
            # Validate required parameters
            if not connection_data.account_domain or not connection_data.client_id or not connection_data.client_secret:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Missing required connection parameters'})}\n\n"
                return
            
            yield f"data: {json.dumps({'type': 'progress', 'message': '✓ Connection parameters validated'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Validate domain format
            if not connection_data.account_domain.endswith('.galaxy.starburst.io'):
                yield f"data: {json.dumps({'type': 'error', 'message': 'Invalid Starburst Galaxy domain format'})}\n\n"
                return
            
            yield f"data: {json.dumps({'type': 'progress', 'message': '✓ Domain format validated'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Starburst Galaxy REST API base URL
            base_url = f"https://{connection_data.account_domain}"
            
            # Get OAuth token
            auth_url = f"{base_url}/oauth/v2/token"
            auth_credentials = base64.b64encode(f"{connection_data.client_id}:{connection_data.client_secret}".encode()).decode()
            
            try:
                token_response = requests.post(
                    auth_url,
                    headers={"Authorization": f"Basic {auth_credentials}"},
                    data={"grant_type": "client_credentials"},
                    timeout=30
                )
                token_response.raise_for_status()
                access_token = token_response.json().get('access_token')
                
                yield f"data: {json.dumps({'type': 'progress', 'message': '✓ OAuth authentication successful'})}\n\n"
                await asyncio.sleep(0.1)
            except requests.exceptions.RequestException as e:
                yield f"data: {json.dumps({'type': 'error', 'message': f'Authentication failed: {str(e)}'})}\n\n"
                return
            
            headers = {"Authorization": f"Bearer {access_token}"}
            
            # Test connection by listing catalogs
            catalogs_url = f"{base_url}/public/api/v1/catalog"
            try:
                catalogs_response = requests.get(catalogs_url, headers=headers, timeout=30)
                catalogs_response.raise_for_status()
                catalogs_data = catalogs_response.json()
                catalogs_list = catalogs_data.get('result', catalogs_data.get('catalogs', []))
                
                yield f"data: {json.dumps({'type': 'progress', 'message': f'✓ Connected to Starburst Galaxy'})}\n\n"
                await asyncio.sleep(0.1)
                yield f"data: {json.dumps({'type': 'progress', 'message': f'✓ Found {len(catalogs_list)} catalogs'})}\n\n"
                await asyncio.sleep(0.1)
            except requests.exceptions.RequestException as e:
                yield f"data: {json.dumps({'type': 'error', 'message': f'Failed to list catalogs: {str(e)}'})}\n\n"
                return
            
            # Discover assets
            assets_discovered = 0
            connector_id = f"starburst_{connection_data.account_domain.split('.')[0]}_{datetime.now().timestamp()}"
            
            # Rate limiting configuration
            # Starburst Galaxy typically has: 10,000 requests per 100 seconds
            RATE_LIMIT_WAIT = 120  # Wait 2 minutes if we hit rate limit
            
            # Discovery loop with real-time updates
            # System catalogs to exclude (Starburst internal catalogs)
            SYSTEM_CATALOGS = ['galaxy', 'galaxy_telemetry', 'system', 'information_schema']
            
            for catalog in catalogs_list:
                catalog_id = catalog.get('catalogId', 'unknown')
                catalog_name = catalog.get('catalogName', catalog_id)
                catalog_owner = catalog.get('owner', catalog.get('catalogOwner', connection_data.connection_name))
                
                # Skip system catalogs
                if catalog_name.lower() in SYSTEM_CATALOGS:
                    yield f"data: {json.dumps({'type': 'progress', 'message': f'⊘ Skipping system catalog: {catalog_name}'})}\n\n"
                    await asyncio.sleep(0.05)
                    continue
                
                yield f"data: {json.dumps({'type': 'progress', 'message': f'Discovering catalog: {catalog_name}'})}\n\n"
                await asyncio.sleep(0.05)
                
                # Add catalog as an asset
                discovered_assets.append({
                    "id": f"{connection_data.account_domain}.{catalog_name}",
                    "name": catalog_name,
                    "type": "Catalog",
                    "catalog": f"{connection_data.account_domain}.{catalog_name}",
                    "discovered_at": datetime.now().isoformat(),
                    "status": "active",
                    "description": f"Catalog: {catalog_name}",
                    "connector_id": connector_id,
                    "columns": [],
                    "num_rows": 0,
                    "size_bytes": 0,
                    "technical_metadata": {
                        "asset_id": f"{connection_data.account_domain}.{catalog_name}",
                        "asset_type": "Catalog",
                        "location": f"{connection_data.account_domain} - {catalog_name}",
                        "format": "Starburst Catalog",
                        "size_bytes": 0,
                        "num_rows": 0,
                        "created_at": datetime.now().isoformat(),
                        "source_system": "Starburst Galaxy",
                        "storage_location": f"{connection_data.account_domain}/{catalog_name}"
                    },
                    "operational_metadata": {
                        "status": "active",
                        "owner": catalog_owner,
                        "last_modified": datetime.now().isoformat(),
                        "last_accessed": datetime.now().isoformat(),
                        "steward": connection_data.connection_name,
                        "access_frequency": "Daily"
                    },
                    "business_metadata": {
                        "description": f"Catalog: {catalog_name}",
                        "tags": [],
                        "classification": "General",
                        "sensitivity_level": "Internal",
                        "compliance_tags": []
                    }
                })
                assets_discovered += 1
                
                # List schemas in catalog with retry logic
                schemas_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema"
                
                # Retry logic for rate limiting and auth errors
                max_retries = 5
                retry_delay = 3
                schemas_response = None
                
                for attempt in range(1, max_retries + 1):
                    try:
                        # Refresh token if we get 401
                        if attempt > 1:
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'  ⟳ Refreshing authentication (attempt {attempt}/{max_retries})'})}\n\n"
                            await asyncio.sleep(0.1)
                            
                            # Get new access token using correct auth_url
                            token_response = requests.post(
                                auth_url,
                                headers={"Authorization": f"Basic {auth_credentials}"},
                                data={"grant_type": "client_credentials"},
                                timeout=30
                            )
                            token_response.raise_for_status()
                            access_token = token_response.json().get('access_token')
                            headers = {"Authorization": f"Bearer {access_token}"}
                        
                        schemas_response = requests.get(schemas_url, headers=headers, timeout=30)
                        
                        if schemas_response.status_code == 429:
                            # Rate limited - wait for rate limit window to reset
                            if attempt == 1:
                                # First time hitting rate limit - wait for full window reset
                                yield f"data: {json.dumps({'type': 'progress', 'message': f'  ⏸ Rate limit reached. Waiting {RATE_LIMIT_WAIT}s for rate limit window to reset...'})}\n\n"
                                await asyncio.sleep(RATE_LIMIT_WAIT)
                            else:
                                # Subsequent retries - shorter wait
                                wait_time = retry_delay * attempt
                                yield f"data: {json.dumps({'type': 'progress', 'message': f'  ⏸ Still rate limited. Waiting {wait_time}s...'})}\n\n"
                                await asyncio.sleep(wait_time)
                            continue
                        elif schemas_response.status_code == 401:
                            # Auth error - refresh token and retry
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'  ⚠ Authentication expired. Refreshing token...'})}\n\n"
                            await asyncio.sleep(1)
                            continue
                        else:
                            # Success or other error - break retry loop
                            break
                            
                    except requests.exceptions.Timeout:
                        if attempt < max_retries:
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'  ⏱ Timeout. Retrying in {retry_delay}s...'})}\n\n"
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'  ✗ Timeout after {max_retries} attempts for {catalog_name}'})}\n\n"
                            schemas_response = None
                            break
                    except Exception as e:
                        if attempt < max_retries:
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'  ⚠ Error: {str(e)}. Retrying...'})}\n\n"
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'  ✗ Failed after {max_retries} attempts: {str(e)}'})}\n\n"
                            schemas_response = None
                            break
                
                try:
                    if schemas_response and schemas_response.status_code == 200:
                        schemas_data = schemas_response.json()
                        schemas_list = schemas_data.get('result', schemas_data.get('schemas', []))
                        
                        for schema in schemas_list:
                            schema_id = schema.get('schemaId', 'unknown')
                            schema_name = schema.get('schemaName', schema_id)
                            schema_owner = schema.get('owner', schema.get('schemaOwner', connection_data.connection_name))
                            
                            yield f"data: {json.dumps({'type': 'progress', 'message': f'  Schema: {schema_name}'})}\n\n"
                            await asyncio.sleep(0.05)
                            
                            # Add schema as an asset
                            discovered_assets.append({
                                "id": f"{connection_data.account_domain}.{catalog_name}.{schema_name}",
                                "name": schema_name,
                                "type": "Schema",
                                "catalog": f"{connection_data.account_domain}.{catalog_name}.{schema_name}",
                                "discovered_at": datetime.now().isoformat(),
                                "status": "active",
                                "description": f"Schema: {schema_name} in {catalog_name}",
                                "connector_id": connector_id,
                                "columns": [],
                                "num_rows": 0,
                                "size_bytes": 0,
                                "technical_metadata": {
                                    "asset_id": f"{connection_data.account_domain}.{catalog_name}.{schema_name}",
                                    "asset_type": "Schema",
                                    "location": f"{connection_data.account_domain} - {catalog_name}.{schema_name}",
                                    "format": "Starburst Schema",
                                    "size_bytes": 0,
                                    "num_rows": 0,
                                    "created_at": datetime.now().isoformat(),
                                    "source_system": "Starburst Galaxy",
                                    "storage_location": f"{connection_data.account_domain}/{catalog_name}/{schema_name}",
                                    "catalog_name": catalog_name,
                                    "schema_name": schema_name
                                },
                                "operational_metadata": {
                                    "status": "active",
                                    "owner": schema_owner,
                                    "last_modified": datetime.now().isoformat(),
                                    "last_accessed": datetime.now().isoformat(),
                                    "steward": connection_data.connection_name,
                                    "access_frequency": "Daily"
                                },
                                "business_metadata": {
                                    "description": f"Schema: {schema_name}",
                                    "tags": [],
                                    "classification": "General",
                                    "sensitivity_level": "Internal",
                                    "compliance_tags": []
                                }
                            })
                            assets_discovered += 1
                            
                            # List tables in schema with retry logic
                            tables_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table"
                            
                            # Retry logic for rate limiting and auth errors
                            max_retries = 5
                            retry_delay = 3
                            tables_response = None
                            
                            for attempt in range(1, max_retries + 1):
                                try:
                                    # Refresh token if we get 401
                                    if attempt > 1:
                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'    ⟳ Refreshing authentication (attempt {attempt}/{max_retries})'})}\n\n"
                                        await asyncio.sleep(0.1)
                                        
                                        # Get new access token using correct auth_url
                                        token_response = requests.post(
                                            auth_url,
                                            headers={"Authorization": f"Basic {auth_credentials}"},
                                            data={"grant_type": "client_credentials"},
                                            timeout=30
                                        )
                                        token_response.raise_for_status()
                                        access_token = token_response.json().get('access_token')
                                        headers = {"Authorization": f"Bearer {access_token}"}
                                    
                                    tables_response = requests.get(tables_url, headers=headers, timeout=30)
                                    
                                    if tables_response.status_code == 429:
                                        # Rate limited - wait for rate limit window to reset
                                        if attempt == 1:
                                            # First time hitting rate limit - wait for full window reset
                                            yield f"data: {json.dumps({'type': 'progress', 'message': f'    ⏸ Rate limit reached. Waiting {RATE_LIMIT_WAIT}s for rate limit window to reset...'})}\n\n"
                                            await asyncio.sleep(RATE_LIMIT_WAIT)
                                        else:
                                            # Subsequent retries - shorter wait
                                            wait_time = retry_delay * attempt
                                            yield f"data: {json.dumps({'type': 'progress', 'message': f'    ⏸ Still rate limited. Waiting {wait_time}s...'})}\n\n"
                                            await asyncio.sleep(wait_time)
                                        continue
                                    elif tables_response.status_code == 401:
                                        # Auth error - refresh token and retry
                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'    ⚠ Authentication expired. Refreshing token...'})}\n\n"
                                        await asyncio.sleep(1)
                                        continue
                                    else:
                                        # Success or other error - break retry loop
                                        break
                                        
                                except requests.exceptions.Timeout:
                                    if attempt < max_retries:
                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'    ⏱ Timeout. Retrying in {retry_delay}s...'})}\n\n"
                                        await asyncio.sleep(retry_delay)
                                        continue
                                    else:
                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'    ✗ Timeout after {max_retries} attempts for {schema_name}'})}\n\n"
                                        tables_response = None
                                        break
                                except Exception as e:
                                    if attempt < max_retries:
                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'    ⚠ Error: {str(e)}. Retrying...'})}\n\n"
                                        await asyncio.sleep(retry_delay)
                                        continue
                                    else:
                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'    ✗ Failed after {max_retries} attempts: {str(e)}'})}\n\n"
                                        tables_response = None
                                        break
                            
                            try:
                                if tables_response and tables_response.status_code == 200:
                                    tables_data = tables_response.json()
                                    tables_list = tables_data.get('result', tables_data.get('tables', []))
                                    
                                    for table in tables_list:
                                        table_id = table.get('tableId', 'unknown')
                                        table_name = table.get('tableName', table_id)
                                        table_type = table.get('tableType', 'BASE TABLE')
                                        asset_type = "View" if table_type == "VIEW" else "Table"
                                        table_owner = table.get('owner', table.get('tableOwner', 'Unknown'))
                                        
                                        # Get columns with retry logic
                                        columns = []
                                        columns_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table/{table_id}/column"
                                        
                                        for col_attempt in range(1, max_retries + 1):
                                            try:
                                                columns_response = requests.get(columns_url, headers=headers, timeout=30)
                                                
                                                if columns_response.status_code == 429:
                                                    # Rate limited - wait for rate limit window to reset
                                                    if col_attempt == 1:
                                                        # First time hitting rate limit - wait for full window reset
                                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'      ⏸ Rate limit reached on columns. Waiting {RATE_LIMIT_WAIT}s for rate limit window to reset...'})}\n\n"
                                                        await asyncio.sleep(RATE_LIMIT_WAIT)
                                                    else:
                                                        # Subsequent retries - shorter wait
                                                        wait_time = retry_delay * col_attempt
                                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'      ⏸ Still rate limited. Waiting {wait_time}s...'})}\n\n"
                                                        await asyncio.sleep(wait_time)
                                                    continue
                                                elif columns_response.status_code == 401:
                                                    # Auth error - refresh token and retry
                                                    yield f"data: {json.dumps({'type': 'progress', 'message': f'      ⚠ Auth expired. Refreshing token...'})}\n\n"
                                                    
                                                    token_response = requests.post(
                                                        auth_url,
                                                        headers={"Authorization": f"Basic {auth_credentials}"},
                                                        data={"grant_type": "client_credentials"},
                                                        timeout=30
                                                    )
                                                    token_response.raise_for_status()
                                                    access_token = token_response.json().get('access_token')
                                                    headers = {"Authorization": f"Bearer {access_token}"}
                                                    await asyncio.sleep(1)
                                                    continue
                                                elif columns_response.status_code == 200:
                                                    columns_data = columns_response.json()
                                                    columns_list = columns_data.get('result', columns_data.get('columns', []))
                                                    
                                                    for column in columns_list:
                                                        column_name = column.get('columnId', column.get('columnName', 'unknown'))
                                                        column_type = column.get('dataType', column.get('type', 'STRING'))
                                                        columns.append({
                                                            "name": column_name,
                                                            "type": column_type,
                                                            "mode": "NULLABLE",
                                                            "description": column.get('comment', '')
                                                        })
                                                    break
                                                else:
                                                    # Other error - log and continue
                                                    if col_attempt == max_retries:
                                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'      ⚠ Column fetch failed (HTTP {columns_response.status_code})'})}\n\n"
                                                    break
                                                    
                                            except requests.exceptions.Timeout:
                                                if col_attempt < max_retries:
                                                    yield f"data: {json.dumps({'type': 'progress', 'message': f'      ⏱ Column fetch timeout. Retrying...'})}\n\n"
                                                    await asyncio.sleep(retry_delay)
                                                    continue
                                                else:
                                                    break
                                            except Exception as e:
                                                if col_attempt < max_retries:
                                                    await asyncio.sleep(retry_delay)
                                                    continue
                                                else:
                                                    break
                                        
                                        # Add table as an asset
                                        discovered_assets.append({
                                            "id": f"{connection_data.account_domain}.{catalog_name}.{schema_name}.{table_name}",
                                            "name": table_name,
                                            "type": asset_type,
                                            "catalog": f"{connection_data.account_domain}.{catalog_name}.{schema_name}",
                                            "discovered_at": datetime.now().isoformat(),
                                            "status": "active",
                                            "description": f"{asset_type}: {table_name}",
                                            "connector_id": connector_id,
                                            "columns": columns,
                                            "num_rows": 0,
                                            "size_bytes": 0,
                                            "technical_metadata": {
                                                "asset_id": f"{connection_data.account_domain}.{catalog_name}.{schema_name}.{table_name}",
                                                "asset_type": asset_type,
                                                "location": f"{connection_data.account_domain} - {catalog_name}.{schema_name}.{table_name}",
                                                "format": f"Starburst {asset_type}",
                                                "size_bytes": 0,
                                                "num_rows": 0,
                                                "created_at": datetime.now().isoformat(),
                                                "source_system": "Starburst Galaxy",
                                                "storage_location": f"{connection_data.account_domain}/{catalog_name}/{schema_name}/{table_name}",
                                                "catalog_name": catalog_name,
                                                "schema_name": schema_name,
                                                "table_name": table_name,
                                                "column_count": len(columns),
                                                "data_type": table_type
                                            },
                                            "operational_metadata": {
                                                "status": "active",
                                                "owner": table_owner,
                                                "last_modified": datetime.now().isoformat(),
                                                "last_accessed": datetime.now().isoformat(),
                                                "steward": connection_data.connection_name,
                                                "access_frequency": "Daily"
                                            },
                                            "business_metadata": {
                                                "description": f"{asset_type}: {table_name}",
                                                "tags": table.get('tags', []),
                                                "classification": "General",
                                                "sensitivity_level": "Internal",
                                                "compliance_tags": []
                                            }
                                        })
                                        assets_discovered += 1
                                        
                                        yield f"data: {json.dumps({'type': 'progress', 'message': f'    ✓ {table_name} ({len(columns)} columns)'})}\n\n"
                                        await asyncio.sleep(0.05)
                                else:
                                    yield f"data: {json.dumps({'type': 'progress', 'message': f'    ✗ Failed to fetch tables for {schema_name} (HTTP {tables_response.status_code})'})}\n\n"
                            except Exception as e:
                                yield f"data: {json.dumps({'type': 'progress', 'message': f'    ✗ Error fetching tables for {schema_name}: {str(e)}'})}\n\n"
                except Exception as e:
                    yield f"data: {json.dumps({'type': 'progress', 'message': f'  ✗ Error fetching schemas for {catalog_name}: {str(e)}'})}\n\n"
            
            # Validate that credentials are not placeholders
            placeholder_credentials = [
                "your-starburst-client-id",
                "your-starburst-client-secret",
                "your-client-id",
                "your-client-secret",
                "your-project-id",
                "YOUR_PRIVATE_KEY",
                "your-service-account@your-project.iam.gserviceaccount.com"
            ]
            
            # Check for placeholder values in credentials
            if any(placeholder in str(connection_data.client_id).lower() for placeholder in placeholder_credentials):
                yield f"data: {json.dumps({'type': 'error', 'message': '❌ Placeholder Client ID detected. Please enter your actual Starburst Galaxy OAuth Client ID.'})}\n\n"
                return
            
            if any(placeholder in str(connection_data.client_secret).lower() for placeholder in placeholder_credentials):
                yield f"data: {json.dumps({'type': 'error', 'message': '❌ Placeholder Client Secret detected. Please enter your actual Starburst Galaxy OAuth Client Secret.'})}\n\n"
                return
            
            # Store the connector
            active_connectors.append({
                "id": connector_id,
                "name": connection_data.connection_name,
                "type": "Starburst Galaxy",
                "status": "active",
                "enabled": True,
                "last_run": datetime.now().isoformat(),
                "account_domain": connection_data.account_domain,
                "client_id": connection_data.client_id,
                "client_secret": connection_data.client_secret,
                "connection_name": connection_data.connection_name,
                "assets_count": assets_discovered
            })
            
            print(f"DEBUG: Created Starburst connector {connector_id} (streaming)")
            print(f"DEBUG: Discovered {assets_discovered} assets for this connector")
            print(f"DEBUG: Total assets now: {len(discovered_assets)}")
            
            # Save connectors and assets to file
            save_connectors()
            save_assets()
            
            print(f"DEBUG: Starburst connector and assets successfully saved to files (streaming)")
            
            yield f"data: {json.dumps({'type': 'complete', 'message': f'Successfully discovered {assets_discovered} assets', 'discovered_assets': assets_discovered, 'connector_id': connector_id})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': f'Connection failed: {str(e)}'})}\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    try:
        # Get list of active connector IDs
        active_connector_ids = set(conn['id'] for conn in active_connectors)
        
        # Filter assets to only include those from active connectors
        filtered_assets = []
        catalogs = set()
        
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            
            # Include assets from active connectors
            if asset_connector_id in active_connector_ids:
                filtered_assets.append(asset)
                catalogs.add(asset.get("catalog", "Unknown"))
            # Also include Starburst assets even if connector ID doesn't match (due to timestamp changes)
            elif asset_connector_id.startswith('starburst_') and any(conn.get('type') == 'Starburst Galaxy' for conn in active_connectors):
                filtered_assets.append(asset)
                catalogs.add(asset.get("catalog", "Unknown"))
            # Also include BigQuery assets for similar reasons
            elif asset_connector_id.startswith('bq_') and any(conn.get('type') == 'BigQuery' for conn in active_connectors):
                filtered_assets.append(asset)
                catalogs.add(asset.get("catalog", "Unknown"))
        
        # Get last scan time from most recent connector
        last_scan = None
        if active_connectors:
            try:
                last_scan = max([datetime.fromisoformat(c["last_run"]) for c in active_connectors])
            except (ValueError, KeyError):
                last_scan = None
        
        return DashboardStats(
            total_assets=len(filtered_assets),
            total_catalogs=len(catalogs),
            active_connectors=len([c for c in active_connectors if c["enabled"]]),
            last_scan=last_scan,
            monitoring_status="Active" if active_connectors else "Disabled"
        )
    except Exception as e:
        print(f"Error in get_dashboard_stats: {str(e)}")
        # Return default values on error to prevent frontend crashes
        return DashboardStats(
            total_assets=0,
            total_catalogs=0,
            active_connectors=0,
            last_scan=None,
            monitoring_status="Error"
        )

@app.get("/api/system/health", response_model=SystemHealth)
async def get_system_health():
    enabled_connectors = len([c for c in active_connectors if c["enabled"]])
    last_scan = None
    if active_connectors:
        last_scan = max([datetime.fromisoformat(c["last_run"]) for c in active_connectors])
    
    return SystemHealth(
        status="healthy",
        monitoring_enabled=len(active_connectors) > 0,
        connectors_enabled=enabled_connectors,
        connectors_total=len(active_connectors),
        last_scan=last_scan
    )

@app.get("/api/assets")
async def get_assets(
    page: int = Query(0, ge=0, description="Page number (0-based)"),
    size: int = Query(50, ge=1, le=100, description="Number of assets per page"),
    search: str = Query(None, description="Search term for asset name, type, or catalog"),
    catalog: str = Query(None, description="Filter by catalog"),
    asset_type: str = Query(None, description="Filter by asset type")
):
    """
    Get paginated assets with optional filtering.
    Returns assets from active connectors only.
    """
    import math
    
    # Filter assets to only include those from active connectors
    active_connector_ids = set(conn['id'] for conn in active_connectors)
    filtered_assets = []
    
    for asset in discovered_assets:
        asset_connector_id = asset.get('connector_id', '')
        
        # Include assets from active connectors ONLY
        if asset_connector_id in active_connector_ids:
            filtered_assets.append(asset)
    
    # Apply search filter
    if search:
        search_lower = search.lower()
        filtered_assets = [
            asset for asset in filtered_assets
            if (search_lower in asset.get('name', '').lower() or
                search_lower in asset.get('type', '').lower() or
                search_lower in asset.get('catalog', '').lower())
        ]
    
    # Apply catalog filter
    if catalog:
        filtered_assets = [
            asset for asset in filtered_assets
            if asset.get('catalog') == catalog
        ]
    
    # Apply asset type filter
    if asset_type:
        filtered_assets = [
            asset for asset in filtered_assets
            if asset.get('type') == asset_type
        ]
    
    # Calculate pagination
    total_assets = len(filtered_assets)
    total_pages = math.ceil(total_assets / size) if total_assets > 0 else 0
    start_idx = page * size
    end_idx = start_idx + size
    paginated_assets = filtered_assets[start_idx:end_idx]
    
    return {
        "assets": paginated_assets,
        "pagination": {
            "page": page,
            "size": size,
            "total": total_assets,
            "total_pages": total_pages,
            "has_next": page < total_pages - 1,
            "has_prev": page > 0
        }
    }


def detect_pii_in_column(column_name: str, column_type: str) -> tuple[bool, Optional[str]]:
    """Detect PII based on column name patterns"""
    column_name_lower = column_name.lower()
    
    # Email patterns
    if any(pattern in column_name_lower for pattern in ['email', 'e_mail', 'mail']):
        return True, "EMAIL"
    
    # Name patterns
    if any(pattern in column_name_lower for pattern in ['first_name', 'firstname', 'last_name', 'lastname', 
                                                          'full_name', 'fullname', 'name', 'customer_name']):
        return True, "NAME"
    
    # Phone patterns
    if any(pattern in column_name_lower for pattern in ['phone', 'mobile', 'cell', 'telephone', 'contact_number']):
        return True, "PHONE"
    
    # Address patterns
    if any(pattern in column_name_lower for pattern in ['address', 'street', 'city', 'zipcode', 'zip_code', 'postal']):
        return True, "ADDRESS"
    
    # SSN/ID patterns
    if any(pattern in column_name_lower for pattern in ['ssn', 'social_security', 'national_id', 'passport', 'license']):
        return True, "SENSITIVE_ID"
    
    # Credit card patterns
    if any(pattern in column_name_lower for pattern in ['credit_card', 'card_number', 'ccn', 'payment_card']):
        return True, "CREDIT_CARD"
    
    # Birth date patterns
    if any(pattern in column_name_lower for pattern in ['birth_date', 'birthdate', 'dob', 'date_of_birth']):
        return True, "DATE_OF_BIRTH"
    
    # Account number patterns
    if any(pattern in column_name_lower for pattern in ['account_number', 'account_no', 'bank_account']):
        return True, "ACCOUNT_NUMBER"
    
    return False, None

@app.get("/api/assets/{asset_id:path}")
async def get_asset_detail(asset_id: str):
    # Find the asset from active connectors only
    active_connector_ids = set(conn['id'] for conn in active_connectors)
    
    # Look for asset with flexible connector matching
    asset = None
    for a in discovered_assets:
        if a["id"] == asset_id:
            asset_connector_id = a.get("connector_id", "")
            # Include if exact match
            if asset_connector_id in active_connector_ids:
                asset = a
                break
            # Include Starburst assets even if connector ID doesn't match
            elif asset_connector_id.startswith('starburst_') and any(conn.get('type') == 'Starburst Galaxy' for conn in active_connectors):
                asset = a
                break
            # Include BigQuery assets even if connector ID doesn't match
            elif asset_connector_id.startswith('bq_') and any(conn.get('type') == 'BigQuery' for conn in active_connectors):
                asset = a
                break
    
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    # Get detailed information including columns and PII detection
    detailed_asset = asset.copy()
    
    # Use the metadata that's ALREADY in the asset (from discovery)
    # Only add metadata if it doesn't exist (for backward compatibility with old assets)
    if "technical_metadata" not in detailed_asset:
        detailed_asset["technical_metadata"] = {
            "asset_id": asset["id"],
            "asset_type": asset["type"],
            "created_at": asset.get("discovered_at"),
            "location": asset.get("location", "N/A"),
            "size_bytes": asset.get("size_bytes", 0),
            "num_rows": asset.get("num_rows", 0),
            "format": "BigQuery Native" if asset.get("connector_id", "").startswith("bq_") else "Starburst" if asset.get("connector_id", "").startswith("starburst_") else "N/A"
        }
    
    if "operational_metadata" not in detailed_asset:
        detailed_asset["operational_metadata"] = {
            "last_modified": asset.get("discovered_at", datetime.now().isoformat()),
            "last_accessed": datetime.now().isoformat(),
            "access_count": "N/A",
            "owner": "Unknown",
            "status": asset.get("status", "active"),
            "data_quality_score": 90
        }
    
    if "business_metadata" not in detailed_asset:
        detailed_asset["business_metadata"] = {
            "description": asset.get("description", "No description available"),
            "business_owner": "Unknown",
            "department": "N/A",
            "classification": "internal",
            "tags": [],
            "sensitivity_level": "medium"
        }
    
    # Get REAL columns with PII detection
    columns = []
    if asset["type"] in ["Table", "View"]:
        # Check if columns are already stored with the asset
        stored_columns = asset.get("columns", [])
        
        if stored_columns:
            # Use columns that were discovered and stored
            for col in stored_columns:
                col_name = col.get("name", "")
                col_type = col.get("type", "STRING")
                col_mode = col.get("mode", "NULLABLE")
                
                # Detect PII based on column name
                pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "nullable": col_mode in ["NULLABLE", "REPEATED"],
                    "description": col.get("description", ""),
                    "pii_detected": pii_detected,
                    "pii_type": pii_type
                })
        else:
            # Fallback: Look for column assets in Starburst (which stores columns as separate assets)
            connector_id = asset.get("connector_id")
            if connector_id and connector_id.startswith("starburst_"):
                table_full_id = asset["id"]
                column_assets = [a for a in discovered_assets if a.get("type") == "Column" and a.get("catalog", "").startswith(table_full_id)]
                
                for col_asset in column_assets:
                    col_name = col_asset["name"]
                    pii_detected, pii_type = detect_pii_in_column(col_name, "STRING")
                    columns.append({
                        "name": col_name,
                        "type": "STRING",
                        "nullable": True,
                        "description": col_asset.get("description", ""),
                        "pii_detected": pii_detected,
                        "pii_type": pii_type
                    })
    
    detailed_asset["columns"] = columns
    
    return detailed_asset

@app.get("/api/connectors")
async def get_connectors():
    return active_connectors

@app.get("/api/activities", response_model=List[Activity])
async def get_activities():
    return []

@app.post("/api/connectors/{connector_id}/toggle")
async def toggle_connector(connector_id: str):
    connector = next((c for c in active_connectors if c["id"] == connector_id), None)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    connector["enabled"] = not connector["enabled"]
    
    # Save updated connectors to file
    save_connectors()
    
    return {"message": f"Connector {connector_id} {'enabled' if connector['enabled'] else 'disabled'}"}

@app.delete("/api/connectors/{connector_id}")
async def delete_connector(connector_id: str):
    global active_connectors, discovered_assets
    
    # Find the connector
    connector = None
    for i, conn in enumerate(active_connectors):
        if conn["id"] == connector_id:
            connector = conn
            break
    
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    # Count assets to be deleted
    assets_before = len(discovered_assets)
    assets_to_delete = [asset for asset in discovered_assets if asset.get("connector_id") == connector_id]
    
    # Remove the connector
    active_connectors = [conn for conn in active_connectors if conn["id"] != connector_id]
    
    # Remove all assets associated with this connector
    discovered_assets = [asset for asset in discovered_assets if asset.get("connector_id") != connector_id]
    
    assets_after = len(discovered_assets)
    assets_deleted = len(assets_to_delete)
    
    print(f"DEBUG: Deleting connector {connector_id}")
    print(f"DEBUG: Assets before: {assets_before}, after: {assets_after}, deleted: {assets_deleted}")
    
    # Save updated data to file
    save_connectors()
    save_assets()
    
    print(f"DEBUG: Connector and {assets_deleted} assets successfully deleted and saved to files")
    
    return {
        "message": f"Connector '{connector['name']}' and {assets_deleted} associated assets have been deleted successfully",
        "assets_deleted": assets_deleted
    }

@app.post("/api/scan/start")

async def start_scan():
    return {"message": "Scan started", "scan_id": "scan_123"}

@app.post("/api/data/reload")
async def reload_data():
    """Reload assets and connectors from files into memory"""
    global discovered_assets, active_connectors
    
    # Reload from files
    load_connectors()
    load_assets()
    
    print(f"DEBUG: Reloaded data - {len(active_connectors)} connectors, {len(discovered_assets)} assets")
    
    return {
        "message": "Data reloaded successfully",
        "connectors": len(active_connectors),
        "assets": len(discovered_assets)
    }

@app.delete("/api/data/clear-all")
async def clear_all_data():
    """Clear all assets and connectors"""
    global discovered_assets, active_connectors
    
    # Count what we're clearing
    assets_count = len(discovered_assets)
    connectors_count = len(active_connectors)
    
    # Clear in memory
    discovered_assets = []
    active_connectors = []
    
    # Save empty arrays to files
    save_connectors()
    save_assets()
    
    print(f"DEBUG: Cleared all data - {connectors_count} connectors and {assets_count} assets deleted")
    
    return {
        "message": "All data cleared successfully",
        "assets_deleted": assets_count,
        "connectors_deleted": connectors_count
    }

@app.get("/api/scheduler/status")
async def get_scheduler_status():
    """Get the status of the background scheduler"""
    return {
        "scheduler_running": scheduler_running,
        "next_run": scheduler.get_job('continuous_sync').next_run_time.isoformat() if scheduler_running and scheduler.get_job('continuous_sync') else None
    }

@app.post("/api/scheduler/start")
async def start_background_scheduler():
    """Manually start the background scheduler"""
    start_scheduler()
    return {"message": "Background scheduler started"}

@app.post("/api/scheduler/stop")
async def stop_background_scheduler():
    """Manually stop the background scheduler"""
    stop_scheduler()
    return {"message": "Background scheduler stopped"}

# Startup and shutdown event handlers
@app.on_event("startup")
async def startup_event():
    """Start background scheduler when the application starts"""
    print("🚀 Application starting up...")
    print(f"📦 Loaded {len(active_connectors)} connectors and {len(discovered_assets)} assets")
    
    # Start the background scheduler
    start_scheduler()

@app.on_event("shutdown")
async def shutdown_event():
    """Stop background scheduler when the application shuts down"""
    print("🛑 Application shutting down...")
    stop_scheduler()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
