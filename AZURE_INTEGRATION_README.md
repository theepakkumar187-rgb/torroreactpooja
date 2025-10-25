# Azure Data Governance Integration

This document describes the Azure Data Governance integration capabilities added to the Torro Data Intelligence Platform.

## Features

### 1. Azure Purview Data Catalogue Integration
- **On-demand extraction**: Extract data catalogue from Azure Purview instantly
- **Scheduled extraction**: Set up periodic data catalogue extractions (hourly, daily, weekly)
- **Asset filtering**: Filter by specific asset types (SQL tables, storage blobs, etc.)
- **Authentication**: Support for Azure AD authentication with client credentials

### 2. Supported Azure Services
- Azure SQL Database
- Azure Storage (Blob Storage)
- Azure Data Factory
- Azure Synapse Analytics
- Azure Databricks
- Custom asset types

### 3. Data Extraction Capabilities
- **Periodic extraction**: Automated scheduled extractions
- **On-demand extraction**: Manual trigger for immediate data catalogue updates
- **Asset metadata**: Extract comprehensive metadata including:
  - Asset names and qualified names
  - Descriptions and owners
  - Tags and labels
  - Creation and modification timestamps
  - Custom properties

## API Endpoints

### Connection Management
- `POST /api/azure/test-connection` - Test Azure Purview connection
- `GET /api/azure/connections` - List all Azure connections
- `DELETE /api/azure/connections/{connector_id}` - Delete a connection

### Data Extraction
- `POST /api/azure/extract-catalogue` - Extract data catalogue on demand
- `POST /api/azure/schedule-extraction` - Schedule periodic extractions

### Scheduler Management
- `GET /api/azure/scheduler/status` - Get scheduler status
- `POST /api/azure/scheduler/stop` - Stop all scheduled extractions

## Configuration

### Azure Purview Setup
1. Create an Azure Purview account
2. Register an Azure AD application
3. Grant the application access to Purview
4. Note down:
   - Purview account name
   - Tenant ID
   - Client ID
   - Client Secret

### Environment Variables
```bash
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
AZURE_PURVIEW_ACCOUNT=your-purview-account
```

## Usage

### 1. Frontend Integration
The frontend now supports Azure Purview as a resource type in the Marketplace page:
- Select "Azure Purview" as the resource type
- Enter Purview account name, Tenant ID, and Client ID
- Click "Search" to extract data catalogue

### 2. Scheduled Extractions
Set up periodic extractions using the API:
```bash
curl -X POST "http://localhost:8000/api/azure/schedule-extraction" \
  -H "Content-Type: application/json" \
  -d '{
    "purview_account_name": "my-purview-account",
    "tenant_id": "12345678-1234-1234-1234-123456789012",
    "client_id": "12345678-1234-1234-1234-123456789012",
    "client_secret": "your-client-secret",
    "connection_name": "scheduled_connection",
    "extraction_type": "scheduled",
    "schedule_interval": "daily",
    "asset_types": ["azure_sql_table", "azure_storage_blob"]
  }'
```

### 3. On-demand Extraction
Extract data catalogue immediately:
```bash
curl -X POST "http://localhost:8000/api/azure/extract-catalogue" \
  -H "Content-Type: application/json" \
  -d '{
    "purview_account_name": "my-purview-account",
    "tenant_id": "12345678-1234-1234-1234-123456789012",
    "client_id": "12345678-1234-1234-1234-123456789012",
    "client_secret": "your-client-secret",
    "connection_name": "on_demand_connection",
    "extraction_type": "on_demand",
    "asset_types": null
  }'
```

## Security Considerations

1. **Client Secret Management**: Store client secrets securely, not in code
2. **Network Security**: Use HTTPS for all API communications
3. **Access Control**: Implement proper RBAC for Azure Purview access
4. **Audit Logging**: Monitor and log all data extraction activities

## Monitoring and Troubleshooting

### Scheduler Status
Check scheduler status:
```bash
curl "http://localhost:8000/api/azure/scheduler/status"
```

### Connection Testing
Test Azure Purview connection:
```bash
curl -X POST "http://localhost:8000/api/azure/test-connection" \
  -H "Content-Type: application/json" \
  -d '{
    "purview_account_name": "my-purview-account",
    "tenant_id": "12345678-1234-1234-1234-123456789012",
    "client_id": "12345678-1234-1234-1234-123456789012",
    "client_secret": "your-client-secret",
    "connection_name": "test_connection"
  }'
```

## Dependencies

The following Python packages are required:
- `azure-purview-catalog==1.0.0b4`
- `azure-identity==1.15.0`
- `azure-core==1.29.5`
- `schedule==1.2.0`
- `apscheduler==3.10.4`

## Future Enhancements

1. **Tag Management**: Support for creating and updating tags in Azure Purview
2. **Data Lineage**: Extract and visualize data lineage from Purview
3. **Custom Connectors**: Support for additional Azure services
4. **Real-time Updates**: Webhook-based real-time data catalogue updates
5. **Advanced Filtering**: More sophisticated filtering and search capabilities
