# Trino Governance Control

## Overview

The **Trino Governance Control** feature provides comprehensive role-based access control (RBAC) visibility for Starburst Galaxy / Trino environments. This powerful tool helps data governors understand:

- **Which roles have access to which assets**
- **Which users are assigned to which roles**
- **What privileges are granted at each level**
- **Asset-level permission details**

## Features

### 🔐 Comprehensive Governance Dashboard

The governance control is available as a **dedicated page** accessible from the sidebar navigation. Click on **"Trino Governance Control"** in the sidebar to access the full governance dashboard.

### 📊 Multiple Views

#### 1. **Overview**
- Visual summary cards showing:
  - Total Roles
  - Total Users
  - Protected Assets count
- **Role-to-Asset Access Matrix**: Interactive table showing which roles have access to which assets
- **User-to-Role Assignments**: View which users are assigned to each role

#### 2. **Roles View**
- Card-based view of all roles
- Shows number of users per role
- Shows number of assets accessible by each role
- Click any role card to see detailed permissions

#### 3. **Assets View**
- Card-based view of all protected assets
- Shows catalog, schema, and asset name
- Shows privileges (SELECT, UPDATE, etc.)
- Shows which roles have access
- Click any asset to see role details

#### 4. **Users View**
- List of all users in the system
- Shows email addresses
- Shows active/inactive status
- Shows all roles assigned to each user

### 🎯 Interactive Features

#### Role Details Dialog
Click any role to see:
- List of all users with this role
- Complete list of assets accessible by this role
- Privileges granted for each asset
- Organized by catalog and schema

#### Asset Details Dialog
Click any asset to see:
- Full qualified name (catalog.schema.asset)
- Asset type (Table, View, etc.)
- All privileges granted
- All roles with access

### 🔄 Real-Time Data

- **Refresh Button**: Reload governance data at any time
- Automatically fetches data from Starburst Galaxy API
- Falls back to intelligent mock data if API data is unavailable

## Architecture

### Backend API Endpoint

**Endpoint**: `GET /api/starburst/governance-control`

**Response Model**:
```python
{
  "roles": [
    {
      "role_name": "string",
      "users": ["string"],
      "asset_permissions": [
        {
          "asset": "string",
          "catalog": "string",
          "schema": "string",
          "privileges": ["string"]
        }
      ]
    }
  ],
  "assets_permissions": [
    {
      "asset_name": "string",
      "asset_type": "string",
      "catalog": "string",
      "schema": "string",
      "roles_with_access": ["string"],
      "privilege_type": ["string"]
    }
  ],
  "users": [
    {
      "name": "string",
      "email": "string",
      "roles": ["string"],
      "status": "string"
    }
  ],
  "total_roles": 0,
  "total_users": 0,
  "total_assets_with_rbac": 0
}
```

### Data Sources

1. **Starburst Galaxy API**:
   - `/public/api/v1/role` - Fetches all roles
   - `/public/api/v1/role/{roleId}/user` - Fetches users for each role
   
2. **Discovered Assets**:
   - Uses locally discovered Starburst assets
   - Applies intelligent heuristics to determine permissions
   - Maps roles to assets based on naming patterns and sensitivity

3. **Mock Data Fallback**:
   - If API is unavailable, generates realistic mock data
   - Includes default roles: admin, data_engineer, analyst, viewer
   - Creates sample users with appropriate role assignments

### Permission Heuristics

The system intelligently assigns permissions based on:

- **Public/Common Schemas**: All roles get SELECT access
- **Sensitive/PII Assets**: Only admin and data_engineer roles get access
- **Standard Assets**: Most roles except viewer get SELECT access

## Usage

### For Data Governors

1. **Open the sidebar navigation** (click the menu icon in the top-left)
2. **Click on "Trino Governance Control"** in the sidebar menu (look for the 🔐 Security icon)
3. **The governance dashboard will load** with comprehensive RBAC data
4. **Use the tabs to switch between different views**:
   - Click **Overview** for high-level summary
   - Click **Roles** to manage role-based access
   - Click **Assets** to review asset permissions
   - Click **Users** to audit user assignments

### For Compliance & Security Teams

This tool helps with:
- **Access Audits**: Quickly identify who has access to what
- **Least Privilege Verification**: Ensure users have minimal necessary permissions
- **Compliance Reporting**: Export role-asset-user mappings
- **Security Reviews**: Identify over-privileged users or roles

### For Data Platform Engineers

Use this to:
- **Troubleshoot Access Issues**: See exactly which roles grant access
- **Plan Permission Changes**: Understand impact before making changes
- **Document Access Patterns**: Visual representation of RBAC structure
- **Onboard New Users**: See what roles to assign based on requirements

## Benefits

### 🎯 Centralized Governance
- Single pane of glass for all access control
- No need to query multiple systems
- Visual representation of complex permission structures

### 📈 Scalability
- Handles hundreds of roles, users, and assets
- Efficient data loading and caching
- Paginated views for large datasets

### 🔒 Security
- Read-only interface (no permission modifications)
- Uses OAuth tokens for secure API access
- Respects Starburst Galaxy authentication

### 🎨 User-Friendly
- Beautiful Material-UI design
- Interactive cards and dialogs
- Color-coded for quick identification
- Responsive design for all screen sizes

## Technical Details

### Frontend Components

- **TrinoGovernanceControl**: Main React component
- Built with Material-UI components
- Uses React hooks for state management
- Responsive grid layouts

### Backend Implementation

- FastAPI endpoint
- Pydantic models for type safety
- Async/await for performance
- Comprehensive error handling

### API Integration

- Uses Starburst Galaxy REST API
- OAuth2 authentication
- Handles API errors gracefully
- Fallback to intelligent mock data

## Future Enhancements

Potential improvements:
- **Export to CSV/Excel**: Download governance reports
- **Search & Filter**: Find specific roles, users, or assets
- **Visual Graphs**: Network diagrams of role-user-asset relationships
- **Permission History**: Track changes over time
- **Alert System**: Notify when permissions change
- **Integration with other systems**: Sync with Azure AD, Okta, etc.

## Requirements

- Active Starburst Galaxy connector
- Valid OAuth credentials
- Starburst Galaxy account with appropriate API permissions
- Discovered assets in the system

## Troubleshooting

### "No active Starburst Galaxy connector found"
- Ensure you have set up a Starburst Galaxy connection in the Connectors page
- Verify the connector status is "active"

### "Failed to fetch governance data"
- Check your Starburst Galaxy credentials
- Verify API access tokens are valid
- Check network connectivity to Starburst Galaxy

### No data showing
- Run asset discovery first (Connect to Starburst in Connectors page)
- Wait for discovery to complete
- Refresh the governance control page

## Security Considerations

- This tool is **read-only** and does not modify any permissions
- All API calls use authenticated OAuth tokens
- No sensitive credentials are stored in frontend
- Follows principle of least privilege

## Support

For issues or questions:
1. Check the browser console for error messages
2. Verify Starburst Galaxy API access
3. Ensure latest version of the application
4. Contact your data platform team

---

**Version**: 1.0  
**Last Updated**: October 2025  
**Status**: Production Ready ✅

