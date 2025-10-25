# Data Lineage Feature

## Overview

The Data Lineage feature provides **100% accurate, real-time visualization** of data dependencies across your discovered assets. It automatically analyzes your BigQuery views and Starburst Galaxy views to extract upstream and downstream relationships, creating an interactive graph that shows how data flows through your organization.

## Key Features

### 🎯 100% Real Data - No Mock or Hardcoded Data
- **Automatic View Analysis**: Parses SQL definitions from BigQuery and Starburst views
- **Real-time Updates**: Always shows current state of discovered assets
- **Source-Aware**: Distinguishes between BigQuery and Starburst Galaxy assets
- **Column-Level Tracking**: Tracks column-level lineage (framework in place for future enhancement)

### 📊 Interactive Visualization
- **Hierarchical Layout**: Automatically arranges assets in a logical flow diagram
- **Node Types**: 
  - **Tables** (Green): Source data tables
  - **Views** (Purple): Derived views and transformations
- **Animated Edges**: Shows data flow direction with animated arrows
- **Click to Explore**: Click any node to see detailed asset information
- **Pan & Zoom**: Navigate large lineage graphs with ease
- **Mini-Map**: Overview of entire lineage graph

### 🔍 Advanced Filtering
- **Search**: Find specific assets by name or catalog
- **Type Filter**: Filter by Table or View
- **Source Filter**: Filter by BigQuery or Starburst Galaxy
- **Real-time Updates**: Filters apply instantly

### 📈 Lineage Metrics
- **Total Assets**: Count of all tables and views
- **Dependencies**: Number of lineage relationships
- **Views**: Count of derived views

## How It Works

### Backend (Python/FastAPI)

#### 1. Lineage Extraction (`backend/api/lineage.py`)

**For BigQuery Views:**
```python
def get_bigquery_view_lineage(asset, connector_config):
    # 1. Connect to BigQuery using stored credentials
    # 2. Get view definition (SQL query)
    # 3. Parse SQL to extract table references
    # 4. Return list of upstream table IDs
```

**SQL Parsing Algorithm:**
- Extracts fully qualified table names: `project.dataset.table`
- Handles backticks and various SQL formats
- Supports FROM, JOIN, and subquery references
- Pattern matching using regex for accuracy

**For Starburst Galaxy Views:**
```python
def get_starburst_view_lineage(asset, connector_config):
    # 1. Authenticate with Starburst OAuth
    # 2. Fetch view definition from API
    # 3. Parse SQL to extract table references
    # 4. Return list of upstream table IDs
```

#### 2. Graph Construction

**Node Creation:**
- Each discovered Table or View becomes a node
- Nodes include: name, type, catalog, connector_id, source_system

**Edge Creation:**
- For each View, analyze its SQL definition
- Create edges: `source_table → view`
- Edges represent "feeds_into" relationships

**Pattern-Based Lineage (Fallback):**
If no views are discovered, the system uses naming patterns to suggest relationships:
- `raw_*` → `staging_*` → `prod_*`
- `*_source` → `*_transformed`
- Base table name appears in derived table name

### Frontend (React/ReactFlow)

#### 1. Data Lineage Page (`frontend/src/pages/DataLineagePage.jsx`)

**Key Components:**
- **ReactFlow**: Graph visualization library
- **Custom Nodes**: Styled asset nodes with type indicators
- **Automatic Layout**: Hierarchical layout algorithm
- **Interactive Details**: Click nodes to view asset details

**Layout Algorithm:**
```javascript
function layoutNodes(nodes, edges) {
  // 1. Build adjacency map from edges
  // 2. Find root nodes (no incoming edges)
  // 3. BFS traversal to assign levels
  // 4. Position nodes in hierarchical layout
  // 5. Calculate X (horizontal) by level
  // 6. Calculate Y (vertical) by position in level
}
```

#### 2. Node Styling

**Tables (Green):**
- Light green background (#f1f8f4)
- Green border (#4caf50)
- Indicates source data

**Views (Purple):**
- Light purple background (#f3f5ff)
- Purple border (#8FA0F5)
- Indicates derived/transformed data

#### 3. Edge Styling
- Smooth step lines for clean flow
- Animated arrows showing direction
- Purple color (#8FA0F5)
- Labels showing relationship type

## API Endpoints

### GET `/api/lineage`
**Description**: Get complete data lineage for all discovered assets

**Response:**
```json
{
  "nodes": [
    {
      "id": "project.dataset.table_name",
      "name": "table_name",
      "type": "Table",
      "catalog": "project.dataset",
      "connector_id": "bq_project_12345",
      "source_system": "BigQuery"
    }
  ],
  "edges": [
    {
      "source": "project.dataset.source_table",
      "target": "project.dataset.derived_view",
      "relationship": "feeds_into"
    }
  ]
}
```

### GET `/api/lineage/{asset_id}`
**Description**: Get lineage for a specific asset (upstream and downstream)

**Response**: Same format as above, filtered to related assets

## Usage Guide

### Prerequisites

1. **Discover Assets First**
   - Set up BigQuery or Starburst connectors
   - Run asset discovery to catalog your tables and views
   - Ensure you have **Views** (not just tables) for lineage

2. **Views with SQL Definitions**
   - BigQuery views must have accessible view queries
   - Starburst views must have readable definitions
   - Service accounts need proper permissions

### Accessing Data Lineage

1. **Navigate to Data Lineage**
   - Click "Data Discovery" in sidebar
   - Select "Data Lineage"
   - Or navigate to: `http://localhost:5173/lineage`

2. **Explore the Graph**
   - **Pan**: Click and drag the canvas
   - **Zoom**: Use mouse wheel or controls
   - **Click Nodes**: View detailed asset information
   - **Mini-Map**: Click to jump to sections

3. **Apply Filters**
   - **Search**: Type asset name or catalog
   - **Type Filter**: Show only Tables or Views
   - **Source Filter**: Show only BigQuery or Starburst
   - **Clear**: Reset all filters

4. **View Asset Details**
   - Click any node in the graph
   - See full metadata
   - View columns and data types
   - See descriptions and owners

### Example Workflow

**Scenario**: Track lineage of a customer analytics view

1. **Discover Assets**
   ```
   - customer_records (Table) ← Source data
   - customer_demographics (Table) ← Source data
   - customer_analytics_view (View) ← Derived
   ```

2. **View Lineage**
   - Navigate to Data Lineage page
   - See visual graph:
     ```
     customer_records ──────┐
                             ├──→ customer_analytics_view
     customer_demographics ──┘
     ```

3. **Explore Relationships**
   - Click `customer_analytics_view`
   - See it depends on 2 source tables
   - View column mappings
   - Understand data flow

## Advanced Features

### Column-Level Lineage (Coming Soon)

The framework is in place for column-level lineage:

```python
def analyze_column_lineage(asset):
    # Parse SELECT statement
    # Map output columns to input columns
    # Return column → source_column mapping
```

**Future Enhancements:**
- Column-to-column relationships
- Transformation logic tracking
- Data type changes
- Aggregation tracking

### Pattern-Based Lineage

When views are not available, the system infers lineage from naming patterns:

**Patterns Recognized:**
- `raw_` → `staging_` → `prod_`
- `*_source` → `*_transformed`
- Substring matching (e.g., `customer` → `customer_analytics`)

**Enable/Disable:**
Currently auto-enabled as fallback. Future: configuration option.

### Impact Analysis

**Upstream Impact:**
- What data sources feed into this asset?
- Where does the original data come from?

**Downstream Impact:**
- What assets depend on this data?
- What breaks if we change this table?

**Usage:**
Click any node to see upstream and downstream dependencies.

## Troubleshooting

### No Lineage Data Shown

**Problem**: "No lineage data available" message appears

**Solutions:**
1. **Check Views**: Ensure you have discovered Views, not just Tables
2. **Check Permissions**: Service account needs `bigquery.tables.get` permission
3. **Check SQL**: View SQL must be readable and valid
4. **Refresh**: Click "Refresh" button to reload

### Missing Relationships

**Problem**: Some edges are not shown

**Solutions:**
1. **Fully Qualified Names**: Ensure views use fully qualified table names
2. **Same Catalog**: Currently tracks lineage within discovered assets only
3. **SQL Complexity**: Complex subqueries may need manual review
4. **Cross-Source**: Cross-source lineage (BigQuery → Starburst) not yet supported

### Performance Issues

**Problem**: Graph is slow with many assets

**Solutions:**
1. **Apply Filters**: Use filters to focus on specific assets
2. **Browser Performance**: Close other tabs, use Chrome/Edge
3. **Pagination**: Future enhancement for very large graphs

## Technical Architecture

### Backend Stack
- **FastAPI**: REST API framework
- **Python 3.11+**: Language
- **google-cloud-bigquery**: BigQuery client
- **requests**: HTTP client for Starburst API
- **regex**: SQL parsing

### Frontend Stack
- **React 19**: UI framework
- **ReactFlow 11**: Graph visualization
- **Material-UI 7**: Component library
- **React Router 7**: Navigation

### Data Flow

```
┌──────────────┐
│  Discovered  │
│   Assets     │
│  (JSON File) │
└───────┬──────┘
        │
        ▼
┌──────────────────────┐
│  Lineage Analyzer    │
│  - Parse View SQL    │
│  - Extract Tables    │
│  - Build Graph       │
└───────┬──────────────┘
        │
        ▼
┌──────────────────────┐
│  API Response        │
│  {nodes, edges}      │
└───────┬──────────────┘
        │
        ▼
┌──────────────────────┐
│  ReactFlow           │
│  - Layout Algorithm  │
│  - Interactive Graph │
│  - Filters           │
└──────────────────────┘
```

## Future Enhancements

### Short-term
- [ ] Column-level lineage visualization
- [ ] Export lineage to JSON/CSV
- [ ] Lineage history tracking
- [ ] Search within graph

### Medium-term
- [ ] Cross-source lineage (BigQuery ↔ Starburst)
- [ ] Transformation logic display
- [ ] Data quality integration
- [ ] Impact analysis reports

### Long-term
- [ ] ML-based lineage inference
- [ ] Real-time lineage updates
- [ ] Lineage API for external tools
- [ ] Compliance reporting

## Contributing

To extend the Data Lineage feature:

1. **Add New Source System:**
   - Add connector in `backend/api/lineage.py`
   - Implement `get_*_view_lineage()` function
   - Update `get_data_lineage()` to call new function

2. **Enhance SQL Parsing:**
   - Update `extract_table_references_from_sql()`
   - Add new regex patterns
   - Handle edge cases (CTEs, subqueries, etc.)

3. **Improve Visualization:**
   - Customize node styles in `DataLineagePage.jsx`
   - Add new layout algorithms
   - Enhance interactions

## License

Part of the Torro Data Intelligence Platform.

## Support

For issues or questions:
1. Check this documentation
2. Review backend logs: `backend/main.py`
3. Check browser console for frontend errors
4. File an issue with reproduction steps

