# Data Lineage - Quick Start Guide

## 🚀 Start Using Data Lineage in 3 Steps

### Step 1: Start the Servers (if not already running)

**Backend:**
```bash
cd backend
python main.py
```
✅ Server should start at `http://localhost:8000`

**Frontend:**
```bash
cd frontend
npm start
```
✅ App should open at `http://localhost:5173`

---

### Step 2: Ensure You Have Discovered Assets

The Data Lineage feature requires **discovered assets** (tables and views) to work.

**If you haven't discovered assets yet:**

1. Go to **Data Discovery** → **Connectors**
2. Add a BigQuery or Starburst connector
3. Run the discovery process
4. Wait for assets to be discovered

**Verify assets exist:**
- Go to **Data Discovery** → **Discovered Assets**
- You should see a list of tables/views
- ✅ At least one **View** is required for lineage relationships

---

### Step 3: View the Lineage

1. Click **Data Discovery** in the sidebar
2. Click **Data Lineage**
3. 🎉 See your interactive lineage graph!

**What you'll see:**
- **Green boxes** = Tables (source data)
- **Purple boxes** = Views (derived data)
- **Arrows** = Data flow (what feeds into what)

**Interact with the graph:**
- 🖱️ **Click and drag** to pan
- 🔍 **Mouse wheel** to zoom
- 👆 **Click any node** to see details
- 🔎 **Use filters** to focus on specific assets

---

## 📊 Example Scenario

### If You Have BigQuery Views

Imagine you have:
```sql
-- customer_orders_summary (View)
SELECT 
  c.customer_id,
  COUNT(o.order_id) as total_orders
FROM `project.dataset.customers` c
JOIN `project.dataset.orders` o 
  ON c.customer_id = o.customer_id
GROUP BY c.customer_id
```

**The lineage graph will show:**
```
customers (Table) ────┐
                      ├──→ customer_orders_summary (View)
orders (Table) ───────┘
```

---

## 🔍 Testing the API Directly

Want to see the raw lineage data?

1. **Open browser**: `http://localhost:8000/api/docs`
2. **Find endpoint**: `GET /api/lineage`
3. **Click "Try it out"**
4. **Click "Execute"**
5. **See response**: JSON with nodes and edges

**Example Response:**
```json
{
  "nodes": [
    {
      "id": "torrodataset.banking_pii.customer_records",
      "name": "customer_records",
      "type": "Table",
      "catalog": "torrodataset.banking_pii",
      "connector_id": "bq_torrodataset_12345",
      "source_system": "BigQuery"
    },
    {
      "id": "torrodataset.banking_pii.customer_summary",
      "name": "customer_summary",
      "type": "View",
      "catalog": "torrodataset.banking_pii",
      "connector_id": "bq_torrodataset_12345",
      "source_system": "BigQuery"
    }
  ],
  "edges": [
    {
      "source": "torrodataset.banking_pii.customer_records",
      "target": "torrodataset.banking_pii.customer_summary",
      "relationship": "feeds_into"
    }
  ]
}
```

---

## ⚡ Quick Troubleshooting

### "No lineage data available"

**Cause**: No Views discovered, or Views don't reference other tables

**Fix**:
1. Ensure you have **Views** (not just Tables)
2. Views must have SQL definitions that reference other tables
3. Click "Refresh" button to reload

---

### "Loading forever"

**Cause**: Backend might not be running or CORS issue

**Fix**:
1. Check backend is running: `http://localhost:8000`
2. Check browser console for errors (F12)
3. Restart backend if needed

---

### Empty graph or missing connections

**Cause**: Views might use aliases or external tables

**Fix**:
1. Check that views use fully qualified table names
2. Ensure referenced tables are in discovered assets
3. Only shows lineage within discovered assets

---

## 🎨 Customize Your View

### Apply Filters

**Search for specific asset:**
- Type in the search box: `customer`
- Only shows assets matching "customer"

**Filter by type:**
- Select "View" to see only views
- Select "Table" to see only tables

**Filter by source:**
- Select "BigQuery" to see only BigQuery assets
- Select "Starburst Galaxy" to see only Starburst assets

**Clear all filters:**
- Click "Clear" button to reset

---

## 🔥 Advanced Tips

### Zoom to Fit
- Click the "Fit View" button in the controls
- Or use Ctrl/Cmd + 0

### Focus on Specific Asset
- Click any node to see its details
- Future: API endpoint for single-asset lineage

### Export (Coming Soon)
- Screenshot: Use browser's screenshot tool
- Export: Feature coming in future update

---

## 📚 More Information

- **Full Documentation**: See `DATA_LINEAGE_README.md`
- **Implementation Details**: See `DATA_LINEAGE_IMPLEMENTATION_SUMMARY.md`
- **API Docs**: Visit `http://localhost:8000/docs`

---

## ✅ You're Ready!

Data Lineage is now part of your Data Discovery toolkit. Use it to:
- 📊 Understand data dependencies
- 🔍 Trace data origins
- 🎯 Assess impact of changes
- 📈 Document data flows

**Navigate to: Data Discovery → Data Lineage and start exploring!**

---

## 🆘 Need Help?

**Check logs:**
- Backend: Terminal running `python main.py`
- Frontend: Browser console (F12)

**Common commands:**
```bash
# Restart backend
cd backend
python main.py

# Restart frontend
cd frontend
npm start

# Check Python version
python --version  # Should be 3.11+

# Check Node version
node --version    # Should be 16+
```

**Still stuck?**
- Review the full documentation in `DATA_LINEAGE_README.md`
- Check that connectors are properly configured
- Ensure assets have been discovered
- Verify service account has proper permissions

---

**Happy Lineage Tracking! 🎉**

