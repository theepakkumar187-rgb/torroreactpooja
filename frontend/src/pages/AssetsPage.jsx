import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Chip,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  TextField,
  InputAdornment,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tabs,
  Tab,
  Divider,
  Alert,
  CircularProgress,
  Pagination,
  Stack,
} from '@mui/material';
import {
  Search,
  Refresh,
  DataObject,
  FilterList,
  Visibility,
  Download,
  Warning,
  CheckCircle,
  Close,
} from '@mui/icons-material';

const AssetsPage = () => {
  const [assets, setAssets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [catalogFilter, setCatalogFilter] = useState('');
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [activeTab, setActiveTab] = useState(0);
  
  // Pagination state
  const [currentPage, setCurrentPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [totalAssets, setTotalAssets] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [allAssets, setAllAssets] = useState([]); // For filters
  const [bigqueryTotal, setBigqueryTotal] = useState(0);
  const [starburstTotal, setStarburstTotal] = useState(0);

  useEffect(() => {
    fetchAssets();
  }, [currentPage, pageSize, searchTerm, typeFilter, catalogFilter]);

  // Fetch totals separately (only when no filters are applied)
  useEffect(() => {
    if (!searchTerm && !typeFilter && !catalogFilter) {
      fetchTotals();
    }
  }, [searchTerm, typeFilter, catalogFilter]);

  const fetchTotals = async () => {
    try {
      // Backend limits size to 100; page through results to compute totals safely
      let page = 0;
      const size = 100;
      let fetchedAll = false;
      let bigqueryCount = 0;
      let starburstCount = 0;
      const aggregatedAssets = [];

      while (!fetchedAll) {
        const resp = await fetch(`http://localhost:8000/api/assets?page=${page}&size=${size}`);
        if (!resp.ok) {
          // Stop early on HTTP errors
          throw new Error(`HTTP ${resp.status}`);
        }
        const data = await resp.json();
        const assetsPage = Array.isArray(data.assets) ? data.assets : [];

        // Accumulate counts and assets for filters
        for (const asset of assetsPage) {
          const id = asset?.connector_id || '';
          if (id.startsWith('bq_')) bigqueryCount += 1;
          else if (id.startsWith('starburst_')) starburstCount += 1;
        }
        aggregatedAssets.push(...assetsPage);

        // Determine if there are more pages
        const hasNext = Boolean(data?.pagination?.has_next);
        if (hasNext) {
          page += 1;
        } else {
          fetchedAll = true;
        }
      }

      console.log('✅ Totals fetched:', { bigqueryCount, starburstCount, totalAssets: aggregatedAssets.length });
      setBigqueryTotal(bigqueryCount);
      setStarburstTotal(starburstCount);
      setAllAssets(aggregatedAssets); // For filter dropdowns
    } catch (error) {
      console.error('Error fetching totals:', error);
      // Fallback to zeros to avoid UI crashes
      setBigqueryTotal(0);
      setStarburstTotal(0);
      setAllAssets([]);
    }
  };

  const fetchAssets = async () => {
    try {
      setLoading(true);
      
      // Build query parameters
      const params = new URLSearchParams({
        page: currentPage.toString(),
        size: pageSize.toString(),
      });
      
      if (searchTerm) params.append('search', searchTerm);
      if (catalogFilter) params.append('catalog', catalogFilter);
      if (typeFilter) params.append('asset_type', typeFilter);
      
      const response = await fetch(`http://localhost:8000/api/assets?${params}`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const data = await response.json();
      const assetsList = Array.isArray(data.assets) ? data.assets : [];
      const pagination = data?.pagination || { total: 0, total_pages: 0 };
      
      setAssets(assetsList);
      setTotalAssets(pagination.total || assetsList.length);
      setTotalPages(pagination.total_pages || 0);
    } catch (error) {
      console.error('Error fetching assets:', error);
    } finally {
      setLoading(false);
    }
  };

  // Get unique types and catalogs for filter dropdowns
  const uniqueTypes = [...new Set(allAssets.map(asset => asset.type))];
  const uniqueCatalogs = [...new Set(allAssets.map(asset => asset.catalog))];
  
  // Count assets from BigQuery data source (use stored totals)
  const bigqueryAssets = bigqueryTotal;
  
  // Count assets from Starburst Galaxy data source (use stored totals)
  const starburstAssets = starburstTotal;

  const getDataSource = (connectorId) => {
    if (!connectorId) return 'Unknown';
    if (connectorId.startsWith('bq_')) return 'BigQuery';
    if (connectorId.startsWith('starburst_')) return 'Starburst Galaxy';
    return 'Unknown';
  };

  const getDataSourceColor = (connectorId) => {
    if (!connectorId) return 'default';
    if (connectorId.startsWith('bq_')) return 'success';
    if (connectorId.startsWith('starburst_')) return 'info';
    return 'default';
  };

  const handleViewAsset = async (assetId) => {
    try {
      setDetailsDialogOpen(true);
      setSelectedAsset(null); // Clear previous data
      const response = await fetch(`http://localhost:8000/api/assets/${encodeURIComponent(assetId)}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setSelectedAsset(data);
      setActiveTab(0);
    } catch (error) {
      console.error('Error fetching asset details:', error);
      alert('Failed to load asset details. Please try again.');
      setDetailsDialogOpen(false);
    }
  };

  const handleCloseDialog = () => {
    setDetailsDialogOpen(false);
    setSelectedAsset(null);
  };

  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
  };

  // Pagination handlers
  const handlePageChange = (event, page) => {
    setCurrentPage(page - 1); // Convert to 0-based
  };

  const handlePageSizeChange = (event) => {
    setPageSize(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  // Search and filter handlers
  const handleSearchChange = (event) => {
    setSearchTerm(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  const handleTypeFilterChange = (event) => {
    setTypeFilter(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  const handleCatalogFilterChange = (event) => {
    setCatalogFilter(event.target.value);
    setCurrentPage(0); // Reset to first page
  };

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
  };

  const formatNumber = (num) => {
    if (!num) return '0';
    return num.toLocaleString();
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1" sx={{ fontWeight: 600, fontFamily: 'Comfortaa' }}>
          Discovered Assets
        </Typography>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={() => {
              fetchAssets();
              if (!searchTerm && !typeFilter && !catalogFilter) {
                fetchTotals();
              }
            }}
            disabled={loading}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<Download />}
            color="primary"
          >
            Export
          </Button>
        </Box>
      </Box>

      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={4}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <DataObject sx={{ mr: 1, color: 'primary.main' }} />
                <Typography variant="h6">Total Assets</Typography>
              </Box>
              <Typography variant="h4" sx={{ fontWeight: 'bold' }}>
                {totalAssets}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={4}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <DataObject sx={{ mr: 1, color: 'success.main' }} />
                <Typography variant="h6">BigQuery Assets</Typography>
              </Box>
              <Typography variant="h4" sx={{ fontWeight: 'bold' }}>
                {bigqueryAssets}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={4}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <DataObject sx={{ mr: 1, color: 'info.main' }} />
                <Typography variant="h6">Starburst Assets</Typography>
              </Box>
              <Typography variant="h4" sx={{ fontWeight: 'bold' }}>
                {starburstAssets}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  placeholder="Search assets..."
                  value={searchTerm}
                  onChange={handleSearchChange}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <Search />
                      </InputAdornment>
                    ),
                  }}
                />
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Type</InputLabel>
                <Select
                  value={typeFilter}
                  label="Type"
                  onChange={handleTypeFilterChange}
                >
                  <MenuItem value="">All Types</MenuItem>
                  {uniqueTypes.map(type => (
                    <MenuItem key={type} value={type}>{type}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth>
                <InputLabel>Catalog</InputLabel>
                <Select
                  value={catalogFilter}
                  label="Catalog"
                  onChange={handleCatalogFilterChange}
                >
                  <MenuItem value="">All Catalogs</MenuItem>
                  {uniqueCatalogs.map(catalog => (
                    <MenuItem key={catalog} value={catalog}>{catalog}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={2}>
              <Button
                fullWidth
                variant="outlined"
                startIcon={<FilterList />}
                onClick={() => {
                  setSearchTerm('');
                  setTypeFilter('');
                  setCatalogFilter('');
                  setCurrentPage(0);
                }}
              >
                Clear
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      <Card>
        <CardContent>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 600, fontFamily: 'Comfortaa' }}>
            Asset Inventory ({totalAssets} assets)
          </Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Name</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>Catalog</TableCell>
                  <TableCell>Data Source</TableCell>
                  <TableCell>Discovered</TableCell>
                  <TableCell>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {assets.map((asset) => (
                  <TableRow key={asset.id}>
                    <TableCell>
                      <Box sx={{ display: 'flex', alignItems: 'center' }}>
                        <DataObject sx={{ mr: 1, color: 'text.secondary' }} />
                        <Typography variant="body2" sx={{ fontWeight: 500, fontFamily: 'Roboto' }}>
                          {asset.name}
                        </Typography>
                      </Box>
                    </TableCell>
                    <TableCell>
                      <Chip 
                        label={asset.type} 
                        size="small" 
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" sx={{ fontFamily: 'Roboto' }}>
                        {asset.catalog}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip 
                        label={getDataSource(asset.connector_id)} 
                        size="small" 
                        color={getDataSourceColor(asset.connector_id)}
                      />
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="text.secondary" sx={{ fontFamily: 'Roboto' }}>
                        {new Date(asset.discovered_at).toLocaleDateString()}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Button
                        size="small"
                        startIcon={<Visibility />}
                        variant="outlined"
                        onClick={() => handleViewAsset(asset.id)}
                      >
                        View
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          
          {/* Pagination Controls */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 3, px: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography variant="body2" color="text.secondary">
                Showing {assets.length} of {totalAssets} assets
              </Typography>
              <FormControl size="small" sx={{ minWidth: 80 }}>
                <Select
                  value={pageSize}
                  onChange={handlePageSizeChange}
                  displayEmpty
                >
                  <MenuItem value={25}>25</MenuItem>
                  <MenuItem value={50}>50</MenuItem>
                  <MenuItem value={100}>100</MenuItem>
                </Select>
              </FormControl>
              <Typography variant="body2" color="text.secondary">
                per page
              </Typography>
            </Box>
            
            <Pagination
              count={totalPages}
              page={currentPage + 1}
              onChange={handlePageChange}
              color="primary"
              showFirstButton
              showLastButton
              disabled={loading}
            />
          </Box>
        </CardContent>
      </Card>

      {/* Asset Details Dialog */}
      <Dialog
        open={detailsDialogOpen}
        onClose={handleCloseDialog}
        maxWidth="lg"
        fullWidth
      >
        {!selectedAsset ? (
          <Box sx={{ p: 4, display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', minHeight: 300, gap: 2 }}>
            <CircularProgress />
            <Typography>Loading asset details...</Typography>
          </Box>
        ) : (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    {selectedAsset.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                    <Chip label={selectedAsset.type} size="small" color="primary" />
                    <Chip label={selectedAsset.catalog} size="small" variant="outlined" />
                  </Box>
                </Box>
                <Button onClick={handleCloseDialog} startIcon={<Close />}>
                  Close
                </Button>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              <Tabs value={activeTab} onChange={handleTabChange} sx={{ mb: 3 }}>
                <Tab label="Technical Metadata" />
                <Tab label="Operational Metadata" />
                <Tab label="Business Metadata" />
                <Tab label="Columns & PII" />
              </Tabs>

              {/* Technical Metadata Tab */}
              {activeTab === 0 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Technical Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure technical_metadata exists and has all required fields
                    const technicalMetadata = selectedAsset?.technical_metadata || {};
                    const safeAssetId = technicalMetadata.asset_id || selectedAsset?.id || 'N/A';
                    const safeAssetType = technicalMetadata.asset_type || selectedAsset?.type || 'Unknown';
                    const safeLocation = technicalMetadata.location || 'N/A';
                    const safeFormat = technicalMetadata.format || 'Unknown';
                    const safeSizeBytes = technicalMetadata.size_bytes || 0;
                    const safeNumRows = technicalMetadata.num_rows || 0;
                    const safeCreatedAt = technicalMetadata.created_at || selectedAsset?.discovered_at || new Date().toISOString();
                    
                    return (
                      <Grid container spacing={2}>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Asset ID
                              </Typography>
                              <Typography variant="body1" sx={{ wordBreak: 'break-all' }}>
                                {safeAssetId}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Asset Type
                              </Typography>
                              <Typography variant="body1">
                                {safeAssetType}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Location
                              </Typography>
                              <Typography variant="body1">
                                {safeLocation}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Format
                              </Typography>
                              <Typography variant="body1">
                                {safeFormat}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Size
                              </Typography>
                              <Typography variant="body1">
                                {formatBytes(safeSizeBytes)}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Number of Rows
                              </Typography>
                              <Typography variant="body1">
                                {formatNumber(safeNumRows)}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Created At
                              </Typography>
                              <Typography variant="body1">
                                {new Date(safeCreatedAt).toLocaleString()}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {/* Operational Metadata Tab */}
              {activeTab === 1 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Operational Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure operational_metadata exists and has all required fields
                    const operationalMetadata = selectedAsset?.operational_metadata || {};
                    const safeStatus = operationalMetadata.status || 'Unknown';
                    const safeOwner = typeof operationalMetadata.owner === 'object' && operationalMetadata.owner?.roleName 
                      ? operationalMetadata.owner.roleName 
                      : operationalMetadata.owner || 'Unknown';
                    const safeLastModified = operationalMetadata.last_modified || selectedAsset?.discovered_at || new Date().toISOString();
                    const safeLastAccessed = operationalMetadata.last_accessed || new Date().toISOString();
                    const safeAccessCount = operationalMetadata.access_count || 'N/A';
                    const safeDataQualityScore = operationalMetadata.data_quality_score || 0;
                    
                    return (
                      <Grid container spacing={2}>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Status
                              </Typography>
                              <Chip 
                                label={safeStatus} 
                                color="success" 
                                size="small"
                              />
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Owner
                              </Typography>
                              <Typography variant="body1">
                                {safeOwner}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Last Modified
                              </Typography>
                              <Typography variant="body1">
                                {new Date(safeLastModified).toLocaleString()}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Last Accessed
                              </Typography>
                              <Typography variant="body1">
                                {new Date(safeLastAccessed).toLocaleString()}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Access Count
                              </Typography>
                              <Typography variant="body1">
                                {safeAccessCount}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Data Quality Score
                              </Typography>
                              <Typography variant="body1">
                                {safeDataQualityScore}%
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {/* Business Metadata Tab */}
              {activeTab === 2 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Business Metadata
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure business_metadata exists and has all required fields
                    const businessMetadata = selectedAsset?.business_metadata || {};
                    const safeDescription = businessMetadata.description || selectedAsset?.description || 'No description available';
                    const safeBusinessOwner = businessMetadata.business_owner || 'Unknown';
                    const safeDepartment = businessMetadata.department || 'N/A';
                    const safeClassification = businessMetadata.classification || 'internal';
                    const safeSensitivityLevel = businessMetadata.sensitivity_level || 'medium';
                    const safeTags = businessMetadata.tags || [];
                    
                    return (
                      <Grid container spacing={2}>
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Description
                              </Typography>
                              <Typography variant="body1">
                                {safeDescription}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Business Owner
                              </Typography>
                              <Typography variant="body1">
                                {safeBusinessOwner}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Department
                              </Typography>
                              <Typography variant="body1">
                                {safeDepartment}
                              </Typography>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Classification
                              </Typography>
                              <Chip 
                                label={safeClassification} 
                                color="primary" 
                                size="small"
                              />
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={6}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom>
                                Sensitivity Level
                              </Typography>
                              <Chip 
                                label={safeSensitivityLevel} 
                                color="warning" 
                                size="small"
                              />
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom sx={{ fontWeight: 600 }}>
                                Table Tags
                              </Typography>
                              <Box sx={{ display: 'flex', gap: 1, mt: 1, flexWrap: 'wrap' }}>
                                {safeTags && safeTags.length > 0 ? (
                                  safeTags.map((tag, index) => (
                                    <Chip 
                                      key={index} 
                                      label={tag} 
                                      size="small" 
                                      variant="outlined"
                                      sx={{ 
                                        backgroundColor: '#e3f2fd', 
                                        color: '#1565c0', 
                                        border: '1px solid #90caf9',
                                        fontWeight: 600
                                      }}
                                    />
                                  ))
                                ) : (
                                  <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                    No table tags
                                  </Typography>
                                )}
                              </Box>
                            </CardContent>
                          </Card>
                        </Grid>
                        <Grid item xs={12}>
                          <Card variant="outlined">
                            <CardContent>
                              <Typography color="text.secondary" gutterBottom sx={{ fontWeight: 600 }}>
                                Column Tags
                              </Typography>
                              <Box sx={{ mt: 1 }}>
                                {selectedAsset?.columns && selectedAsset.columns.length > 0 ? (
                                  selectedAsset.columns.map((column, colIndex) => {
                                    // ONLY show real tags from column.tags field, NOT from description
                                    const columnTags = column.tags || [];
                                    
                                    if (columnTags.length > 0) {
                                      return (
                                        <Box key={colIndex} sx={{ mb: 2, pb: 2, borderBottom: colIndex < selectedAsset.columns.length - 1 ? '1px solid #e0e0e0' : 'none' }}>
                                          <Typography variant="body2" sx={{ fontWeight: 600, mb: 1, color: '#1976d2' }}>
                                            {column.name}
                                          </Typography>
                                          <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                                            {columnTags.map((tag, tagIndex) => (
                                              <Chip 
                                                key={tagIndex} 
                                                label={tag} 
                                                size="small" 
                                                variant="outlined"
                                                sx={{ 
                                                  backgroundColor: '#f3e5f5', 
                                                  color: '#7b1fa2', 
                                                  border: '1px solid #ce93d8',
                                                  fontWeight: 600
                                                }}
                                              />
                                            ))}
                                          </Box>
                                        </Box>
                                      );
                                    }
                                    return null;
                                  }).filter(Boolean)
                                ) : null}
                                {(!selectedAsset?.columns || selectedAsset.columns.length === 0 || 
                                  !selectedAsset.columns.some(col => col.tags && col.tags.length > 0)) && (
                                  <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                    No column tags
                                  </Typography>
                                )}
                              </Box>
                            </CardContent>
                          </Card>
                        </Grid>
                      </Grid>
                    );
                  })()}
                </Box>
              )}

              {/* Columns & PII Tab */}
              {activeTab === 3 && (
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, fontWeight: 600 }}>
                    Columns & PII Detection
                  </Typography>
                  {(() => {
                    // Defensive programming: ensure columns exist and handle missing data
                    const columns = selectedAsset?.columns || [];
                    const piiColumns = columns.filter(col => col.pii_detected);
                    
                    if (columns.length > 0) {
                      return (
                        <>
                          {piiColumns.length > 0 && (
                            <Alert severity="warning" sx={{ mb: 2 }}>
                              {piiColumns.length} column(s) contain PII data
                            </Alert>
                          )}
                          <TableContainer component={Paper} variant="outlined">
                            <Table>
                              <TableHead>
                                <TableRow>
                                  <TableCell>Column Name</TableCell>
                                  <TableCell>Data Type</TableCell>
                                  <TableCell>Nullable</TableCell>
                                  <TableCell>Description</TableCell>
                                  <TableCell>PII Status</TableCell>
                                </TableRow>
                              </TableHead>
                              <TableBody>
                                {columns.map((column, index) => (
                                  <TableRow key={index}>
                                    <TableCell>
                                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                        {column.name || 'Unknown'}
                                      </Typography>
                                    </TableCell>
                                    <TableCell>
                                      <Chip label={column.type || 'Unknown'} size="small" variant="outlined" />
                                    </TableCell>
                                    <TableCell>
                                      {column.nullable ? 'Yes' : 'No'}
                                    </TableCell>
                                    <TableCell>
                                      <Typography variant="body2" color="text.secondary">
                                        {column.description || 'No description'}
                                      </Typography>
                                    </TableCell>
                                    <TableCell>
                                      {column.pii_detected ? (
                                        <Chip 
                                          icon={<Warning />}
                                          label={`PII: ${column.pii_type || 'Unknown'}`} 
                                          color="error" 
                                          size="small"
                                        />
                                      ) : (
                                        <Chip 
                                          icon={<CheckCircle />}
                                          label="No PII" 
                                          color="success" 
                                          size="small"
                                        />
                                      )}
                                    </TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </TableContainer>
                        </>
                      );
                    } else {
                      return (
                        <Alert severity="info">
                          No column information available for this asset type.
                        </Alert>
                      );
                    }
                  })()}
                </Box>
              )}
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialog} variant="outlined">
                Close
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>
    </Box>
  );
};

export default AssetsPage;
