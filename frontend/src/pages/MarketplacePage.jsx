import React, { useState } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Button,
  TextField,
  FormControl,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Link,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Snackbar,
  Select,
  MenuItem,
  InputLabel,
} from '@mui/material';
import {
  ArrowBack,
  Search,
  Label,
  TableChart,
  Add,
  Delete,
  Publish,
} from '@mui/icons-material';

const MarketplacePage = () => {
  const [resourceType, setResourceType] = useState('GCP');
  const [gcpProject, setGcpProject] = useState('');
  const [dataset, setDataset] = useState('');
  const [tableName, setTableName] = useState('');
  const [loading, setLoading] = useState(false);
  const [tableData, setTableData] = useState(null);
  const [error, setError] = useState(null);
  
  // Tag management states
  const [columnTagDialogOpen, setColumnTagDialogOpen] = useState(false);
  const [tableTagDialogOpen, setTableTagDialogOpen] = useState(false);
  const [selectedColumn, setSelectedColumn] = useState(null);
  const [selectedColumnForTag, setSelectedColumnForTag] = useState('');
  const [newTag, setNewTag] = useState('');
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [snackbarMessage, setSnackbarMessage] = useState('');
  const [publishing, setPublishing] = useState(false);
  const [sqlDialogOpen, setSqlDialogOpen] = useState(false);
  const [sqlCommands, setSqlCommands] = useState([]);
  const [billingInfo, setBillingInfo] = useState({ requiresBilling: false, message: '' });

  const handleSearch = async () => {
    if (!gcpProject || !dataset || !tableName) {
      setError('Please fill in all required fields');
      return;
    }

    setLoading(true);
    setError(null);
    setTableData(null);

    try {
      let response;
      
      if (resourceType === 'GCP') {
        // API call to BigQuery table details endpoint
        response = await fetch('http://localhost:8000/api/bigquery/table-details', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            projectId: gcpProject,
            datasetId: dataset,
            tableId: tableName,
          }),
        });
      } else if (resourceType === 'Starburst Galaxy') {
        // API call to Starburst table details endpoint
        response = await fetch('http://localhost:8000/api/starburst/table-details', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            catalog: gcpProject,  // Using gcpProject state for catalog
            schema: dataset,       // Using dataset state for schema
            tableId: tableName,
          }),
        });
      } else if (resourceType === 'Azure Purview') {
        // API call to Azure Purview data catalogue endpoint
        response = await fetch('http://localhost:8000/api/azure/extract-catalogue', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            purview_account_name: gcpProject,  // Using gcpProject state for purview account
            tenant_id: dataset,                // Using dataset state for tenant ID
            client_id: tableName,              // Using tableName state for client ID
            client_secret: '',                 // This should be provided securely
            connection_name: 'marketplace_connection',
            extraction_type: 'on_demand',
            asset_types: null
          }),
        });
      } else {
        throw new Error('Unsupported resource type');
      }

      if (!response.ok) {
        throw new Error('Failed to fetch table details');
      }

      const data = await response.json();
      setTableData(data);
    } catch (err) {
      console.error('API call failed:', err.message);
      setError(`Failed to fetch table details: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Tag management handlers
  const handleAddColumnTag = (columnName = null) => {
    if (columnName && typeof columnName === 'string') {
      setSelectedColumnForTag(columnName);
    } else {
      setSelectedColumnForTag('');
    }
    setColumnTagDialogOpen(true);
  };

  const handleAddTableTag = () => {
    setTableTagDialogOpen(true);
  };

  const handleAddTag = () => {
    if (!newTag.trim()) return;

    if (selectedColumnForTag) {
      // Add tag to specific column
      const updatedColumns = tableData.columns.map(col => 
        col.name === selectedColumnForTag 
          ? { ...col, tags: [...(col.tags || []), newTag.trim()] }
          : col
      );
      setTableData({ ...tableData, columns: updatedColumns });
      setSnackbarMessage(`Tag "${newTag}" added to column "${selectedColumnForTag}"`);
    } else {
      // Add tag to all columns (table-level tag)
      const updatedColumns = tableData.columns.map(col => ({
        ...col,
        tags: [...(col.tags || []), newTag.trim()]
      }));
      setTableData({ ...tableData, columns: updatedColumns });
      setSnackbarMessage(`Tag "${newTag}" added to all columns`);
    }

    setNewTag('');
    setSelectedColumnForTag('');
    setColumnTagDialogOpen(false);
    setTableTagDialogOpen(false);
    setSnackbarOpen(true);
  };

  const handlePublishTags = async () => {
    if (!tableData) return;

    setPublishing(true);
    try {
      let response;
      
      if (resourceType === 'GCP') {
        // Prepare the data for BigQuery publishing
        const publishData = {
          projectId: gcpProject,
          datasetId: dataset,
          tableId: tableName,
          columns: tableData.columns.map(col => ({
            name: col.name,
            tags: col.tags || []
          }))
        };

        response = await fetch('http://localhost:8000/api/bigquery/publish-tags', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(publishData),
        });
      } else if (resourceType === 'Starburst Galaxy') {
        // Prepare the data for Starburst publishing
        const publishData = {
          catalog: gcpProject,
          schema: dataset,
          tableId: tableName,
          columnTags: tableData.columns.map(col => ({
            columnName: col.name,
            tags: col.tags || []
          }))
        };

        response = await fetch('http://localhost:8000/api/starburst/publish-tags', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(publishData),
        });
      } else if (resourceType === 'Azure Purview') {
        // Azure Purview tag publishing (placeholder - would need specific implementation)
        throw new Error('Azure Purview tag publishing is not yet implemented');
      } else {
        throw new Error('Unsupported resource type');
      }

      if (!response.ok) {
        throw new Error(`Failed to publish tags to ${resourceType}`);
      }

      const result = await response.json();
      setSqlCommands(result.sqlCommands || []);
      setBillingInfo({
        requiresBilling: result.requiresBilling || false,
        message: result.billingMessage || ''
      });
      setSqlDialogOpen(true);
    } catch (err) {
      console.error('Publish failed:', err.message);
      setSnackbarMessage(`Failed to publish tags: ${err.message}`);
      setSnackbarOpen(true);
    } finally {
      setPublishing(false);
    }
  };

  const handleRemoveTag = (columnName, tagToRemove) => {
    const updatedColumns = tableData.columns.map(col => 
      col.name === columnName 
        ? { ...col, tags: col.tags.filter(tag => tag !== tagToRemove) }
        : col
    );
    setTableData({ ...tableData, columns: updatedColumns });
    setSnackbarMessage(`Tag "${tagToRemove}" removed from column "${columnName}"`);
    setSnackbarOpen(true);
  };

  const handleCloseDialogs = () => {
    setColumnTagDialogOpen(false);
    setTableTagDialogOpen(false);
    setSelectedColumn(null);
    setSelectedColumnForTag('');
    setNewTag('');
  };

  return (
    <Box sx={{ 
      width: '100%',
      minHeight: '100vh',
      p: 3,
      bgcolor: '#fafafa'
    }}>
      <Card sx={{ 
        width: '100%',
        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.1)',
        borderRadius: 2
      }}>
        <CardContent sx={{ p: 4 }}>
          {/* Header */}
          <Typography 
            variant="h4" 
            component="h1" 
            sx={{ 
              fontWeight: 600, 
              color: '#1976d2',
              mb: 4,
              textAlign: 'left'
            }}
          >
            Publish Data Assets to Marketplace
          </Typography>

          {/* Resource Type Selection */}
          <FormControl component="fieldset" sx={{ mb: 4 }}>
            <FormLabel 
              component="legend" 
              sx={{ 
                color: '#1976d2', 
                fontWeight: 600, 
                mb: 2,
                fontSize: '1rem'
              }}
            >
              Resource Type *
            </FormLabel>
            <RadioGroup
              value={resourceType}
              onChange={(e) => setResourceType(e.target.value)}
              row
              sx={{ gap: 2 }}
            >
              <FormControlLabel 
                value="GCP" 
                control={<Radio />} 
                label="GCP" 
                sx={{ 
                  '& .MuiFormControlLabel-label': { 
                    fontWeight: 500,
                    color: '#333'
                  }
                }}
              />
              <FormControlLabel 
                value="Starburst Galaxy" 
                control={<Radio />} 
                label="Starburst Galaxy" 
                sx={{ 
                  '& .MuiFormControlLabel-label': { 
                    fontWeight: 500,
                    color: '#333'
                  }
                }}
              />
              <FormControlLabel 
                value="Azure Purview" 
                control={<Radio />} 
                label="Azure Purview" 
                sx={{ 
                  '& .MuiFormControlLabel-label': { 
                    fontWeight: 500,
                    color: '#333'
                  }
                }}
              />
            </RadioGroup>
          </FormControl>

          {/* Data Asset Details */}
          <Grid container spacing={3} sx={{ mb: 4 }}>
            {resourceType === 'GCP' && (
              <>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      GCP Project *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="GCP Project"
                      value={gcpProject}
                      onChange={(e) => setGcpProject(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Dataset *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="Dataset"
                      value={dataset}
                      onChange={(e) => setDataset(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Table name *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="Table"
                      value={tableName}
                      onChange={(e) => setTableName(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
              </>
            )}
            
            {resourceType === 'Starburst Galaxy' && (
              <>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Catalog *
              </Typography>
                    <TextField
                      fullWidth
                      placeholder="Catalog"
                      value={gcpProject}
                      onChange={(e) => setGcpProject(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
        </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Schema *
                    </Typography>
              <TextField
                fullWidth
                      placeholder="Schema"
                      value={dataset}
                      onChange={(e) => setDataset(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
            </Grid>
                <Grid item xs={12} sm={5}>
              <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Table name *
                    </Typography>
                    <TextField
                      fullWidth
                      placeholder="Table"
                      value={tableName}
                      onChange={(e) => setTableName(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
              </FormControl>
            </Grid>
              </>
            )}
            
            {resourceType === 'Azure Purview' && (
              <>
                <Grid item xs={12} sm={5}>
              <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Purview Account Name *
                    </Typography>
                    <TextField
                      fullWidth
                      placeholder="my-purview-account"
                      value={gcpProject}
                      onChange={(e) => setGcpProject(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
              </FormControl>
            </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Tenant ID *
                    </Typography>
                    <TextField
                fullWidth
                      placeholder="12345678-1234-1234-1234-123456789012"
                      value={dataset}
                      onChange={(e) => setDataset(e.target.value)}
                variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
                </Grid>
                <Grid item xs={12} sm={5}>
                  <FormControl fullWidth>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: '#1976d2', 
                        fontWeight: 600, 
                        mb: 1,
                        fontSize: '0.875rem'
                      }}
                    >
                      Client ID *
                    </Typography>
                    <TextField
                      fullWidth
                      placeholder="12345678-1234-1234-1234-123456789012"
                      value={tableName}
                      onChange={(e) => setTableName(e.target.value)}
                      variant="outlined"
                      size="small"
                      style={{ width: '100%', minWidth: '300px' }}
                      sx={{
                        width: '100% !important',
                        minWidth: '300px !important',
                        '& .MuiOutlinedInput-root': {
                          backgroundColor: '#f5f5f5',
                          width: '100% !important',
                          minWidth: '300px !important',
                          '& fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&:hover fieldset': {
                            borderColor: '#e0e0e0',
                          },
                          '&.Mui-focused fieldset': {
                            borderColor: '#1976d2',
                          },
                        },
                      }}
                    />
                  </FormControl>
            </Grid>
              </>
            )}
          </Grid>

          {/* Action Button */}
          <Box sx={{ 
            display: 'flex', 
            justifyContent: 'flex-end', 
            alignItems: 'center' 
          }}>
            <Button
              variant="outlined"
              startIcon={loading ? <CircularProgress size={20} /> : <Search />}
              onClick={handleSearch}
              disabled={loading}
              sx={{
                borderColor: '#1976d2',
                color: '#1976d2',
                fontWeight: 500,
                px: 3,
                py: 1,
                '&:hover': {
                  borderColor: '#1565c0',
                  backgroundColor: 'rgba(25, 118, 210, 0.04)',
                },
                '&:disabled': {
                  borderColor: '#e0e0e0',
                  color: '#9e9e9e',
                },
              }}
            >
              {loading ? 'Searching...' : 'Search'}
            </Button>
          </Box>

          {/* Error Message */}
          {error && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {error}
            </Alert>
          )}

          {/* Table Results */}
          {tableData && (
            <Box sx={{ mt: 4 }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  Table: {tableData.tableName}
          </Typography>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button
                    variant="outlined"
                    startIcon={<Label />}
                    onClick={handleAddTableTag}
                    sx={{
                      color: '#1976d2',
                      borderColor: '#1976d2',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#f5f5f5',
                        borderColor: '#1976d2',
                      },
                    }}
                  >
                    Add Table Tags
                  </Button>
                  <Button
                    variant="outlined"
                    startIcon={<TableChart />}
                    onClick={handleAddColumnTag}
                    sx={{
                      color: '#1976d2',
                      borderColor: '#1976d2',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#f5f5f5',
                        borderColor: '#1976d2',
                      },
                    }}
                  >
                    Add Column Tags
                  </Button>
                  <Button
                    variant="contained"
                    startIcon={publishing ? <CircularProgress size={20} /> : <Publish />}
                    onClick={handlePublishTags}
                    disabled={publishing || !tableData}
                    sx={{
                      backgroundColor: '#4caf50',
                      color: 'white',
                      px: 2,
                      py: 1,
                      fontSize: '0.875rem',
                      fontWeight: 500,
                      textTransform: 'none',
                      '&:hover': {
                        backgroundColor: '#45a049',
                      },
                      '&:disabled': {
                        backgroundColor: '#e0e0e0',
                        color: '#9e9e9e',
                      },
                    }}
                  >
                    {publishing ? 'Publishing...' : 'Publish'}
                  </Button>
                </Box>
              </Box>
              <TableContainer component={Paper} sx={{ mt: 2 }}>
            <Table>
              <TableHead>
                    <TableRow sx={{ backgroundColor: '#f5f5f5' }}>
                      <TableCell sx={{ fontWeight: 600 }}>Column Name</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Type</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Mode</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Description</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>PII Found</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>Tags</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                    {tableData.columns.map((column, index) => (
                      <TableRow key={index}>
                        <TableCell sx={{ fontWeight: 500 }}>{column.name}</TableCell>
                    <TableCell>
                      <Chip 
                            label={column.type} 
                        size="small" 
                            color="primary" 
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell>
                      <Chip 
                            label={column.mode} 
                        size="small" 
                            color={column.mode === 'REQUIRED' ? 'error' : 'default'}
                            variant="outlined"
                      />
                    </TableCell>
                        <TableCell>{column.description}</TableCell>
                    <TableCell>
                          <Chip 
                            label={column.piiFound ? 'Yes' : 'No'} 
                            size="small" 
                            color={column.piiFound ? 'error' : 'success'}
                            variant="filled"
                          />
                    </TableCell>
                    <TableCell>
                          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', alignItems: 'center' }}>
                            {column.tags && column.tags.length > 0 ? (
                              column.tags.map((tag, tagIndex) => (
                                <Chip 
                                  key={tagIndex}
                                  label={tag} 
                          size="small"
                                  onDelete={() => handleRemoveTag(column.name, tag)}
                                  deleteIcon={<Delete fontSize="small" />}
                                  sx={{
                                    backgroundColor: '#e3f2fd',
                                    color: '#1565c0',
                                    border: '1px solid #90caf9',
                                    fontWeight: 600,
                                    fontSize: '0.75rem',
                                    '&:hover': {
                                      backgroundColor: '#bbdefb',
                                    },
                                    '& .MuiChip-deleteIcon': {
                                      color: '#1565c0',
                                      '&:hover': {
                                        color: '#d32f2f',
                                      }
                                    }
                                  }}
                                  variant="filled"
                                />
                              ))
                            ) : (
                              <Typography 
                                variant="body2" 
                                sx={{ 
                                  color: '#666', 
                                  fontStyle: 'italic',
                                  fontSize: '0.75rem'
                                }}
                              >
                                NIL
                              </Typography>
                            )}
                            <IconButton
                          size="small"
                              onClick={() => handleAddColumnTag(column.name)}
                              sx={{
                                ml: 1,
                                color: '#1976d2',
                                '&:hover': {
                                  backgroundColor: 'rgba(25, 118, 210, 0.04)',
                                }
                              }}
                            >
                              <Add fontSize="small" />
                            </IconButton>
                      </Box>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
            </Box>
          )}

          {/* Column Tag Dialog */}
          <Dialog open={columnTagDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
            <DialogTitle>
              {selectedColumnForTag && selectedColumnForTag !== '' ? `Add Tag to Column: ${selectedColumnForTag}` : 'Add Tag to Column'}
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
                <FormControl fullWidth>
                  <InputLabel>Select Column</InputLabel>
                  <Select
                    value={selectedColumnForTag}
                    onChange={(e) => setSelectedColumnForTag(e.target.value)}
                    label="Select Column"
                  >
                    {tableData?.columns.map((column) => (
                      <MenuItem key={column.name} value={column.name}>
                        {column.name}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <TextField
                  fullWidth
                  label="Tag Name"
                  value={newTag}
                  onChange={(e) => setNewTag(e.target.value)}
                  placeholder="Enter tag name (e.g., PII, SENSITIVE, REQUIRED)"
                  variant="outlined"
                  onKeyPress={(e) => {
                    if (e.key === 'Enter') {
                      handleAddTag();
                    }
                  }}
                />
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Cancel</Button>
              <Button 
                onClick={handleAddTag} 
                variant="contained"
                disabled={!newTag.trim() || !selectedColumnForTag}
              >
                Add Tag
              </Button>
            </DialogActions>
          </Dialog>

          {/* Table Tag Dialog */}
          <Dialog open={tableTagDialogOpen} onClose={handleCloseDialogs} maxWidth="sm" fullWidth>
            <DialogTitle>Add Tag to All Columns</DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2 }}>
                <TextField
                  fullWidth
                  label="Tag Name"
                  value={newTag}
                  onChange={(e) => setNewTag(e.target.value)}
                  placeholder="Enter tag name (e.g., PRODUCTION, ANALYTICS, COMPLIANCE)"
                  variant="outlined"
                  onKeyPress={(e) => {
                    if (e.key === 'Enter') {
                      handleAddTag();
                    }
                  }}
                />
                <Typography variant="body2" sx={{ mt: 1, color: '#666' }}>
                  This tag will be added to all columns in the table.
                </Typography>
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialogs}>Cancel</Button>
              <Button 
                onClick={handleAddTag} 
                variant="contained"
                disabled={!newTag.trim()}
              >
                Add to All Columns
              </Button>
            </DialogActions>
          </Dialog>

          {/* Log Message Dialog */}
          <Dialog open={sqlDialogOpen} onClose={() => setSqlDialogOpen(false)} maxWidth="md" fullWidth>
            <DialogTitle>
              BigQuery Operation Result
            </DialogTitle>
            <DialogContent>
              <Box sx={{ mt: 2 }}>
                {/* Show the actual log message */}
                <Alert severity={billingInfo.message?.includes('✅') ? 'success' : 'error'} sx={{ mb: 3 }}>
                  <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
                    {billingInfo.message?.includes('✅') ? '✅ Operation Successful' : '❌ Operation Failed'}
                  </Typography>
                  <Typography variant="body2" sx={{ mb: 2, fontFamily: 'monospace', fontSize: '0.875rem' }}>
                    {billingInfo.message || 'No log message available. Check the backend logs for details.'}
                  </Typography>
                </Alert>

                {/* Show SQL commands if available */}
                {sqlCommands.length > 0 && (
                  <Box sx={{ mt: 3 }}>
                    <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                      Generated SQL Commands:
                    </Typography>
                    <Paper sx={{ p: 2, backgroundColor: '#f5f5f5', maxHeight: '300px', overflow: 'auto' }}>
                      {sqlCommands.map((command, index) => (
                        <Box key={index} sx={{ mb: 2 }}>
                          <Typography variant="body2" component="pre" sx={{ 
                            fontFamily: 'monospace', 
                            fontSize: '0.875rem',
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-word',
                            backgroundColor: 'white',
                            p: 2,
                            borderRadius: 1,
                            border: '1px solid #e0e0e0'
                          }}>
                            {command}
                          </Typography>
                        </Box>
                      ))}
                    </Paper>
                  </Box>
                )}
              </Box>
            </DialogContent>
            <DialogActions>
              <Button onClick={() => setSqlDialogOpen(false)}>
                Close
              </Button>
            </DialogActions>
          </Dialog>

          {/* Snackbar for notifications */}
          <Snackbar
            open={snackbarOpen}
            autoHideDuration={3000}
            onClose={() => setSnackbarOpen(false)}
            message={snackbarMessage}
          />
        </CardContent>
      </Card>
    </Box>
  );
};

export default MarketplacePage;
