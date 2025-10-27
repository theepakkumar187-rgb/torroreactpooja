import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Grid,
  Button,
  Avatar,
  Chip,
  Divider,
  Dialog,
  DialogContent,
  IconButton,
  Stepper,
  Step,
  StepLabel,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  RadioGroup,
  FormControlLabel,
  Radio,
  Alert,
  CircularProgress,
} from '@mui/material';
import {
  Add,
  Refresh,
  CloudSync,
  CheckCircle,
  Error,
  CloudQueue,
  Storage,
  Cloud,
  Close,
  ArrowBack,
  ArrowForward,
  Delete,
} from '@mui/icons-material';

const ConnectorsPage = () => {
  const [myConnections, setMyConnections] = useState([]);
  const [loading, setLoading] = useState(false);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [connectionToDelete, setConnectionToDelete] = useState(null);
  
  // Connection wizard state
  const [wizardOpen, setWizardOpen] = useState(false);
  const [selectedConnector, setSelectedConnector] = useState(null);
  const [activeStep, setActiveStep] = useState(0);
  const [connectionType, setConnectionType] = useState('');
  const [config, setConfig] = useState({});
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [discoveryProgress, setDiscoveryProgress] = useState([]);

  useEffect(() => {
    fetchMyConnections();
  }, []);

  const fetchMyConnections = async () => {
    try {
      setLoading(true);
      const response = await fetch('http://localhost:8000/api/connectors');
      const data = await response.json();
      setMyConnections(data);
    } catch (error) {
      console.error('Error fetching connections:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteClick = (connection) => {
    setConnectionToDelete(connection);
    setDeleteDialogOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!connectionToDelete) return;
    
    try {
      // Call delete API endpoint
      const response = await fetch(`http://localhost:8000/api/connectors/${connectionToDelete.id}`, {
        method: 'DELETE',
      });
      
      if (response.ok) {
        // Remove from local state
        setMyConnections(prev => prev.filter(conn => conn.id !== connectionToDelete.id));
        setDeleteDialogOpen(false);
        setConnectionToDelete(null);
      } else {
        console.error('Failed to delete connection');
      }
    } catch (error) {
      console.error('Error deleting connection:', error);
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialogOpen(false);
    setConnectionToDelete(null);
  };

  const availableConnectors = [
    {
      id: 'bigquery',
      name: 'BigQuery',
      description: 'Google Cloud data warehouse for analytics',
      logo: 'https://www.vectorlogo.zone/logos/google_bigquery/google_bigquery-icon.svg',
      fallbackIcon: <CloudQueue />,
      color: '#4285F4',
      connectionTypes: ['Service Account'],
    },
    {
      id: 'starburst',
      name: 'Starburst Galaxy',
      description: 'Distributed SQL query engine for data lakes',
      logo: 'https://www.vectorlogo.zone/logos/starburst/starburst-icon.svg',
      fallbackIcon: <Storage />,
      color: '#00D4AA',
      connectionTypes: ['API Token'],
    },
    {
      id: 'azure',
      name: 'Azure Purview',
      description: 'Microsoft unified data governance service',
      logo: 'https://www.vectorlogo.zone/logos/microsoft_azure/microsoft_azure-icon.svg',
      fallbackIcon: <Cloud />,
      color: '#0078D4',
      connectionTypes: ['Service Principal', 'Managed Identity'],
    },
  ];

  const wizardSteps = [
    'Connection Type',
    'Configuration',
    'Test Connection',
    'Summary'
  ];

  const handleConnectClick = (connector) => {
    setSelectedConnector(connector);
    setActiveStep(0);
    setConnectionType('');
    setConfig({});
    setTestResult(null);
    setWizardOpen(true);
  };

  const handleNext = () => {
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleWizardClose = () => {
    setWizardOpen(false);
    setSelectedConnector(null);
    setActiveStep(0);
    setConnectionType('');
    setConfig({});
    setTestResult(null);
  };

  const handleTestConnection = async () => {
    setTesting(true);
    setTestResult(null);
    setDiscoveryProgress([]);
    
    try {
      let url;
      let body;
      
      // Call appropriate endpoint based on connection type
      if (connectionType === 'Service Account' && selectedConnector?.id === 'bigquery') {
        url = 'http://localhost:8000/api/connectors/bigquery/test-stream';
        body = {
          project_id: config.projectId,
          service_account_json: config.serviceAccount,
          connection_name: config.name
        };
      } else if (connectionType === 'API Token' && selectedConnector?.id === 'starburst') {
        url = 'http://localhost:8000/api/connectors/starburst/test-stream';
        body = {
          account_domain: config.accountDomain,
          client_id: config.clientId,
          client_secret: config.clientSecret,
          connection_name: config.name
        };
      } else if (connectionType === 'Service Principal' && selectedConnector?.id === 'azure') {
        url = 'http://localhost:8000/api/azure/test-connection';
        body = {
          purview_account_name: config.purviewAccountName,
          tenant_id: config.tenantId,
          client_id: config.clientId,
          client_secret: config.clientSecret,
          connection_name: config.name
        };
      } else if (connectionType === 'Managed Identity' && selectedConnector?.id === 'azure') {
        url = 'http://localhost:8000/api/azure/test-connection';
        body = {
          purview_account_name: config.purviewAccountName,
          tenant_id: config.tenantId,
          managed_identity_client_id: config.managedIdentityClientId || null,
          connection_name: config.name
        };
      } else {
        // For other connectors, simulate success for now
        setTimeout(() => {
          setTestResult({ success: true, message: 'Connection successful!' });
          setTesting(false);
        }, 2000);
        return;
      }
      
      // Make the POST request and stream the response
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body)
      });
      
      if (!response.body) {
        throw new Error('ReadableStream not supported');
      }
      
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      
      while (true) {
        const { done, value } = await reader.read();
        
        if (done) break;
        
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || ''; // Keep incomplete line in buffer
        
        for (const line of lines) {
          if (line.trim().startsWith('data: ')) {
            const jsonStr = line.replace('data: ', '').trim();
            if (jsonStr) {
              try {
                const progressData = JSON.parse(jsonStr);
                
                if (progressData.type === 'progress') {
                  setDiscoveryProgress(prev => [...prev, progressData.message]);
                } else if (progressData.type === 'complete') {
                  setTestResult({ 
                    success: true, 
                    message: progressData.message,
                    discoveredAssets: progressData.discovered_assets,
                    connectorId: progressData.connector_id
                  });
                  await fetchMyConnections();
                } else if (progressData.type === 'error') {
                  setTestResult({ 
                    success: false, 
                    message: progressData.message 
                  });
                }
              } catch (e) {
                console.error('Error parsing SSE data:', e);
              }
            }
          }
        }
      }
    } catch (error) {
      console.error('Error testing connection:', error);
      setTestResult({ 
        success: false, 
        message: `Connection failed: ${error.message}` 
      });
    } finally {
      setTesting(false);
    }
  };

  const renderStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Select Connection Type for {selectedConnector?.name}
            </Typography>
            <FormControl component="fieldset">
              <RadioGroup
                value={connectionType}
                onChange={(e) => setConnectionType(e.target.value)}
              >
                {selectedConnector?.connectionTypes.map((type) => (
                  <FormControlLabel
                    key={type}
                    value={type}
                    control={<Radio />}
                    label={type}
                  />
                ))}
              </RadioGroup>
            </FormControl>
          </Box>
        );
      
      case 1:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Configuration
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Connection Name"
                  value={config.name || ''}
                  onChange={(e) => setConfig({...config, name: e.target.value})}
                />
              </Grid>
              {connectionType === 'Service Account' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Project ID"
                      value={config.projectId || ''}
                      onChange={(e) => setConfig({...config, projectId: e.target.value})}
                      placeholder="your-gcp-project-id"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      multiline
                      rows={10}
                      label="Service Account JSON"
                      value={config.serviceAccount || ''}
                      onChange={(e) => setConfig({...config, serviceAccount: e.target.value})}
                      placeholder={`Enter your service account JSON and make sure you have all the required permissions:\n\nRequired Permissions:\n• bigquery.datasets.get - View dataset metadata\n• bigquery.tables.list - List tables in datasets\n• bigquery.tables.get - View table metadata and schema\n• bigquery.tables.getData - Read table data for profiling\n• bigquery.tables.create - Create views\n• bigquery.tables.update - Modify tables and views\n• bigquery.tables.updateTag - Add/remove tags\n• datacatalog.taxonomies.get - Access data catalog\n• datacatalog.entries.list - List catalog entries\n• datacatalog.entries.updateTag - Manage data catalog tags\n\nPaste your service account JSON here...`}
                      helperText="Ensure the service account has all permissions listed above for data discovery, lineage, tagging, and view management"
                    />
                  </Grid>
                </>
              )}
              {connectionType === 'API Token' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Account Domain"
                      value={config.accountDomain || ''}
                      onChange={(e) => setConfig({...config, accountDomain: e.target.value})}
                      placeholder="e.g., mycompany.galaxy.starburst.io"
                      helperText="Your Starburst Galaxy account domain"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Client ID"
                      value={config.clientId || ''}
                      onChange={(e) => setConfig({...config, clientId: e.target.value})}
                      helperText="OAuth Client ID from Starburst Galaxy"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Client Secret"
                      type="password"
                      value={config.clientSecret || ''}
                      onChange={(e) => setConfig({...config, clientSecret: e.target.value})}
                      helperText="OAuth Client Secret from Starburst Galaxy"
                    />
                  </Grid>
                </>
              )}
              {connectionType === 'Service Principal' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Purview Account Name"
                      value={config.purviewAccountName || ''}
                      onChange={(e) => setConfig({...config, purviewAccountName: e.target.value})}
                      placeholder="my-purview-account"
                      helperText="Your Azure Purview account name"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Tenant ID"
                      value={config.tenantId || ''}
                      onChange={(e) => setConfig({...config, tenantId: e.target.value})}
                      placeholder="12345678-1234-1234-1234-123456789012"
                      helperText="Azure AD Tenant ID"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Client ID"
                      value={config.clientId || ''}
                      onChange={(e) => setConfig({...config, clientId: e.target.value})}
                      placeholder="12345678-1234-1234-1234-123456789012"
                      helperText="Azure AD Application (Client) ID"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Client Secret"
                      type="password"
                      value={config.clientSecret || ''}
                      onChange={(e) => setConfig({...config, clientSecret: e.target.value})}
                      helperText="Azure AD Application Client Secret"
                    />
                  </Grid>
                </>
              )}
              {connectionType === 'Managed Identity' && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Purview Account Name"
                      value={config.purviewAccountName || ''}
                      onChange={(e) => setConfig({...config, purviewAccountName: e.target.value})}
                      placeholder="my-purview-account"
                      helperText="Your Azure Purview account name"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      required
                      label="Tenant ID"
                      value={config.tenantId || ''}
                      onChange={(e) => setConfig({...config, tenantId: e.target.value})}
                      placeholder="12345678-1234-1234-1234-123456789012"
                      helperText="Azure AD Tenant ID"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Managed Identity Client ID"
                      value={config.managedIdentityClientId || ''}
                      onChange={(e) => setConfig({...config, managedIdentityClientId: e.target.value})}
                      placeholder="12345678-1234-1234-1234-123456789012"
                      helperText="Optional: User-assigned managed identity client ID. Leave empty for system-assigned."
                    />
                  </Grid>
                </>
              )}
            </Grid>
          </Box>
        );
      
      case 2:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Test Connection
            </Typography>
            <Box sx={{ textAlign: 'center', py: 4 }}>
              {testing ? (
                <Box>
                  <CircularProgress sx={{ mb: 2 }} />
                  <Typography sx={{ mb: 3, fontWeight: 600 }}>Testing connection...</Typography>
                  
                  {/* Real-time Discovery Progress */}
                  {discoveryProgress.length > 0 && (
                    <Box sx={{ 
                      maxHeight: '300px', 
                      overflowY: 'auto', 
                      bgcolor: '#f5f5f5', 
                      p: 2, 
                      borderRadius: 1,
                      textAlign: 'left',
                      fontFamily: 'monospace',
                      fontSize: '0.875rem'
                    }}>
                      {discoveryProgress.map((message, index) => (
                        <Box key={index} sx={{ 
                          py: 0.5,
                          color: message.includes('✓') ? 'success.main' : 
                                 message.includes('✗') ? 'error.main' : 
                                 message.includes('Discovering') ? 'primary.main' : 'text.primary',
                          display: 'flex',
                          alignItems: 'center',
                          gap: 1
                        }}>
                          {message.includes('✓') && <CheckCircle sx={{ fontSize: 16 }} />}
                          <span>{message}</span>
                        </Box>
                      ))}
                    </Box>
                  )}
                </Box>
              ) : testResult ? (
                <Box>
                  <Alert severity={testResult.success ? 'success' : 'error'} sx={{ mb: 2 }}>
                    {testResult.message}
                  </Alert>
                  {testResult.success && testResult.discoveredAssets > 0 && (
                    <Card variant="outlined" sx={{ p: 2, bgcolor: 'success.light', color: 'success.dark' }}>
                      <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
                        🎉 Discovery Complete!
                      </Typography>
                      <Typography variant="body1">
                        <strong>{testResult.discoveredAssets}</strong> assets discovered and ready to explore
                      </Typography>
                      <Typography variant="body2" sx={{ mt: 1, opacity: 0.9 }}>
                        View them in the "Discovered Assets" section
                      </Typography>
                    </Card>
                  )}
                  
                  {/* Show final discovery log */}
                  {discoveryProgress.length > 0 && (
                    <Box sx={{ mt: 3 }}>
                      <Typography variant="subtitle2" sx={{ mb: 1, textAlign: 'left' }}>
                        Discovery Log:
                      </Typography>
                      <Box sx={{ 
                        maxHeight: '200px', 
                        overflowY: 'auto', 
                        bgcolor: '#f5f5f5', 
                        p: 2, 
                        borderRadius: 1,
                        textAlign: 'left',
                        fontFamily: 'monospace',
                        fontSize: '0.75rem'
                      }}>
                        {discoveryProgress.map((message, index) => (
                          <Box key={index} sx={{ py: 0.25, color: 'text.secondary' }}>
                            {message}
                          </Box>
                        ))}
                      </Box>
                    </Box>
                  )}
                </Box>
              ) : (
                <Button
                  variant="contained"
                  onClick={handleTestConnection}
                  disabled={
                    !connectionType || 
                    !config.name ||
                    (connectionType === 'Service Account' && (!config.projectId || !config.serviceAccount)) ||
                    (connectionType === 'API Token' && (!config.accountDomain || !config.clientId || !config.clientSecret)) ||
                    (connectionType === 'Service Principal' && (!config.purviewAccountName || !config.tenantId || !config.clientId || !config.clientSecret)) ||
                    (connectionType === 'Managed Identity' && (!config.purviewAccountName || !config.tenantId))
                  }
                >
                  Test Connection
                </Button>
              )}
            </Box>
          </Box>
        );
      
      case 3:
        return (
          <Box>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Summary
            </Typography>
            <Card variant="outlined" sx={{ p: 2 }}>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connector:</strong> {selectedConnector?.name}
              </Typography>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connection Type:</strong> {connectionType}
              </Typography>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                <strong>Connection Name:</strong> {config.name}
              </Typography>
              {testResult && (
                <Alert severity={testResult.success ? 'success' : 'error'} sx={{ mt: 2 }}>
                  {testResult.message}
                </Alert>
              )}
            </Card>
          </Box>
        );
      
      default:
        return 'Unknown step';
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 4 }}>
        <Typography variant="h4" component="h1" sx={{ fontWeight: 600 }}>
          Connectors
        </Typography>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={fetchMyConnections}
            disabled={loading}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<Add />}
            onClick={() => setDialogOpen(true)}
          >
            New Connector
          </Button>
        </Box>
      </Box>

      {/* My Connections Section */}
      <Card sx={{ mb: 4 }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <CloudSync sx={{ mr: 1.5, color: 'primary.main', fontSize: 28 }} />
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              My Connections
            </Typography>
          </Box>
          
          {myConnections.length > 0 ? (
            <Grid container spacing={2}>
                  {myConnections.map((connection) => (
                    <Grid item xs={12} sm={6} md={4} key={connection.id}>
                      <Card variant="outlined" sx={{ p: 2, height: '100%', position: 'relative' }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                          <Avatar sx={{ bgcolor: 'primary.main', mr: 2 }}>
                            <CloudSync />
                          </Avatar>
                          <Box sx={{ flex: 1 }}>
                            <Typography variant="h6" sx={{ fontWeight: 600 }}>
                              {connection.name}
                            </Typography>
                            <Chip
                              label={connection.status}
                              size="small"
                              color={connection.status === 'active' ? 'success' : 'error'}
                              sx={{ mt: 0.5 }}
                            />
                          </Box>
                          <IconButton
                            size="small"
                            onClick={() => handleDeleteClick(connection)}
                            sx={{ 
                              color: 'error.main',
                              '&:hover': {
                                backgroundColor: 'error.light',
                                color: 'error.dark'
                              }
                            }}
                          >
                            <Delete fontSize="small" />
                          </IconButton>
                        </Box>
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                          Type: {connection.type}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Last run: {connection.last_run ? new Date(connection.last_run).toLocaleString() : 'Never'}
                        </Typography>
                        {connection.assets_count && (
                          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                            Assets: {connection.assets_count}
                          </Typography>
                        )}
                      </Card>
                    </Grid>
                  ))}
            </Grid>
          ) : (
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <CloudSync sx={{ fontSize: 48, color: 'text.secondary', mb: 2, opacity: 0.5 }} />
              <Typography variant="h6" color="text.secondary" sx={{ mb: 1 }}>
                No active connections
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Connect to data sources below to get started
              </Typography>
            </Box>
          )}
        </CardContent>
      </Card>

      {/* Available Connectors Section */}
      <Card>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <Add sx={{ mr: 1.5, color: 'primary.main', fontSize: 28 }} />
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              Available Connectors
            </Typography>
          </Box>
          
          <Grid container spacing={3}>
            {availableConnectors.map((connector) => (
              <Grid item xs={12} sm={4} md={4} key={connector.id}>
                <Card 
                  variant="outlined" 
                  sx={{ 
                    p: 3, 
                    height: '100%',
                    cursor: 'pointer',
                    transition: 'all 0.2s ease-in-out',
                    '&:hover': {
                      boxShadow: 3,
                      transform: 'translateY(-2px)',
                    }
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                    <Avatar 
                      sx={{ 
                        bgcolor: connector.color, 
                        mr: 2,
                        width: 48,
                        height: 48,
                      }}
                    >
                      <img 
                        src={connector.logo} 
                        alt={connector.name}
                        style={{ 
                          width: '32px', 
                          height: '32px',
                          objectFit: 'contain'
                        }}
                        onError={(e) => {
                          e.target.style.display = 'none';
                          e.target.nextSibling.style.display = 'block';
                        }}
                      />
                      <Box sx={{ display: 'none', color: 'white' }}>
                        {connector.fallbackIcon}
                      </Box>
                    </Avatar>
                    <Box>
                      <Typography variant="h6" sx={{ fontWeight: 600 }}>
                        {connector.name}
                      </Typography>
                    </Box>
                  </Box>
                  
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                    {connector.description}
                  </Typography>
                  
                  <Button
                    variant="contained"
                    fullWidth
                    onClick={() => handleConnectClick(connector)}
                    sx={{
                      bgcolor: connector.color,
                      '&:hover': {
                        bgcolor: connector.color,
                        opacity: 0.9,
                      }
                    }}
                  >
                    Connect
                  </Button>
                </Card>
              </Grid>
            ))}
          </Grid>
        </CardContent>
      </Card>

      {/* New Connector Dialog */}
      <Dialog 
        open={dialogOpen} 
        onClose={() => setDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <Box sx={{ p: 3, position: 'relative' }}>
          <IconButton 
            onClick={() => setDialogOpen(false)} 
            sx={{ 
              position: 'absolute', 
              right: 8, 
              top: 8 
            }}
          >
            <Close />
          </IconButton>
          
          <Typography variant="h5" sx={{ fontWeight: 600, mb: 3, textAlign: 'center' }}>
            Select Connector
          </Typography>
          
          <Grid container spacing={2}>
            {availableConnectors.map((connector) => (
              <Grid item xs={4} key={connector.id}>
                <Card 
                  variant="outlined" 
                  sx={{ 
                    p: 2,
                    cursor: 'pointer',
                    transition: 'all 0.2s ease-in-out',
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    '&:hover': {
                      boxShadow: 3,
                      transform: 'translateY(-2px)',
                      borderColor: connector.color,
                    }
                  }}
                  onClick={() => {
                    console.log('Selected connector:', connector.name);
                    setDialogOpen(false);
                  }}
                >
                  <Avatar 
                    sx={{ 
                      bgcolor: connector.color, 
                      width: 64,
                      height: 64,
                      mb: 1.5,
                    }}
                  >
                    <img 
                      src={connector.logo} 
                      alt={connector.name}
                      style={{ 
                        width: '48px', 
                        height: '48px',
                        objectFit: 'contain'
                      }}
                      onError={(e) => {
                        e.target.style.display = 'none';
                        e.target.nextSibling.style.display = 'block';
                      }}
                    />
                    <Box sx={{ display: 'none', color: 'white', fontSize: 40 }}>
                      {connector.fallbackIcon}
                    </Box>
                  </Avatar>
                  
                  <Typography 
                    variant="body2" 
                    sx={{ 
                      fontWeight: 600,
                      textAlign: 'center',
                      fontSize: '0.875rem'
                    }}
                  >
                    {connector.name}
                  </Typography>
                </Card>
              </Grid>
            ))}
          </Grid>
        </Box>
      </Dialog>

      {/* Connection Wizard Dialog */}
      <Dialog 
        open={wizardOpen} 
        onClose={handleWizardClose}
        maxWidth="lg"
        fullWidth
      >
        <Box sx={{ p: 3 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
            <Typography variant="h5" sx={{ fontWeight: 600 }}>
              Connect to {selectedConnector?.name}
            </Typography>
            <IconButton onClick={handleWizardClose}>
              <Close />
            </IconButton>
          </Box>
          
          <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
            {wizardSteps.map((label) => (
              <Step key={label}>
                <StepLabel>{label}</StepLabel>
              </Step>
            ))}
          </Stepper>
          
          <Box sx={{ mb: 3, minHeight: 300 }}>
            {renderStepContent(activeStep)}
          </Box>
          
          <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
            <Button
              disabled={activeStep === 0}
              onClick={handleBack}
              startIcon={<ArrowBack />}
            >
              Back
            </Button>
            <Box>
              <Button onClick={handleWizardClose} sx={{ mr: 1 }}>
                Cancel
              </Button>
              {activeStep === wizardSteps.length - 1 ? (
                <Button variant="contained" onClick={handleWizardClose}>
                  Complete
                </Button>
              ) : (
                <Button
                  variant="contained"
                  onClick={handleNext}
                  endIcon={<ArrowForward />}
                  disabled={
                    (activeStep === 0 && !connectionType) ||
                    (activeStep === 1 && (
                      !config.name || 
                      (connectionType === 'Service Account' && (!config.projectId || !config.serviceAccount)) ||
                      (connectionType === 'API Token' && (!config.accountDomain || !config.clientId || !config.clientSecret)) ||
                      (connectionType === 'Service Principal' && (!config.purviewAccountName || !config.tenantId || !config.clientId || !config.clientSecret)) ||
                      (connectionType === 'Managed Identity' && (!config.purviewAccountName || !config.tenantId))
                    )) ||
                    (activeStep === 2 && !testResult)
                  }
                >
                  Next
                </Button>
              )}
            </Box>
          </Box>
        </Box>
          </Dialog>

          {/* Delete Confirmation Dialog */}
          <Dialog
            open={deleteDialogOpen}
            onClose={handleDeleteCancel}
            maxWidth="sm"
            fullWidth
          >
            <Box sx={{ p: 3 }}>
              <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
                Delete Connection
              </Typography>
              
              <Typography variant="body1" sx={{ mb: 3 }}>
                Are you sure you want to delete the connection "{connectionToDelete?.name}"? 
                This action cannot be undone and will also remove all associated discovered assets.
              </Typography>
              
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 2 }}>
                <Button
                  variant="outlined"
                  onClick={handleDeleteCancel}
                >
                  Cancel
                </Button>
                <Button
                  variant="contained"
                  color="error"
                  onClick={handleDeleteConfirm}
                >
                  Delete
                </Button>
              </Box>
            </Box>
          </Dialog>
        </Box>
      );
    };

    export default ConnectorsPage;
