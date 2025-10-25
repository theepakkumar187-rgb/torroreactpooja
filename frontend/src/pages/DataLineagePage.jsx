import React, { useState, useEffect, useCallback } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
  Panel,
  Handle,
  Position,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Chip,
  Button,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Grid,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  InputAdornment,
  IconButton,
  Tooltip,
  Autocomplete,
} from '@mui/material';
import {
  Refresh,
  ZoomIn,
  ZoomOut,
  FitScreen,
  Info,
  DataObject,
  Search,
  Close,
  AccountTree,
  FilterList,
} from '@mui/icons-material';

// Custom node component for better styling
const CustomNode = ({ data }) => {
  const isSelected = data.isSelected;
  
  return (
    <>
      <Handle type="target" position={Position.Left} style={{ display: 'none' }} />
      <Handle type="source" position={Position.Right} style={{ display: 'none' }} />
      
      <Box
        sx={{
          px: 3,
          py: 2,
          borderRadius: 2,
          border: '2px solid',
          borderColor: isSelected ? '#ff6b6b' : (data.type === 'View' ? '#8FA0F5' : '#4caf50'),
          backgroundColor: isSelected ? '#ffe5e5' : (data.type === 'View' ? '#f3f5ff' : '#f1f8f4'),
          minWidth: 200,
          cursor: 'pointer',
          transition: 'all 0.3s ease',
          boxShadow: isSelected ? '0 0 0 3px rgba(255, 107, 107, 0.3)' : 'none',
          '&:hover': {
            boxShadow: isSelected ? '0 0 0 3px rgba(255, 107, 107, 0.5)' : '0 4px 12px rgba(0,0,0,0.15)',
            transform: 'translateY(-2px)',
          },
        }}
        onClick={() => data.onNodeClick && data.onNodeClick(data.id)}
      >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <DataObject sx={{ fontSize: 20, color: data.type === 'View' ? '#8FA0F5' : '#4caf50' }} />
        <Typography variant="body2" sx={{ fontWeight: 700, fontSize: 14 }}>
          {data.name}
        </Typography>
      </Box>
      <Chip 
        label={data.type} 
        size="small" 
        sx={{ 
          height: 20, 
          fontSize: 11,
          backgroundColor: data.type === 'View' ? '#8FA0F5' : '#4caf50',
          color: 'white',
          fontWeight: 600,
        }} 
      />
      <Box sx={{ mt: 1 }}>
        <Chip 
          label={data.source_system} 
          size="small" 
          variant="outlined"
          sx={{ 
            height: 20, 
            fontSize: 10,
            borderColor: data.source_system === 'BigQuery' ? '#4285f4' : '#ff6f00',
            color: data.source_system === 'BigQuery' ? '#4285f4' : '#ff6f00',
          }} 
        />
      </Box>
    </Box>
    </>
  );
};

const nodeTypes = {
  custom: CustomNode,
};

const DataLineagePage = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [assets, setAssets] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('all');
  const [filterSource, setFilterSource] = useState('all');
  const [columnRelationships, setColumnRelationships] = useState(0);
  const [selectedEdge, setSelectedEdge] = useState(null);
  const [edgeDetailsOpen, setEdgeDetailsOpen] = useState(false);
  const [selectedAssetForLineage, setSelectedAssetForLineage] = useState(null);
  const [fullLineageData, setFullLineageData] = useState({ nodes: [], edges: [] });

  // Fetch lineage data
  const fetchLineage = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await fetch('http://localhost:8000/api/lineage');
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      
      console.log('Lineage data:', data);
      console.log('Column relationships:', data.column_relationships);
      
      setColumnRelationships(data.column_relationships || 0);
      
      if (!data.nodes || data.nodes.length === 0) {
        setError('No lineage data available. Please ensure you have discovered assets with views.');
        setNodes([]);
        setEdges([]);
        return;
      }

      // Store full data but don't render anything by default
      // User must select an asset to see lineage
      setFullLineageData({ nodes: [], edges: [], rawData: data });
      setNodes([]);
      setEdges([]);
      
      // For autocomplete dropdown, we need nodes but not displayed
      const layoutedNodes = layoutNodes(data.nodes, data.edges);
      const flowNodes = layoutedNodes.map((node) => ({
        id: node.id, // Use original ID directly
        type: 'custom',
        position: node.position,
        sourcePosition: 'right',
        targetPosition: 'left',
        data: {
          label: node.name,
          name: node.name,
          type: node.type,
          catalog: node.catalog,
          connector_id: node.connector_id,
          source_system: node.source_system,
          id: node.id,
          onNodeClick: handleNodeClick,
        },
      }));

      const flowEdges = data.edges.map((edge, index) => {
        const columnCount = edge.column_lineage ? edge.column_lineage.length : 0;
        const label = columnCount > 0 
          ? `${columnCount} columns` 
          : edge.relationship || 'feeds into';
        
        // Use original IDs directly
        const sourceExists = flowNodes.find(n => n.id === edge.source);
        const targetExists = flowNodes.find(n => n.id === edge.target);
        
        if (!sourceExists || !targetExists) {
          console.warn(`Edge ${index} skipped: source or target node not found`, { 
            source: edge.source, 
            target: edge.target,
            availableNodeIds: flowNodes.map(n => n.id)
          });
          return null;
        }
        
        return {
          id: `edge-${index}`,
          source: edge.source,
          target: edge.target,
          type: 'smoothstep',
          animated: columnCount > 0,
          strokeDasharray: columnCount > 0 ? '0' : '5,5',
          markerEnd: {
            type: MarkerType.ArrowClosed,
            width: 20,
            height: 20,
            color: '#1976d2',
          },
          style: {
            strokeWidth: 4,
            stroke: '#1976d2',
          },
          label: label,
          labelStyle: { 
            fill: '#ffffff', 
            fontWeight: 700, 
            fontSize: 12 
          },
          labelBgStyle: { fill: '#1976d2', fillOpacity: 0.9 },
          data: {
            column_lineage: edge.column_lineage || [],
            relationship: edge.relationship,
            onEdgeClick: handleEdgeClick,
          },
        };
      }).filter(edge => edge !== null); // Filter out null edges

      // Store for dropdown and future use, but don't display by default
      setFullLineageData({ nodes: flowNodes, edges: flowEdges, rawData: data });
      
    } catch (error) {
      console.error('Error fetching lineage:', error);
      setError(`Failed to load lineage data: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Fetch assets for details
  const fetchAssets = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/assets');
      const data = await response.json();
      setAssets(data);
    } catch (error) {
      console.error('Error fetching assets:', error);
    }
  };

  useEffect(() => {
    fetchLineage();
    fetchAssets();
  }, []);

  // Automatic hierarchical layout algorithm
  const layoutNodes = (nodes, edges) => {
    // Create adjacency map for graph traversal
    const adjacencyMap = new Map();
    nodes.forEach(node => adjacencyMap.set(node.id, { node, children: [], parents: [] }));
    
    edges.forEach(edge => {
      const sourceNode = adjacencyMap.get(edge.source);
      const targetNode = adjacencyMap.get(edge.target);
      if (sourceNode && targetNode) {
        sourceNode.children.push(edge.target);
        targetNode.parents.push(edge.source);
      }
    });

    // Find root nodes (nodes with no parents)
    const rootNodes = nodes.filter(node => {
      const nodeData = adjacencyMap.get(node.id);
      return nodeData.parents.length === 0;
    });

    // If no root nodes, use all nodes as potential roots
    const startNodes = rootNodes.length > 0 ? rootNodes : nodes;

    // BFS to assign levels
    const levels = new Map();
    const visited = new Set();
    const queue = startNodes.map(node => ({ id: node.id, level: 0 }));

    while (queue.length > 0) {
      const { id, level } = queue.shift();
      if (visited.has(id)) continue;
      
      visited.add(id);
      levels.set(id, level);

      const nodeData = adjacencyMap.get(id);
      if (nodeData) {
        nodeData.children.forEach(childId => {
          if (!visited.has(childId)) {
            queue.push({ id: childId, level: level + 1 });
          }
        });
      }
    }

    // Handle nodes not reached by BFS
    nodes.forEach(node => {
      if (!levels.has(node.id)) {
        levels.set(node.id, 0);
      }
    });

    // Group nodes by level
    const nodesByLevel = new Map();
    levels.forEach((level, nodeId) => {
      if (!nodesByLevel.has(level)) {
        nodesByLevel.set(level, []);
      }
      nodesByLevel.get(level).push(nodeId);
    });

    // Calculate positions
    const levelSpacing = 250;
    const nodeSpacing = 150;
    const layoutedNodes = [];

    nodes.forEach(node => {
      const level = levels.get(node.id);
      const nodesInLevel = nodesByLevel.get(level);
      const indexInLevel = nodesInLevel.indexOf(node.id);
      
      const x = level * levelSpacing;
      const totalHeight = (nodesInLevel.length - 1) * nodeSpacing;
      const y = indexInLevel * nodeSpacing - totalHeight / 2 + 300;

      layoutedNodes.push({
        ...node,
        position: { x, y },
      });
    });

    return layoutedNodes;
  };

  // Handle node click
  const handleNodeClick = async (nodeId) => {
    try {
      // Find asset in local assets array
      const asset = assets.find(a => a.id === nodeId);
      if (!asset) {
        // Fetch from API if not in local array
        const response = await fetch(`http://localhost:8000/api/assets/${encodeURIComponent(nodeId)}`);
        if (response.ok) {
          const data = await response.json();
          setSelectedNode(data);
        } else {
          setSelectedNode({ id: nodeId, name: nodeId, error: 'Asset details not found' });
        }
      } else {
        // Fetch detailed info
        const response = await fetch(`http://localhost:8000/api/assets/${encodeURIComponent(nodeId)}`);
        if (response.ok) {
          const data = await response.json();
          setSelectedNode(data);
        } else {
          setSelectedNode(asset);
        }
      }
      setDetailsDialogOpen(true);
    } catch (error) {
      console.error('Error fetching node details:', error);
      setSelectedNode({ id: nodeId, name: nodeId, error: error.message });
      setDetailsDialogOpen(true);
    }
  };

  const handleCloseDialog = () => {
    setDetailsDialogOpen(false);
    setSelectedNode(null);
  };

  const handleEdgeClick = (edgeData) => {
    setSelectedEdge(edgeData);
    setEdgeDetailsOpen(true);
  };

  const handleCloseEdgeDialog = () => {
    setEdgeDetailsOpen(false);
    setSelectedEdge(null);
  };

  const onEdgeClick = (event, edge) => {
    if (edge.data && edge.data.column_lineage) {
      handleEdgeClick(edge.data);
    }
  };

  // Filter lineage for selected asset
  const handleAssetSelection = (assetId) => {
    if (!assetId) {
      // Show all
      setSelectedAssetForLineage(null);
      setNodes([]);
      setEdges([]);
      return;
    }

    setSelectedAssetForLineage(assetId);

    // Use rawData from API response
    const rawNodes = fullLineageData.rawData.nodes;
    const rawEdges = fullLineageData.rawData.edges;

    // Find related nodes (upstream + downstream + selected)
    const relatedNodeIds = new Set([assetId]);
    
    // Find upstream (sources feeding into selected asset)
    const upstreamEdges = rawEdges.filter(e => e.target === assetId);
    upstreamEdges.forEach(edge => {
      relatedNodeIds.add(edge.source);
      // Also add their sources (2 levels up)
      const secondLevelUp = rawEdges.filter(e => e.target === edge.source);
      secondLevelUp.forEach(e2 => relatedNodeIds.add(e2.source));
    });

    // Find downstream (assets depending on selected asset)
    const downstreamEdges = rawEdges.filter(e => e.source === assetId);
    downstreamEdges.forEach(edge => {
      relatedNodeIds.add(edge.target);
      // Also add their targets (2 levels down)
      const secondLevelDown = rawEdges.filter(e => e.source === edge.target);
      secondLevelDown.forEach(e2 => relatedNodeIds.add(e2.target));
    });

    // Filter nodes and edges
    const filteredNodes = rawNodes.filter(n => relatedNodeIds.has(n.id));
    const filteredEdges = rawEdges.filter(e => 
      relatedNodeIds.has(e.source) && relatedNodeIds.has(e.target)
    );

    // Re-layout the filtered graph
    const layoutedNodes = layoutNodes(filteredNodes, filteredEdges);
    
    const flowNodes = layoutedNodes.map((node) => {
      const originalNode = filteredNodes.find(n => n.id === node.id);
      
      return {
        id: node.id, // Use original ID directly - no index suffix
        type: 'custom',
        position: node.position,
        sourcePosition: 'right',
        targetPosition: 'left',
        data: {
          label: node.name,
          name: node.name,
          type: node.type,
          catalog: node.catalog,
          connector_id: node.connector_id,
          source_system: node.source_system,
          id: node.id,
          isSelected: node.id === assetId,
          onNodeClick: handleNodeClick,
        },
      };
    });

    // Create flow edges with proper structure
    const flowEdges = filteredEdges.map((edge, index) => {
      const columnCount = edge.column_lineage ? edge.column_lineage.length : 0;
      const label = columnCount > 0 
        ? `${columnCount} columns` 
        : edge.relationship || 'feeds into';
      
      // Use original IDs directly - no mapping needed
      const sourceExists = flowNodes.find(n => n.id === edge.source);
      const targetExists = flowNodes.find(n => n.id === edge.target);
      
      if (!sourceExists || !targetExists) {
        console.warn(`Edge ${index} skipped: source or target node not found`, { 
          source: edge.source, 
          target: edge.target,
          availableNodeIds: flowNodes.map(n => n.id)
        });
        return null;
      }
      
      return {
        id: `edge-${index}`,
        source: edge.source,
        target: edge.target,
        sourceHandle: null,
        targetHandle: null,
        type: 'smoothstep',
        animated: columnCount > 0,
        strokeDasharray: columnCount > 0 ? '0' : '5,5',
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: '#1976d2',
        },
        style: {
          strokeWidth: 6,
          stroke: '#1976d2',
        },
        label: label,
        labelStyle: { 
          fill: '#fff', 
          fontWeight: 700, 
          fontSize: 10 
        },
        labelBgStyle: { 
          fill: '#1976d2', 
          fillOpacity: 0.9,
          padding: '2px 6px',
          borderRadius: '4px' 
        },
        data: {
          column_lineage: edge.column_lineage || [],
          relationship: edge.relationship,
          onEdgeClick: handleEdgeClick,
        },
      };
    }).filter(edge => edge !== null); // Filter out null edges

      console.log(`✅ Created ${flowEdges.length} edges for ${flowNodes.length} nodes`);
      console.log('Sample edge:', flowEdges[0]);
      console.log('All edges:', flowEdges);
      console.log('Setting nodes:', flowNodes.length);
      console.log('Setting edges:', flowEdges.length);

    setNodes(flowNodes);
    setEdges(flowEdges);
    
    console.log('✅ Nodes and edges set successfully');
  };

  // Apply filters
  const filteredNodes = nodes.filter(node => {
    const matchesSearch = !searchTerm || 
      node.data.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      node.data.catalog.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesType = filterType === 'all' || node.data.type === filterType;
    const matchesSource = filterSource === 'all' || node.data.source_system === filterSource;
    return matchesSearch && matchesType && matchesSource;
  });

  const filteredEdges = edges.filter(edge => {
    const sourceExists = filteredNodes.find(n => n.id === edge.source);
    const targetExists = filteredNodes.find(n => n.id === edge.target);
    return sourceExists && targetExists;
  });
  
  console.log('🔍 Rendering:', { 
    totalNodes: nodes.length, 
    filteredNodes: filteredNodes.length, 
    totalEdges: edges.length, 
    filteredEdges: filteredEdges.length,
    sampleEdge: filteredEdges[0]
  });
  
  if (filteredEdges.length > 0) {
    console.log('🔵 EDGE DETAILS:', JSON.stringify(filteredEdges[0], null, 2));
  }

  // Get unique types and sources for filters
  const uniqueTypes = [...new Set(nodes.map(n => n.data.type))];
  const uniqueSources = [...new Set(nodes.map(n => n.data.source_system))];

  return (
    <Box sx={{ height: '100vh', display: 'flex', flexDirection: 'column', p: 3 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h4" component="h1" sx={{ fontWeight: 600, fontFamily: 'Comfortaa', display: 'flex', alignItems: 'center', gap: 1 }}>
            <AccountTree sx={{ fontSize: 40, color: '#8FA0F5' }} />
            Data Lineage
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            Visualize data flow and dependencies across your discovered assets
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<Refresh />}
          onClick={fetchLineage}
          disabled={loading}
          sx={{ height: 40 }}
        >
          Refresh
        </Button>
      </Box>


      {/* Filters */}
      <Card sx={{ mb: 3 }}>
        <CardContent sx={{ py: 2 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={5}>
              <Autocomplete
                options={fullLineageData.rawData?.nodes ? fullLineageData.rawData.nodes.map((n, idx) => ({
                  id: n.id,
                  uniqueKey: `${n.id}-${idx}`,
                  label: `${n.name} (${n.type})`,
                  name: n.name,
                  type: n.type,
                  source: n.source_system,
                })) : []}
                getOptionLabel={(option) => option.label || ''}
                isOptionEqualToValue={(option, value) => option.id === value.id}
                value={fullLineageData.rawData?.nodes?.find(n => n.id === selectedAssetForLineage) ? {
                  id: selectedAssetForLineage,
                  label: fullLineageData.rawData.nodes.find(n => n.id === selectedAssetForLineage)?.name,
                } : null}
                onChange={(event, newValue) => {
                  handleAssetSelection(newValue ? newValue.id : null);
                }}
                disabled={!fullLineageData.rawData?.nodes || fullLineageData.rawData.nodes.length === 0}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    size="small"
                    label="Focus on Asset"
                    placeholder="Select asset to view its lineage..."
                    InputProps={{
                      ...params.InputProps,
                      startAdornment: (
                        <>
                          <InputAdornment position="start">
                            <AccountTree />
                          </InputAdornment>
                          {params.InputProps.startAdornment}
                        </>
                      ),
                    }}
                  />
                )}
                renderOption={(props, option) => (
                  <li {...props} key={option.uniqueKey}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                      <DataObject sx={{ fontSize: 18, color: option.type === 'View' ? '#8FA0F5' : '#4caf50' }} />
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          {option.name}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {option.type} • {option.source}
                        </Typography>
                      </Box>
                    </Box>
                  </li>
                )}
              />
            </Grid>
            <Grid item xs={12} md={2}>
              <TextField
                fullWidth
                size="small"
                placeholder="Search..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
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
              <FormControl fullWidth size="small">
                <InputLabel>Type</InputLabel>
                <Select
                  value={filterType}
                  label="Type"
                  onChange={(e) => setFilterType(e.target.value)}
                >
                  <MenuItem value="all">All Types</MenuItem>
                  {uniqueTypes.map(type => (
                    <MenuItem key={type} value={type}>{type}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={3}>
              <FormControl fullWidth size="small">
                <InputLabel>Source</InputLabel>
                <Select
                  value={filterSource}
                  label="Source"
                  onChange={(e) => setFilterSource(e.target.value)}
                >
                  <MenuItem value="all">All Sources</MenuItem>
                  {uniqueSources.map(source => (
                    <MenuItem key={source} value={source}>{source}</MenuItem>
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
                  setFilterType('all');
                  setFilterSource('all');
                  handleAssetSelection(null);
                }}
              >
                Clear All
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* Lineage Graph */}
      <Card sx={{ flexGrow: 1, position: 'relative', minHeight: 500 }}>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
            <CircularProgress />
          </Box>
        ) : error ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%', p: 4 }}>
            <Alert severity="info" sx={{ maxWidth: 600 }}>
              <Typography variant="h6" gutterBottom>
                No Lineage Data Available
              </Typography>
              <Typography variant="body2">
                {error}
              </Typography>
              <Typography variant="body2" sx={{ mt: 2 }}>
                To see lineage:
              </Typography>
              <ul style={{ marginTop: 8 }}>
                <li>Discover assets with Views (not just Tables)</li>
                <li>Views must reference other tables/views in their SQL definition</li>
                <li>Ensure connectors are properly configured</li>
              </ul>
            </Alert>
          </Box>
        ) : filteredNodes.length === 0 ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%', p: 4 }}>
            <Alert severity="info" sx={{ maxWidth: 600 }}>
              <Typography variant="h6" gutterBottom>
                Select an Asset to View Lineage
              </Typography>
              <Typography variant="body2">
                Use the dropdown above to select an asset and see its data lineage:
              </Typography>
              <ul style={{ marginTop: 12, marginBottom: 0 }}>
                <li><strong>Upstream:</strong> Where this data comes from</li>
                <li><strong>Downstream:</strong> What depends on this data</li>
                <li><strong>Columns:</strong> Column-level relationships shown</li>
              </ul>
              <Box sx={{ mt: 3, display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                <Chip 
                  label={`${fullLineageData.rawData?.nodes?.length || 0} Total Assets`} 
                  color="primary" 
                  size="small"
                />
                <Chip 
                  label={`${columnRelationships} Column Links`} 
                  color="success" 
                  size="small"
                />
                <Chip 
                  label={`${fullLineageData.rawData?.edges?.length || 0} Relationships`} 
                  color="info" 
                  size="small"
                />
              </Box>
            </Alert>
          </Box>
        ) : (
          <ReactFlow
            nodes={filteredNodes}
            edges={filteredEdges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onEdgeClick={onEdgeClick}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            minZoom={0.1}
            maxZoom={4}
            defaultEdgeOptions={{
              type: 'smoothstep',
              animated: true,
              style: { strokeWidth: 6, stroke: '#1976d2' },
            }}
            attributionPosition="bottom-left"
          >
            <Background color="#aaa" gap={16} />
            <Controls />
            <MiniMap
              nodeColor={(node) => node.data.type === 'View' ? '#8FA0F5' : '#4caf50'}
              maskColor="rgba(0, 0, 0, 0.1)"
            />
            <Panel position="top-right">
              <Card sx={{ p: 2, backgroundColor: 'rgba(255, 255, 255, 0.95)' }}>
                <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                  Legend
                </Typography>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 16, height: 16, backgroundColor: '#f1f8f4', border: '2px solid #4caf50', borderRadius: 1 }} />
                    <Typography variant="caption">Table</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 16, height: 16, backgroundColor: '#f3f5ff', border: '2px solid #8FA0F5', borderRadius: 1 }} />
                    <Typography variant="caption">View</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 16, height: 16, backgroundColor: '#ffe5e5', border: '2px solid #ff6b6b', borderRadius: 1 }} />
                    <Typography variant="caption">Selected</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 16, height: 2, backgroundColor: '#1976d2' }} />
                    <Typography variant="caption">Data Flow</Typography>
                  </Box>
                </Box>
              </Card>
            </Panel>
          </ReactFlow>
        )}
      </Card>

      {/* Asset Details Dialog */}
      <Dialog
        open={detailsDialogOpen}
        onClose={handleCloseDialog}
        maxWidth="md"
        fullWidth
      >
        {selectedNode && (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    {selectedNode.name}
                  </Typography>
                  <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                    <Chip label={selectedNode.type} size="small" color="primary" />
                    <Chip label={selectedNode.catalog} size="small" variant="outlined" />
                  </Box>
                </Box>
                <IconButton onClick={handleCloseDialog}>
                  <Close />
                </IconButton>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              {selectedNode.error ? (
                <Alert severity="error">{selectedNode.error}</Alert>
              ) : (
                <Grid container spacing={2}>
                  <Grid item xs={12}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Description
                    </Typography>
                    <Typography variant="body1">
                      {selectedNode.description || 'No description available'}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Asset ID
                    </Typography>
                    <Typography variant="body2" sx={{ wordBreak: 'break-all' }}>
                      {selectedNode.id}
                    </Typography>
                  </Grid>
                  <Grid item xs={6}>
                    <Typography variant="subtitle2" color="text.secondary">
                      Status
                    </Typography>
                    <Chip label={selectedNode.status || 'active'} size="small" color="success" />
                  </Grid>
                  {selectedNode.columns && selectedNode.columns.length > 0 && (
                    <Grid item xs={12}>
                      <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                        Columns ({selectedNode.columns.length})
                      </Typography>
                      <TableContainer component={Paper} variant="outlined">
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell>Name</TableCell>
                              <TableCell>Type</TableCell>
                              <TableCell>Description</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {selectedNode.columns.slice(0, 10).map((col, index) => (
                              <TableRow key={index}>
                                <TableCell>{col.name}</TableCell>
                                <TableCell>
                                  <Chip label={col.type} size="small" variant="outlined" />
                                </TableCell>
                                <TableCell>{col.description || '-'}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                      {selectedNode.columns.length > 10 && (
                        <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                          Showing 10 of {selectedNode.columns.length} columns
                        </Typography>
                      )}
                    </Grid>
                  )}
                </Grid>
              )}
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialog}>Close</Button>
            </DialogActions>
          </>
        )}
      </Dialog>

      {/* Edge Details Dialog - Column Lineage */}
      <Dialog
        open={edgeDetailsOpen}
        onClose={handleCloseEdgeDialog}
        maxWidth="md"
        fullWidth
      >
        {selectedEdge && (
          <>
            <DialogTitle>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                    Column-Level Lineage
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                    {selectedEdge.column_lineage.length} column relationships
                  </Typography>
                </Box>
                <IconButton onClick={handleCloseEdgeDialog}>
                  <Close />
                </IconButton>
              </Box>
            </DialogTitle>
            <DialogContent dividers>
              {selectedEdge.column_lineage && selectedEdge.column_lineage.length > 0 ? (
                <TableContainer component={Paper} variant="outlined">
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Source Column</TableCell>
                        <TableCell>→</TableCell>
                        <TableCell>Target Column</TableCell>
                        <TableCell>Relationship</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {selectedEdge.column_lineage.map((colRel, index) => (
                        <TableRow key={index}>
                          <TableCell>
                            <Box>
                              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                {colRel.source_column}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {colRel.source_table.split('.').pop()}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Box sx={{ display: 'flex', justifyContent: 'center' }}>
                              →
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Box>
                              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                {colRel.target_column}
                              </Typography>
                              <Typography variant="caption" color="text.secondary">
                                {colRel.target_table.split('.').pop()}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell>
                            <Chip 
                              label={colRel.relationship_type.replace('_', ' ')} 
                              size="small" 
                              color={
                                colRel.relationship_type === 'direct_match' ? 'success' : 
                                colRel.relationship_type === 'transformed' ? 'warning' : 'info'
                              }
                            />
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              ) : (
                <Alert severity="info">
                  No column-level lineage information available for this relationship.
                </Alert>
              )}
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseEdgeDialog}>Close</Button>
            </DialogActions>
          </>
        )}
      </Dialog>
    </Box>
  );
};

export default DataLineagePage;

