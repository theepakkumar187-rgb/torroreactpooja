import React, { useState, useEffect } from 'react';
import { Outlet, useLocation } from 'react-router-dom';
import {
  Box,
} from '@mui/material';

// Components
import Header from './Header';
import SummaryCards from './SummaryCards';
import Sidebar from './Sidebar';

const Layout = () => {
  const location = useLocation();
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [stats, setStats] = useState({
    total_assets: 0,
    total_catalogs: 0,
    active_connectors: 0,
    last_scan: null,
    monitoring_status: 'Unknown'
  });

  useEffect(() => {
    fetchDashboardData();
    // Refresh stats every 30 seconds
    const interval = setInterval(fetchDashboardData, 30000);
    return () => clearInterval(interval);
  }, []);

  const fetchDashboardData = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/dashboard/stats');
      const data = await response.json();
      setStats(data);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
    }
  };

  const handleMenuClick = () => {
    setSidebarOpen(true);
  };

  const handleSidebarClose = () => {
    setSidebarOpen(false);
  };

  // Only show summary cards on dashboard page
  const showSummaryCards = location.pathname === '/';

  return (
    <Box sx={{ 
      flexGrow: 1, 
      minHeight: '100vh', 
      backgroundColor: 'background.default',
      width: '100%',
      maxWidth: 'none'
    }}>
      <Header onRefresh={fetchDashboardData} onMenuClick={handleMenuClick} />
      {showSummaryCards && <SummaryCards stats={stats} />}
      <Sidebar open={sidebarOpen} onClose={handleSidebarClose} />
      <Outlet />
    </Box>
  );
};

export default Layout;
