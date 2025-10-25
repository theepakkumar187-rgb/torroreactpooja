import React, { useState, useEffect } from 'react';
import {
  Box,
  Grid,
} from '@mui/material';

// Dashboard specific components
import SystemHealthPanel from '../components/SystemHealthPanel';
import RecentActivityPanel from '../components/RecentActivityPanel';
import DiscoveryStatisticsPanel from '../components/DiscoveryStatisticsPanel';

const DashboardPage = () => {
  const [stats, setStats] = useState({});
  const [systemHealth, setSystemHealth] = useState({});
  const [activities, setActivities] = useState([]);

  useEffect(() => {
    fetchDashboardData();
  }, []);

  const fetchDashboardData = async () => {
    try {
      const [statsRes, healthRes, activitiesRes] = await Promise.all([
        fetch('http://localhost:8000/api/dashboard/stats'),
        fetch('http://localhost:8000/api/system/health'),
        fetch('http://localhost:8000/api/activities'),
      ]);

      const statsData = await statsRes.json();
      const healthData = await healthRes.json();
      const activitiesData = await activitiesRes.json();

      setStats(statsData);
      setSystemHealth(healthData);
      setActivities(activitiesData);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
    }
  };

  return (
    <Box className="dashboard-container" sx={{ 
      width: '100%',
      display: 'grid',
      gridTemplateColumns: '1fr 1fr',
      gridTemplateRows: '240px 250px',
      gap: '24px',
      padding: '16px 24px',
      margin: 0
    }}>
      <Box sx={{ display: 'flex' }}>
        <SystemHealthPanel systemHealth={systemHealth} />
      </Box>
      <Box sx={{ display: 'flex' }}>
        <RecentActivityPanel activities={activities} />
      </Box>
      <Box sx={{ display: 'flex', gridColumn: '1 / -1' }}>
        <DiscoveryStatisticsPanel stats={[]} />
      </Box>
    </Box>
  );
};

export default DashboardPage;
