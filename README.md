# Torro Data Intelligence Platform

A modern data intelligence platform built with React MUI v7 frontend and Python FastAPI backend.

## Project Structure

```
TorroDiscovery/
├── frontend/          # React MUI v7 + Vite frontend
├── backend/           # Python FastAPI backend
└── README.md
```

## Features

- **Modern Dashboard**: Clean, responsive UI built with Material-UI v7
- **System Health Monitoring**: Real-time system status and health indicators
- **Data Asset Management**: Track and manage data assets across multiple catalogs
- **Connector Management**: Monitor and control data connectors
- **Activity Tracking**: Real-time activity feed and system events
- **RESTful API**: FastAPI backend with automatic API documentation

## Quick Start

### Prerequisites

- Node.js 18+ and npm
- Python 3.8+
- pip (Python package manager)

### Backend Setup

1. Navigate to the backend directory:
   ```bash
   cd backend
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Start the backend server:
   ```bash
   python main.py
   ```

The API will be available at `http://localhost:8000`
API documentation: `http://localhost:8000/docs`

### Frontend Setup

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm run dev
   ```

The frontend will be available at `http://localhost:5173`

## API Endpoints

### Dashboard
- `GET /api/dashboard/stats` - Get dashboard statistics
- `GET /api/system/health` - Get system health status

### Assets
- `GET /api/assets` - Get all data assets

### Connectors
- `GET /api/connectors` - Get all connectors
- `POST /api/connectors/{id}/toggle` - Toggle connector status

### Activities
- `GET /api/activities` - Get recent activities

### Scans
- `POST /api/scan/start` - Start a new scan

## Development

### Backend Development

The backend uses FastAPI with the following key features:
- Automatic API documentation with Swagger UI
- CORS enabled for frontend communication
- Pydantic models for data validation
- Mock data for development and testing

### Frontend Development

The frontend is built with:
- React 19 with TypeScript
- Material-UI v7 for components
- Vite for fast development and building
- Responsive design with MUI Grid system

### Adding New Features

1. **Backend**: Add new endpoints in `main.py` with proper Pydantic models
2. **Frontend**: Create new components and integrate with backend APIs
3. **API Integration**: Update the frontend to call new backend endpoints

## Deployment

### Backend Deployment

1. Install production dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run with production server:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

### Frontend Deployment

1. Build the production bundle:
   ```bash
   npm run build
   ```

2. Serve the `dist` folder with any static file server

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License.

