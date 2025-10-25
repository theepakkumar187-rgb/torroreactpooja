# Torro Data Intelligence Platform - Backend

FastAPI backend for the Torro Data Intelligence Platform.

## Features

- **FastAPI**: Modern, fast web framework for building APIs
- **Automatic Documentation**: Swagger UI and ReDoc documentation
- **CORS Support**: Configured for frontend communication
- **Pydantic Models**: Type-safe data validation
- **Mock Data**: Development-ready with sample data

## API Documentation

Once the server is running, visit:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Available Endpoints

### Dashboard
- `GET /` - Root endpoint
- `GET /api/dashboard/stats` - Dashboard statistics
- `GET /api/system/health` - System health status

### Data Management
- `GET /api/assets` - List all data assets
- `GET /api/connectors` - List all connectors
- `GET /api/activities` - List recent activities

### Operations
- `POST /api/connectors/{connector_id}/toggle` - Toggle connector status
- `POST /api/scan/start` - Start a new system scan

## Development

### Running the Server

```bash
# Install dependencies
pip install -r requirements.txt

# Run development server
python main.py

# Or with uvicorn directly
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Adding New Endpoints

1. Define Pydantic models for request/response data
2. Add endpoint functions with proper decorators
3. Update this README with new endpoint documentation

### Database Integration

The current implementation uses mock data. To integrate with a real database:

1. Set up your database (PostgreSQL, MySQL, etc.)
2. Update the data models to use SQLAlchemy
3. Replace mock data with database queries
4. Add database migrations with Alembic

## Production Deployment

### Using Uvicorn

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Using Gunicorn

```bash
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### Environment Variables

Create a `.env` file for production configuration:

```env
DATABASE_URL=postgresql://user:password@localhost/torro_db
REDIS_URL=redis://localhost:6379
SECRET_KEY=your-secret-key
DEBUG=False
```

## Dependencies

- **FastAPI**: Web framework
- **Uvicorn**: ASGI server
- **Pydantic**: Data validation
- **SQLAlchemy**: Database ORM (for future use)
- **Alembic**: Database migrations (for future use)
- **Redis**: Caching and task queue (for future use)
- **Celery**: Background tasks (for future use)

