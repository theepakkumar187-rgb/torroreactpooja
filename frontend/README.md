# Torro Data Intelligence Platform - Frontend

React frontend for the Torro Data Intelligence Platform built with Material-UI v7 and Vite.

## Features

- **React 19**: Latest React with TypeScript
- **Material-UI v7**: Modern component library
- **Vite**: Fast build tool and development server
- **Responsive Design**: Mobile-first, responsive layout
- **Real-time Updates**: Live data from backend APIs

## Development

### Prerequisites

- Node.js 18 or higher
- npm or yarn

### Getting Started

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

### Available Scripts

- `npm run dev` - Start development server with hot reload
- `npm run build` - Build for production
- `npm run preview` - Preview production build locally
- `npm run lint` - Run ESLint
- `npm start` - Alias for `npm run dev`

## Project Structure

```
frontend/
├── public/                 # Static assets
├── src/
│   ├── App.tsx            # Main application component
│   ├── main.tsx           # Application entry point
│   ├── App.css            # Global styles
│   └── vite-env.d.ts      # Vite type definitions
├── package.json           # Dependencies and scripts
├── tsconfig.json          # TypeScript configuration
├── vite.config.ts         # Vite configuration
└── README.md
```

## Components

### Dashboard
- **Summary Cards**: Display key metrics (assets, connectors, scans)
- **System Health Panel**: Real-time system status and monitoring
- **Recent Activity**: Live activity feed
- **Discovery Statistics**: Data discovery metrics (placeholder)

### Navigation
- **Tab Navigation**: Switch between different platform sections
- **Header**: Platform branding and system status

## API Integration

The frontend communicates with the backend through REST APIs:

```typescript
// Example API call
const fetchDashboardData = async () => {
  const response = await fetch('http://localhost:8000/api/dashboard/stats');
  const data = await response.json();
  return data;
};
```

### Available API Endpoints

- `GET /api/dashboard/stats` - Dashboard statistics
- `GET /api/system/health` - System health status
- `GET /api/assets` - Data assets
- `GET /api/connectors` - Connectors
- `GET /api/activities` - Recent activities

## Styling

The application uses Material-UI's theming system:

```typescript
const theme = createTheme({
  palette: {
    primary: {
      main: '#7c3aed', // Purple theme
    },
    secondary: {
      main: '#f3f4f6', // Light gray
    },
  },
});
```

## Building for Production

```bash
# Build the application
npm run build

# The built files will be in the 'dist' directory
# Serve with any static file server
```

## Environment Configuration

Create a `.env` file for environment-specific configuration:

```env
VITE_API_BASE_URL=http://localhost:8000
VITE_APP_TITLE=Torro Data Intelligence Platform
```

## Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)

## Contributing

1. Follow the existing code style
2. Use TypeScript for all new code
3. Add proper type definitions
4. Test your changes thoroughly
5. Update documentation as needed