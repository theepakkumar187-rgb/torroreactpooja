#!/bin/bash

# Torro Data Intelligence Platform - Start Script
# This script starts both the backend and frontend servers

echo "🚀 Starting Torro Data Intelligence Platform..."

# Function to check if a port is in use
check_port() {
    if lsof -Pi :$1 -sTCP:LISTEN -t >/dev/null ; then
        echo "❌ Port $1 is already in use"
        return 1
    else
        echo "✅ Port $1 is available"
        return 0
    fi
}

# Check if ports are available
echo "🔍 Checking port availability..."
if ! check_port 8000; then
    echo "Please stop the process using port 8000 or change the backend port"
    exit 1
fi

if ! check_port 5173; then
    echo "Please stop the process using port 5173 or change the frontend port"
    exit 1
fi

# Start backend
echo "🐍 Starting Python backend..."
cd backend
if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv venv
fi

echo "📦 Activating virtual environment and installing dependencies..."
source venv/bin/activate
pip install -r requirements.txt > /dev/null 2>&1

echo "🚀 Starting FastAPI server on http://localhost:8000"
python main.py &
BACKEND_PID=$!

# Wait a moment for backend to start
sleep 3

# Start frontend
echo "⚛️  Starting React frontend..."
cd ../frontend

echo "📦 Installing frontend dependencies..."
npm install > /dev/null 2>&1

echo "🚀 Starting Vite development server on http://localhost:5173"
npm run dev &
FRONTEND_PID=$!

echo ""
echo "🎉 Torro Data Intelligence Platform is starting up!"
echo ""
echo "📊 Frontend: http://localhost:5173"
echo "🔧 Backend API: http://localhost:8000"
echo "📚 API Docs: http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop both servers"

# Function to cleanup processes on exit
cleanup() {
    echo ""
    echo "🛑 Stopping servers..."
    kill $BACKEND_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    echo "✅ Servers stopped"
    exit 0
}

# Set trap to cleanup on script exit
trap cleanup SIGINT SIGTERM

# Wait for both processes
wait

