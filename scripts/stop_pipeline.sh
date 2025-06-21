#!/bin/bash

echo "Stopping Real-Time Stock Market Analytics Pipeline..."

# Stop all Docker services
docker-compose down

# Remove volumes (optional - uncomment if you want to clear all data)
# docker-compose down -v

echo "Pipeline stopped successfully!"
