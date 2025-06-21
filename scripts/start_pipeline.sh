#!/bin/bash

echo "Starting Real-Time Stock Market Analytics Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

# Start infrastructure services
echo "Starting infrastructure services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 30

# Check if Kafka is ready
echo "Checking Kafka connectivity..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create Kafka topic if it doesn't exist
echo "Creating Kafka topic..."
docker exec kafka kafka-topics --create --if-not-exists \
    --topic stock-data \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

# Initialize database schema
echo "Initializing database schema..."
docker exec -it postgres psql -U postgres -d trading -f /sql/create_tables.sql

echo "Infrastructure is ready!"
echo "You can now run the producer and consumer:"
echo "  Python producer: python src/producer/stock_producer.py"
echo "  Spark consumer: python src/consumer/spark_streaming_job.py"
