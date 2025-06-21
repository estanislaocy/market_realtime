@echo off
REM Start the Real-Time Stock Market Analytics Pipeline (Windows)

REM Step 1: Start Docker services
echo Starting Docker services...
docker-compose up -d

REM Step 2: Wait for services to initialize
echo Waiting 30 seconds for services to initialize...
timeout /t 30

REM Step 3: Create Kafka topic if it doesn't exist
echo Creating Kafka topic...
docker exec kafka kafka-topics --create --if-not-exists --topic stock-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

REM Step 4: Copy SQL schema file into the Postgres container
echo Copying database schema file...
docker cp sql\create_tables.sql postgres:/create_tables.sql

REM Step 5: Initialize database schema
echo Initializing database schema...
docker exec -i postgres psql -U postgres -d trading -f /create_tables.sql

echo.
echo Infrastructure is ready!
echo.
echo To start the producer, open a new terminal and run:
echo     python src\producer\stock_producer.py
echo To start the consumer, open another terminal and run:
echo     python src\consumer\spark_streaming_job.py
pause
