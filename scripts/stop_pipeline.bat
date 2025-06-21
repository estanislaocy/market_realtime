@echo off
REM Stop the Real-Time Stock Market Analytics Pipeline (Windows)

echo Stopping Docker services...
docker-compose down

REM Uncomment the next line if you want to remove all Docker volumes (deletes data)
REM docker-compose down -v

echo Pipeline stopped successfully!
pause
