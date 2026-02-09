@echo off
REM ============================================================================
REM Pulse ML Pipeline - Full Stack Launcher
REM Starts all components with 600x speedup (1 hour = 6 seconds)
REM Usage: run_pipeline.bat [--clean]
REM ============================================================================

setlocal EnableDelayedExpansion

REM Configuration
set SPEEDUP=600
set BATCH_SIZE=65536
set IDLE_TIMEOUT=10
REM Set to --quiet for silent mode, or leave empty for verbose output (demos)
set QUIET_FLAG=--quiet
set PROJECT_DIR=%~dp0
cd /d "%PROJECT_DIR%"

REM Handle --clean flag
if "%1"=="--clean" (
    echo ============================================
    echo  Cleaning previous run data...
    echo ============================================
    echo.
    echo Removing Arrow shards...
    if exist "_data\arrow" rmdir /s /q "_data\arrow"
    echo Removing trained models...
    if exist "_data\models" rmdir /s /q "_data\models"
    echo Removing producer state...
    if exist "producer_state.json" del /f /q "producer_state.json"
    echo Removing SQLite databases...
    if exist "_data\stream_stats.sqlite" del /f /q "_data\stream_stats.sqlite"
    if exist "_data\stream_stats.sqlite-wal" del /f /q "_data\stream_stats.sqlite-wal"
    if exist "_data\stream_stats.sqlite-shm" del /f /q "_data\stream_stats.sqlite-shm"
    echo Removing lakehouse data...
    if exist "_data\store_stats" rmdir /s /q "_data\store_stats"
    if exist "_data\store_ml" rmdir /s /q "_data\store_ml"
    echo Removing training logs...
    if exist "_data\logs" rmdir /s /q "_data\logs"
    echo Removing consumer checkpoints...
    if exist "_data\checkpoints" rmdir /s /q "_data\checkpoints"
    echo Resetting Kafka consumer groups...
    docker exec redpanda rpk group delete redshift-stats-consumer >nul 2>&1
    docker exec redpanda rpk group delete anomalous-lakehouse-sink >nul 2>&1
    docker exec redpanda rpk group delete redshift-stats-consumer-optimized >nul 2>&1
    echo Removing QuixStreams internal topics...
    for /f "tokens=1" %%t in ('docker exec redpanda rpk topic list 2^>nul ^| findstr "repartition__ changelog__"') do (
        docker exec redpanda rpk topic delete %%t >nul 2>&1
    )
    echo Killing stale dashboard on port 8507...
    for /f "tokens=5" %%p in ('netstat -aon ^| findstr ":8507" ^| findstr "LISTENING"') do (
        taskkill /PID %%p /F >nul 2>&1
    )
    echo.
    echo Done! Starting fresh run...
    echo.
)

echo ============================================
echo  Pulse ML Pipeline Launcher
echo  Speedup: %SPEEDUP%x (4 days = ~10 minutes)
echo ============================================
echo.

REM Check if Kafka/Redpanda is running
echo [1/6] Checking Kafka (Redpanda)...
docker ps --filter "name=redpanda" --format "{{.Names}}" | findstr /i "redpanda" > nul
if %ERRORLEVEL% NEQ 0 (
    echo Starting Redpanda...
    cd kafka_stream
    start /min cmd /c "docker-compose -f docker-compose-local.yml up"
    cd ..
    echo Waiting 10s for Kafka to start...
    timeout /t 10 /nobreak > nul
) else (
    echo Redpanda is already running.
)
echo.

REM Start Aggregate Consumer (stream analytics)
echo [2/6] Starting Aggregate Consumer...
start "Aggregate Consumer" cmd /k "python pipeline/consumer_aggregate.py %QUIET_FLAG%"
timeout /t 2 /nobreak > nul

REM Start ML Consumer (anomaly sink)
echo [3/6] Starting ML Consumer...
start "ML Consumer" cmd /k "python pipeline/consumer_ml.py %QUIET_FLAG%"
timeout /t 2 /nobreak > nul

REM Start Engine Consumer (ML inference)
echo [4/6] Starting Engine Consumer...
start "Engine Consumer" cmd /k "python pipeline/consumer.py %QUIET_FLAG% --inference-batch-size %BATCH_SIZE% --idle-timeout %IDLE_TIMEOUT%"
timeout /t 3 /nobreak > nul

REM Start Producer with 600x speedup
echo [5/6] Starting Producer (speedup=%SPEEDUP%x)...
start "Producer" cmd /k "python pipeline/producer.py --speedup %SPEEDUP% --reset"
timeout /t 2 /nobreak > nul

REM Start Dashboard (Preact)
echo [6/6] Starting Dashboard (Preact)...
start "Dashboard" cmd /k "python dashboard/api.py"
timeout /t 3 /nobreak > nul

echo.
echo ============================================
echo  All components started!
echo ============================================
echo.
echo  Dashboard:  http://localhost:8507
echo  Kafka UI:   http://localhost:8080
echo.
echo  Windows opened:
echo    - Producer (speedup %SPEEDUP%x)
echo    - Engine Consumer (ML inference)
echo    - Aggregate Consumer (stream analytics)
echo    - ML Consumer (anomaly sink)
echo    - Dashboard (Preact)
echo.
echo  Press any key to close this launcher...
echo  (Other windows will keep running)
echo.
pause > nul
