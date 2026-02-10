@echo off
REM ============================================================================
REM Pulse ML Pipeline - Stop All Components (Windows)
REM ============================================================================

echo Stopping Pulse Pipeline...
echo.

REM Kill Python processes by window title
echo Stopping Producer...
taskkill /FI "WINDOWTITLE eq Producer*" /F 2>nul

echo Stopping Engine Consumer...
taskkill /FI "WINDOWTITLE eq Engine Consumer*" /F 2>nul

echo Stopping Aggregate Consumer...
taskkill /FI "WINDOWTITLE eq Aggregate Consumer*" /F 2>nul

echo Stopping ML Consumer...
taskkill /FI "WINDOWTITLE eq ML Consumer*" /F 2>nul

echo Stopping Dashboard...
taskkill /FI "WINDOWTITLE eq Dashboard*" /F 2>nul

echo.
echo ============================================
echo  All components stopped.
echo ============================================
echo.
echo Note: Kafka (Redpanda) is still running.
echo To stop Kafka: cd kafka_stream ^&^& docker-compose -f docker-compose-local.yml down
echo.
pause
