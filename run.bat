@echo off 
echo Starting Spark Restaurant Weather ETL Pipeline... 
where python >nul 2>&1
if %errorlevel%==0 (
    python src\restaurant_weather_etl.py
) else (
    where py >nul 2>&1
    if %errorlevel%==0 (
        py -3 src\restaurant_weather_etl.py
    ) else (
        echo Python runtime is not installed or not available in PATH.
        exit /b 1
    )
)
echo. 
echo Pipeline finished. 
pause 
