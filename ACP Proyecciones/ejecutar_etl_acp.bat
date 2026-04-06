@echo off
setlocal

set "BASE_DIR=%~dp0"
set "PYTHON_EXE=%BASE_DIR%.venv\Scripts\python.exe"
set "ETL_DIR=%BASE_DIR%ETL"
set "PIPELINE_SCRIPT=%ETL_DIR%\pipeline.py"
set "EXIT_CODE=1"
set "PYTHONUTF8=1"

title ETL ACP DWH

echo ============================================================
echo   Ejecutando ETL ACP DWH
echo ============================================================
echo Python : "%PYTHON_EXE%"
echo Proyecto: "%ETL_DIR%"
echo.

if not exist "%PYTHON_EXE%" (
    echo [ERROR] No existe el entorno virtual en ".venv\Scripts\python.exe".
    goto :fin
)

if not exist "%PIPELINE_SCRIPT%" (
    echo [ERROR] No existe el script "ETL\pipeline.py".
    goto :fin
)

pushd "%ETL_DIR%"
"%PYTHON_EXE%" "%PIPELINE_SCRIPT%"
set "EXIT_CODE=%ERRORLEVEL%"
popd

if not "%EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] El ETL finalizo con codigo %EXIT_CODE%.
    goto :fin
)

echo.
echo [OK] El ETL finalizo correctamente.

:fin
echo.
pause
exit /b %EXIT_CODE%
