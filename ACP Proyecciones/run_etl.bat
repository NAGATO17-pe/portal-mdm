@echo off
setlocal

set "ROOT=%~dp0"
set "PYTHON_EXE="

if exist "%ROOT%venv\Scripts\python.exe" set "PYTHON_EXE=%ROOT%venv\Scripts\python.exe"
if not defined PYTHON_EXE if exist "%ROOT%.venv\Scripts\python.exe" set "PYTHON_EXE=%ROOT%.venv\Scripts\python.exe"

if not defined PYTHON_EXE (
    echo [ERROR] No se encontro Python en "venv\Scripts\python.exe" ni ".venv\Scripts\python.exe".
    goto :fail
)

if not exist "%ROOT%ETL\pipeline.py" (
    echo [ERROR] No se encontro "ETL\pipeline.py".
    goto :fail
)

set "PYTHONUTF8=1"

echo ============================================================
echo   Ejecutando ETL ACP DWH
echo ============================================================
echo Python : "%PYTHON_EXE%"
echo Proyecto: "%ROOT%ETL"
echo.

pushd "%ROOT%ETL" >nul
"%PYTHON_EXE%" pipeline.py
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul

echo.
if not "%EXIT_CODE%"=="0" (
    echo [ERROR] El ETL finalizo con codigo %EXIT_CODE%.
    goto :exit_with_code
)

echo [OK] ETL finalizado correctamente.
goto :exit_with_code

:fail
set "EXIT_CODE=1"

:exit_with_code
if /i "%~1"=="--no-pause" exit /b %EXIT_CODE%
echo.
pause
exit /b %EXIT_CODE%
