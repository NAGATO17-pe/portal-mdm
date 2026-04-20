@echo off
chcp 65001 > nul

:: Auto-wrap: garantiza que la ventana persiste con doble clic
if "%~1"=="--child" goto SKIP_WRAP
cmd /k "%~f0" --child
exit

:SKIP_WRAP
title ACP Platform - Consola de Control

set BASE=%~dp0
set VENV=%BASE%.venv\Scripts
set BACKEND=%BASE%backend
set PORTAL=%BASE%acp_mdm_portal
set LOG_BACK=%BACKEND%\logs\backend.log
set LOG_RUN=%BACKEND%\logs\runner.log

:: Crear carpeta logs si no existe
if not exist "%BACKEND%\logs" mkdir "%BACKEND%\logs"

echo.
echo  ========================================================
echo     ACP PLATFORM - Consola de Control
echo     Comandos: ON  OFF  RESTART  STATUS  LOGS  EXIT
echo  ========================================================
echo.

:MENU
set CMD=
set /p CMD= Comando ^>
if /i "%CMD%"=="ON"      goto START
if /i "%CMD%"=="OFF"     goto STOP
if /i "%CMD%"=="RESTART" goto RESTART
if /i "%CMD%"=="STATUS"  goto STATUS
if /i "%CMD%"=="LOGS"    goto LOGS
if /i "%CMD%"=="EXIT"    goto FIN
echo  [!] Comandos validos: ON  OFF  RESTART  STATUS  LOGS  EXIT
goto MENU


:START
echo.

:: ── Backend FastAPI ─────────────────────────────────────────
tasklist /FI "WINDOWTITLE eq ACP: Backend API*" 2>nul | find /I "cmd.exe" > nul
if not errorlevel 1 (
    echo  [~] Backend ya corriendo - se omite
) else (
    echo  [1/3] Iniciando Backend FastAPI en puerto 8000...
    start "ACP: Backend API" cmd /k "title ACP: Backend API && cd /d "%BACKEND%" && "%VENV%\uvicorn.exe" main:aplicacion --host 0.0.0.0 --port 8000"
    timeout /t 3 /nobreak > nul
    echo       OK ^> http://localhost:8000
)

:: ── Runner ETL ──────────────────────────────────────────────
tasklist /FI "WINDOWTITLE eq ACP: Runner ETL*" 2>nul | find /I "cmd.exe" > nul
if not errorlevel 1 (
    echo  [~] Runner ETL ya corriendo - se omite
) else (
    echo  [2/3] Iniciando Runner ETL daemon...
    start "ACP: Runner ETL" cmd /k "title ACP: Runner ETL && cd /d "%BACKEND%" && "%VENV%\python.exe" -m runner.runner"
    timeout /t 2 /nobreak > nul
    echo       OK ^> Runner escuchando cola
)

:: ── Portal Streamlit ─────────────────────────────────────────
tasklist /FI "WINDOWTITLE eq ACP: Portal MDM*" 2>nul | find /I "cmd.exe" > nul
if not errorlevel 1 (
    echo  [~] Portal MDM ya corriendo - se omite
) else (
    echo  [3/3] Iniciando Portal Streamlit en puerto 8501...
    start "ACP: Portal MDM" cmd /k "title ACP: Portal MDM && cd /d "%PORTAL%" && "%VENV%\streamlit.exe" run app.py"
    echo       OK ^> http://localhost:8501
)

echo.
echo  Servicios activos.
echo.
goto MENU


:STOP
echo.
echo  Deteniendo servicios...
taskkill /FI "WINDOWTITLE eq ACP: Backend API*" /F /T > nul 2>&1
taskkill /FI "WINDOWTITLE eq ACP: Runner ETL*"  /F /T > nul 2>&1
taskkill /FI "WINDOWTITLE eq ACP: Portal MDM*"  /F /T > nul 2>&1
echo  Todos los servicios detenidos.
echo.
goto MENU


:RESTART
echo.
echo  Reiniciando servicios...
taskkill /FI "WINDOWTITLE eq ACP: Backend API*" /F /T > nul 2>&1
taskkill /FI "WINDOWTITLE eq ACP: Runner ETL*"  /F /T > nul 2>&1
taskkill /FI "WINDOWTITLE eq ACP: Portal MDM*"  /F /T > nul 2>&1
timeout /t 2 /nobreak > nul
goto START


:STATUS
echo.
echo  -- Estado de Servicios --
tasklist /FI "WINDOWTITLE eq ACP: Backend API*" 2>nul | find /I "cmd.exe" > nul
if errorlevel 1 (echo  Backend API  : DETENIDO) else (echo  Backend API  : CORRIENDO ^> http://localhost:8000)
tasklist /FI "WINDOWTITLE eq ACP: Runner ETL*" 2>nul | find /I "cmd.exe" > nul
if errorlevel 1 (echo  Runner ETL   : DETENIDO) else (echo  Runner ETL   : CORRIENDO)
tasklist /FI "WINDOWTITLE eq ACP: Portal MDM*" 2>nul | find /I "cmd.exe" > nul
if errorlevel 1 (echo  Portal MDM   : DETENIDO) else (echo  Portal MDM   : CORRIENDO ^> http://localhost:8501)
echo.
goto MENU


:LOGS
echo.
echo  -- Ultimas 20 lineas: Backend --
if exist "%LOG_BACK%" (
    powershell -NoProfile -command "Get-Content '%LOG_BACK%' -Tail 20"
) else (
    echo  Sin log aun.
)
echo.
echo  -- Ultimas 20 lineas: Runner ETL --
if exist "%LOG_RUN%" (
    powershell -NoProfile -command "Get-Content '%LOG_RUN%' -Tail 20"
) else (
    echo  Sin log aun.
)
echo.
goto MENU


:FIN
exit
