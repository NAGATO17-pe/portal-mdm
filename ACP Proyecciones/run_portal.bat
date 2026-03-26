@echo off
setlocal

:: ================================================================
:: run_portal.bat - Portal MDM ACP
:: Borra cache de Streamlit y lanza el portal
:: Ejecutar desde: d:\Proyecto2026\ACP_DWH\ACP Proyecciones\
:: ================================================================

set "PORTAL_DIR=%~dp0acp_mdm_portal"
set "PYTHON=%PORTAL_DIR%\.venv\Scripts\python.exe"
set "STREAMLIT=%PORTAL_DIR%\.venv\Scripts\streamlit.exe"

echo.
echo  ============================================
echo   ACP MDM Portal - Inicio seguro
echo  ============================================
echo.

:: Verificar que existe el entorno virtual correcto
if not exist "%STREAMLIT%" (
    echo  [ERROR] No se encontro streamlit en:
    echo          %STREAMLIT%
    echo.
    echo  Asegurate de haber instalado las dependencias con:
    echo    cd acp_mdm_portal
    echo    .venv\Scripts\pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

:: Limpiar cache de Streamlit
echo  [1/2] Limpiando cache de Streamlit...
"%PYTHON%" -m streamlit cache clear 2>nul
if exist "%USERPROFILE%\.streamlit\cache" (
    rmdir /s /q "%USERPROFILE%\.streamlit\cache" 2>nul
)
echo       Cache limpiado correctamente.
echo.

:: Lanzar el portal desde su directorio
echo  [2/2] Iniciando portal...
echo        URL: http://localhost:8501
echo.
echo  Presiona Ctrl+C para detener el servidor.
echo  ============================================
echo.

cd /d "%PORTAL_DIR%"
"%STREAMLIT%" run app.py --server.runOnSave true --server.fileWatcherType poll

endlocal
