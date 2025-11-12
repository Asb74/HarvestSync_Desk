@echo off
setlocal enabledelayedexpansion
title Build HarvestSync_Desk - v1

REM === CONFIGURACION ===
set "PY_EXE=python"
set "VENV_DIR=.venv"
set "MAIN_PY=HarvestSync_Desk.py"
set "ICON_FILE=icono_app.ico"
set "JSON_FILE=HarvestSync.json"
set "APP_NAME=HarvestSync_Desk"
set "INNO_ISS=setup_harvestsync_with_json.iss"
REM set "ISCC_EXE=C:\Program Files (x86)\Inno Setup 6\ISCC.exe"

echo.
echo ===============================
echo  Paso 1/5: Crear entorno venv
echo ===============================
%PY_EXE% -m venv "%VENV_DIR%"
if errorlevel 1 goto :err1

echo.
echo ===============================
echo  Paso 2/5: Activar venv
echo ===============================
call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 goto :err2

echo.
echo ===============================
echo  Paso 3/5: Instalar dependencias
echo ===============================
python -m pip install --upgrade pip
if exist requirements.txt (
  pip install -r requirements.txt
) else (
  pip install reportlab pillow tkcalendar pandas firebase_admin google-cloud-firestore pyinstaller
)
if errorlevel 1 goto :err3

echo.
echo ===============================
echo  Paso 4/5: Compilar EXE con PyInstaller
echo ===============================
if not exist "%MAIN_PY%" goto :err_missing_main
if not exist "%JSON_FILE%" goto :err_missing_json

if exist "%ICON_FILE%" (
  set "ICON_PARAM=-i \"%ICON_FILE%\""
) else (
  set "ICON_PARAM="
  echo [WARN] No se encontro %ICON_FILE%. Se compila sin icono.
)

pyinstaller -F -w -n "%APP_NAME%" %ICON_PARAM% --add-data "%JSON_FILE%;." "%MAIN_PY%"
if errorlevel 1 goto :err_pyinstaller

echo.
echo ===============================
echo  Paso 5/5: Compilar instalador Inno (opcional)
echo ===============================
if exist "%INNO_ISS%" (
  if defined ISCC_EXE (
    "%ISCC_EXE%" "%INNO_ISS%"
  ) else (
    where ISCC >nul 2>nul
    if %errorlevel%==0 (
      ISCC "%INNO_ISS%"
    ) else (
      echo [INFO] No se encontro ISCC.exe en PATH. Abre Inno Setup y compila manualmente: "%INNO_ISS%"
    )
  )
) else (
  echo [INFO] No se encontro "%INNO_ISS%". Saltando compilacion del instalador.
)

echo.
echo ====== RESULTADO ======
if exist "dist\%APP_NAME%.exe" (
  echo EXE: %CD%\dist\%APP_NAME%.exe
) else (
  echo [WARN] No se encontro el EXE en dist\. Revisa los mensajes anteriores.
)
echo.
echo Si se compilo el instalador, estara en la carpeta "output" definida en tu .iss.
goto :end

:err1
echo [ERROR] No se pudo crear el entorno virtual.
goto :end
:err2
echo [ERROR] No se pudo activar el entorno virtual.
goto :end
:err3
echo [ERROR] Fallo instalando dependencias.
goto :end
:err_missing_main
echo [ERROR] No se encontro %MAIN_PY% en %CD%
goto :end
:err_missing_json
echo [ERROR] No se encontro %JSON_FILE% en %CD%
goto :end
:err_pyinstaller
echo [ERROR] PyInstaller fallo.
goto :end

:end
echo.
echo [Build finalizado]
pause
