INSTRUCCIONES RAPIDAS — Build de HarvestSync_Desk
=================================================

1) Copia estos archivos en tu carpeta del proyecto (donde esta HarvestSync_Desk.py):
   - requirements.txt
   - build_harvestsync.bat

   Asegurate de que en esa carpeta existan tambien:
   - HarvestSync_Desk.py
   - HarvestSync.json
   - icono_app.ico (opcional)
   - setup_harvestsync_with_json.iss (si quieres compilar instalador con Inno)

2) Doble click en build_harvestsync.bat  (o ejecútalo desde CMD).
   El script:
   - Crea y activa un venv (.venv)
   - Instala dependencias
   - Compila el EXE con PyInstaller (incluye HarvestSync.json)
   - Compila el instalador si encuentra ISCC.exe o lo abres luego en Inno

3) El ejecutable quedara en: dist\HarvestSync_Desk.exe
   Si Inno compila, el instalador se generará en la carpeta "output" de tu .iss.

Nota: Python 3.11 recomendado por compatibilidad.
