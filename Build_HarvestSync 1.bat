pyinstaller --clean -F -w ^
  -n HarvestSync_Desk ^
  -i icono_app.ico ^
  --hidden-import pyodbc --collect-all pyodbc ^
  --add-data "HarvestSync.json;." ^
  --add-data "COOPERATIVA.png;." ^
  HarvestSync_Desk.py
