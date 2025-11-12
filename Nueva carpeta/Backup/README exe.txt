cd C:\HarvestSync_Desk
sin ventana
pyinstaller --noconfirm --onefile --windowed ^
  --icon=icono_app.ico ^
  --add-data "HarvestSync.json;." ^
  --add-data "COOPERATIVA.png;." ^
  --add-data "icono_app.png;." ^
  --add-data "icono_app.ico;." ^
  HarvestSync_Desk.py

con ventana
pyinstaller --noconfirm --onefile ^
  --icon=icono_app.ico ^
  --add-data "HarvestSync.json;." ^
  --add-data "COOPERATIVA.png;." ^
  --add-data "icono_app.png;." ^
  --add-data "icono_app.ico;." ^
  HarvestSync_Desk.py
