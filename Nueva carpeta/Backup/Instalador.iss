; Script generado para HarvestSync Desk
[Setup]
AppName=HarvestSync Desk
AppVersion=1.0
DefaultDirName={pf}\\HarvestSync Desk
DefaultGroupName=HarvestSync Desk
OutputDir=.
OutputBaseFilename=HarvestSyncDesk_Installer
Compression=lzma
SolidCompression=yes
SetupIconFile=icono_app.ico

[Files]
Source: "HarvestSync_Desk.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "HarvestSync.json"; DestDir: "{app}"; Flags: ignoreversion
Source: "icono_app.png"; DestDir: "{app}"; Flags: ignoreversion
Source: "README.txt"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\\HarvestSync Desk"; Filename: "{app}\\HarvestSync_Desk.exe"; IconFilename: "{app}\\icono_app.ico"
Name: "{commondesktop}\\HarvestSync Desk"; Filename: "{app}\\HarvestSync_Desk.exe"; IconFilename: "{app}\\icono_app.ico"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Crear icono en el escritorio"; GroupDescription: "Iconos adicionales:"
