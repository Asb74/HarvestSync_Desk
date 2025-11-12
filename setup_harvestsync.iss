
; ==============================
; Instalador Inno Setup 6.4.3 - HarvestSync Desk
; (como Sansebassms, incluye JSON y desinstalador)
; ==============================

#define MyAppName        "HarvestSync Desk"
#define MyAppVersion     "1.0.0"
#define MyAppPublisher   "Perceco"
#define MyAppURL         "https://example.com/HarvestSyncDesk"
#define MyAppExeName     "HarvestSync_Desk.exe"
#define MyBuildFolder    "HarvestSync_Desk"
#define MyIconFile       "HarvestSync_Desk\icono_app.ico"

#define OutputBaseName   MyAppName + "-Setup-" + MyAppVersion
#define OutputDir        "output"

; GUID con llaves dobles (escape correcto)
#define MyAppId          "{{E9A6D70F-4668-4E13-B55D-C873FAA54081}}"

[Setup]
AppId={#MyAppId}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={pf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableDirPage=no
DisableProgramGroupPage=no
UsePreviousLanguage=no
Compression=lzma
SolidCompression=yes
OutputDir={#OutputDir}
OutputBaseFilename={#OutputBaseName}
WizardStyle=modern
PrivilegesRequired=admin
UninstallDisplayIcon={app}\{#MyAppExeName}
#ifdef MyIconFile
SetupIconFile={#MyIconFile}
#endif
; LicenseFile=license.txt  ; opcional

[Languages]
Name: "es"; MessagesFile: "compiler:Languages\Spanish.isl"
Name: "en"; MessagesFile: "compiler:Default.isl"

[Files]
; Ejecutable principal
Source: "{#MyBuildFolder}\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
; JSON de credenciales Firebase (línea explícita)
Source: "{#MyBuildFolder}\HarvestSync.json"; DestDir: "{app}"; Flags: ignoreversion
; Resto de archivos y subcarpetas
Source: "{#MyBuildFolder}\*"; Excludes: "{#MyBuildFolder}\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Desinstalar {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{userdesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Crear icono en el escritorio"; GroupDescription: "Tareas opcionales:"; Flags: unchecked

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Iniciar {#MyAppName}"; Flags: nowait postinstall skipifsilent
