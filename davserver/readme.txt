Webdav als Laufwerk unter Windows:
net use Z: \\localhost:8080

Falls Webdav unter Windows extrem langsam ist:
Internetoptionen -> Verbindungen -> LAN-Einstellungen
Häkchen entfernen
Windows XP: "Automatische Suche der Einstellungen"
Windows 7: "Einstellungen automatisch erkennen"

Internet Options -> Connections tab -> LAN Settings
Uncheck "Automatically detect settings"

Alternative, soll auch gut / möglicherweise besser sein:
http://www.netdrive.net

Falls Webdav unter Windows keine Dateien >50MB akzeptiert:
HKLM\SYSTEM\CurrentControlSet\services\WebClient\Parameters
FileSizeLimitInBytes anpassen, Maximum 0xffffffff (?)
Reboot erforderlich
