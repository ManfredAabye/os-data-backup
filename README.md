# os-data-backup Version 1.0.0 Alpha

Tabellenweises MySQL Backup/Restore/Sync Addon fuer Robust.

Das Addon stellt Robust-Konsolenkommandos bereit, mit denen einzelne Tabellen oder alle Tabellen exportiert, wieder importiert, zwischen Datenbanken kopiert, verglichen, geprueft und repariert werden koennen.

Format pro Tabelle:

- Dateiendung: `.otb`
- Inhalt: `tar.gz` (gzip-komprimiertes tar-Archiv)
- Nutzlast: eine SQL-Datei (Schema + Daten) fuer genau eine Tabelle
- Bei grossen Exporten kann eine Tabelle auf mehrere Teile gesplittet werden: `.part0001.otb`, `.part0002.otb`, ...

## Funktionen

- Tabellen auflisten (`list`)
- Einzelne Tabelle exportieren (`export <table>` oder `export <table> <datei.otb|url>`)
- Alle Tabellen exportieren (`export all <ordner|url>`)
- Einzelne Tabelle importieren (`import <table> <datei.otb|url> [modus]`)
- Alle Tabellen importieren (`import all <ordner|url> [modus]`)
- Tabellen direkt in eine zweite MySQL-Datenbank kopieren (`copy <table|all> [modus]`)
- Quell- und Ziel-Datenbank vergleichen (`compare <table|all>`)
- Quell- und/oder Zieltabellen mit `CHECK TABLE` pruefen (`check <table|all> [source|target|both]`)
- Quell- und Zieltabellen reparieren/synchronisieren (`repair <table|all> [modus]`)
- Lokales Dateisystem und WebDAV-basierte externe Server/Cloud-Speicher als Backup-Ziel/-Quelle

## Einbindung in Robust

1. In der `ServiceList` den Connector aktivieren:

```ini
SqlDataBackupConnector = "${Const|PrivatePort}/OpenSim.Addons.SqlDataBackup.dll:SqlDataBackup"
```

1. Konfigurationsabschnitt aktivieren:

```ini
[SqlDataBackup]
Enabled = true
; Optional: eigener DB-String, sonst wird [DatabaseService] verwendet
;ConnectionString = ""
; Optional: Ziel-DB fuer copy/compare/check/repair
;TargetConnectionString = ""
CommandPrefix = "sqlbackup"
; Lokaler Ordner oder WebDAV-URL
BackupFolder = "backupOTB"
; Optional fuer WebDAV-Zugriff
;RemoteUser = ""
;RemotePassword = ""
RemoteTimeoutSeconds = 120
; replace | skip | error | merge-replace | merge-skip
DefaultConflictMode = "replace"
; Harte Obergrenze fuer die geschaetzte Groesse einer einzelnen Tabelle
MaxSingleTableExportBytes = 1073741824
; Maximale Groesse eines einzelnen OTB-Teils
MaxOtbPartBytes = 2147483648
```

## Konsolenbefehle

Mit Standard-Praefix:

```text
sqlbackup list
sqlbackup export <table>
sqlbackup export <table> <datei.otb|url>
sqlbackup export all <ordner|url>
sqlbackup import <table> <datei.otb|url> [replace|skip|error|merge-replace|merge-skip]
sqlbackup import all <ordner|url> [replace|skip|error|merge-replace|merge-skip]
sqlbackup copy <table|all> [replace|skip|error|merge-replace|merge-skip]
sqlbackup compare <table|all>
sqlbackup check <table|all> [source|target|both]
sqlbackup repair <table|all> [replace|skip|error|merge-replace|merge-skip]
```

## Konfliktmodi

- `replace`: Zielobjekte werden ersetzt. Beim Import aus OTB bedeutet das das bisherige Verhalten mit `DROP TABLE IF EXISTS` und anschliessendem Neuaufbau.
- `skip`: Wenn die Tabelle im Ziel bereits existiert, wird sie komplett uebersprungen.
- `error`: Wenn die Tabelle im Ziel bereits existiert, wird mit Fehler abgebrochen.
- `merge-replace`: Vorhandene Tabelle bleibt bestehen, Zeilen werden mit `REPLACE INTO` geschrieben.
- `merge-skip`: Vorhandene Tabelle bleibt bestehen, Zeilen werden mit `INSERT IGNORE INTO` geschrieben.

## Externer Speicher

- `BackupFolder` kann ein normaler Ordner oder eine WebDAV-URL sein.
- Einzelne Export-/Importkommandos koennen ebenfalls direkt mit einer Datei-URL oder einem WebDAV-Ordner arbeiten.
- Fuer WebDAV werden `RemoteUser` und `RemotePassword` verwendet.
- Typische Beispiele sind Nextcloud, ownCloud, WebDAV-faehige NAS-Systeme oder ein externer HTTPS/WebDAV-Backup-Server.

## Verhalten

- `sqlbackup export <table>` ohne Dateiname speichert automatisch nach `backupOTB/` oder in das konfigurierte `BackupFolder`.
- Der Dateiname wird dann automatisch erzeugt: `<tabellenname>_yyyyMMdd_HHmmss.otb`.
- Bei `export all` wird pro Tabelle mindestens eine Datei erzeugt.
- Wenn eine Tabelle fuer einen einzelnen OTB-Teil zu gross wird, wird sie in mehrere Teile gesplittet.
- Split-Dateien sehen z.B. so aus: `users_20260311_101530.part0001.otb`, `users_20260311_101530.part0002.otb`.
- `import all` verarbeitet auch diese Split-Dateien und ordnet sie ueber den Dateinamen wieder der richtigen Tabelle zu.
- Wenn bei Einzel-Export/Import die Endung fehlt, wird `.otb` automatisch ergaenzt.
- Tabellen werden nicht parallel verarbeitet, sondern strikt hintereinander.
- `export all`, `import all`, `copy all`, `compare all`, `check all` und `repair all` laufen als Hintergrundjob, damit die Konsole nicht blockiert.
- `copy`, `compare`, `check` und `repair` verwenden `TargetConnectionString` als Zielsystem.
- `repair` fuehrt erst `REPAIR TABLE` auf Quelle und Ziel aus und synchronisiert anschliessend abweichende Tabellen in Richtung Ziel.

## Beispiele

```text
sqlbackup list
sqlbackup export users
sqlbackup export users backups/users.otb
sqlbackup export all https://backup.example.org/remote.php/dav/files/admin/opensim/
sqlbackup export all backups
sqlbackup import users backups/users.otb replace
sqlbackup import all https://backup.example.org/remote.php/dav/files/admin/opensim/ merge-skip
sqlbackup copy users replace
sqlbackup compare all
sqlbackup check all both
sqlbackup repair users replace
sqlbackup import all backups
```

## Sicherheit und Verhalten

- Tabellennamen werden validiert (`[A-Za-z0-9_]+`).
- Export schreibt SQL mit `DROP TABLE IF EXISTS`, danach `CREATE TABLE` und `INSERT` Zeilen.
- Import fuehrt das SQL aus dem `.otb` Archiv direkt gegen die konfigurierte MySQL-Datenbank aus.
- Bei `merge-replace` und `merge-skip` wird das SQL fuer bestehende Tabellen angepasst statt die Tabelle zu droppen.
- WebDAV-Zugriffe erfolgen ueber HTTP/HTTPS. Der Zielserver muss `PUT`, `GET`, `DELETE` und `PROPFIND` unterstuetzen.
- Bei `all` werden Fehler pro Tabelle/Festdatei gemeldet, der Rest laeuft weiter.

## Einschraenkungen

- Das Addon ist auf MySQL ausgelegt (`MySql.Data`).
- Das aktuelle OTB-Format arbeitet weiterhin mit SQL-Text im Speicher und ist deshalb nicht fuer echte Multi-TB-Tabellen geeignet.
- `REPAIR TABLE` ist engine-abhaengig. Bei InnoDB kann MySQL hier nur eingeschraenkt oder gar nicht reparieren.
- Der neue `copy`-Pfad arbeitet ebenfalls ueber SQL-Text im Speicher und ist damit fuer extrem grosse Tabellen nicht optimal.
- `MaxSingleTableExportBytes` ist eine Schutzgrenze gegen instabile Exporte sehr grosser Tabellen.
- `MaxOtbPartBytes` teilt die Ausgabe in mehrere Dateien, loest aber nicht das Grundproblem extrem grosser Tabellen im RAM.
- Fuer sehr grosse Tabellen sind externe Streaming-/Snapshot-Backups weiterhin die bessere Wahl.

## Beispielkonfigurationen

Lokales Backup plus zweite Ziel-Datenbank:

```ini
[SqlDataBackup]
Enabled = true
ConnectionString = "Data Source=127.0.0.1;Database=robust;User ID=opensim;Password=secret;Old Guids=true;SslMode=None;"
TargetConnectionString = "Data Source=10.0.0.20;Database=robust_copy;User ID=opensim;Password=secret;Old Guids=true;SslMode=None;"
BackupFolder = "backupOTB"
DefaultConflictMode = "replace"
```

Backup auf WebDAV/Cloud:

```ini
[SqlDataBackup]
Enabled = true
BackupFolder = "https://backup.example.org/remote.php/dav/files/admin/opensim/"
RemoteUser = "admin"
RemotePassword = "secret"
RemoteTimeoutSeconds = 300
DefaultConflictMode = "merge-skip"
```

