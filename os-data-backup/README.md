# os-data-backup

Tabellenweises MySQL Backup/Restore Addon fuer Robust.

Das Addon stellt ein Robust-Konsolenkommando bereit, mit dem einzelne Tabellen oder alle Tabellen exportiert und wieder importiert werden koennen.

Format pro Tabelle:

- Dateiendung: `.otb`
- Inhalt: `tar.gz` (gzip-komprimiertes tar-Archiv)
- Nutzlast: eine SQL-Datei (Schema + Daten) fuer genau eine Tabelle

## Funktionen

- Tabellen auflisten (`list`)
- Einzelne Tabelle exportieren (`export <table> <datei.otb>`)
- Alle Tabellen exportieren (`export all <ordner>`)
- Einzelne Tabelle importieren (`import <table> <datei.otb>`)
- Alle Tabellen importieren (`import all <ordner>`)

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
CommandPrefix = "sqlbackup"
BackupFolder = "sqlbackup"
```

## Konsolenbefehle

Mit Standard-Praefix:

```text
sqlbackup list
sqlbackup export <table> <datei.otb>
sqlbackup export all <ordner>
sqlbackup import <table> <datei.otb>
sqlbackup import all <ordner>
```

Hinweise:

- Bei `export all` wird pro Tabelle eine Datei `<tabellenname>.otb` erzeugt.
- Bei `import all` werden alle `.otb` Dateien aus dem Ordner geladen.
- Wenn bei Einzel-Export/Import die Endung fehlt, wird `.otb` automatisch ergaenzt.

## Beispiele

```text
sqlbackup list
sqlbackup export users backups/users.otb
sqlbackup export all backups
sqlbackup import users backups/users.otb
sqlbackup import all backups
```

## Sicherheit und Verhalten

- Tabellennamen werden validiert (`[A-Za-z0-9_]+`).
- Export schreibt SQL mit `DROP TABLE IF EXISTS`, danach `CREATE TABLE` und `INSERT` Zeilen.
- Import fuehrt das SQL aus dem `.otb` Archiv direkt gegen die konfigurierte MySQL-Datenbank aus.

## Einschraenkungen

- Das Addon ist auf MySQL ausgelegt (`MySql.Data`).
- Sehr grosse Tabellen fuehren zu grossen SQL-Skripten und entsprechend laengerer Laufzeit.
