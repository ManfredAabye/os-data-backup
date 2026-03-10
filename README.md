# os-data-backup

Tabellenweises MySQL Backup/Restore Addon fuer Robust.

Das Addon stellt ein Robust-Konsolenkommando bereit, mit dem einzelne Tabellen oder alle Tabellen exportiert und wieder importiert werden koennen.

Format pro Tabelle:

- Dateiendung: `.otb`
- Inhalt: `tar.gz` (gzip-komprimiertes tar-Archiv)
- Nutzlast: eine SQL-Datei (Schema + Daten) fuer genau eine Tabelle
- Bei grossen Exporten kann eine Tabelle auf mehrere Teile gesplittet werden: `.part0001.otb`, `.part0002.otb`, ...

## Funktionen

- Tabellen auflisten (`list`)
- Einzelne Tabelle exportieren (`export <table>` oder `export <table> <datei.otb>`)
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
BackupFolder = "backupOTB"
; Harte Obergrenze fuer die geschaetzte Groesse einer einzelnen Tabelle
MaxSingleTableExportBytes = 1073741824
; Maximale Groesse eines einzelnen OTB-Teils
MaxOtbPartBytes = 536870912
```

## Konsolenbefehle

Mit Standard-Praefix:

```text
sqlbackup list
sqlbackup export <table>
sqlbackup export <table> <datei.otb>
sqlbackup export all <ordner>
sqlbackup import <table> <datei.otb>
sqlbackup import all <ordner>
```

## Verhalten

- `sqlbackup export <table>` ohne Dateiname speichert automatisch nach `backupOTB/` oder in das konfigurierte `BackupFolder`.
- Der Dateiname wird dann automatisch erzeugt: `<tabellenname>_yyyyMMdd_HHmmss.otb`.
- Bei `export all` wird pro Tabelle mindestens eine Datei erzeugt.
- Wenn eine Tabelle fuer einen einzelnen OTB-Teil zu gross wird, wird sie in mehrere Teile gesplittet.
- Split-Dateien sehen z.B. so aus: `users_20260311_101530.part0001.otb`, `users_20260311_101530.part0002.otb`.
- `import all` verarbeitet auch diese Split-Dateien und ordnet sie ueber den Dateinamen wieder der richtigen Tabelle zu.
- Wenn bei Einzel-Export/Import die Endung fehlt, wird `.otb` automatisch ergaenzt.
- Tabellen werden nicht parallel verarbeitet, sondern strikt hintereinander.
- `export all` und `import all` laufen als Hintergrundjob, damit die Konsole nicht blockiert.

## Beispiele

```text
sqlbackup list
sqlbackup export users
sqlbackup export users backups/users.otb
sqlbackup export all backups
sqlbackup import users backups/users.otb
sqlbackup import all backups
```

## Sicherheit und Verhalten

- Tabellennamen werden validiert (`[A-Za-z0-9_]+`).
- Export schreibt SQL mit `DROP TABLE IF EXISTS`, danach `CREATE TABLE` und `INSERT` Zeilen.
- Import fuehrt das SQL aus dem `.otb` Archiv direkt gegen die konfigurierte MySQL-Datenbank aus.
- Bei `all` werden Fehler pro Tabelle/Festdatei gemeldet, der Rest laeuft weiter.

## Einschraenkungen

- Das Addon ist auf MySQL ausgelegt (`MySql.Data`).
- Das aktuelle OTB-Format arbeitet weiterhin mit SQL-Text im Speicher und ist deshalb nicht fuer echte Multi-TB-Tabellen geeignet.
- `MaxSingleTableExportBytes` ist eine Schutzgrenze gegen instabile Exporte sehr grosser Tabellen.
- `MaxOtbPartBytes` teilt die Ausgabe in mehrere Dateien, loest aber nicht das Grundproblem extrem grosser Tabellen im RAM.
- Fuer sehr grosse Tabellen sind externe Streaming-/Snapshot-Backups weiterhin die bessere Wahl.
