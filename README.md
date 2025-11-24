# Nextcloud Automation

Dieses Repository beinhaltet Skripte zur Automatisierung gewisser Abläufe in einer Nextcloud.

## Konfiguration

Die Konfiguration (vor allem von User Credentials mit `NEXTCLOUD_USER` und `NEXTCLOUD_PASSWORD`)
erfolgt über Umgebungsvariablen. Diese können normal mit `export` oder `set` (je nach OS) gesetzt
werden oder in einer `.env` Datei definiert werden. Für eine Übersicht relevanter Schlüssel,
siehe `.env.example`.

## Verfügbare Skripe

### Backup Nextcloud Tables

Mit `scripts/run_backup.sh` können Backups aller Nextcloud Tables durchgeführt werden.

Beispiel für einen monatlichen Cronjob:

```sh
~/kita_nextcloud_automation/scripts/run_backup.sh ~/backup/monthly 12
```

Hierdurch werden alle Tabellen der Nextcloud-Tables-App *auf die der User `NEXTCLOUD_USER` Zugriff
hat* in den Ordner `~/backup/monthly` gespeichert. Es werden je Tabelle maximal 12 Kopien vorgehalten.

Beispiel für einen täglichen Cronjob:

```sh
~/kita_nextcloud_automation/scripts/run_backup.sh ~/backup/daily 10
```

## Erstellung der Stundeliste

Um die konsolidierte Kita-Stundeliste zu Erstellen braucht der User Zugriff auf die Adressliste und
die Stundeliste. Mit dem Skript `scripts/run_pipeline.sh` oder mit `uv run pipeline.py` wird die
Pipeline angestoßen. Es werden die zwei EIngangstabellen gelesen, die Daten entsprechen transformiert
und dann in die Ausgangstabelle geschrieben. Die IDs der jeweiligen Tabellen sind als Umgebungsvariablen
oder in `.env` definiert.
