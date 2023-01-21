# Verwendung des Dedup Dateisystems

Dies ist die Schnellstart-Anleitung. Die vollständige Beschreibung finden Sie hier: [README.html](README.html).

Wenn Sie diesen Text in einer Archivdatei wie `dedupfs-[Version].zip` gefunden haben und schnell loslegen wollen, lesen Sie [Ein neues Dedup Dateisystem starten](#ein-neues-dedup-dateisystem-starten). Wenn Sie diesen Text lesen, weil Sie ihn in einem Verzeichnis gefunden haben, das mehr oder weniger so aussieht

```
[Verzeichnis]  data
[Verzeichnis]  dedupfs-[Version]
[Verzeichnis]  fsdb
[   Datei   ]  SCHNELLSTART.html
```

und Sie wollen den Inhalt des Verzeichnisses nutzen, dann lesen Sie weiter.

## Ein vorhandenes Dedup Dateisystem verwenden

Sie haben wahrscheinlich ein Verzeichnis gefunden, das ein Dateisystem enthält, und dieses Dateisystem kann interessante Dinge enthalten. Um einen Blick darauf zu werfen, gehen Sie wie folgt vor:

* Öffnen Sie das Verzeichnis `dedupfs-[Version]`.
* Führen Sie das Skript `readonly.bat` (`readonly` unter Linux) aus.
* Jetzt könnte etwas schiefgehen. Wenn das der Fall ist und Sie unter Windows arbeiten, haben Sie vielleicht `WinFSP` nicht installiert. Viel Glück beim Lesen der [README.html](README.html).
* Wenn alles gut geht, sollten Sie eine Ausgabe wie diese sehen:

```
[...] - Dedup file system settings:
[...] - Repository:  [Verzeichnis]
[...] - Mount point: J:\
[...] - Readonly:    true
[...] - Starting the dedup file system now...
The service java has been started.
```

Das Interessanteste ist die Meldung "`Mount point: J:\`". In diesem Beispiel (es stammt von einem Windows-System) sagt die Meldung, dass Sie das Laufwerk `J:\` öffnen können, um diese interessanten Dinge wie Backups oder andere Dateien zu finden. Viel Spaß dabei!

Wenn Sie mit dem Durchsuchen der Dateien im Laufwerk "J:\" fertig sind, wechseln Sie zu dem Fenster mit der obigen Ausgabe und drücken Sie "STRG-C", um das Dateisystem anzuhalten. Sie werden eine Ausgabe wie diese sehen:

```
The service java has been stopped.
[...] - Stopping dedup file system...
[...] - Dedup file system is stopped.
[...] - Shutdown complete.
```

Das ist alles für den Schnellstart. Die vollständige Beschreibung finden Sie hier: [README.html](README.html).

## Ein neues Dedup Dateisystem starten

Wenn Sie diesen Text in einer Archivdatei wie `dedupfs-[Version].zip` gefunden haben und schnell loslegen wollen, gehen Sie wie folgt vor:

* Wenn Sie mit Windows arbeiten, laden Sie [WinFSP](https://github.com/billziss-gh/winfsp/releases) herunter und installieren Sie es.
* Erstellen Sie irgendwo ein neues Verzeichnis mit dem Namen `dedup_storage`, z.B. auf Ihrer Backup-USB-Festplatte, und entpacken Sie das Archiv dorthin.
* Öffnen Sie das Verzeichnis `dedup_storage/dedupfs-<Version>`.
* Führen Sie das Skript `repo-init.bat` (`repo-init` unter Linux) aus. Damit wird das Dedup Dateisystem in `dedup_storage` initialisiert.
* Starten Sie das Skript `dedupfs.bat` (`dedupfs` unter Linux). Dadurch wird das Dedup-Dateisystem gestartet.
* Wenn alles gut geht, sollten Sie eine Ausgabe wie diese sehen:

```
[...] - Dedup file system settings:
[...] - Repository:  [Verzeichnis]
[...] - Mount point: J:\
[...] - Readonly:    false
[...] - Starting the dedup file system now...
The service java has been started.
```

Das Interessanteste ist die Meldung "`Mount point: J:\`". In diesem Beispiel (es stammt von einem Windows-System) sagt die Meldung, dass Sie das Laufwerk `J:\` öffnen und Dateien, die Sie sichern wollen, dorthin kopieren können. Viel Spaß!

Wenn Sie mit dem Kopieren von Dateien auf das Laufwerk `J:\` fertig sind, wechseln Sie zu dem Fenster mit der Ausgabe und drücken Sie `STRG-C`, um das Dateisystem zu stoppen. Sie werden eine Ausgabe wie diese sehen:

```
The service java has been stopped.
[...] - Stopping dedup file system...
[...] - Dedup file system is stopped.
[...] - Shutdown complete.
```

Wann immer Sie weitere Backups hinzufügen oder auf Ihre bestehenden Backups zugreifen wollen, führen Sie das Skript `dedupfs.bat` (`dedupfs` unter Linux) erneut aus.

Das Schöne an der Verwendung von DedupFS für Backups ist, dass die gespeicherten Dateien dedupliziert werden, das heißt: Wenn Sie die gleichen Dateien mehrmals speichern, wächst der Speicherplatz (fast) nicht! Zum Beispiel können Sie heute ein Backup aller Ihrer Dokumente in DedupFS im Verzeichnis `/documents/2022.12.30` speichern. Wenn Sie nächste Woche eine weitere Sicherung all Ihrer Dokumente in DedupFS speichern, dieses Mal im Verzeichnis `/documents/2023.01.06`, wird sie fast keinen zusätzlichen Platz auf dem Laufwerk beanspruchen, auf dem sich Ihr Ordner `dedup_storage` befindet. Im Allgemeinen können Sie sich DedupFS also als ein Backup-Speicherlaufwerk vorstellen, auf dem Sie wesentlich mehr Dateien speichern können als auf einem normalen Laufwerk.

Das ist alles für den Schnellstart. Die vollständige Beschreibung finden Sie hier: [README.html](README.html).

Übersetzt mit [DeepL](https://www.DeepL.com/Translator).
