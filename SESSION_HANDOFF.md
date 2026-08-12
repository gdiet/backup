# Handoff: Julius (Windows/WinFSP) session

**An dieses Dokument**: entstanden aus einer Zusammenarbeit zwischen einer Linux/WSL2-Session
(georgs Hauptarbeitsumgebung, per SSH) und mehreren nativen Claude-Code-Sessions hier auf julius,
2026-08-12. Konsolidiert (vorher: mehrere übereinandergeschichtete "Update"-Abschnitte einzelner
Sessions) zu einer linearen Zusammenfassung - die einzelnen ursprünglichen Fund-Details sind
weiterhin in `docs/plans/long-path-handling-audit.md` (im Repo, versioniert) erhalten.

**Für künftige cross-environment TODOs**: bitte `rust/agent-todos/` im Repo selbst nutzen (siehe
dessen `README.md` und die entsprechende Sektion in `rust/AGENTS.md`) statt eines Ad-hoc-Dokuments
wie diesem hier - versioniert, für jede Umgebung sichtbar, mit klarer "erledigt → done/"-Konvention.
Aktuell dort offen: `agent-todos/smb-wire-protocol-name-length-test.md` (braucht einen echten
Nicht-Win32-SMB-Client mit Netzwerkzugriff auf einen WinFSP-Share - weder hier auf julius noch auf
der Linux-Seite allein möglich, siehe die Datei für Details).

**Dieses Dokument selbst lag bis 2026-08-12 unversioniert am `bdev`-Workspace-Root** (`bdev` selbst
ist kein Git-Repo). Ab jetzt lebt es hier in `rust/` und wird mit dem Repo versioniert/gepusht -
der alte Root-Pfad wurde absichtlich entfernt, um Drift zwischen zwei Kopien zu vermeiden.

## Repo-Zustand

- `C:\Dateien\Computer\git\bdev\rust`, Branch `rust`, sauber, in Sync mit `origin/rust`.
- `cargo build`/`cargo test` hier sind überraschend schnell, solange `target/` schon vorhandene
  Abhängigkeiten cached hat (Einzel-Crate-Rebuild ~6s, `cargo test --workspace` inkl.
  Neukompilierung mehrerer Crates ~47s) - Cross-Compile-und-Kopieren von Linux aus lohnt sich
  normalerweise nicht, direkt hier arbeiten ist einfacher.
- Voller Workspace-Test, `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D
  warnings`: zuletzt alle grün.

## Was gemeinsam erledigt wurde (Linux-Session + native Sessions hier, WinFSP-Namenslängen-Thema)

Ausgangspunkt: `docs/plans/long-path-handling-audit.md` fand, dass `mount --read-write`s
`mkdir`/`create`/`rename` Namenslänge gar nicht prüften. Fix zunächst in `cli/src/mount.rs`,
gegen echtes WinFSP hier verifiziert - dann auf der Linux-Seite refactored: die Prüfung lebt jetzt
in `mountfs` selbst (`mountfs::MAX_NAME_BYTES`/`reject_if_name_too_long`, aufgerufen aus
`dispatch_mkdir`/`dispatch_create`/`dispatch_rename` auf beiden Plattformen, *bevor* der jeweilige
`MountFilesystem`-Implementor überhaupt aufgerufen wird) - Begründung: Eigenschaft der
Mount-Protokoll-Schicht, nicht Backup-spezifisch, weder libfuse noch WinFSP setzen das selbst
durch. Zwei Tests dafür, beide hier nativ gegen echtes WinFSP grün:
`mountfs/tests/windows_mount.rs::dispatch_rejects_a_name_over_max_name_bytes_before_reaching_the_filesystem`
und `cli/src/restore.rs`s eigenes
`restore_warns_and_continues_past_a_name_too_long_for_the_destination_filesystem`.

Danach drei offene Diagnose-Fragen von nativen Sessions hier untersucht (kein Code-Fix nötig, rein
diagnostisch):

1. **Ctrl-C/`on_unmount`-Test bleibt dauerhaft nicht automatisierbar** - siehe
   `mountfs/tests/windows_mount.rs`s eigenen (aktualisierten) Kommentar bei
   `on_unmount_runs_and_process_exits_cleanly_on_ctrl_c` für die genaue Ursache: nicht Windows-
   Session 0 vs. 1 (beide durchprobiert, beide scheitern gleich), sondern dass der Claude-Code-
   Harness seine Shell-Kindprozesse grundsätzlich ohne angehängtes Konsolen-Handle startet -
   `AllocConsole()` behebt das nicht zuverlässig. Braucht ein echtes, von einem Menschen selbst
   geöffnetes Terminalfenster, kein von irgendeinem Agent gestarteter Prozess reicht.
2. **`\\?\`-Präfix funktioniert korrekt gegen einen WinFSP-Verzeichnis-Mount** - ein früherer,
   abweichender Befund (beide Testfälle scheiterten identisch) ließ sich nicht reproduzieren;
   erneut über `New-Item`/.NET getestet: 255-Byte-Name gelingt, 256-Byte-Name scheitert mit einer
   Windows-eigenen "Syntax ungültig"-Meldung - Windows' NTFS-Schicht blockt das schon unterhalb
   von WinFSPs Callback, die 255-Byte-Grenze dieses Projekts deckt sich exakt mit Windows' eigener
   Durchsetzung.
3. **SMB-Re-Export eines WinFSP-Mounts**: ein WinFSP-Mountpoint kann nicht selbst Freigabe-Root
   sein (`ERROR_SHARING_VIOLATION`) - Workaround: das übergeordnete Verzeichnis freigeben, der
   Mount ist dann als Unterordner voll erreichbar. Namenslängen-Verhalten darüber identisch zum
   direkten lokalen Zugriff. Echter Nicht-Win32-SMB-Client zum vollständigen Ausschluss der
   SMB-*Wire-Protokoll*-eigenen Limits fehlte hier (kein Docker, kein `smbclient`) - siehe
   `agent-todos/smb-wire-protocol-name-length-test.md` oben.

Details zu allen dreien: `docs/plans/long-path-handling-audit.md`.

## Praktische Hinweise fürs Arbeiten auf/mit julius (aus mehreren Sessions gelernt)

Primär relevant für **Fernsteuerung per SSH** - für eine native, interaktive Session hier direkt
relevant nur, falls sie selbst Hintergrundprozesse/Mounts startet und über mehrere eigene
Shell-Aufrufe hinweg beobachten will:

- **SSH-Exec-Sessions (Win32-OpenSSH) hängen gestartete Kindprozesse an ein Job-Object** - endet
  die SSH-Verbindung, sterben alle Kindprozesse mit, auch mit `start /b`. Workaround: über den
  Task Scheduler starten (`schtasks /create ... /sc once /st 23:59 /f` dann
  `schtasks /run /tn ...`) - läuft unabhängig vom SSH-Job-Object weiter.
- **Laufwerksbuchstaben- und Verzeichnis-basierte WinFSP-Mounts sind an die Logon-Session
  gebunden**, die sie erzeugt hat - von einer anderen SSH-Session (= neue Logon-Session) aus war
  der Mount-Inhalt nicht sichtbar, obwohl der Mount-Prozess nachweislich lief. Workaround: Mount
  UND Beobachtung/Test im selben Skript/Prozess/Session bündeln.
- `powershell -File ...` ist standardmäßig durch die Execution Policy blockiert -
  `-ExecutionPolicy Bypass` nötig.
- Mehrschichtiges Shell-Escaping (bash → ssh → cmd → powershell) ist fehleranfällig - bei
  komplexeren Kommandos lieber ein Skript auf julius ablegen (`scp`) und ausführen, statt alles
  inline zu verschachteln.
- Der Hostname "julius" selbst ist von WSL2 aus nicht auflösbar (kein NetBIOS/mDNS dort) -
  `ping julius` in `cmd.exe` liefert eine link-local IPv6-Adresse, von WSL2 aus (eigenes
  virtuelles Netz) nicht erreichbar; `ping /4 julius` liefert die brauchbare IPv4-LAN-Adresse.
- Ein WinFSP-Mountpoint kann nicht selbst SMB-Freigabe-Root sein (`ERROR_SHARING_VIOLATION`) -
  das übergeordnete Verzeichnis freigeben stattdessen.

## Update 2026-08-12 (weitere native Session, Performance-Benchmark)

Andere Baustelle als oben (Schreib-Performance, nicht Namenslängen), aber ebenfalls native/julius-
spezifisch (echtes WinFSP, echte langsame/schnelle Laufwerke) - daher hier vermerkt statt in einem
eigenen Handoff-Dokument.

`docs/plans/implemented/copy-performance-comparison.md` (lokale Kopie vs. Mount vs. `store`, klein-
vs. großdateilastig, auf einer echten langsamen USB-Stick- und einer schnellen SSD-Umgebung
gemessen) wurde ausgeführt und ausgewertet. Zwei daraus entstandene, noch offene Folgefragen:

- `docs/plans/persist-worker-thread-pool.md` - Idee, `persist_worker` (aktuell strikt
  single-threaded) analog zu `store`s eigenem Writer-Thread-Muster zu poolen; mit Code-Evidenz
  belegt, aber explizit nicht implementiert/validiert - drei offene Gegenfragen darin, u.a. eine
  Abhängigkeit vom nächsten Punkt.
- `docs/plans/store-vs-mount-slow-drive-write-path.md` - offene Frage, warum `store`s physischer
  Chunk-Schreibpfad auf einem langsamen Laufwerk teurer ist als der des Mounts, bei gleicher
  gemessener Nebenläufigkeit (`--store-io-parallelism`-Sweep hat Thread-Kontention bereits als
  Ursache ausgeschlossen) - noch nicht root-caused.

Beide sind Skizzen/offene Fragen, keine ausführungsbereiten Pläne - vor einer Umsetzung erst lesen.
