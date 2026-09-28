# Rekursion in die Deletion-History hinein, wenn ein ganzer Ordner gelöscht wird

## Cascading-Sichtbarkeit einer bereits gelöschten Verzeichnis-History im `[deleted]`-View
Status: idea

Diese Datei ist eine Plan-Skizze für eine noch offene Diskussion, kein entschiedenes Design. Sie
hält fest, was schon geklärt ist, was schon (überraschenderweise) bereits implementiert ist, und wo
ein noch ungelöster Zielkonflikt mit einer bereits "agreed" Anforderung liegt.

### Das Szenario

```
Ordner 1
-- Ordner 1.1
---- Ordner 1.1.1
------ Datei 1.1.1.1
------ Datei 1.1.1.2
```

1. Datei 1.1.1.1 wird gelöscht -> `Ordner 1.1.1/[deleted]/Datei 1.1.1.1` erscheint. Bereits
   implementiert.
2. Ordner 1.1.1 selbst wird (z. B. im Explorer) gelöscht. Gewünscht: `Ordner 1.1/[deleted]` zeigt
   danach `Ordner 1.1.1`, und darunter finden sich sowohl Datei 1.1.1.1 (schon vorher gelöscht) als
   auch Datei 1.1.1.2 (erst jetzt, durch das Löschen des Ordners, mit-betroffen).

Frage: ist das in sich stimmig, für `mount` und `dfs del` gleichermaßen umsetzbar - und
widerspricht es einer bereits "agreed" Anforderung?

### Fund 1: Das eigentliche Browsing/Adressierungs-Schema ist bereits vollständig implementiert

Das war die größte Überraschung bei der Recherche: das im Szenario beschriebene Verhalten
existiert schon, inklusive Test.

- [`crates/db/src/tree.rs`](../../crates/db/src/tree.rs)'s `rmdir` verlangt nur, dass ein Verzeichnis
  keine **live** Kinder mehr hat (REQ-TREE-008) - bereits gelöschte Kinder bleiben unangetastet, mit
  ihrem eigenen `parent_id` weiterhin auf das jetzt selbst gelöschte Verzeichnis zeigend. Die Daten
  sind schon da, nichts an der DB muss sich ändern.
- [`crates/cli/src/deleted.rs`](../../crates/cli/src/deleted.rs)'s `resolve`/`resolve_deleted_children`
  lösen `[deleted]` **an jeder Stelle im Pfad** auf, nicht nur am Ende - mit genau der Begründung, die
  hier gebraucht wird ("a soft-deleted directory's own children are themselves always soft-deleted
  too (REQ-TREE-008) and so need their own `[deleted]` step to reach").
- Der Test `resolve_descends_into_an_already_deleted_directorys_own_deleted_children` (`deleted.rs`)
  bildet das Szenario 1:1 nach: Verzeichnis `a` anlegen, Datei `f.txt` darin löschen, `a` selbst per
  `rmdir` löschen, danach `/[deleted]/a/[deleted]/f.txt` auflösen - funktioniert bereits.
- [`crates/cli/src/dedup_fs.rs`](../../crates/cli/src/dedup_fs.rs)'s `push_deleted_marker` fügt den
  `[deleted]`-Marker "one level down" genauso in die Kindliste eines bereits gelöschten Verzeichnisses
  ein wie in die eines lebenden - der eigene Kommentar zitiert REQ-TREE-008 dafür.

Der Pfad zu Datei 1.1.1.2 wäre demnach `Ordner 1.1/[deleted]/Ordner 1.1.1/[deleted]/Datei 1.1.1.2`
(zwei `[deleted]`-Segmente hintereinander) - nicht `.../Ordner 1.1.1/Datei 1.1.1.2` ohne das zweite
Segment. Das folgt zwingend aus REQ-TREE-008 (ein gelöschtes Verzeichnis hat nie lebende Kinder,
also müssen auch seine eigenen sichtbaren Kinder wieder über `[deleted]` erreicht werden) und ist
konsistent mit der Ein-Ebenen-Adressierung, die REQ-TREE-009 schon für die Wurzel beschreibt.

**Für das reine Browsing/Wiederherstellen ist hier also nichts Neues zu entwerfen** - nur zu
verifizieren, dass die Mount-Seite (`readdir`/`getattr`/`rename`-Recovery) tatsächlich beliebig tief
rekursiv funktioniert, nicht nur eine Ebene (dafür spricht der zitierte Test, aber er deckt nur
Adressierung ab, nicht `readdir` selbst).

### Fund 2: Für `mount` ist kein Kaskaden-Löschen auf DB-Ebene nötig

REQ-TREE-008 verlangt für den Mount ausdrücklich *kein* kaskadierendes Löschen - `rmdir` verweigert
sich, solange noch lebende Kinder da sind. Das bleibt unangetastet richtig, weil ein Explorer (oder
jedes andere "delete folder") ein nicht-leeres Verzeichnis ohnehin von innen nach außen leert -
jede Datei einzeln `unlink`, jedes Unterverzeichnis erst rekursiv geleert, dann `rmdir` - bevor es
das ursprüngliche Zielverzeichnis selbst entfernt. Zum Zeitpunkt, an dem Ordner 1.1.1 selbst
gelöscht wird, hat es (aus Sicht von `tree_entries`) also bereits keine lebenden Kinder mehr; nur
noch die schon vorher entstandene Historie. `rmdir` sieht davon nichts anderes als sonst.

Diese Annahme ("Explorer räumt selbst rekursiv leer, bevor es den Ordner entfernt") ist exakt die
schon in REQ-MOUNT-007 dokumentierte, ausdrücklich noch nicht empirisch verifizierte
Arbeitshypothese ("Not yet verified for real: how Explorer/Thunar/Nautilus/`rm -rf` actually behave
here"). Für dieses Feature wird sie zur tragenden Voraussetzung - lohnt sich, bei der ohnehin noch
ausstehenden Verifikation gegen einen echten Mount mit abzuprüfen.

### Fund 3 (Nebenfund): `dfs del --recursive` existiert schon, macht serverseitig dasselbe

`crates/cli/src/del.rs`'s `delete_live_children` läuft rekursiv über `list_children` (nur lebende
Kinder) und löscht bottom-up - Dateien `unlink_file`, Verzeichnisse erst rekursiv geleert, dann
`rmdir`. Exakt dieselbe Bottom-up-Logik wie ein Explorer, nur serverseitig statt durch viele
einzelne FUSE/WinFSP-Aufrufe. Für `dfs del` ist das Szenario also bereits ohne jede Änderung
nutzbar.

Nebenbei entdeckt: REQ-CLI-003s Text ("the exact flag(s) for that opt-in... are not yet decided")
ist damit inzwischen veraltet - `--recursive` ist längst entschieden und implementiert. Sollte bei
Gelegenheit korrigiert werden, unabhängig von diesem Feature.

### Der eigentliche, noch ungelöste Konflikt: Löschen *innerhalb* der `[deleted]`/`[time]`-Ansicht selbst

Hier liegt der Punkt, den Du selbst schon vermutet hast.

Sobald `--show-deleted` aktiv ist, taucht `[deleted]` als ganz normaler Verzeichniseintrag in
Ordner 1.1.1s eigener `readdir`-Liste auf (`push_deleted_marker`, sobald mindestens ein Kind
gelöscht ist - hier: Datei 1.1.1.1). Ein Explorer, der Ordner 1.1.1 rekursiv leert, sieht diesen
Eintrag also als eines der Dinge, die er (vermeintlich) mit-entfernen muss, bevor er Ordner 1.1.1
selbst per `rmdir` entfernen kann. Er würde also versuchen:

- `Ordner 1.1.1/[deleted]/Datei 1.1.1.1` zu löschen (ein `MountPath::Deleted(entry)`),
- danach `Ordner 1.1.1/[deleted]/[time]` zu leeren und zu entfernen (dieselben Einträge nochmal, als
  `MountPath::TimeChildren`),
- danach `Ordner 1.1.1/[deleted]` selbst per `rmdir` zu entfernen (`MountPath::DeletedChildren`).

**Aktuelles Verhalten** (`crates/cli/src/dedup_fs.rs`, `unlink`/`rmdir`): alle drei Fälle liefern
ohne `--purge` unconditional `EACCES` - und zwar der `[deleted]`/`[time]`-Marker selbst *immer*
`EACCES`, unabhängig von `--purge` ("the view itself is never a delete target"). Ein Explorer, der
mitten in seiner Rekursion einen Fehler bekommt, bricht die Operation typischerweise ab (oder zeigt
zumindest einen Fehler für den ganzen Vorgang) - Ordner 1.1.1 selbst würde dann nie erreicht, das
gewünschte Szenario käme so nie zustande.

Dein Vorschlag: für diesen Fall statt `EACCES` ein "OK" (No-Op) zurückgeben, sowohl für einzelne
bereits gelöschte Einträge als auch für `[deleted]`/`[time]` selbst, solange kein `--purge` aktiv
ist.

**Das widerspricht direkt einer schon "agreed" Anforderung.** REQ-MOUNT-007 lehnt genau das
wörtlich ab:

> Rejected: reporting success without actually purging under only the base opt-in, so a delete
> attempt against the view would appear to succeed but do nothing. This would make the mount's
> return value inconsistent with the repository's actual state. A caller - a script, or a file
> manager's optimistic UI update - would see the entry still present or reappearing. That reads as
> a bug, not as a deliberate safety feature; an honest refusal communicates the same safety
> property without that risk.

Das ist fast wortgleich das, was jetzt als Lösung vorgeschlagen wird. Kein Zufall: die damalige
Ablehnung war eine bewusste Entscheidung gegen genau dieses Verhalten, aus genau dem Grund, den Du
jetzt in Kauf nehmen müsstest (ein Skript oder ein optimistisches Explorer-UI-Update sieht
"gelöscht", obwohl der Eintrag unverändert weiter existiert).

#### Eine mögliche Unterscheidung, die den Widerspruch entschärfen könnte

Zwei Teilfälle sind nicht gleich verdächtig:

1. **`[deleted]`/`[time]` sind bereits leer** (alles darunter längst gepurged oder nie vorhanden) -
   ein `rmdir` darauf mit "OK" zu beantworten, ist keine Lüge: es gibt tatsächlich nichts mehr zu
   schützen, das Verzeichnis ist real leer. Aktuell wird das trotzdem hart abgelehnt (der Match-Arm
   prüft gar nicht erst, ob `list_deleted_children` leer ist) - das wirkt eher wie eine
   unspezifizierte Lücke als eine bewusste Entscheidung. Diese Verfeinerung würde **nicht** gegen
   REQ-MOUNT-007 verstoßen und würde zusammen mit `--purge` (das jeden Eintrag einzeln wirklich
   entfernt) eine vollständig REQ-MOUNT-007-konforme Lösung ergeben: Explorer purged mit `--purge`
   alles einzeln weg, und erreicht am Ende ein tatsächlich leeres `[deleted]`/`[time]`, das sich
   dann auch entfernen lässt.
2. **`[deleted]`/`[time]` haben noch echten Inhalt, und `--purge` ist nicht aktiv** - genau der
   Fall, den REQ-MOUNT-007 schon einmal explizit durchdacht und verworfen hat. Hier bräuchte es
   entweder eine bewusste Revision dieser Anforderung (mit neuer Begründung, warum das Risiko jetzt
   tragbar ist) oder die Erkenntnis, dass das Szenario ohne `--purge` schlicht nicht
   Explorer-tauglich sein wird.

Mit anderen Worten: **mit `--purge` aktiv braucht es vermutlich nur die schwache, unstrittige
Verfeinerung (1)** - REQ-MOUNT-007s eigene Rationale sagt das sogar fast wörtlich schon voraus
("a recursive delete... descending into a directory's view under the second opt-in purges its
history as it goes, incidentally settling that such a directory can eventually be fully removed").
Nur ohne `--purge` bleibt der echte Zielkonflikt bestehen.

### Offene Fragen für die Entscheidung

1. Soll das Szenario **nur mit `--purge`** funktionieren müssen (dann kein Widerspruch, nur die
   schwache Verfeinerung (1) oben nötig), oder soll es **auch ohne `--purge`** funktionieren (dann
   muss REQ-MOUNT-007s "Rejected"-Klausel bewusst revidiert werden)?
2. Falls (1) reicht: gilt die "leer -> OK"-Regel für `rmdir` auf `[deleted]`/`[time]` genauso wie für
   `unlink`? (Vermutlich ja - beide sind synthetische, nie persistente Objekte.)
3. Falls REQ-MOUNT-007 revidiert werden soll: soll die Ausnahme eng gefasst werden (nur
   `unlink`/`rmdir`, `rename`/`create`/`utimens` bleiben wie bisher hart abgelehnt), oder generell?
4. Die REQ-MOUNT-007-Verifikation gegen einen echten Mount (Explorer/Thunar/Nautilus/`rm -rf`) steht
   ohnehin noch aus - dieses Feature sollte in diese Verifikation mit aufgenommen werden, statt
   separat.
5. `[time]` zeigt exakt dieselben Einträge wie `[deleted]`, nur anders benannt/sortiert (REQ-MOUNT-008)
   - beim rekursiven Fall würde ein Explorer sie potenziell doppelt "löschen" (einmal über
   `[deleted]/<Name>`, einmal über `[deleted]/[time]/<Zeitstempel Name>`). Zweiter Versuch auf einen
   dann schon gepurgten Eintrag müsste sauber wie "schon weg" behandelt werden, nicht als Fehler.

### Nächste Schritte (noch nicht umgesetzt)

Reine Planung bis hier. Kein Code geändert, keine Anforderung geändert. Warte auf Deine Einschätzung
zu den offenen Fragen oben, insbesondere Frage 1, bevor hieraus eine `REQ-...`/`DESIGN-...`-ID und
eine Umsetzung wird.

Die Fragen oben wurden anschließend in zwei experimentellen Branches praktisch durchgespielt:
`rust-noop-purgeless-delete` (No-Op-Rückgabewert bei "delete" auf die Sicht selbst - live als
wirkungslos verifiziert, da ein Explorer/`.NET`s eigene Leerheits-Prüfung schon vor dem ersten
`rmdir`-Aufruf erfolgt und sich durch einen gelogenen Rückgabewert nicht täuschen lässt) und
`rust-purge-empty-view-removal` (echtes Leeren-und-Entfernen von `[deleted]`/`[time]` sobald
`--purge` sie tatsächlich geräumt hat - funktioniert, stößt aber strukturell darauf, dass das letzte
lebende Kind eines Verzeichnisses zu entfernen selbst wieder ein gewöhnlicher Soft-Delete ist
(REQ-TREE-002) und dadurch frischen `[deleted]`-Inhalt erzeugt, den ein einzelner rekursiver
Lösch-Durchlauf nie erneut sieht). Details dazu in den jeweiligen Branches selbst, nicht hier
dupliziert.

## Alternative: synthetische Wurzel-Ordner statt Inline-Einblendung
Status: idea

Statt `[deleted]`/`[time]` inline in jedes lebende Verzeichnis einzublenden (und damit über
`--show-deleted`/`--purge` das Verhalten gewöhnlicher Pfade zu verändern - siehe die
REQ-OPERABILITY-007-Lücken, die genau daraus entstanden sind): der normale Mount zeigt ausschließlich
den echten, lebenden Baum, ganz ohne synthetischen Inhalt. Zusätzlich blendet die Mount-Wurzel zwei
weitere, synthetische Verzeichnisse ein:

- `[show-deleted]` - rein lesend. Darunter erscheint dieselbe Baumstruktur wie im lebenden Baum,
  ergänzt an jeder Stelle um genau die `[deleted]`/`[time]`-Sicht, die heute inline im lebenden Baum
  selbst erscheint.
- `[purge-deleted]` - dieselbe Struktur, aber Einträge innerhalb von `[deleted]`/`[time]` dürfen dort
  tatsächlich (hart) entfernt werden.

(Einheitlich "purge" statt "prune" - "purge" ist der in diesem Projekt längst etablierte Begriff für
das unwiderrufliche Entfernen von Dedup-Historie, `--purge`, `allow_purge`, `dfs del --purge`, REQ-
MOUNT-007 eingeschlossen. "Prune" tauchte nur kurz in diesem Dokument selbst auf und wird hier nicht
als zweiter Begriff für dieselbe Sache eingeführt.)

`--show-deleted` als mount-weites Flag, das das Verhalten gewöhnlicher Pfade verändert, entfällt
damit vollständig. `--purge` bleibt bestehen - siehe die Sichtbarkeitsregel im nächsten Abschnitt.

### Sichtbarkeit der beiden Ordner

`mount`s eigenes `--show-deleted`-Flag wird überflüssig und entfällt (nicht zu verwechseln mit `dfs
list --show-deleted`/`dfs find --show-deleted` - eigenständige, von `mount` unabhängige
CLI-Oberflächen, von dieser Entscheidung unberührt). `--purge` bleibt dagegen bestehen, ebenfalls
unverändert als eigener Opt-in über `--read-write` hinaus:

- `[show-deleted]` wird immer angezeigt, unabhängig von `--read-write`/read-only und unabhängig von
  `--purge` - reines Lesen braucht kein Schreibrecht auf den Mount und keine Erlaubnis, Historie zu
  vernichten.
- `[purge-deleted]` wird nur angezeigt, wenn sowohl `--read-write` als auch `--purge` gegeben sind.
  Ohne `--read-write` ergäbe er ohnehin keinen Sinn (jeder Löschversuch darunter würde wie jede andere
  Schreiboperation mit `EROFS` scheitern); ohne `--purge` bliebe die bewusste, zusätzliche Hürde vor
  dem unwiderruflichen Vernichten von Dedup-Historie sonst ungeschützt bestehen. Beides dort gar nicht
  erst zu zeigen, statt sichtbar-aber-verweigernd, ist konsistent mit REQ-OPERABILITY-007s eigenem
  Prinzip: eine im Kontext sinnlose oder nicht erlaubte Fähigkeit wird verweigert/versteckt, nicht
  sichtbar-aber-wirkungslos angeboten.

Damit bleibt die heute schon bestehende, feinere Sicherheitsstufe erhalten: `--read-write` allein
erlaubt weiterhin nur normale Datei-Bearbeitung, das unwiderrufliche Vernichten von Historie bleibt
ein eigener, zusätzlicher Opt-in.

### Das löst den strukturellen Rest aus `rust-purge-empty-view-removal` nebenbei mit auf

Der Grund, warum das letzte lebende Kind eines Verzeichnisses zu entfernen dort einen neuen
`[deleted]`-Eintrag *im selben, gerade geleerten Verzeichnis* erzeugt, ist, dass `[deleted]` inline im
lebenden Baum sitzt - Löschen und die Sicht auf die Historie sind also unvermeidlich derselbe
Namensraum. Unter `[purge-deleted]` gibt es diese Kollision nicht mehr: alles dort ist per Definition
bereits tot, es gibt keine lebenden Geschwister, die durch ein "letztes Kind weg" erneut einen
frischen Soft-Delete auslösen könnten. Ein rekursives Räumen unter `[purge-deleted]` kann daher
bottom-up echt (hart) entfernen, ohne dass sich unter der Hand wieder etwas regeneriert.

### Adressierung durch bereits tote Vorfahren - keine neue Fähigkeit nötig

Naheliegende Sorge: wenn ein ganzer Zwischenpfad selbst schon tot ist (z. B. `a` wurde gelöscht,
bevor man `[show-deleted]/a/b` erreichen will), braucht die Pfadauflösung dann eine neue,
identitätsbasierte Adressierung statt eines Live-Baum-Walks? **Nein** - das ist bereits vollständig
implementiert und getestet, siehe "Fund 1" oben sowie
[`crates/db/src/tree.rs`](../../crates/db/src/tree.rs)'s `list_deleted_children` (Kommentar: "may
itself be a live directory or an already soft-deleted one - REQ-TREE-008 guarantees a soft-deleted
directory's own children are always soft-deleted too, so nested `[deleted]` navigation keeps working
the same way one level down") und den Test `list_deleted_children_descends_into_an_already_deleted_directory`.
Auf Mount-Seite läuft [`dedup_fs.rs`](../../crates/cli/src/dedup_fs.rs)'s `resolve_mount_path` über
`continue_from_deleted_entry` bereits beliebig tief in einen toten Teilbaum hinein, sobald einmal ein
`[deleted]`-Segment erreicht ist.

Neu ist also nicht die Traversierung selbst, sondern nur, dass sie künftig über einen neuen
Wurzel-Präfix erreichbar sein muss statt inline im normalen Baum - im Kern eine Routing-Frage: den
`[show-deleted]`/`[purge-deleted]`-Präfix am Pfadanfang erkennen, abschneiden, und den Rest an die
(im Wesentlichen bestehende) Auflösungslogik übergeben, mit "Sicht auf Historie an" bzw. zusätzlich
"Purge erlaubt" statt der heutigen `self.show_deleted`/`self.allow_purge`-Mount-Flags.

Konkret, am Beispiel `Ordner a` mit Kind `Ordner a/b`:

- Solange `a` lebt und `b` darin gelöscht wird: `/[show-deleted]/a/[deleted]/b-mit-Timestamp` und
  `/[show-deleted]/a/[deleted]/[time]/Timestamp-b`.
- Wird `a` selbst gelöscht, rutscht `a` als Ganzes eine Ebene höher, in das `[deleted]`/`[time]`-Paar
  seines eigenen Elternverzeichnisses (hier: Wurzel): `/[show-deleted]/[deleted]/a-mit-Timestamp` und
  `/[show-deleted]/[time]/Timestamp-a`.
- Innerhalb von `a-mit-Timestamp` (über welchen der beiden Pfade auch erreicht) erscheint
  `b-mit-Timestamp` **direkt**, ohne ein weiteres verschachteltes `[deleted]`/`[time]`-Paar - unterhalb
  einer bereits toten Wurzel gibt es keine Lebend/Tot-Unterscheidung mehr zu treffen, alles darunter
  ist per Definition schon tot. Das gilt rekursiv beliebig tief.

### `[deleted]`/`[time]` bleiben unverändert ein festes Paar

`[time]` ist Kind von `[deleted]`, keine Geschwister-Beziehung - unverändert wie heute. Der einzige
Unterschied zwischen beiden ist die Position des Zeitstempels im Namen: `<Dateiname> <Zeitstempel>
<ggf. ID> <Extension>` bei `[deleted]`, `<Zeitstempel> <Dateiname> <ggf. ID> <Extension>` bei
`[time]` - nicht Inhalt oder Tiefe. Diese ganze Namens-/Sortierlogik lässt sich unverändert
wiederverwenden.

### Noch offen: wo landet Wiederherstellen per rename?

`[show-deleted]` ist laut obiger Beschreibung rein lesend, `[purge-deleted]` nur zum endgültigen
(harten) Löschen. Keins von beiden passt offensichtlich zu "einen gelöschten Eintrag per rename
zurück in den lebenden Baum verschieben". Drei Möglichkeiten, noch nicht entschieden:

1. Zusätzlich über `[purge-deleted]` erlauben - kein dritter Namensraum, dort wo ohnehin
   schreibend/löschend zugegriffen werden darf, auch Rename zurück in den Live-Baum zulassen.
2. Eigener dritter Ordner `[recover-deleted]` - saubere begriffliche Trennung "endgültig löschen" vs.
   "wiederherstellen", aber ein weiterer Namensraum mit eigener Pfadauflösung.
3. Vorerst nur über die CLI, nicht über den Mount - der Mount bleibt für gelöschte Inhalte rein
   lesend bzw. purge-fähig, Wiederherstellung bleibt ein separates `dfs`-Kommando.

Ein zusätzlicher, damit zusammenhängender Punkt: ein Rename von `[show-deleted]/a/b/[deleted]/c` (oder
wo auch immer Recovery am Ende andockt) zurück nach `/a/b/c` wäre ein Rename über zwei getrennte
Wurzel-Namensräume hinweg - technisch aufwendiger als der heutige Rename innerhalb desselben
Verzeichnisses, auch wenn strukturell konsistent (das Wiederherstellen von `a` würde `a` exakt in den
Zustand zurückversetzen, den das heutige Inline-Design ohnehin schon kennt: ein lebendes Verzeichnis
mit eigenem `[deleted]`-Marker für `b`).

### Nächste Schritte (noch nicht umgesetzt)

Reine Planung bis hier, auf diesem Branch (`rust-deleted-view-synthetic-roots`). Kein Code geändert,
keine Anforderung geändert. Offen: die Recovery-Frage oben, danach eine `REQ-...`/`DESIGN-...`-ID und
eine Umsetzungsreihenfolge.
