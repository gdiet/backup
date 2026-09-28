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

- `[show-deleted]` - Browsen und Wiederherstellen (Rename zurück in den Live-Baum). Darunter erscheint
  dieselbe Baumstruktur wie im lebenden Baum, ergänzt an jeder Stelle um genau die
  `[deleted]`/`[time]`-Sicht, die heute inline im lebenden Baum selbst erscheint.
- `[purge-deleted]` - dieselbe Struktur, zusätzlich dürfen Einträge innerhalb der Sicht dort auch
  tatsächlich (hart) entfernt werden. Details zur genauen Abgrenzung im Abschnitt "Sichtbarkeit der
  beiden Ordner" weiter unten.

(Einheitlich "purge" statt "prune" - "purge" ist der in diesem Projekt längst etablierte Begriff für
das unwiderrufliche Entfernen von Dedup-Historie, `--purge`, `allow_purge`, `dfs del --purge`, REQ-
MOUNT-007 eingeschlossen. "Prune" tauchte nur kurz in diesem Dokument selbst auf und wird hier nicht
als zweiter Begriff für dieselbe Sache eingeführt.)

`--show-deleted` als mount-weites Flag, das das Verhalten gewöhnlicher Pfade verändert, entfällt
damit vollständig. `--purge` bleibt bestehen - siehe die Sichtbarkeitsregel im nächsten Abschnitt.

### Sichtbarkeit der beiden Ordner

`mount`s eigenes `--show-deleted`-Flag wird überflüssig und entfällt (nicht zu verwechseln mit `dfs
list --show-deleted`/`dfs find --show-deleted` - eigenständige, von `mount` unabhängige
CLI-Oberflächen, siehe den eigenen Abschnitt weiter unten dazu). `--purge` bleibt bestehen, unverändert
als eigener Opt-in über `--read-write` hinaus:

- `[show-deleted]` wird **immer** angezeigt - unabhängig von `--read-write`/read-only (also auch auf
  einem rein lesenden Mount browsbar) und unabhängig von `--purge`. **Korrektur gegenüber einer
  früheren Fassung dieses Abschnitts:** `[show-deleted]` ist trotzdem nicht rein lesend. Auf einem
  read-write-Mount erlaubt es zusätzlich das Wiederherstellen per Rename (einen Eintrag aus der
  Ansicht heraus in den lebenden Baum verschieben) - REQ-MOUNT-004 verlangt dafür schon heute nur
  `--read-write`, ausdrücklich nicht `--purge`. Auf einem read-only-Mount ist auch das natürlich nicht
  möglich (`require_read_write` verweigert jede mutierende Operation ohnehin), nur das Browsen bleibt
  dort übrig. Das eigentliche, unwiderrufliche Entfernen (`unlink`/`rmdir` *innerhalb* der Ansicht)
  bleibt in jedem Fall verweigert, außer über `[purge-deleted]`.
- `[purge-deleted]` wird nur angezeigt, wenn sowohl `--read-write` als auch `--purge` gegeben sind -
  aus denselben Gründen wie zuvor (ein Löschversuch ohne `--read-write` liefe ohnehin auf `EROFS`;
  ohne `--purge` bliebe die bewusste Hürde vor dem Vernichten von Historie ungeschützt). Dort ist
  zusätzlich zum Wiederherstellen auch das endgültige Entfernen erlaubt.

Damit bleibt die heute schon bestehende, feinere Sicherheitsstufe erhalten: `--read-write` allein
erlaubt Wiederherstellen, das unwiderrufliche Vernichten von Historie bleibt ein eigener,
zusätzlicher Opt-in - und die alte Frage "wo landet Wiederherstellen per rename" ist damit
beantwortet: über `[show-deleted]` selbst, nicht über einen dritten Namensraum (siehe die inzwischen
überholte frühere Fassung dieses Abschnitts weiter unten).

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

Konkret, am Beispiel `Ordner a` mit Kind `Ordner a/b.txt` (Namen hier an die unten beschriebene
`[deleted]`/`[all]`/`[all]/[by-time]`-Struktur angepasst):

- Solange `a` lebt und `b.txt` darin zum ersten Mal gelöscht wird: `/[show-deleted]/a/[deleted]/b.txt`
  (Originalname, da einzige/neueste Löschung dieses Namens), dieselbe Information zusätzlich unter
  `/[show-deleted]/a/[deleted]/[all]/b <Zeitstempel>.txt` und
  `/[show-deleted]/a/[deleted]/[all]/[by-time]/<Zeitstempel> b.txt`.
- Wird `a` selbst gelöscht, rutscht `a` als Ganzes eine Ebene höher, in die `[deleted]`-Ansicht seines
  eigenen Elternverzeichnisses (hier: Wurzel): `/[show-deleted]/[deleted]/a` (Originalname) bzw.
  `/[show-deleted]/[deleted]/[all]/a <Zeitstempel>` bzw. `.../[all]/[by-time]/<Zeitstempel> a`.
- Innerhalb von `a` (über welchen der drei Pfade auch erreicht - dieselbe zugrunde liegende ID)
  erscheint `b.txt` weiterhin, aber (siehe die Umfangs-Empfehlung weiter unten) wieder disambiguiert
  und flach, ohne die "nur neueste Version"-Vereinfachung: `a/b <Zeitstempel>.txt`, ohne ein weiteres
  verschachteltes `[deleted]`/`[all]`-Paar - unterhalb einer bereits toten Wurzel gibt es keine
  Lebend/Tot-Unterscheidung mehr zu treffen, alles darunter ist per Definition schon tot. Das gilt
  rekursiv beliebig tief.

### `[deleted]` zeigt nur die neueste Version je Name, mit Originalname - volle Historie unter `[all]`/`[all]/[by-time]`

Auslöser: ein einfaches Wiederherstellen per Drag&Drop/Cut&Paste bekäme sonst immer den
zeitgestempelten Anzeigenamen mit ins Ziel - schlechte UX für den mit Abstand häufigsten Fall ("letzte
Löschung rückgängig machen"). Empirisch bestätigt (siehe unten): Explorer und Total Commander
übergeben bei einem einfachen Verschieben immer denselben Namen wie die Quelle, nie einen neu
gewählten - der angezeigte Name *ist* also der Name, den man nach einem Move bekommt.

Lösung: `[deleted]` selbst zeigt pro Verzeichnis nur noch **die zuletzt gelöschte Version jedes
einzelnen Original-Namens**, unter ihrem **unveränderten** Namen - kein Zeitstempel-Suffix, da pro
Name per Konstruktion höchstens ein Eintrag existiert (das jeweils neueste `deleted_at` für diesen
Namen), also naturgemäß eindeutig.

- `[deleted]/[all]` zeigt das, was `[deleted]` bisher gezeigt hat: die vollständige Historie, mit
  REQ-TREE-009s Disambiguierungs-Suffix (`<Name> <Zeitstempel> <ggf. ID>.<Extension>`).
- `[deleted]/[all]/[by-time]` zeigt dieselbe vollständige Historie, nur mit vorangestelltem statt
  angehängtem Zeitstempel, für chronologisches Browsen.

`[all-by-time]` als direktes Geschwister von `[all]` (ein früherer Zwischenstand dieses Dokuments)
wäre nach derselben Logik falsch, mit der zuvor schon `[time]` als Kind (nicht Geschwister) von
`[deleted]` festgelegt wurde: `[all]` und `[by-time]` zeigen exakt denselben Inhalt, nur
unterschiedlich benannt - genau die Beziehung, die vorher zwischen `[deleted]` und `[time]` bestand.
Also `[deleted]/[all]/[by-time]`, ein Kind von `[all]`, nicht `[deleted]/[all-by-time]` als
eigenständiges drittes Geschwister.

Das ist mehr als eine Umbenennung: Kopieren (statt Verschieben) einer gelöschten Datei zurück in den
Live-Baum geht nie über `rename()`, sondern über `open`/`read`/`create`/`write` - eine rein
rename-seitige Lösung (siehe "`rename()` bleibt immer ehrlich" unten) hilft dabei grundsätzlich
nicht, weil sie nur an einer einzigen Operation ansetzt. Weil der Anzeigename in `[deleted]` selbst
aber schon sauber ist, bevor überhaupt irgendeine Operation stattfindet, funktioniert Kopieren genauso
wie Verschieben, ganz ohne Sonderbehandlung.

#### Umfang: nur an der äußeren Grenze, nicht rekursiv in bereits toten Teilbäumen (Empfehlung, noch nicht ausdrücklich bestätigt)

Offene Frage dabei: gilt "nur neueste Version, Originalname" auch für die *eigenen* Kinder eines
bereits toten Verzeichnisses (z. B. innerhalb von `a` im Beispiel oben, nachdem `a` selbst gelöscht
wurde), oder bleibt es dort bei "immer volle Historie, immer disambiguiert", wie ursprünglich für
diesen Fall entworfen? Empfehlung: **nur an der äußeren Grenze** (die direkte `[deleted]`-Ansicht
eines noch lebenden Verzeichnisses) - das deckt den weitaus häufigsten Fall ab, ohne die
Rekursions-Logik zusätzlich zu verkomplizieren. Wer sich tiefer in schon toten Verzeichnissen
umschaut, ist ohnehin schon im "Historie durchforsten"-Modus, wo die volle, disambiguierte Sicht eher
erwartet wird. Diese Empfehlung ist noch nicht ausdrücklich bestätigt.

### Empirisch bestätigt: `rename()` bekommt beim Verschieben immer den vollständigen Zielpfad

Live gegen einen echten WinFSP-Mount getestet (`dfs mount --read-write --debug-log ...`, Verzeichnisse
`/a`, `/b`, Datei `/a/x`), über drei unterschiedliche Tools:

```
rename  /a/x  /b/x   (Cut & Paste, Explorer)
rename  /b/x  /a/x   (Drag & Drop, Explorer)
rename  /a/x  /b/x   (Total Commander)
```

Jedes Mal ein vollständiger Zielpfad mit demselben Basisnamen wie die Quelle - nie nur das
Zielverzeichnis. Das ist kein Zufall, sondern folgt aus POSIX' `rename(2)` und Windows'
`MoveFileEx`/der entsprechenden Rename-Operation, die beide immer einen vollständigen Zielpfad
verlangen; das aufrufende Tool konstruiert den Namen selbst, bevor überhaupt ein Aufruf beim Mount
ankommt. Bestätigt die Grundlage sowohl für "`[deleted]` zeigt nur die neueste Version" (oben) als
auch für `--restore-original-names` (unten): bei einem einfachen Move landet exakt der angezeigte
Name auch am Ziel.

### `rename()` bleibt immer ehrlich - kein automatisches Umbenennen ohne Opt-in

Ursprünglich erwogen: beim Wiederherstellen per Move automatisch den zeitgestempelten Namen gegen den
echten Original-Namen tauschen, sobald der Aufrufer keinen eigenen, abweichenden Namen angegeben hat.
**Verworfen als Standardverhalten:** `rename()` gibt am Protokoll keinen "so wurde tatsächlich
benannt"-Rückgabewert zurück, nur Erfolg/Fehler. Ein Rückgabewert, der nicht zum tatsächlichen
Ergebnis passt, ist exakt das, was REQ-MOUNT-007 an anderer Stelle schon verwirft ("This would make
the mount's return value inconsistent with the repository's actual state ... That reads as a bug, not
as a deliberate safety feature") - hier nur für `rename` statt `unlink`/`rmdir`. Ein Skript, das nach
einem gemeldeten Erfolg gezielt den angeforderten Zielnamen anspricht, würde ihn nicht finden.

Deshalb: `rename()` durch den Mount liefert standardmäßig immer exakt den angeforderten Namen, nie
einen anderen - unabhängig davon, ob die Quelle `[deleted]`, `[all]` oder `[all]/[by-time]` ist.

### `--restore-original-names`: Opt-in für automatisches Umbenennen bei Wiederherstellung älterer Versionen

Da `[deleted]` selbst (siehe oben) für den häufigsten Fall schon ohne jede Sonderbehandlung einen
sauberen Namen liefert, bleibt automatisches Umbenennen nur für das Wiederherstellen einer *älteren*
Version (über `[all]`/`[all]/[by-time]`) interessant. Dafür ein neues, eigenes Mount-Flag,
`--restore-original-names`: wenn gesetzt, liefert ein Move aus `[all]`/`[all]/[by-time]` heraus in den
Live-Baum automatisch den echten Original-Namen, unabhängig vom angegebenen Zielnamen - sonst
(Default) bleibt `rename()` ehrlich wie im vorigen Abschnitt beschrieben, der Zeitstempel bleibt im
Namen, bis manuell umbenannt wird.

Kein Widerspruch zum vorigen Abschnitt: die Ehrlichkeits-Garantie gilt für den *generischen*
`rename()`-Aufruf, den irgendein beliebiges Tool jederzeit absetzen könnte. `--restore-original-names`
ist dagegen ein bewusster, dokumentierter Opt-in des Mount-*Betreibers* - dasselbe Muster wie
`--purge`: das Risiko (ein Skript könnte von der Namensänderung überrascht werden) liegt bei
demjenigen, der das Flag aktiviert hat, nicht bei einem beliebigen, ahnungslosen Aufrufer unter einem
Default-Mount.

Wirkt nur auf `rename()` - eine Kopie aus `[all]`/`[all]/[by-time]` bekommt weiterhin den
zeitgestempelten Namen, das Flag hat darauf keinen Einfluss. Wie `--purge` sollte
`--restore-original-names` ohne `--read-write` sinnlos sein (Wiederherstellen gibt es nur auf einem
read-write-Mount) und nach REQ-OPERABILITY-007 verweigert werden, statt es wirkungslos zu ignorieren.

### Teilt sich die Struktur mit `dfs list`/`dfs find`/`dfs restore`/`dfs del`?

Die `[deleted]`/`[all]`/`[all]/[by-time]`-Adressierung selbst spricht nichts dagegen, geteilt zu
werden - sie baut auf [`deleted.rs`](../../crates/cli/src/deleted.rs) auf, das schon heute
ausdrücklich für beide Aufrufer gedacht ist ("the right choice for a caller with none of its own
(`dfs list`/`dfs restore`'s terminal-facing paths)"). Ein einheitliches Adressierungsschema überall
ist also grundsätzlich der richtige Default.

Zwei Einschränkungen dabei:

- `[purge-deleted]` braucht `dfs list`/`dfs find`/`dfs restore` nicht zu zeigen - diese Kommandos
  mutieren nie, `[purge-deleted]` wäre dort nur ein inhaltsgleiches Duplikat von `[show-deleted]` ohne
  eigene Bedeutung.
- Ob `[show-deleted]` als Wurzel-Präfix auch für `dfs list`/`dfs find`/`dfs restore` navigierbar sein
  soll - zusätzlich zu, oder anstelle von, deren eigenem, bereits bestehendem `--show-deleted`-Flag -
  ist eine eigene, größere Entscheidung, die hier nicht mitentschieden wird. Dieses Flag ändert heute
  schon das Verhalten gewöhnlicher Pfade, genau die Art Problem, die REQ-OPERABILITY-007 für `mount`
  gerade behoben hat. Ebenso offen: ob `dfs del --purge` durch Adressierung über
  `/[purge-deleted]/...` ersetzt oder ergänzt werden sollte. Beides für eine spätere, eigene
  Entscheidung vorgemerkt, nicht Teil dieser hier.

### Nächste Schritte (noch nicht umgesetzt)

Reine Planung bis hier, auf diesem Branch (`rust-deleted-view-synthetic-roots`). Kein Code geändert,
keine Anforderung geändert. Offen: die Rekursionsumfang-Empfehlung oben (ausdrückliche Bestätigung
steht noch aus), ob/wie `dfs list`/`dfs find`/`dfs restore`/`dfs del` den `[show-deleted]`-Präfix
zusätzlich zu ihren eigenen Flags navigieren sollen, danach eine `REQ-...`/`DESIGN-...`-ID und eine
Umsetzungsreihenfolge.
