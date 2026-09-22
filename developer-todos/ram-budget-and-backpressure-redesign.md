# RAM budget, mount handle caching, and store/backpressure redesign

**Noted**: 2026-09-22, handed over across several design-conversation turns on the `memory-design`
branch. Follows on from `docs/design/settle-whole-file-memory-bound.md` (opened earlier in the same
branch/conversation) - this TODO's Store/Ingest section supersedes that document's two-pass
hash-then-write approach; see "Notes and open questions" below for why.
**Size**: large - confirm with the developer before starting. Touches requirements
(`requirements/functional/storage.md`, likely a new non-functional area too), several design docs
(some already `Status: implemented` and needing their status reset, not a silent rewrite - see
below), and a genuinely large code surface across `crates/db`, `crates/cdc`, and `crates/cli`.
**Context**: `crates/cli/src/write_cache.rs` (`MemoryBudget`, `DEFAULT_BUDGET_BYTES`),
`crates/cli/src/backpressure.rs` (`write_backpressure_delay`, DESIGN-MOUNT-006),
`crates/cli/src/settle.rs` / `crates/cli/src/settle_pool.rs` (the shared settle engine and its job
pool), `crates/cli/src/ingest.rs`, `crates/cli/src/pending_files.rs` (`PendingFiles`,
`handle_count`), `crates/cdc/src/lib.rs` (`ChunkerConfig`), `crates/db/src/connection.rs` (SQLite
pragma configuration), `docs/design/mount-write-path.md` (DESIGN-MOUNT-006/010),
`docs/design/settle-whole-file-memory-bound.md`, `requirements/functional/storage.md`
(REQ-STORAGE-003), `requirements/non-functional/performance.md`.

**Per the developer's own instruction handing this over**: changes to requirements, design docs, or
code that follow directly from the text below may be carried straight into `agreed`/`decided`/
`implemented` status during implementation. Where implementing this needs a farther-reaching change
than the text below states outright, reset that status back to `draft`/`idea` instead of assuming
agreement - "Notes and open questions" below already identifies several concrete places this
applies (in particular: DESIGN-MOUNT-006's delay formula and DESIGN-MOUNT-010's budget semantics,
both currently `implemented`, both changing in ways bigger than a constant tweak).

## The developer's design sketch (verbatim, unedited)

Generell sollen die DedupFS Tools ja mit geringem Ressourcenverbrauch eine gute Leistung bringen. Gerade in Bezug auf die RAM-Verwaltung besteht meiner Meinung nach noch Potenzial, das wir ausnutzen können. Bitte gib mir zunächst Feedback, ob das, was ich hier skizziere, schlüssig ist und umsetzbar sein sollte - erst mal noch keine Änderungen im Repository. Die im folgenden skizzierten Sachen stehen unter Umständen im Widerspruch zu dem, was wir schon committed / designed / beschlossen / implementiert / dokumentiert haben. Betrachte bitte erst mal dieses Dokument unabhängig davon, ob es zu früheren Entscheidungen im Widerspruch steht.

**Ausgangspunkt**

Technischer Ausgangspunkt ist, dass die App zu jeder Zeit weiß, wie viel RAM sie (ungefähr) maximal verwenden darf. Für den ersten Wurf schlage ich vor, dass die App 256 MB als Default annimmt, und es die Möglichkeit gibt, das per CLI-Option zu konfigurieren. Das RAM-Limit ändert sich also zur Laufzeit der App nicht. Ausgefeiltere Methoden wie "nimm dynamisch 30% des verfügbaren nicht-Swap-RAM" können wir für spätere Ausbaustufen vorsehen.

Wir ermitteln/schätzen, wie viel RAM für die Datenbank und die normale Programmausführung benötigt werden. Für SQLite verwenden wir PRAGMA cache_size, wir haben ja immer nur eine Connection, da ist das genau ermittelbar. Und cache_size machen wir auch CLI-konfigurierbar, und sehen als Default vor, den rusqlite Default nur auszulesen (und nicht zu ändern). Für die Rust Laufzeitumgebung, die Variablen, den Stack etc. schätzen wir (eventuell gestützt durch ein paar Messungen), und rechnen für jeden Thread, den wir erwarten, die 2 MB Stack dazu. Das betrifft den CDC/Hash/Store Thread Pool - trifft das genauso auch für den FUSE Thread Pool zu? Haben wir für den Angaben, wie groß er werden kann?

So kommen wir zu einer Zahl, wie viel RAM wir für das Caching von Daten verwenden können. Diese Zahl nenne ich "RAM-Budget". Den Anteil vom RAM-Budget der aktuell nicht verwendet wird, bezeichne ich als "verfügbares RAM-Budget".

**Idee für Mount**

Im Mount Fall soll es ja möglich sein, mehrere Dateien gleichzeitig offen zu haben, wobei jede Datei potenziell mehrere Gigabyte groß sein könnte. Deshalb haben wir das Spillover-To-Disk eingeführt.

Zunächst mal kann jeder Write auf ein File Handle dazu führen, dass wir für das File Handle gerne Daten im RAM cachen möchten. Dabei gehen wir so vor: RAM-Caching lassen wir bei jedem Write zu bis zu der Grenze "für dieses Handle im RAM ge-cache Größe ist maximal so groß wie noch verfügbares RAM-Budget".

sizeToCacheInRAM = max(0, min(writeLength, (currentHandleRAMCacheSize + availableRAMCacheBudget)/2 - currentHandleRAMCacheSize))

oder vereinfacht

sizeToCacheInRAM = max(0, min(writeLength, (availableRAMCacheBudget - currentHandleRAMCacheSize)/2))

Das heißt also, wenn  nur ein File Handle offen ist, kann es nach und nach bis zu 50% des verfügbaren RAM-Budgets anfordern, danach fängt Spillover-To-Disk an. Ein zweites Handle auf eine andere Datei, das danach geöffnet wird, kann dann von den verbliebenen 50% nach und nach wiederum die Hälfte in Anspruch nehmen - dann ist für dieses zweite Handle 25% vom Gesamtbudget == 25% noch verfügbares RAM-Budget.

Die Berechnung sollte dabei atomar/exklusiv laufen, um Race Conditions zu vermeiden.

Der RAM-Cache für ein File Handle wird zur Lebenszeit des Handles nicht verkleinert - wenn also eine große Datei schon 50% des kompletten RAM-Budgets hat, und eine zweite große Datei nimmt sich von den verbleibenden 50% dann nochmal die Hälfte bevor dort das Spillover startet, dann wird der RAM-Cache des ersten File Handle nicht verringert. (Das wäre zu aufwändig zu implementieren, bei unklarem Nutzen.)

Begründung: Es ist durchaus sinnvoll denkbar, dass ein Benutzer 1-2 große Dateien offen hat, und nebenher auch immer wieder kleine Dateien bearbeitet werden. Auf diese Weise würden zwei große Dateien bis zu 75% des Gesamt-Budgets beanspruchen, und für die kleinen Dateien stehen immer noch 25% des Gesamt-Budgets zur Verfügung, was die Verarbeitung der kleinen Dateien auch noch deutlich beschleunigen kann, selbst wenn bereits 1-2 große Dateien offen sind.

Wenn ein File Handle geschlossen wird, **dann** startet die Backpressure-Berechnung: Sobald / solange es geschlossene File Handles gibt, die noch nicht verarbeitet und weggeräumt sind, bremsen wir Schreibvorgänge auf offene Handles gemäß der Formel:

storeDelayMillis = max(0, bytesInPersistQueue - freeZoneBytes) * writeLength / SLOPE_DIVISOR

mit den Defaults (als CLI-Option konfigurierbar)

freeZoneBytes  = 1_000_000_000    (1 GB)
SLOPE_DIVISOR  = 180_000_000_000

Die Defaults sind dabei so berechnet, dass bis 1 GB Queue kein Backpressure stattfindet, und bei 10 GB Queue die Backpressure 500 ms für einen 10 kB Write beträgt.

Wenn beim Persistieren ein Chunk fertig bearbeitet ist (persistiert oder dedupliziert), dann wird sofort bytesInPersistQueue entsprechend angepasst - nicht erst, wenn die komplette Datei gespeichert ist. bytesInPersistQueue ist also die Summe aller gequeued-ten, aber noch nicht persistierten/deduplizierten Bytes.

storeDelayMillis soll kein Upper Limit haben. Das Tool ist (mit den Default-Settings von oben) für Dateien bis z.B. 5 GB Größe eingerichtet. Eine einzelne 5-GB-Datei würde in dem Fall initial beim persist von 10 kB zu einem Delay von 222ms führen, und je mehr sich ansammelt, desto langsamer werden die Schreibvorgänge, wodurch es sich (solange einzelne Dateien nicht größer als 5 GB sind) für den Benutzer als "graceful degradation" anfühlt, wenn viele Daten für das Persistieren in der Warteschlange sind. In den vorgesehen Use Cases sollte das Ausbremsen der Writes bei langsamen Storage Medien auf diese Weise automatisch zu einer Balance zwischen "Benutzer schreibt" und "DedupFS persistiert" führen, ohne dass des zu einer gefühlten "Persist-Pause" für den Benutzer kommt.

"Kein Upper Limit" ist bewusst so gewählt: Da für den Abbau des Delays aber ein anderer Thread Pool zuständig ist als für die FUSE-Operationen (nämlich der CDC/Hash/Persist-Pool), erwarte ich keine wirklich unangenehmen vermeidbaren Folgen. Wir behalten die Möglichkeit von Thread-Starvation-Szenarien im Blick und schauen gegebenenfalls, ob wir in Tests (gegebenenfalls mit speziell konfigurierten Einstellungen statt den Defaults) eindeutige negative Effekte nachweisen können und Szenarien beschreiben können, bei denen es für einen Benutzer unangenehm wird.

**Idee für Store**

Anders beim Store Tool. Hier kann es verschiedene Szenarien geben, sowie eine Kombination aus ihnen:

1. Source Read und Store Write sind sehr schnell, I/O ist nicht der limitierende Faktor, sondern CPU für Hashing und CDC. Eine praxisrelevante Variante davon ist: Source Read ist sehr schnell, und weil nur 1% der Daten tatsächlich geändert wurde, ist Store Write auch nicht limitierend, sondern der limitierende Faktor ist CPU für Hashing und CDC. Für dieses Szenario ist es wichtig, dass Hashing und CDC für mehrere Dateien parallelisiert ausgeführt wird.
2. Source Read ist langsam. Da können wir nicht viel machen, deswegen müssen wir auch nicht viel dafür optimieren. Wir sollten nur schauen, ob File System Traversal eventuell parallel zu Source Data Read stattfinden sollte/kann, um diesen Fall so performant wir möglich zu gestalten?
3. Store Write ist langsam. In dem Fall sollten wir darauf achten, dass wir nicht zu viel Disk-Spillover verwenden, das könnte die Gesamt-Performance ausbremsen. Vor allem, wenn Store Write und Spillover sich einen I/O-Kanal teilen.

Wir verwenden ja CDC. Anders als bisher bieten wir nicht die "kein Chunking" Konfiguration an (bewusste Änderung der bisherigen Anforderungen). So können wir jederzeit berechnen, wie groß ein Chunk maximal sein kann, und müssen uns gar nicht extra um den Sonderfall "sehr große Datei, z.B. 10 GB" kümmern, weil wir die Datei einfach Chunk-für-Chunk lesen können ohne je zum Spillover greifen zu müssen. Bei 23 Bit ist die theoretische Max Chunk Size 96 MB. Wir verwenden 23 Bit als harte Obergrenze - CDC-Anwendungen, die von größeren Chunk-Sizes profitieren, sind nicht im Fokus von unseren Tools.

Wenn wir also unser RAM Budget kennen, dann müssen wir erst prüfen, ob es größer ist als die theoretische Max Chunk Size für dieses Repository (abhängig von der CDC Size Bits). Wenn nicht -> abbrechen mit Fehlermeldung. Wenn es größer ist, können wir die maximale Anzahl parallel im RAM zu cachender Chunks berechnen, und die Parallelisierung entsprechend auslegen, vermutlich am besten auch limitiert durch die Anzahl der CPU-Kerne.

So können wir dann Source-Dateien sequenziell in den RAM lesen bis wir eine Chunk-Grenze finden, und dann den Chunk gleich deduplizieren oder persistieren. Und das ganze eben parallelisiert, wobei wir jede Datei für sich sequenziell in einem Thread behandeln. Intra-Datei-Parallelisierung erfassen wir als mögliche zukünftige Erweiterung, aber implementieren sie nicht (yagni).

Und alles ohne ein Spillover To File.

## Notes and open questions (added when filing this TODO)

### Already-existing building blocks (this rides on real infrastructure, not a blank slate)

- `crates/cli/src/write_cache.rs::MemoryBudget` already exists: an `AtomicU64`-backed shared
  budget with lock-free `try_acquire`/`release` (`fetch_update` CAS), `DEFAULT_BUDGET_BYTES = 256 *
  1024 * 1024` - the developer's proposed 256 MB default already matches this constant exactly.
  The per-handle `(availableRAMCacheBudget - currentHandleRAMCacheSize)/2` formula above is a new
  *method* on this same type (a CAS loop computing and reserving the clamped increment in one
  step), not a new component.
- `PRAGMA cache_size` is directly readable via `conn.query_row("PRAGMA cache_size", [], |row|
  row.get(0))` (or `pragma_query`), the same pattern `crates/db/src/connection.rs` already uses for
  `journal_mode`. Confirms the developer's own "da ist das genau ermittelbar" - `db::Repository`
  holds exactly one connection for its whole lifetime (its own doc comment says so explicitly), so
  this is a single, well-defined number, not an estimate.
- `crates/cli/src/settle_pool.rs::JobPool` already runs a configurable-size worker pool
  (`available_parallelism()`-sized today) separate from FUSE dispatch - the Store-side "N workers,
  each one file at a time" model is the same shape, just applied to `ingest`/`store` instead of the
  mount's settle jobs.
- `crates/cli/src/pending_files.rs::PendingFiles` is already keyed by `file_id` (not per FUSE
  handle) with an explicit `handle_count`, and already only hands a generation to the settle pool
  once `handle_count` reaches 0 (`release()`) - confirmed via a dedicated existing test
  (`release_with_more_than_one_handle_open_keeps_the_generation_writable`). Multiple concurrent
  write handles on one file (a real POSIX/FUSE scenario) are therefore already handled correctly;
  nothing new needed there.

### This changes, not just tunes, two already-`implemented` design decisions

- **DESIGN-MOUNT-006's delay formula** (`crates/cli/src/backpressure.rs`) currently reacts to
  `JobPool::backlog_spilled_bytes()` - deliberately *spill-only*, not all queued bytes (see that
  module's own doc comment: "a Rust spilled byte is a worse state than a Scala queued byte"). The
  developer's `bytesInPersistQueue` above is the broader signal (all bytes queued for persist,
  memory-cached or spilled, decremented per completed *chunk*) - closer to the original Scala
  signal this project's own delay formula deliberately diverged from. Implementing this needs a new
  counter, updated from inside `Settler::complete_chunk` (`crates/cli/src/settle.rs`) as each chunk
  finishes, threaded through `settle_pool.rs` to wherever `DedupFs::write` reads it today - a
  genuinely different code path, not a constant swap. DESIGN-MOUNT-006's `Status: implemented`
  should go back to `draft` (or a fresh `DESIGN-...` id, since the mechanism itself changes) rather
  than being silently kept as `implemented` with new text under it.
- **DESIGN-MOUNT-010's `MemoryBudget`/`DEFAULT_BUDGET_BYTES`** currently *is* the whole cache
  budget. Under the developer's model above, 256 MB is the *gross* limit the whole process works
  within, with the SQLite/Rust-runtime reserve subtracted *before* what is left becomes the actual
  `MemoryBudget` instance's `total_bytes`. That is a reinterpretation of an existing, shipped
  constant's meaning, not only a value change - recorded here explicitly so it gets a deliberate
  decision rather than a silent conflation of "256 MB total" and "256 MB for caching."

### `docs/design/settle-whole-file-memory-bound.md` becomes obsolete, not just related

That document (still `Status: idea`) explores a two-pass hash-then-write scheme specifically to
bound `Settler::chunk_buffer` for `--whole-file` mode, where a chunk's size was previously
unbounded. Once `--whole-file` is eliminated and CDC is capped at 23 bits (96 MiB hard maximum chunk
size), `chunk_buffer`'s worst case becomes a small, known, checkable constant - the exact problem
that document solves no longer exists in its previous form. Recommend marking it
`Status: superseded-by DESIGN-...` (pointing at whichever new design id this work gets) rather than
leaving two, now-contradictory open ideas about the same buffer sitting in `docs/design/`
simultaneously.

### Questions for the developer (would like an answer before or during implementation)

1. **FUSE/WinFSP dispatch-thread-pool size and stack size.** Checked what is verifiable from this
   repo alone: `crates/mountfs/src/linux/sys.rs` marks the FUSE `init` callback (where
   `fuse_conn_info`, including thread-pool-relevant negotiation, would be read/set) as
   `Unimplemented` - this project does not query or configure libfuse3's pool size today, and I
   could not find a local libfuse3 installation in this sandbox to check its actual default. Two
   sub-questions, both currently open: (a) how large can the pool actually grow (needed to size the
   "N threads x stack size" reserve), and (b) what stack size do *those* threads actually use -
   likely **not** the same 2 MiB Rust's `std::thread::Builder` defaults to, since libfuse3's worker
   threads are created by its own C code via plain `pthread_create`, which inherits the *process's*
   default pthread stack size (commonly, but not universally, 8 MiB on Linux, distinct from Rust's
   own default). The WinFSP side is equally unverified here. Suggest resolving this empirically as
   the first implementation step (instrument a real mount under load, per the existing calibration
   precedent in `agent-todos/done/wire-write-backpressure-delay.md`) rather than blocking on it -
   but the developer should confirm that is acceptable rather than wanting it answered from
   documentation first.
2. **Backward compatibility for repositories already created before this change.** REQ-STORAGE-003
   currently frames the chunking strategy as "fixed for the repository's lifetime" once created. An
   already-existing repository created with `--whole-file` (`cdc_target_size_bits IS NULL`) or with
   `cdc_target_size_bits > 23` would still need *some* code path to settle new content into it after
   this change ships, unless such repositories are explicitly refused going forward. Is a clean cut
   acceptable (no real production repositories exist yet, given this project's pre-release status),
   or does opening/writing into a pre-existing incompatible repository need to keep working via the
   old, unbounded path (with the new memory-bound guarantee then only holding for repositories
   created after this change)? This decides how much of the current whole-file code actually gets
   deleted versus kept as a narrower legacy path.

### Recommendations for a few implementation-shape choices (not blocking, offered for override)

- Keep `crates/cdc`'s own `ChunkerConfig` general (`target_size_bits` validated up to 30, and
  `SingleChunkChunker`/`None` kept) - `cdc` is documented as a crate with standalone value, held to
  a narrow-but-general public API (`.claude/rules/rust-code-quality.md`). Enforce the 23-bit cap and
  the "no whole-file" rule one layer up, at `create-repo`'s own validation (REQ-CLI-005) - the
  requirement change belongs to this application, not to the general-purpose chunking library.
- Implement the per-handle formula as a new method directly on `MemoryBudget`
  (`try_acquire_share(&self, handle_used: u64, requested: u64) -> u64` or similar), keeping the CAS
  loop and the `max(0, ...)` clamp inside the type that already owns the atomic, rather than
  reimplementing the loop in `write_cache.rs` around a newly-`pub` `try_acquire`.
- Check the RAM budget against the *repository's own* configured `cdc_target_size_bits`-derived max
  chunk size, not unconditionally against the 23-bit ceiling's own 96 MiB - a repository configured
  with smaller bits needs a correspondingly smaller minimum RAM budget to run at all.

## Implementation plan (if picked up as-is)

Order roughly follows this repo's own "requirements/design before code" convention
(`.claude/rules/design-docs.md`, `.claude/rules/requirements.md`).

1. **Resolve or explicitly defer** the two questions above (FUSE/WinFSP pool characteristics can
   defer to an early empirical step within this same plan; the back-compat question should be
   settled before requirements text is written, since it changes the requirement's own scope).
2. **Requirements**: `requirements/functional/storage.md` REQ-STORAGE-003 - remove the "or a
   cheaper whole-file mode" framing, state CDC as the sole strategy with a bit-range cap (visible
   maximum 23, not 30). New non-functional requirement(s) for the RAM budget's existence and its
   min/default/configurable-max shape - candidate home `requirements/non-functional/performance.md`
   alongside `REQ-PERFORMANCE-*`, or a new area if this grows past a few entries (per
   `requirements/README.md`'s own splitting guidance) - not yet decided which.
3. **Design docs**:
   - New `docs/design/` entry (or entries - Mount-side and Store-side are separable decisions that
     both cite a shared RAM-budget concept) covering: the RAM-budget computation (gross limit minus
     SQLite `cache_size` minus thread-stack reserve), the per-handle mount formula (with the
     convergence reasoning: `(A-H)/2` applied once per write converges each handle to the
     equilibrium `A = H`, reproducing the 50%/25%/... sequence for staggered opens), the mount
     backpressure formula and its constants' derivation, and the Store/Ingest bounded parallel
     chunk pipeline.
   - Update `docs/design/mount-write-path.md`: DESIGN-MOUNT-006 (new formula/signal, status reset -
     see above) and DESIGN-MOUNT-010 (budget reinterpretation, status reset - see above).
   - Mark `docs/design/settle-whole-file-memory-bound.md` `Status: superseded-by DESIGN-...` (see
     above).
4. **`crates/cdc`**: no functional change if the recommendation above (keep it general) is taken -
   confirm the 6..=30 validation range stays as-is there.
5. **`crates/db/src/connection.rs`**: expose (or inline at the call site) a `cache_size` readback;
   decide whether the CLI-configurable override path sets it via a new `configure_write_connection`
   parameter or a separate call after open.
6. **`crates/cli/src/write_cache.rs`**: the new `MemoryBudget` method for the per-handle formula
   (see recommendation above), plus wherever the gross-256MB-minus-reserves computation lives (a
   small new module, e.g. `crates/cli/src/ram_budget.rs`, computed once at startup and passed down
   to both the mount and store/ingest code paths).
7. **`crates/cli/src/backpressure.rs` + `crates/cli/src/settle.rs` + `crates/cli/src/settle_pool.rs`**:
   the new `bytesInPersistQueue` counter (incremented on generation hand-off, decremented per
   completed chunk inside `Settler::complete_chunk`), the new formula reading it, wired into
   `DedupFs::write` in place of the current `backlog_spilled_bytes`-based call.
8. **`crates/cli/src/main.rs` / `create_repo.rs`**: remove `--whole-file`; tighten
   `--cdc-target-size-bits` validation (reject an implicit "no chunking", cap at 23) with an
   actionable error message; new CLI flags for the RAM budget total, the `cache_size` override, and
   `freeZoneBytes`/`SLOPE_DIVISOR`.
9. **`crates/cli/src/ingest.rs`** (the largest single code change here): replace the current
   single-threaded, whole-file-via-one-`settle::settle`-call flow with the bounded, parallel,
   chunk-by-chunk pipeline - read sequentially into RAM up to one chunk boundary, dedupe/persist
   that chunk immediately, `N = min(ram_budget / max_chunk_size, available_parallelism())` files in
   flight at a time, one thread per file, no intra-file parallelism (explicit YAGNI per the sketch
   above), no spillover. Also: refuse to start if `ram_budget < max_chunk_size` for the repository's
   own configured bits, with a clear error message.
10. **Verification**: full suite per `AGENTS.md` ("Verification Of Changes"); in particular, a real
    libfuse3-mount-backed test exercising the mount handle-cap formula's convergence and the new
    backpressure signal (mirroring the existing calibration precedent), and an Ingest test large
    enough to distinguish bounded from unbounded memory use for a multi-chunk file (tying back to
    `docs/design/settle-whole-file-memory-bound.md`'s own still-open note on how to actually assert
    a memory bound in a test, e.g. a counting `#[global_allocator]` for the test binary rather than
    OS-level RSS sampling).
11. **`migration/feature-comparison.md`**: revisit the `reclaimSpace`/whole-file-dedup rows and any
    other row whose description assumes `--whole-file` still exists.
