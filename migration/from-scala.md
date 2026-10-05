# Migrating From Scala DedupFS

How to migrate an existing Scala-DedupFS repository to this implementation, with `dfs
migrate-scala-repo`. The design behind it is in
[`../docs/design/scala-migration-tool.md`](../docs/design/scala-migration-tool.md).

The command is a temporary tool. It ships only in the first releases that can migrate. Anyone who
needs it later runs the migration with one of those releases and then upgrades as usual.

## What migration does

The repository is adopted in place. Nothing is copied.

- The stored byte content is reused as it is. DESIGN-STORE-001 in
  [`../docs/design/byte-store.md`](../docs/design/byte-store.md) matches the Scala byte-store layout
  exactly, so the Scala `data/` directory is this implementation's `data/` directory. Migration
  only reads those bytes, to derive new content-defined chunk boundaries and new BLAKE3 hashes
  (REQ-MIGRATION-002 in
  [`../requirements/functional/repository-migration.md`](../requirements/functional/repository-migration.md)).
- Migration never writes to `data/`, never moves anything in it, and never touches the Scala `fsdb/`
  directory (REQ-MIGRATION-005).
- The result is one new metadata database per requested chunk size, next to `data/`.
- The complete tree is carried over, including every soft-deleted entry (REQ-MIGRATION-001).
  Deleted entries stay reachable through `[deleted]`, for example `dfs list --show-deleted
  /photos/[deleted]`.

## Before migrating

1. Stop everything that writes to the Scala repository. Changes made after the export in the next
   step are not part of the migrated result.
2. Create the export with the Scala tool, using its `fsc` launcher:

   ```
   fsc repo=<scala repository> db-backup
   ```

   The export is a zip file in the repository's `fsdb/` directory, named like
   `dedupfs-<version>_<date>_<time>_backup.zip`. `dfs` reads the zip directly. An already unzipped
   `.sql` file works as well.
3. Check that the repository root contains its `data/` directory. Migration refuses a directory
   without one, because that means the path is wrong.
4. Make sure there is free disk space for the results. Each metadata database is small next to the
   content it describes. In addition, migration keeps a working file, `migrate-staging.db`, in the
   repository root while it runs. Allow space of the same order as the export file for it.

## Running the migration

```
dfs migrate-scala-repo --repository <scala repository> --script <export.zip> --cdc-target-size-bits 20
```

`--repository` is required and has no default, so a wrong guess can never point the migration at an
unrelated repository. `--cdc-target-size-bits` is the chunking target size of the new repository. It
is fixed for the repository's lifetime, exactly as for `dfs create-repo`.

The staging file defaults to `migrate-staging.db` inside the repository. `--staging <path>` puts it
elsewhere.

With exactly one target size, the result is the repository's normal metadata directory, `meta/`. It
is usable immediately. Point every other `dfs` command at the same repository directory.

### Choosing the chunk size

A smaller value finds more duplicate content, so `data/` needs less space, but the metadata database
grows, because it describes many more chunks. A larger value does the opposite. The right value is
the one with the smallest total for the repository, including every metadata backup that is kept
(REQ-MAINTENANCE-001): the physical size plus the metadata size times the number of copies.

To compare candidates on the real data, request several sizes in one run. The source is read only
once, however many sizes are requested.

```
dfs migrate-scala-repo --repository <scala repository> --script <export.zip> \
    --cdc-target-size-bits 16 --cdc-target-size-bits 18 --cdc-target-size-bits 20
```

Each size gets its own metadata directory, `meta-16bit/`, `meta-18bit/` and so on. None of them is
usable by other commands yet, because every other command looks for `meta/`. The command says so
when it finishes.

All these databases describe the same `data/` directory. Therefore:

- Compare them one at a time. Rename a directory to `meta/`, run `dfs stats` (its physical size and
  its metadata size), then rename it back. These read-only commands are safe.
- Never use more than one of them with a command that writes, such as `dfs ingest`, `dfs reclaim` or
  a read-write mount. Each database only knows its own chunks, so a write through one can overwrite
  content that another still refers to.
- Once one size is chosen, keep its directory as `meta/` and delete the other `meta-*bit`
  directories. They contain only metadata.

## If old data is missing

Migration reads all stored content once. If a data file that the metadata refers to is missing or
too short, the migration stops at that content (REQ-MIGRATION-006). The error names the missing
files under `data/`, the affected content, and a few of the files that use it. Nothing of that
content is migrated. Starting the migration again gets quickly to the same place and stops again.

There are two ways forward:

- Restore the missing data files, for example from a backup of the Scala repository, and run the
  same command again.
- Continue anyway with `--tolerate-missing-data`. The missing bytes are then taken as zeros, and
  the migration reports the gaps as it finds them. When it finishes, it prints a summary and writes
  the full list to `migrate-missing-data.txt` in the repository root.

A file that was migrated with a gap stays damaged. `dfs` reports the missing data whenever such a
file is read, exactly as long as the data files stay missing (`dfs restore` fails such a file unless
it runs with `--best-effort`, and a mount returns an I/O error unless it runs with
`--best-effort` too). Restoring the data files later repairs the file itself, as long as no
modified version was saved through a `--best-effort` mount in the meantime. The checksums of the affected chunks cannot match, so `dfs restore
--verify` reports them, since their content was never known.

The option only covers missing or too short data. Any other read error stops the migration.

## Checking the result

Compare the numbers with the Scala repository's own `fsc stats`: the counts of files and folders,
live and deleted. `dfs stats` reports the live counts and the logical and physical size. Spot-check
a few paths, including a deleted one, with `dfs list` and `dfs list --show-deleted`.

## Interruptions

Run the same command again. A migration that was interrupted, by a killed process or a power loss,
continues where it stopped, without manual cleanup (REQ-MIGRATION-003). A destination that was
already migrated completely is left alone.

The migrated entries and the note that they were migrated are written together, so an interruption
never leaves one without the other. Everything since the last commit is redone, which is a small
part of the work.

While it runs, the working file `migrate-staging.db` stays in the repository root, and each
destination keeps two bookkeeping tables. Both are removed automatically when the migration has
finished. Do not delete them while a migration is unfinished. To start over completely, delete the `meta*` directories and
`migrate-staging.db`, then run the command again.

Migration time depends mostly on how fast the source disk reads, since all content is read once.
Each additional chunk size adds computing time but no second read.

## Going back

The Scala repository is unchanged by migration, so going back is possible.

- Before the first write through `dfs`: delete the new `meta*` directories and `migrate-staging.db`.
  The repository is then exactly as it was, and Scala can use it again.
- After the first write through `dfs`: do not use the repository with Scala again. Writing through
  `dfs`, for example with `dfs ingest` or a read-write mount, stores new chunks in `data/`, in gaps
  or after the last stored byte. Scala does not know about them and would overwrite them when it
  next writes. Reading through `dfs` does not change anything.

For the time of the transition, keep a separate copy of the Scala metadata (the export from the
steps above) if a way back is needed.
