//! Imports a Scala-DedupFS `fsc db-backup` SQL export into a small, durable, queryable staging
//! database (DESIGN-MIGRATION-001/002 in `docs/design/scala-migration-tool.md`). Hand-parses only
//! enough to find each top-level SQL statement and tell which of the two tables this migration
//! actually needs (if any) it inserts into, delegating the row data itself - column lists,
//! multi-row `VALUES` tuples, string/number/`NULL`/binary-literal syntax - to SQLite's own
//! statement execution rather than a second, hand-written value parser.
//!
//! The export's row data is always written as `INSERT INTO <table> VALUES (...), (...), ...` -
//! one `INSERT` per table, potentially covering every row in that table as a single multi-tuple
//! statement - alongside schema-definition statements (`CREATE TABLE`/`CREATE SEQUENCE`/
//! `CREATE USER`/`ALTER TABLE ... ADD CONSTRAINT`) in the source system's own SQL dialect, not
//! portable as-is. Confirmed directly against a real production export (~550 MB, 6.7 million tree
//! entries): every row is captured this way, and nothing this migration needs lives in any other
//! statement type - including the source's own `Context` table (a single schema-version marker
//! row), which is parsed only far enough to recognize and discard.

use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use rusqlite::{Connection, OpenFlags, OptionalExtension};

/// `TreeEntries`' own column order in the source export - a `VALUES` tuple with no explicit
/// column list (the export's own form) is positional against this.
const TREE_ENTRIES_COLUMNS: &str =
    "ID INTEGER, PARENTID INTEGER, NAME TEXT, TIME INTEGER, DELETED INTEGER, DATAID INTEGER";
/// `DataEntries`' own column order in the source export - see [`TREE_ENTRIES_COLUMNS`].
const DATA_ENTRIES_COLUMNS: &str =
    "ID INTEGER, SEQ INTEGER, LENGTH INTEGER, START INTEGER, STOP INTEGER, HASH BLOB";

#[derive(Debug)]
pub enum ImportError {
    Io(io::Error),
    Sqlite(rusqlite::Error),
    Zip(zip::result::ZipError),
    /// The export's zip archive contains no `.sql` entry.
    NoScriptInZip,
    /// A statement classified as row data for a table this migration cares about could not
    /// actually be executed - almost certainly a genuinely different export format/version than
    /// the one this parser was built against, not a transient failure.
    Statement {
        statement: String,
        source: rusqlite::Error,
    },
}

impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ImportError::Io(err) => write!(f, "{err}"),
            ImportError::Sqlite(err) => write!(f, "{err}"),
            ImportError::Zip(err) => write!(f, "{err}"),
            ImportError::NoScriptInZip => write!(f, "no .sql entry found in the zip archive"),
            ImportError::Statement { statement, source } => write!(
                f,
                "failed to import a statement, likely an unsupported export format ({source}): {}",
                truncate_for_error(statement)
            ),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<io::Error> for ImportError {
    fn from(err: io::Error) -> Self {
        ImportError::Io(err)
    }
}

impl From<rusqlite::Error> for ImportError {
    fn from(err: rusqlite::Error) -> Self {
        ImportError::Sqlite(err)
    }
}

impl From<zip::result::ZipError> for ImportError {
    fn from(err: zip::result::ZipError) -> Self {
        ImportError::Zip(err)
    }
}

fn truncate_for_error(s: &str) -> String {
    const LIMIT: usize = 200;
    if s.len() <= LIMIT {
        s.to_string()
    } else {
        format!("{}...", &s[..LIMIT])
    }
}

/// Row counts actually imported, for a caller's own progress/sanity reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ImportStats {
    pub tree_entries: u64,
    pub data_entries: u64,
}

/// Reads `path` as the export's SQL text - either a plain `.sql` file, or the zipped form
/// `fsc db-backup` actually produces (detected by its `PK\x03\x04` magic, not by file extension).
pub fn load_script_text(path: &Path) -> Result<String, ImportError> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    let is_zip = file.read(&mut magic)? == 4 && magic == *b"PK\x03\x04";
    if !is_zip {
        let mut text = String::new();
        File::open(path)?.read_to_string(&mut text)?;
        return Ok(text);
    }
    let mut archive = zip::ZipArchive::new(File::open(path)?)?;
    let sql_index = (0..archive.len())
        .find(|&i| {
            archive
                .by_index(i)
                .is_ok_and(|entry| entry.name().to_ascii_lowercase().ends_with(".sql"))
        })
        .ok_or(ImportError::NoScriptInZip)?;
    let mut text = String::new();
    archive.by_index(sql_index)?.read_to_string(&mut text)?;
    Ok(text)
}

/// True if `staging_path` already holds a complete, reusable import (DESIGN-MIGRATION-001) - safe
/// to skip [`import`] and go straight to [`open`]. False for a missing file, a file left behind by
/// an interrupted import, or anything else that fails the check - all treated the same way,
/// "not reusable, rebuild from the source export".
pub fn is_reusable(staging_path: &Path) -> bool {
    (|| -> rusqlite::Result<bool> {
        let conn = Connection::open_with_flags(staging_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.query_row("SELECT 1 FROM import_complete", [], |_| Ok(()))
            .optional()
            .map(|found| found.is_some())
    })()
    .unwrap_or(false)
}

/// Opens an existing, [`is_reusable`]-confirmed staging database read-only - phase 2 only ever
/// queries it, never writes.
pub fn open(staging_path: &Path) -> Result<Connection, ImportError> {
    Ok(Connection::open_with_flags(
        staging_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?)
}

/// Parses `script_text` (see [`load_script_text`]) into a fresh staging database at
/// `staging_path`, replacing anything already there. The whole import - schema creation, every
/// row, and the completion marker [`is_reusable`] checks for - runs inside one transaction, so a
/// run interrupted at any point leaves nothing at all rather than something that merely looks
/// complete (DESIGN-MIGRATION-001's "Detecting a reusable import").
///
/// A brand new, otherwise-empty in-memory connection does the actual work, with `staging_path`
/// itself attached under the alias `public` - not because the destination is transient (it is an
/// ordinary durable file, opened directly by [`open`] afterward), but so the export's own
/// `"PUBLIC"."<table>"`-qualified `INSERT` statements execute completely unmodified against it,
/// with no text rewriting of their own between reading and executing them.
pub fn import(script_text: &str, staging_path: &Path) -> Result<ImportStats, ImportError> {
    let _ = std::fs::remove_file(staging_path);
    let mut coordinator = Connection::open_in_memory()?;
    let staging_path_str = staging_path.to_string_lossy();
    coordinator.execute("ATTACH DATABASE ?1 AS public", [staging_path_str.as_ref()])?;

    let tx = coordinator.transaction()?;
    tx.execute_batch(&format!(
        "CREATE TABLE public.treeentries ({TREE_ENTRIES_COLUMNS});
         CREATE TABLE public.dataentries ({DATA_ENTRIES_COLUMNS});
         CREATE TABLE public.import_complete (id INTEGER PRIMARY KEY);"
    ))?;

    let cleaned = strip_line_comments(script_text);
    for stmt in iter_statements(&cleaned) {
        if classify_insert(stmt).is_some() {
            let rewritten = rewrite_unicode_escape_literals(stmt);
            tx.execute_batch(&rewritten)
                .map_err(|source| ImportError::Statement {
                    statement: stmt.to_string(),
                    source,
                })?;
        }
    }

    let stats = stats(&tx)?;
    tx.execute("INSERT INTO public.import_complete (id) VALUES (1)", [])?;
    tx.commit()?;
    Ok(stats)
}

/// Row counts currently in `conn` - works against both the coordinator connection [`import`] uses
/// while building a fresh staging database (where the tables live in the attached `public` schema)
/// and a plain [`open`] of an already-built one (where they live in `main`): unqualified names
/// resolve to whichever schema actually has them, and only one ever does.
pub fn stats(conn: &Connection) -> Result<ImportStats, ImportError> {
    let tree_entries: i64 = conn.query_row("SELECT COUNT(*) FROM treeentries", [], |r| r.get(0))?;
    let data_entries: i64 = conn.query_row("SELECT COUNT(*) FROM dataentries", [], |r| r.get(0))?;
    Ok(ImportStats {
        tree_entries: tree_entries as u64,
        data_entries: data_entries as u64,
    })
}

/// One row from the staging `treeentries` table, live or soft-deleted alike, exactly as the source
/// export recorded it - REQ-MIGRATION-001's full history needs both, so nothing here filters
/// either out.
#[derive(Debug, Clone)]
pub struct StagingTreeEntry {
    pub id: i64,
    pub name: String,
    pub time: i64,
    /// `None` for a live entry - the source's own `0`-means-live encoding, already translated so
    /// callers never need to know it. `Some(timestamp)` otherwise.
    pub deleted_at: Option<i64>,
    /// `None` for a directory, `Some(-1)` for an explicit zero-length file, `Some(id) if id >= 0`
    /// for a real reference into `dataentries` - confirmed by
    /// `import_preserves_the_null_vs_minus_one_dataid_distinction` below.
    pub data_id: Option<i64>,
}

/// Every child of `parent_id` in the staging tree, live and soft-deleted alike, in ascending `id`
/// order (a stable, deterministic order for a resumable walk - not otherwise meaningful). The
/// staging schema has no separate "live children only" query; a caller wanting only live entries
/// filters `deleted_at` itself.
///
/// The root's own row (`id = 0`) is `parentid = 0` too (self-parented, the source's own
/// convention) - calling this with `parent_id = 0` therefore also returns the root's own row
/// alongside its real children; callers walking from the root filter it out themselves.
pub fn staging_children(
    conn: &Connection,
    parent_id: i64,
) -> Result<Vec<StagingTreeEntry>, ImportError> {
    let mut stmt = conn.prepare(
        "SELECT id, name, time, deleted, dataid FROM treeentries WHERE parentid = ?1 ORDER BY id",
    )?;
    let rows = stmt
        .query_map([parent_id], |row| {
            let deleted: i64 = row.get(3)?;
            Ok(StagingTreeEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                time: row.get(2)?,
                deleted_at: if deleted == 0 { None } else { Some(deleted) },
                data_id: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// `data_id`'s ordered `(start, stop)` byte ranges in the old data store, from `dataentries`'
/// `seq`-ordered rows - concatenating the bytes at these ranges, in this order, reproduces the old
/// file's bytes exactly (a file's storage is not always contiguous, hence more than one row can
/// share one `data_id`). `length`/`hash` are not read here - migration recomputes its own chunk
/// hashes from the bytes directly rather than trusting the source's whole-file ones. Empty if
/// `data_id` is not actually a real content reference (see [`StagingTreeEntry::data_id`]).
pub fn staging_data_parts(conn: &Connection, data_id: i64) -> Result<Vec<(u64, u64)>, ImportError> {
    let mut stmt =
        conn.prepare("SELECT start, stop FROM dataentries WHERE id = ?1 ORDER BY seq")?;
    let rows = stmt
        .query_map([data_id], |row| {
            let start: i64 = row.get(0)?;
            let stop: i64 = row.get(1)?;
            Ok((start as u64, stop as u64))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

enum Table {
    TreeEntries,
    DataEntries,
}

/// Recognizes an `INSERT INTO <name>` statement's target table, ignoring any schema qualifier and
/// whether either part is quoted - `None` for every other statement (schema definitions, the
/// `Context` version marker, or an `INSERT` into some other table entirely).
fn classify_insert(stmt: &str) -> Option<Table> {
    let rest = strip_prefix_ci(stmt.trim_start(), "insert")?;
    let rest = strip_prefix_ci(rest.trim_start(), "into")?;
    let (segments, _) = read_dotted_name(rest)?;
    match segments.last()?.to_ascii_uppercase().as_str() {
        "TREEENTRIES" => Some(Table::TreeEntries),
        "DATAENTRIES" => Some(Table::DataEntries),
        _ => None,
    }
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    (s.len() >= prefix.len()
        && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes()))
    .then(|| &s[prefix.len()..])
}

/// Reads one SQL identifier - a `"..."`-quoted identifier (`""`-doubling escapes an embedded
/// quote) or a bare `[A-Za-z0-9_$]+` run - returning it and the remainder of `s` right after it.
fn read_identifier(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    if let Some(rest) = s.strip_prefix('"') {
        let bytes = rest.as_bytes();
        let mut ident = String::new();
        let mut i = 0usize;
        loop {
            if i >= bytes.len() {
                return None; // unterminated quoted identifier
            }
            if bytes[i] == b'"' {
                if bytes.get(i + 1) == Some(&b'"') {
                    ident.push('"');
                    i += 2;
                    continue;
                }
                return Some((ident, &rest[i + 1..]));
            }
            let ch = rest[i..]
                .chars()
                .next()
                .expect("i is a valid char boundary");
            ident.push(ch);
            i += ch.len_utf8();
        }
    } else {
        let end = s
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '$'))
            .unwrap_or(s.len());
        (end > 0).then(|| (s[..end].to_string(), &s[end..]))
    }
}

/// Reads one or more dot-separated identifiers (e.g. `"PUBLIC"."TREEENTRIES"`), returning them in
/// order and the remainder of `s` right after the last one.
fn read_dotted_name(s: &str) -> Option<(Vec<String>, &str)> {
    let (first, mut rest) = read_identifier(s)?;
    let mut segments = vec![first];
    while let Some(after_dot) = rest.trim_start().strip_prefix('.') {
        let (next, remainder) = read_identifier(after_dot)?;
        segments.push(next);
        rest = remainder;
    }
    Some((segments, rest))
}

/// Rewrites every `U&'...'` Unicode-escape string literal in `stmt` into an equivalent plain
/// `'...'` literal (see [`convert_unicode_escape_literal`]) - confirmed against a real production
/// export: the source system emits this standard SQL literal form for a stored name containing a
/// character its export apparently cannot represent directly (e.g. `U&'Decathlon
/// R\00fccksendung.pdf'` for a name containing "ü"), and SQLite has no support for it at all, so
/// this is the one part of a kept statement's own value syntax this migration still has to touch -
/// everything else, including every ordinary `'...'` literal (copied through byte for byte,
/// `''`-escaping and all), passes through completely unmodified. Returns `stmt` itself, unchanged,
/// when it contains no such literal at all - the overwhelmingly common case, and the only one a
/// cheap upfront scan needs to confirm before doing any real work.
fn rewrite_unicode_escape_literals(stmt: &str) -> std::borrow::Cow<'_, str> {
    if !contains_unicode_escape_marker(stmt) {
        return std::borrow::Cow::Borrowed(stmt);
    }
    let mut out = String::with_capacity(stmt.len());
    let mut rest = stmt;
    while !rest.is_empty() {
        if let Some((literal, remainder)) = convert_unicode_escape_literal(rest) {
            out.push_str(&literal);
            rest = remainder;
            continue;
        }
        if let Some(after_quote) = rest.strip_prefix('\'') {
            // Copy one ordinary '...' literal through unchanged, respecting '' escaping, so its
            // content - which could coincidentally itself contain the text "u&'" - is never
            // mistaken for the start of a real Unicode-escape literal on the next loop iteration.
            let bytes = after_quote.as_bytes();
            let mut i = 0usize;
            loop {
                if i >= bytes.len() {
                    break;
                }
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push('\'');
            out.push_str(&after_quote[..i]);
            rest = &after_quote[i..];
        } else {
            let ch = rest.chars().next().expect("rest is non-empty");
            out.push(ch);
            rest = &rest[ch.len_utf8()..];
        }
    }
    std::borrow::Cow::Owned(out)
}

/// Cheap, allocation-free check for whether `stmt` might contain a `U&'...'` literal at all -
/// [`rewrite_unicode_escape_literals`]'s fast path for the common case where it does not. A plain
/// byte-level scan is safe even amid multi-byte UTF-8 content: every byte of a UTF-8 continuation
/// sequence is >= 0x80, so none can ever falsely match the all-ASCII pattern being searched for.
fn contains_unicode_escape_marker(stmt: &str) -> bool {
    stmt.as_bytes()
        .windows(3)
        .any(|w| w[0].eq_ignore_ascii_case(&b'u') && w[1] == b'&' && w[2] == b'\'')
}

/// Converts one `U&'...'` Unicode-escape string literal (SQL standard syntax) starting at `s`
/// (which must begin with `U&`, case-insensitive) into an ordinary `'...'` literal, decoding its
/// `\XXXX`/`\+XXXXXX` codepoint escapes and re-escaping any resulting `'` character for the plain
/// literal it becomes. Returns the converted literal and the remainder of `s` right after it,
/// including past a trailing `UESCAPE '<char>'` clause if present (SQL standard: redefines which
/// character introduces an escape within *this* literal; `\` is the default when omitted). `None`
/// if `s` does not actually hold a valid literal of this form after all.
fn convert_unicode_escape_literal(s: &str) -> Option<(String, &str)> {
    let after_prefix = strip_prefix_ci(s, "u&")?.strip_prefix('\'')?;
    let bytes = after_prefix.as_bytes();
    let mut i = 0usize;
    let mut raw = String::new();
    loop {
        if i >= bytes.len() {
            return None; // unterminated literal
        }
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                raw.push('\'');
                i += 2;
                continue;
            }
            i += 1;
            break;
        }
        let ch = after_prefix[i..]
            .chars()
            .next()
            .expect("i is a valid char boundary");
        raw.push(ch);
        i += ch.len_utf8();
    }
    let mut rest = &after_prefix[i..];
    let mut escape_char = '\\';

    if let Some(after_keyword) = strip_prefix_ci(rest.trim_start(), "uescape") {
        let quoted = after_keyword.trim_start().strip_prefix('\'')?;
        let mut chars = quoted.chars();
        escape_char = chars.next()?;
        rest = chars.as_str().strip_prefix('\'')?;
    }

    let decoded = decode_unicode_escapes(&raw, escape_char)?;
    let mut literal = String::with_capacity(decoded.len() + 2);
    literal.push('\'');
    for ch in decoded.chars() {
        if ch == '\'' {
            literal.push('\'');
        }
        literal.push(ch);
    }
    literal.push('\'');
    Some((literal, rest))
}

/// Decodes `\XXXX` (4 hex digits) and `\+XXXXXX` (6 hex digits) codepoint escapes, and a doubled
/// escape character as a literal instance of it, using `escape_char` as the introducer.
fn decode_unicode_escapes(s: &str, escape_char: char) -> Option<String> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != escape_char {
            out.push(c);
            continue;
        }
        match chars.peek().copied() {
            Some(next) if next == escape_char => {
                out.push(escape_char);
                chars.next();
            }
            Some('+') => {
                chars.next();
                let hex: String = (0..6).map(|_| chars.next()).collect::<Option<String>>()?;
                out.push(char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?);
            }
            Some(_) => {
                let hex: String = (0..4).map(|_| chars.next()).collect::<Option<String>>()?;
                out.push(char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?);
            }
            None => return None,
        }
    }
    Some(out)
}

/// Strips `--`-to-end-of-line comments, respecting single-quoted string literals (`''`-doubling
/// escapes an embedded quote) so a `--` inside a stored name is never mistaken for a comment - the
/// export's own tool emits row-count sanity-check comments between statements
/// (`-- 123 +/- SELECT COUNT(*) FROM ...;`).
fn strip_line_comments(script: &str) -> String {
    let bytes = script.as_bytes();
    let mut out = String::with_capacity(script.len());
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_string = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if b == b'\'' {
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => {
                in_string = true;
                i += 1;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                out.push_str(&script[start..i]);
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                start = i;
            }
            _ => i += 1,
        }
    }
    out.push_str(&script[start..]);
    out
}

/// Splits `script` into top-level, semicolon-terminated statements, respecting single-quoted
/// string literals (`''`-doubling) so a `;` inside a stored name is never mistaken for a statement
/// boundary. Empty statements (blank stretches between real ones) are skipped.
fn iter_statements(script: &str) -> impl Iterator<Item = &str> {
    let mut rest = script;
    std::iter::from_fn(move || {
        loop {
            if rest.is_empty() {
                return None;
            }
            let bytes = rest.as_bytes();
            let mut in_string = false;
            let mut end = None;
            let mut i = 0usize;
            while i < bytes.len() {
                let b = bytes[i];
                if in_string {
                    if b == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        in_string = false;
                    }
                } else if b == b'\'' {
                    in_string = true;
                } else if b == b';' {
                    end = Some(i);
                    break;
                }
                i += 1;
            }
            let (stmt, remainder) = match end {
                Some(i) => (&rest[..i], &rest[i + 1..]),
                None => (rest, ""),
            };
            rest = remainder;
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            return Some(trimmed);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn staging_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
        dir.path().join("staging.db")
    }

    #[test]
    fn convert_unicode_escape_literal_decodes_the_real_export_example() {
        // 'ü' is U+00FC - the exact case found in a real production export (a filename
        // "Decathlon Rücksendung.pdf").
        let (literal, rest) =
            convert_unicode_escape_literal(r"U&'Decathlon R\00fccksendung.pdf', 1)").unwrap();
        assert_eq!(literal, "'Decathlon Rücksendung.pdf'");
        assert_eq!(rest, ", 1)");
    }

    #[test]
    fn convert_unicode_escape_literal_handles_six_digit_and_doubled_escape_and_embedded_quote() {
        // \+01F600 is U+1F600 (an emoji, outside the BMP); \\ is a literal backslash; a decoded
        // apostrophe must come out re-escaped as '' for the resulting plain literal.
        let (literal, rest) =
            convert_unicode_escape_literal(r"U&'a\\b\+01F600c\0027d' more").unwrap();
        assert_eq!(literal, "'a\\b\u{1F600}c''d'");
        assert_eq!(rest, " more");
    }

    #[test]
    fn convert_unicode_escape_literal_honors_a_trailing_uescape_clause() {
        let (literal, rest) =
            convert_unicode_escape_literal(r"U&'a!00fcb' UESCAPE '!' , 2)").unwrap();
        assert_eq!(literal, "'a\u{fc}b'");
        assert_eq!(rest, " , 2)");
    }

    #[test]
    fn rewrite_unicode_escape_literals_leaves_ordinary_statements_unchanged() {
        let stmt = "INSERT INTO T VALUES ('plain', 1), ('also plain', 2)";
        assert!(matches!(
            rewrite_unicode_escape_literals(stmt),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    #[test]
    fn rewrite_unicode_escape_literals_does_not_mistake_ordinary_content_for_the_marker() {
        // A stored name that happens to literally contain the text "u&'" as ordinary data (inside
        // a normal '...' literal) must survive unchanged, not be misread as starting a real
        // Unicode-escape literal.
        let stmt = "INSERT INTO T VALUES ('a u&''b'' file', 1)";
        let rewritten = rewrite_unicode_escape_literals(stmt);
        assert_eq!(rewritten, stmt);
    }

    #[test]
    fn rewrite_unicode_escape_literals_converts_only_the_real_literal_and_leaves_the_rest() {
        let stmt = r"INSERT INTO T VALUES (1, 'plain'), (2, U&'Gr\00df')";
        let rewritten = rewrite_unicode_escape_literals(stmt);
        assert_eq!(
            rewritten,
            "INSERT INTO T VALUES (1, 'plain'), (2, 'Gr\u{df}')"
        );
    }

    #[test]
    fn import_handles_a_row_with_a_unicode_escape_literal_name() {
        let dir = tempfile::tempdir().unwrap();
        let script = "CREATE CACHED TABLE \"PUBLIC\".\"TREEENTRIES\"(\"ID\" BIGINT, \"PARENTID\" BIGINT, \"NAME\" VARCHAR, \"TIME\" BIGINT, \"DELETED\" BIGINT, \"DATAID\" BIGINT);\n\
             INSERT INTO \"PUBLIC\".\"TREEENTRIES\" VALUES\n\
             (0, 0, '', 1000, 0, NULL),\n\
             (1, 0, U&'Decathlon R\\00fccksendung.pdf', 1001, 0, -1);";
        let stats = import(script, &staging_path(&dir)).unwrap();
        assert_eq!(stats.tree_entries, 2);
        let conn = open(&staging_path(&dir)).unwrap();
        let name: String = conn
            .query_row("SELECT NAME FROM treeentries WHERE ID = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(name, "Decathlon Rücksendung.pdf");
    }

    #[test]
    fn strip_line_comments_removes_h2_row_count_comments_but_keeps_string_content() {
        let script = "INSERT INTO T VALUES ('a--b', 1);\n-- 3 +/- SELECT COUNT(*) FROM T;\nINSERT INTO T VALUES ('c', 2);";
        let cleaned = strip_line_comments(script);
        assert!(
            cleaned.contains("'a--b'"),
            "string content must survive: {cleaned}"
        );
        assert!(
            !cleaned.contains("SELECT COUNT"),
            "the comment must be gone: {cleaned}"
        );
    }

    #[test]
    fn iter_statements_splits_on_semicolons_outside_quoted_strings() {
        let script = "INSERT INTO T VALUES ('a;b');INSERT INTO T VALUES ('c');";
        let stmts: Vec<&str> = iter_statements(script).collect();
        assert_eq!(stmts.len(), 2, "got {stmts:?}");
        assert!(stmts[0].contains("'a;b'"));
    }

    #[test]
    fn iter_statements_skips_blank_statements() {
        let script = "INSERT INTO T VALUES (1);   ;\n\nINSERT INTO T VALUES (2);";
        let stmts: Vec<&str> = iter_statements(script).collect();
        assert_eq!(stmts.len(), 2, "got {stmts:?}");
    }

    #[test]
    fn classify_insert_recognizes_tree_and_data_entries_case_and_quote_insensitively() {
        assert!(matches!(
            classify_insert(r#"INSERT INTO "PUBLIC"."TREEENTRIES" VALUES (1)"#),
            Some(Table::TreeEntries)
        ));
        assert!(matches!(
            classify_insert("insert into dataentries values (1)"),
            Some(Table::DataEntries)
        ));
    }

    #[test]
    fn classify_insert_ignores_context_and_other_statements() {
        assert!(
            classify_insert(r#"INSERT INTO "PUBLIC"."CONTEXT" VALUES ('db version', '3')"#)
                .is_none()
        );
        assert!(classify_insert(r#"CREATE CACHED TABLE "PUBLIC"."TREEENTRIES"(...)"#).is_none());
        assert!(
            classify_insert(r#"ALTER TABLE "PUBLIC"."TREEENTRIES" ADD CONSTRAINT x"#).is_none()
        );
    }

    /// A small script mirroring the real export's own shape: DDL this migration cannot and must
    /// not execute, a multi-row `VALUES` `INSERT` per table, the `Context` version marker, and the
    /// `dataId` `NULL`/`-1`/real-reference distinction all in one file.
    fn sample_script() -> &'static str {
        r#"
CREATE USER IF NOT EXISTS "SA" SALT 'x' HASH 'y' ADMIN;
CREATE SEQUENCE "PUBLIC"."IDSEQ" START WITH 3;
CREATE CACHED TABLE "PUBLIC"."CONTEXT"(
    "KEY" VARCHAR(255) NOT NULL,
    "VALUE" VARCHAR(255) NOT NULL
);
INSERT INTO "PUBLIC"."CONTEXT" VALUES
('db version', '3');
CREATE CACHED TABLE "PUBLIC"."TREEENTRIES"(
    "ID" BIGINT NOT NULL,
    "PARENTID" BIGINT NOT NULL,
    "NAME" CHARACTER VARYING(255) NOT NULL,
    "TIME" BIGINT NOT NULL,
    "DELETED" BIGINT DEFAULT 0 NOT NULL,
    "DATAID" BIGINT DEFAULT NULL
);
ALTER TABLE "PUBLIC"."TREEENTRIES" ADD CONSTRAINT "PUBLIC"."PK_TREEENTRIES" PRIMARY KEY("ID");
-- 3 +/- SELECT COUNT(*) FROM PUBLIC.TREEENTRIES;
INSERT INTO "PUBLIC"."TREEENTRIES" VALUES
(0, 0, '', 1000, 0, NULL),
(1, 0, 'a.txt', 1001, 0, -1),
(2, 0, 'b.txt', 1002, 0, 5);
CREATE CACHED TABLE "PUBLIC"."DATAENTRIES"(
    "ID" BIGINT NOT NULL,
    "SEQ" INTEGER NOT NULL,
    "LENGTH" BIGINT,
    "START" BIGINT NOT NULL,
    "STOP" BIGINT NOT NULL,
    "HASH" BINARY(16)
);
-- 1 +/- SELECT COUNT(*) FROM PUBLIC.DATAENTRIES;
INSERT INTO "PUBLIC"."DATAENTRIES" VALUES
(5, 1, 3, 100, 103, X'0102030405060708090a0b0c0d0e0f10');
"#
    }

    #[test]
    fn import_reports_row_counts_matching_the_scripts_own_sanity_comments() {
        let dir = tempfile::tempdir().unwrap();
        let stats = import(sample_script(), &staging_path(&dir)).unwrap();
        assert_eq!(stats.tree_entries, 3);
        assert_eq!(stats.data_entries, 1);
    }

    #[test]
    fn import_preserves_the_null_vs_minus_one_dataid_distinction() {
        let dir = tempfile::tempdir().unwrap();
        import(sample_script(), &staging_path(&dir)).unwrap();
        let conn = open(&staging_path(&dir)).unwrap();
        let data_id = |id: i64| -> Option<i64> {
            conn.query_row("SELECT DATAID FROM treeentries WHERE ID = ?1", [id], |r| {
                r.get(0)
            })
            .unwrap()
        };
        assert_eq!(data_id(0), None, "the root directory has no dataId at all");
        assert_eq!(data_id(1), Some(-1), "an explicit zero-length file");
        assert_eq!(data_id(2), Some(5), "a real reference into DataEntries");
    }

    #[test]
    fn import_ignores_context_and_survives_ddl_it_cannot_execute() {
        // The sample script's CREATE USER/CREATE SEQUENCE/CREATE CACHED TABLE/ALTER TABLE
        // statements are not valid SQLite syntax at all - import() must never attempt to execute
        // them, only the statements classify_insert() actually recognizes.
        let dir = tempfile::tempdir().unwrap();
        import(sample_script(), &staging_path(&dir)).unwrap();
        let conn = open(&staging_path(&dir)).unwrap();
        let context_table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'context'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            context_table_exists, 0,
            "the Context table must never be created"
        );
    }

    #[test]
    fn staging_children_returns_both_live_children_ordered_by_id() {
        let dir = tempfile::tempdir().unwrap();
        import(sample_script(), &staging_path(&dir)).unwrap();
        let conn = open(&staging_path(&dir)).unwrap();

        // Root (id 0) is its own parent in the source's own convention - calling with parent_id 0
        // also returns the root's own row, which a caller walking from the root must filter out
        // itself (see this function's own doc comment).
        let children = staging_children(&conn, 0).unwrap();
        let ids: Vec<i64> = children.iter().map(|c| c.id).collect();
        assert_eq!(ids, vec![0, 1, 2]);

        let a = &children[1];
        assert_eq!(a.name, "a.txt");
        assert_eq!(a.deleted_at, None);
        assert_eq!(a.data_id, Some(-1));

        let b = &children[2];
        assert_eq!(b.name, "b.txt");
        assert_eq!(b.data_id, Some(5));
    }

    #[test]
    fn staging_children_translates_a_nonzero_deleted_column_into_some_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let script = format!(
            "CREATE CACHED TABLE \"PUBLIC\".\"TREEENTRIES\"({TREE_ENTRIES_COLUMNS});\n\
             INSERT INTO \"PUBLIC\".\"TREEENTRIES\" VALUES\n\
             (0, 0, '', 1000, 0, NULL),\n\
             (1, 0, 'gone.txt', 1001, 2000, -1);"
        );
        import(&script, &staging_path(&dir)).unwrap();
        let conn = open(&staging_path(&dir)).unwrap();
        let children = staging_children(&conn, 0).unwrap();
        let gone = children.iter().find(|c| c.id == 1).unwrap();
        assert_eq!(gone.deleted_at, Some(2000));
    }

    #[test]
    fn staging_data_parts_returns_ordered_ranges_for_a_multi_part_data_id() {
        let dir = tempfile::tempdir().unwrap();
        let script = format!(
            "CREATE CACHED TABLE \"PUBLIC\".\"DATAENTRIES\"({DATA_ENTRIES_COLUMNS});\n\
             INSERT INTO \"PUBLIC\".\"DATAENTRIES\" VALUES\n\
             (5, 2, NULL, 900, 950, NULL),\n\
             (5, 1, 53, 100, 103, X'0102030405060708090a0b0c0d0e0f10');"
        );
        import(&script, &staging_path(&dir)).unwrap();
        let conn = open(&staging_path(&dir)).unwrap();
        let parts = staging_data_parts(&conn, 5).unwrap();
        // Ordered by seq (1 before 2), not by insertion order (2 was inserted first above).
        assert_eq!(parts, vec![(100, 103), (900, 950)]);
    }

    #[test]
    fn staging_data_parts_is_empty_for_an_unknown_data_id() {
        let dir = tempfile::tempdir().unwrap();
        import(sample_script(), &staging_path(&dir)).unwrap();
        let conn = open(&staging_path(&dir)).unwrap();
        assert_eq!(staging_data_parts(&conn, 999).unwrap(), Vec::new());
    }

    #[test]
    fn is_reusable_is_false_for_a_missing_or_incomplete_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!is_reusable(&staging_path(&dir)), "no file at all");

        // A file that exists but was never actually imported (e.g. left behind by something
        // unrelated, or a from-scratch empty file) must not be mistaken for a complete import.
        std::fs::write(staging_path(&dir), b"not a database").unwrap();
        assert!(!is_reusable(&staging_path(&dir)));
    }

    #[test]
    fn is_reusable_is_true_only_after_a_successful_import() {
        let dir = tempfile::tempdir().unwrap();
        let path = staging_path(&dir);
        assert!(!is_reusable(&path));
        import(sample_script(), &path).unwrap();
        assert!(is_reusable(&path));
    }

    #[test]
    fn a_second_import_call_rebuilds_from_scratch_rather_than_appending() {
        let dir = tempfile::tempdir().unwrap();
        let path = staging_path(&dir);
        import(sample_script(), &path).unwrap();
        let stats = import(sample_script(), &path).unwrap();
        assert_eq!(
            stats.tree_entries, 3,
            "must not accumulate across repeated imports"
        );
    }

    #[test]
    fn load_script_text_reads_a_plain_sql_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("script.sql");
        std::fs::write(&path, sample_script()).unwrap();
        let text = load_script_text(&path).unwrap();
        assert_eq!(text, sample_script());
    }

    #[test]
    fn load_script_text_reads_the_sql_entry_out_of_a_zip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backup.zip");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        writer
            .start_file("script.sql", zip::write::SimpleFileOptions::default())
            .unwrap();
        use std::io::Write;
        writer.write_all(sample_script().as_bytes()).unwrap();
        writer.finish().unwrap();

        let text = load_script_text(&path).unwrap();
        assert_eq!(text, sample_script());
    }

    // A test against the real, machine-local sample export under `.local/scala-example-db/`
    // (see that directory's own sidecar `.md`) deliberately does not live here: that export is
    // this developer's own data, not something any other clone or environment could ever have in
    // exactly that form, and every finding it turned up while this was under development (the
    // NULL/-1/real-reference dataId distinction, the U&'...' Unicode-escape literal) is already
    // captured above as a portable, no-fixture-needed unit test - nothing is lost by not also
    // keeping a hardcoded-row-count integration test tied to one specific personal file. To
    // re-check a real export by hand instead, `dfs migrate-scala-repo --script <path> --staging
    // <path>` (this module's own CLI entry point) already reports exactly these counts.
}
