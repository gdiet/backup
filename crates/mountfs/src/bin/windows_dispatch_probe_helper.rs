//! Test-only helper binary: mounts a writable, in-memory `MountFilesystem` at the path given as
//! `argv[1]` and, on every `write()` dispatch, records two things a real WinFSP mount's own
//! dispatch-thread pool determines - not this crate's own code - which
//! `agent-todos/determine-winfsp-dispatch-pool-and-stack-size.md` needed real numbers for:
//!
//! - the peak number of `write()` calls WinFSP is willing to run *concurrently* against this
//!   process (an artificial per-call delay, set via `argv[3]`, gives concurrent client writers time
//!   to actually overlap instead of finishing one at a time before the next starts);
//! - each distinct dispatch thread's actual stack size, via `GetCurrentThreadStackLimits`.
//!
//! Same "separate child process, killed rather than cleanly unmounted" shape as
//! `windows_mount_spike_helper.rs` - see that file's own doc comment for why. Results are written
//! to `argv[2]` from a background thread every 200 ms (not from inside `write()` itself, to keep
//! file I/O off the path being timed) - reading it captures the last snapshot before the test kills
//! this process, no clean-shutdown handshake needed.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem};

#[link(name = "kernel32")]
unsafe extern "system" {
    /// Win32 API (Windows 8+): the low/high address bounds of the *calling* thread's own stack.
    /// `high - low` is that thread's actual stack size - the number this probe needs, not an
    /// assumption about what Windows' default happens to be.
    fn GetCurrentThreadStackLimits(low_limit: *mut usize, high_limit: *mut usize);
}

fn current_thread_stack_size() -> usize {
    let mut low: usize = 0;
    let mut high: usize = 0;
    unsafe { GetCurrentThreadStackLimits(&mut low, &mut high) };
    high - low
}

struct ProbeState {
    /// Currently-executing `write()` calls right now, and the highest that count has ever
    /// reached - the second value is what actually answers "how big is the dispatch pool."
    concurrent_writes: AtomicUsize,
    peak_concurrent_writes: AtomicUsize,
    /// One entry per distinct dispatch thread that has run a `write()` call, keyed so a thread
    /// reused across multiple `write()` calls is only measured (and stored) once.
    stack_sizes_by_thread: Mutex<std::collections::HashMap<std::thread::ThreadId, usize>>,
    delay: Duration,
    created_files: Mutex<BTreeSet<String>>,
    next_handle: AtomicU64,
}

/// Cheap-to-clone handle onto [`ProbeState`] - `mountfs::mount` takes its filesystem by value, so
/// this is what actually gets moved into it, while the reporter thread keeps its own clone of the
/// same underlying `Arc` to read the live counters from outside.
#[derive(Clone)]
struct Probe(Arc<ProbeState>);

impl MountFilesystem for Probe {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        if path == "/" {
            return Ok(Attr {
                kind: FileKind::Directory,
                size: 0,
                mtime_millis: 0,
            });
        }
        if self
            .0
            .created_files
            .lock()
            .expect("not poisoned")
            .contains(path)
        {
            Ok(Attr {
                kind: FileKind::File,
                size: 0,
                mtime_millis: 0,
            })
        } else {
            Err(Errno::ENOENT)
        }
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        if path != "/" {
            return Err(Errno::ENOTDIR);
        }
        Ok(self
            .0
            .created_files
            .lock()
            .expect("not poisoned")
            .iter()
            .map(|p| DirEntry {
                name: p.trim_start_matches('/').to_string(),
                kind: FileKind::File,
            })
            .collect())
    }

    fn open(&self, path: &str, _write_intent: bool) -> Result<Handle, Errno> {
        if self
            .0
            .created_files
            .lock()
            .expect("not poisoned")
            .contains(path)
        {
            Ok(Handle(self.0.next_handle.fetch_add(1, Ordering::Relaxed)))
        } else {
            Err(Errno::ENOENT)
        }
    }

    fn read(&self, _handle: Handle, _offset: u64, _size: u32) -> Result<Vec<u8>, Errno> {
        Ok(Vec::new())
    }

    fn release(&self, _handle: Handle) {}

    fn statfs(&self) -> Result<mountfs::StatfsInfo, Errno> {
        Ok(mountfs::StatfsInfo {
            block_size: 512,
            max_name_length: 255,
            ..Default::default()
        })
    }

    fn create(&self, path: &str) -> Result<Handle, Errno> {
        self.0
            .created_files
            .lock()
            .expect("not poisoned")
            .insert(path.to_string());
        Ok(Handle(self.0.next_handle.fetch_add(1, Ordering::Relaxed)))
    }

    fn truncate(&self, _path: &str, _size: u64) -> Result<(), Errno> {
        Ok(())
    }

    /// The one instrumented call - content itself is discarded (this probe only cares about
    /// dispatch-thread behavior, not correctness of stored bytes).
    fn write(&self, _handle: Handle, _offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let now = self.0.concurrent_writes.fetch_add(1, Ordering::AcqRel) + 1;
        self.0
            .peak_concurrent_writes
            .fetch_max(now, Ordering::AcqRel);

        self.0
            .stack_sizes_by_thread
            .lock()
            .expect("not poisoned")
            .entry(std::thread::current().id())
            .or_insert_with(current_thread_stack_size);

        std::thread::sleep(self.0.delay);

        self.0.concurrent_writes.fetch_sub(1, Ordering::AcqRel);
        Ok(data.len() as u32)
    }
}

fn write_results(state: &ProbeState, results_path: &PathBuf) {
    let peak = state.peak_concurrent_writes.load(Ordering::Acquire);
    let sizes = state.stack_sizes_by_thread.lock().expect("not poisoned");
    let sizes_line = sizes
        .values()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let _ = std::fs::write(
        results_path,
        format!("peak_concurrent_writes={peak}\nstack_sizes_bytes={sizes_line}\n"),
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mountpoint = args.next().expect(
        "usage: windows-dispatch-probe-helper <mountpoint> <results-path> [delay-ms, default 150]",
    );
    let results_path = PathBuf::from(
        args.next()
            .expect("usage: windows-dispatch-probe-helper <mountpoint> <results-path> [delay-ms]"),
    );
    let delay_ms: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(150);

    let state = Arc::new(ProbeState {
        concurrent_writes: AtomicUsize::new(0),
        peak_concurrent_writes: AtomicUsize::new(0),
        stack_sizes_by_thread: Mutex::new(std::collections::HashMap::new()),
        delay: Duration::from_millis(delay_ms),
        created_files: Mutex::new(BTreeSet::new()),
        next_handle: AtomicU64::new(1),
    });

    let reporter_state = Arc::clone(&state);
    let reporter_results_path = results_path.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_millis(200));
            write_results(&reporter_state, &reporter_results_path);
        }
    });

    let fs = Probe(state);
    if let Err(err) = mountfs::mount(fs, &PathBuf::from(mountpoint), true) {
        eprintln!("mount failed: {err}");
        std::process::exit(1);
    }
}
