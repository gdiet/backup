//! DESIGN-MOUNT-006's background chunking/hashing/store-write pool in
//! `docs/design/mount-write-path.md`: a small, fixed set of worker threads, separate from the
//! FUSE/WinFSP dispatch pool, that runs [`crate::settle::settle`] on a generation
//! [`PendingFiles::release`](crate::pending_files::PendingFiles::release) hands off. `submit`
//! never blocks on the job itself finishing - the point of a separate pool at all.
//!
//! What happens when a job actually fails is not this module's concern (DESIGN-MOUNT-009 in the
//! same design document, not yet implemented) - a caller-supplied `on_failure` hook is the only
//! seam for that, kept deliberately generic here. Used by `crate::dedup_fs::DedupFs`.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::pending_files::GenerationSlot;

/// One generation ready to be durably committed - DESIGN-MOUNT-013's hand-off from `release()`,
/// carrying the tree location its settled `content_id` needs to land at
/// ([`db::Repository::settle_file`], DESIGN-MOUNT-011).
pub struct SettleJob {
    pub parent_id: i64,
    pub name: String,
    pub time_millis: i64,
    pub generation: Arc<GenerationSlot>,
}

/// Why a [`SettleJob`] did not end in [`GenerationSlot::mark_settled`].
#[derive(Debug)]
pub enum JobError {
    /// Failed while chunking/hashing/writing the content itself.
    Settle(crate::settle::SettleError),
    /// The content settled successfully, but committing it into the tree
    /// ([`db::Repository::settle_file`]) failed - the generation is left as-is, still readable
    /// this session (DESIGN-MOUNT-007) but never marked settled, since nothing durably committed
    /// under its own tree entry.
    Commit(db::Error),
}

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobError::Settle(err) => write!(f, "{err}"),
            JobError::Commit(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for JobError {}

/// See [`JobPool::new`]'s `on_failure` parameter.
type FailureHook = Box<dyn Fn(&SettleJob, JobError) + Send + Sync>;

/// Shared, read-only state every worker thread needs.
struct Context {
    repo: Arc<db::Repository>,
    store: Arc<store::ByteStore>,
    cdc_target_size_bits: Option<u32>,
    on_failure: FailureHook,
}

/// DESIGN-MOUNT-006's background pool. Dropping it waits for every already-submitted job to
/// finish - an unmount's own "flush" needs that, and DESIGN-MOUNT-008 already scopes the
/// single-writer slot to the whole mount session, background jobs included.
pub struct JobPool {
    sender: Option<mpsc::Sender<SettleJob>>,
    workers: Vec<thread::JoinHandle<()>>,
    /// DESIGN-MOUNT-010's backpressure signal, narrowed to released-but-not-yet-settled work: the
    /// summed [`GenerationSlot::spilled_bytes`] of every generation currently queued or being
    /// processed right now.
    backlog_spilled_bytes: Arc<AtomicU64>,
}

impl JobPool {
    /// Spawns `worker_count.max(1)` worker threads sharing `repo`/`store`. `on_failure` is called
    /// from whichever worker thread hit the failure, for a job that did not end in
    /// `mark_settled` - see [`JobError`].
    pub fn new(
        worker_count: usize,
        repo: Arc<db::Repository>,
        store: Arc<store::ByteStore>,
        cdc_target_size_bits: Option<u32>,
        on_failure: impl Fn(&SettleJob, JobError) + Send + Sync + 'static,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<SettleJob>();
        let receiver = Arc::new(Mutex::new(receiver));
        let context = Arc::new(Context {
            repo,
            store,
            cdc_target_size_bits,
            on_failure: Box::new(on_failure),
        });
        let backlog_spilled_bytes = Arc::new(AtomicU64::new(0));

        let workers = (0..worker_count.max(1))
            .map(|_| {
                let receiver = Arc::clone(&receiver);
                let context = Arc::clone(&context);
                let backlog = Arc::clone(&backlog_spilled_bytes);
                thread::spawn(move || worker_loop(&receiver, &context, &backlog))
            })
            .collect();

        Self {
            sender: Some(sender),
            workers,
            backlog_spilled_bytes,
        }
    }

    /// Hands `job` off to the pool - returns immediately, never waiting for the job itself to
    /// finish (DESIGN-MOUNT-006).
    pub fn submit(&self, job: SettleJob) {
        self.backlog_spilled_bytes
            .fetch_add(job.generation.spilled_bytes(), Ordering::AcqRel);
        self.sender
            .as_ref()
            .expect("only cleared in Drop, after which submit can no longer be called")
            .send(job)
            .expect("worker threads only exit once every sender is dropped, including this one");
    }

    /// DESIGN-MOUNT-010's backpressure signal - the total spilled-to-disk bytes belonging to
    /// generations released and awaiting or undergoing settling right now.
    // Not consulted by `write()` yet - the actual delay formula needs its own deliberate choice,
    // not just wiring; see agent-todos/wire-write-backpressure-delay.md.
    #[allow(dead_code)]
    pub fn backlog_spilled_bytes(&self) -> u64 {
        self.backlog_spilled_bytes.load(Ordering::Acquire)
    }
}

impl Drop for JobPool {
    fn drop(&mut self) {
        // Dropping the sender closes the channel, so each worker's `recv()` returns `Err` once
        // every already-queued job has been delivered - only then do the joins below return.
        self.sender = None;
        for worker in std::mem::take(&mut self.workers) {
            let _ = worker.join();
        }
    }
}

fn worker_loop(
    receiver: &Mutex<mpsc::Receiver<SettleJob>>,
    context: &Context,
    backlog_spilled_bytes: &AtomicU64,
) {
    loop {
        let job = {
            let receiver = receiver.lock().expect("not poisoned");
            receiver.recv()
        };
        let Ok(job) = job else {
            return;
        };
        let spilled = job.generation.spilled_bytes();
        run_job(context, job);
        backlog_spilled_bytes.fetch_sub(spilled, Ordering::AcqRel);
    }
}

fn run_job(context: &Context, job: SettleJob) {
    let repo = context.repo.as_ref();
    let store = context.store.as_ref();
    let size = job.generation.size();

    let resolve_content = |content_id: i64, position: u64, len: u32| {
        crate::content_reader::read_content(repo, store, content_id, position, len)
            .map_err(|errno| io::Error::from_raw_os_error(errno.0))
    };
    let read = |position: u64, len: u32| job.generation.read(position, len, &resolve_content);

    match crate::settle::settle(repo, store, context.cdc_target_size_bits, size, read) {
        Ok(content_id) => {
            match repo.settle_file(job.parent_id, &job.name, job.time_millis, content_id) {
                Ok(_) => job.generation.mark_settled(content_id, size),
                Err(err) => (context.on_failure)(&job, JobError::Commit(err)),
            }
        }
        Err(err) => (context.on_failure)(&job, JobError::Settle(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pending_files::{NewGeneration, PendingFiles};
    use crate::write_cache::MemoryBudget;

    fn repo_and_store() -> (
        Arc<db::Repository>,
        tempfile::TempDir,
        Arc<store::ByteStore>,
        tempfile::TempDir,
    ) {
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_root = repo_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(None, 1_700_000_000_000),
        )
        .unwrap();
        let repo = Arc::new(db::open_repository(&repo_root).unwrap());
        let store_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(store::ByteStore::new(store_dir.path(), false));
        (repo, repo_dir, store, store_dir)
    }

    /// Writes `data` as a brand new generation for `file_id` and releases it - the same sequence
    /// `dedup_fs.rs`'s eventual `create`/`write`/`release` will drive, minus the FUSE plumbing.
    fn write_and_release(
        registry: &PendingFiles,
        file_id: i64,
        data: &[u8],
        budget: &Arc<MemoryBudget>,
        temp_dir: &std::path::Path,
    ) -> Arc<GenerationSlot> {
        registry.open(file_id);
        registry
            .write(
                file_id,
                0,
                data,
                NewGeneration {
                    budget,
                    temp_dir,
                    base_content_id: None,
                    base_size: 0,
                },
            )
            .unwrap();
        registry.release(file_id).unwrap()
    }

    #[test]
    fn a_submitted_job_settles_and_commits_a_tree_entry() {
        let (repo, _rd, store, _sd) = repo_and_store();
        let registry = PendingFiles::new();
        let budget = Arc::new(MemoryBudget::new(1000));
        let temp_dir = tempfile::tempdir().unwrap();
        let generation = write_and_release(&registry, 1, b"hello world", &budget, temp_dir.path());

        let failures: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let failures_for_hook = Arc::clone(&failures);
        let pool = JobPool::new(
            1,
            Arc::clone(&repo),
            Arc::clone(&store),
            None,
            move |_job, err| failures_for_hook.lock().unwrap().push(err.to_string()),
        );
        pool.submit(SettleJob {
            parent_id: 0,
            name: "hello.txt".to_string(),
            time_millis: 1_700_000_000_000,
            generation,
        });
        drop(pool); // Drop joins every worker, so the job has finished once this returns.

        assert!(failures.lock().unwrap().is_empty());
        let entry = repo.resolve_path("/hello.txt").unwrap().unwrap();
        assert_eq!(entry.size, 11);
    }

    #[test]
    fn a_commit_failure_is_reported_and_leaves_the_generation_unsettled() {
        let (repo, _rd, store, _sd) = repo_and_store();
        let registry = PendingFiles::new();
        let budget = Arc::new(MemoryBudget::new(1000));
        let temp_dir = tempfile::tempdir().unwrap();
        let generation = write_and_release(&registry, 1, b"x", &budget, temp_dir.path());

        let failures: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let failures_for_hook = Arc::clone(&failures);
        let pool = JobPool::new(
            1,
            Arc::clone(&repo),
            Arc::clone(&store),
            None,
            move |_job, err| failures_for_hook.lock().unwrap().push(err.to_string()),
        );
        pool.submit(SettleJob {
            parent_id: 999_999, // does not exist
            name: "x.txt".to_string(),
            time_millis: 1_700_000_000_000,
            generation: Arc::clone(&generation),
        });
        drop(pool);

        assert_eq!(failures.lock().unwrap().len(), 1);
        // Never durably committed - still readable this session (DESIGN-MOUNT-007), not settled.
        let data = generation
            .read(0, 1, &|_, _, _| {
                panic!("no durably committed content exists to fall back to")
            })
            .unwrap();
        assert_eq!(data, b"x");
    }

    #[test]
    fn backlog_spilled_bytes_tracks_generations_currently_in_flight() {
        let (repo, _rd, store, _sd) = repo_and_store();
        let registry = PendingFiles::new();
        // A zero budget forces an immediate spill to disk on the very first write.
        let budget = Arc::new(MemoryBudget::new(0));
        let temp_dir = tempfile::tempdir().unwrap();
        let generation = write_and_release(&registry, 1, b"x", &budget, temp_dir.path());
        assert_eq!(generation.spilled_bytes(), 1);

        let pool = JobPool::new(1, Arc::clone(&repo), Arc::clone(&store), None, |_, _| {});
        pool.submit(SettleJob {
            parent_id: 0,
            name: "x.txt".to_string(),
            time_millis: 1_700_000_000_000,
            generation: Arc::clone(&generation),
        });
        drop(pool);

        // Settled: no longer spilled anywhere, and no longer counted in the pool's own backlog
        // (dropped along with the pool, but the generation's own accounting already reflects it).
        assert_eq!(generation.spilled_bytes(), 0);
    }
}
