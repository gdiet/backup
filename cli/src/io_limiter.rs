//! Bounds how many `store::LongTermStore` I/O calls are in flight at once,
//! independent of how many worker threads are running the surrounding
//! pipeline (see `docs/plans/implemented/bounded-memory-io-pipeline.md`,
//! "`store`'s I/O-vs-CPU concurrency split": `store`'s `--concurrency`
//! sizes one pool for CPU-bound chunking/hashing *and* the I/O calls it
//! makes into the store, even though the hardware-optimal degree of
//! parallelism for each can be very different - e.g. a spinning disk or a
//! network share behind the repository wants far fewer concurrent I/O
//! calls than there are CPU cores doing chunking).
//!
//! Deliberately a plain counting semaphore, not a second thread pool: every
//! call site already blocks the calling worker thread until its own I/O
//! call completes, so routing through a second pool would only add a
//! cross-thread hand-off without letting the caller do anything else in the
//! meantime. This gates access in place, on whichever thread already wants
//! to do the I/O - no extra OS threads, no copying the read/write buffer
//! across a channel.
//!
//! Lives in `cli`, not in the `store` crate: how many concurrent I/O calls
//! are hardware-appropriate is a property of the workload/deployment (which
//! command is running, what's behind the repository), not of
//! `LongTermStore` itself - it stays the simple, stateless-per-call,
//! call-from-any-thread primitive it already is, unaware of how many
//! callers there are or how they're scheduled.

use std::sync::{Condvar, Mutex};

/// A counting semaphore with `permits` slots. [`acquire`][Self::acquire]
/// blocks the calling thread until a slot is free; the returned
/// [`IoPermit`] releases it back on drop.
pub struct IoLimiter {
    available: Mutex<u32>,
    freed: Condvar,
}

impl IoLimiter {
    /// # Panics
    /// If `permits` is 0 - a limiter with no slots would deadlock every
    /// caller forever, which is never useful (an actually-unbounded caller
    /// should just not use a limiter at all).
    pub fn new(permits: u32) -> Self {
        assert!(permits > 0, "IoLimiter needs at least one permit");
        Self {
            available: Mutex::new(permits),
            freed: Condvar::new(),
        }
    }

    /// Blocks until a permit is available, then takes it. The permit is
    /// returned to the pool when the returned guard is dropped.
    pub fn acquire(&self) -> IoPermit<'_> {
        let mut available = self.available.lock().expect("IoLimiter mutex poisoned");
        while *available == 0 {
            available = self.freed.wait(available).expect("IoLimiter mutex poisoned");
        }
        *available -= 1;
        IoPermit { limiter: self }
    }
}

/// RAII guard for one permit taken from an [`IoLimiter`] - releases it back
/// on drop, including on an early return via `?`.
pub struct IoPermit<'a> {
    limiter: &'a IoLimiter,
}

impl Drop for IoPermit<'_> {
    fn drop(&mut self) {
        let mut available = self
            .limiter
            .available
            .lock()
            .expect("IoLimiter mutex poisoned");
        *available += 1;
        self.limiter.freed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn a_single_permit_serializes_two_threads() {
        // With only one permit, a second `acquire` must not proceed until
        // the first permit is dropped - checked by having thread A hold its
        // permit across a short sleep and recording whether B observed A's
        // "still holding" flag as still set at the moment B got in.
        let limiter = Arc::new(IoLimiter::new(1));
        let a_holding = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(2));

        let (l1, h1, s1) = (Arc::clone(&limiter), Arc::clone(&a_holding), Arc::clone(&start));
        let a = thread::spawn(move || {
            s1.wait();
            let _permit = l1.acquire();
            h1.store(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(50));
            h1.store(0, Ordering::SeqCst);
        });

        let (l2, h2, s2) = (Arc::clone(&limiter), Arc::clone(&a_holding), Arc::clone(&start));
        let observed_while_a_held = Arc::new(AtomicUsize::new(2)); // sentinel: never observed
        let observed = Arc::clone(&observed_while_a_held);
        let b = thread::spawn(move || {
            s2.wait();
            thread::sleep(Duration::from_millis(10)); // let A grab the permit first
            let _permit = l2.acquire(); // must block until A's permit is dropped
            observed.store(h2.load(Ordering::SeqCst), Ordering::SeqCst);
        });

        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(
            observed_while_a_held.load(Ordering::SeqCst),
            0,
            "B must only acquire its permit after A released its own"
        );
    }

    #[test]
    fn permits_up_to_the_limit_run_concurrently() {
        // With N permits, N threads should be able to hold one each at the
        // same time - checked via a barrier that only releases once all N
        // have actually acquired.
        const N: usize = 4;
        let limiter = Arc::new(IoLimiter::new(N as u32));
        let barrier = Arc::new(Barrier::new(N));

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let limiter = Arc::clone(&limiter);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let _permit = limiter.acquire();
                    barrier.wait(); // deadlocks (and the test times out) if fewer than N ran concurrently
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn a_permit_is_released_on_drop_and_reusable() {
        let limiter = IoLimiter::new(1);
        {
            let _permit = limiter.acquire();
        }
        let _permit = limiter.acquire(); // would block forever if the first permit wasn't released
    }
}
