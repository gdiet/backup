//! `perf-gen <size> <seed>` writes exactly `<size>` bytes of a fast, seeded pseudo-random stream to
//! stdout - a shared content generator for `performance/scripts/file*-{create,read}.{ps1,sh}`
//! (see `developer-todos/perf-gen-shared-content-generator.md`), so both the PowerShell and shell
//! variants of the file-content workloads write genuinely, uniformly random per-file content
//! without a slow in-shell generator (PowerShell's `System.Random`) dominating the timed loop at
//! larger file sizes, and without comparing two different platform RNG implementations at the
//! largest size the way `/dev/urandom` on Linux vs. `RNGCryptoServiceProvider` on Windows would.
//!
//! Not cryptographically secure - xoshiro256++ (Blackman & Vigna, public domain) trades that for
//! raw throughput, which is all this needs: every chunk-sized window of output must differ between
//! two different `<seed>` values, not resist an attacker. `<seed>` is meant to be a per-file
//! counter, so distinct files never share a byte of content.

use std::env::args;
use std::io::{self, BufWriter, Write};
use std::process::ExitCode;

/// xoshiro256++ (<https://prng.di.unimi.it/>), seeded via SplitMix64 - the algorithm's own
/// recommended way to turn a single 64-bit seed into well-mixed initial state, since seeding the
/// four words directly from the input (e.g. all but one word zero) would produce a visibly weaker
/// stream for the first few calls.
struct Xoshiro256PlusPlus {
    s: [u64; 4],
}

impl Xoshiro256PlusPlus {
    fn new(seed: u64) -> Self {
        let mut splitmix_state = seed;
        let mut splitmix_next = || {
            splitmix_state = splitmix_state.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = splitmix_state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^ (z >> 31)
        };
        Self {
            s: [
                splitmix_next(),
                splitmix_next(),
                splitmix_next(),
                splitmix_next(),
            ],
        }
    }

    fn next_u64(&mut self) -> u64 {
        let result = self.s[0]
            .wrapping_add(self.s[3])
            .rotate_left(23)
            .wrapping_add(self.s[0]);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }
}

fn try_main() -> Result<(), String> {
    let mut positional = args().skip(1);
    let (size_arg, seed_arg) = match (positional.next(), positional.next()) {
        (Some(size), Some(seed)) => (size, seed),
        _ => return Err("usage: perf-gen <size-in-bytes> <seed>".to_string()),
    };
    let size: u64 = size_arg
        .parse()
        .map_err(|_| format!("<size-in-bytes> must be a non-negative integer, got {size_arg:?}"))?;
    let seed: u64 = seed_arg
        .parse()
        .map_err(|_| format!("<seed> must be an integer, got {seed_arg:?}"))?;

    let mut rng = Xoshiro256PlusPlus::new(seed);
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut written = 0u64;
    while written < size {
        let block = rng.next_u64().to_le_bytes();
        let take = block.len().min((size - written) as usize);
        out.write_all(&block[..take])
            .map_err(|err| format!("write error: {err}"))?;
        written += take as u64;
    }
    out.flush().map_err(|err| format!("write error: {err}"))
}

fn main() -> ExitCode {
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("perf-gen: {message}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_is_deterministic() {
        let mut a = Xoshiro256PlusPlus::new(42);
        let mut b = Xoshiro256PlusPlus::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge_immediately() {
        let mut a = Xoshiro256PlusPlus::new(1);
        let mut b = Xoshiro256PlusPlus::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn different_seeds_never_collide_across_many_seeds() {
        // Sanity check for the actual usage pattern (seed = a small, sequential per-file counter):
        // the first output word for 1000 consecutive seeds must be pairwise distinct - a collision
        // here would mean two "different" files could start out byte-identical.
        let firsts: Vec<u64> = (0..1000u64)
            .map(|seed| Xoshiro256PlusPlus::new(seed).next_u64())
            .collect();
        let mut sorted = firsts.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            firsts.len(),
            "collision among first-word outputs"
        );
    }
}
