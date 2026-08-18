# Test that mountfs handles a missing libfuse3/WinFSP gracefully, not just the found case

**Needs**: either a genuinely isolated environment/container without libfuse3 installed (Linux
side), or a decision to refactor `exports()` for dependency injection instead - see the two
options below.
**Size**: medium (confirm approach with the user before starting)
**Opened**: 2026-08-18, by Desktop App session (surfaced while reviewing `mountfs` for adoption
into a sibling project).

`linux::sys`/`windows::sys`'s `exports()` documents that the platform mount library is a
runtime-only dependency, resolved via `dlopen`/`LoadLibraryW`, and reports absence as a clean
`io::Result::Err` rather than panicking. No test actually exercises that path - every existing
mount test requires the real library to be present and working.

Two ways to close this:

1. Run the check in a child process where the library is genuinely unresolvable by the dynamic
   linker - on Linux this needs more than clearing `LD_LIBRARY_PATH`, since `libfuse3.so.3` loads
   via the standard system search path/`ld.so.cache`; a container or mount-namespace trick that
   actually hides the `.so` from that process would be needed. Confirms the real, unmodified
   production code path end to end, at the cost of a more involved test setup.
2. Refactor `exports()` (or the `dlopen`/`LoadLibraryW` call it wraps) behind a small seam a test
   can inject a resolution failure into, without needing the library to be genuinely absent from
   the system running the test. Simpler and more reliable to run anywhere (including CI), at the
   cost of a production-code change purely for testability.

Not urgent (the documented behavior is a reasoned expectation from reading the code, not a known
bug) - but worth closing before relying on "mounting fails gracefully when the library is missing"
as a tested guarantee rather than an assumption.
