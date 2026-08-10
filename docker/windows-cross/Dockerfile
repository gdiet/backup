# Cross-compiles `backup.exe` (the `cli` crate's Windows binary) for
# x86_64-pc-windows-msvc from Linux, via cargo-xwin (downloads the needed
# MSVC CRT/SDK pieces on first build - no Visual Studio install needed).
#
# See ../../docs/plans/implemented/windows-docker-cross-build.md for why MSVC/xwin
# instead of mingw-w64: this project hand-binds WinFSP's C API
# (mountfs/src/windows/sys.rs), and the only Windows build verified so far
# was built with MSVC - this keeps the same ABI instead of testing an
# untested GNU/mingw combination.
#
# Build/run via scripts/build-windows-docker.sh (from the rust/ repo root),
# which sets up the cache volumes this image expects.

FROM rust:1.97-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        lld \
        llvm \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-pc-windows-msvc \
    && cargo install cargo-xwin

WORKDIR /workspace

CMD ["cargo", "xwin", "build", "--release", "--target", "x86_64-pc-windows-msvc", "-p", "cli"]
