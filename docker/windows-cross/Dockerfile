# Cross-compiles the workspace for x86_64-pc-windows-msvc from Linux, via
# cargo-xwin (downloads the needed MSVC CRT/SDK pieces on first build - no
# Visual Studio install needed).
#
# See ../../docs/design/mount-abstraction.md for why MSVC/xwin instead of
# mingw-w64: mountfs hand-binds WinFSP's C API (crates/mountfs/src/windows/sys.rs),
# and only an MSVC build has ever been verified against real WinFSP - this
# keeps the same ABI instead of introducing an untested GNU/mingw
# combination alongside it.
#
# Build/run via scripts/build-windows-docker.sh (from the repo root), which
# sets up the cache volumes this image expects.

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

CMD ["cargo", "xwin", "build", "--release", "--target", "x86_64-pc-windows-msvc", "-p", "mountfs"]
