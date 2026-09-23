# Mounts a DedupFS repository via FUSE inside the container and re-exports
# it as a Samba (SMB) share, so a Windows client can browse it over the
# network without installing WinFSP at all - an alternative to the native
# Windows/WinFSP mount and the Linux/WSL2 mount this project also supports
# natively. Experimental, kept as a template rather than a maintained
# product - see README.md in this directory for the tradeoffs.
#
# Build from the repo root:
#   docker build -t dedup-samba-mount -f docker/samba-mount/Dockerfile .
# Run (needs FUSE device access; /repo is not bind-mounted read-only even for a read-only
# session - see README.md's "Build and run" for why):
#   docker run --rm --init --cap-add SYS_ADMIN --device /dev/fuse \
#       -v /path/to/repository:/repo -p 445:445 dedup-samba-mount

FROM rust:1.97-slim-bookworm AS builder
WORKDIR /workspace
COPY . .
# No libfuse3-dev/pkg-config needed here - libfuse3 is a dlopen'd
# runtime-only dependency, not linked at build time.
RUN cargo build --release -p cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libfuse3-3 \
        fuse3 \
        samba \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/target/release/dfs /usr/local/bin/dfs
COPY docker/samba-mount/smb.conf /etc/samba/smb.conf
COPY docker/samba-mount/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 445
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
