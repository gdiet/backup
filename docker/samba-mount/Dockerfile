# Mounts a dedup-backup repository via FUSE inside the container and
# re-exports it as a Samba (SMB) share, so Windows clients can browse it
# over the network without installing WinFSP at all - an alternative to
# the native Windows/WinFSP mount this project also supports. Experimental
# (branch `docker-samba-mount`, not yet merged) - see
# ../../docs/plans/docker-samba-mount.md for the tradeoffs.
#
# Build from the rust/ repo root:
#   docker build -t dedup-samba-mount -f docker/samba-mount/Dockerfile .
# Run (needs FUSE device access):
#   docker run --rm --cap-add SYS_ADMIN --device /dev/fuse \
#       -v /path/to/repository:/repo:ro -p 445:445 dedup-samba-mount

FROM rust:1.97-slim-bookworm AS builder
WORKDIR /workspace
COPY . .
# No libfuse3-dev/pkg-config needed here - libfuse3 is a dlopen'd
# runtime-only dependency, not linked at build time (see
# ../../docs/plans/implemented/linux-libfuse3-lazy-loading.md).
RUN cargo build --release -p cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libfuse3-3 \
        fuse3 \
        samba \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/target/release/backup /usr/local/bin/backup
COPY docker/samba-mount/smb.conf /etc/samba/smb.conf
COPY docker/samba-mount/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 445
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
