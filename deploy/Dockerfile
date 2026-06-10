# live-mutex-mills `lmxd` node image.
#
# Build context MUST be the repo root (needs Cargo.toml, src/, vendor/, build.rs,
# .cli-flags.toml). Build with:
#   docker build -f deploy/Dockerfile -t live-mutex-mills:dev .
#
# The vendored flags2env parser is compiled from C via build.rs, so the builder
# stage needs a C toolchain (the rust image ships gcc).

# ---- builder ---------------------------------------------------------------
FROM rust:1-bookworm AS builder
WORKDIR /build

# Cache deps: copy manifests first, fetch, then copy sources.
COPY Cargo.toml Cargo.lock ./
COPY build.rs .cli-flags.toml ./
COPY vendor ./vendor
# A stub main lets `cargo fetch` resolve the dep graph without the real sources.
RUN mkdir -p src/bin && echo 'fn main() {}' > src/bin/lmxd.rs && \
    echo '' > src/lib.rs && \
    cargo fetch
COPY src ./src
COPY examples ./examples
RUN cargo build --release --bin lmxd && \
    strip target/release/lmxd

# ---- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
# getent (glibc) is used by the entrypoint to wait for the pod's own DNS A
# record before lmxd binds its peer FQDN. ca-certificates is harmless future-proofing.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/lmxd /usr/local/bin/lmxd
COPY deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 9000 = peer mesh; 6971 = client HTTP API.
EXPOSE 9000 6971
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
