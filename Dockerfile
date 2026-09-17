FROM rust:1.91-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release --locked

COPY src ./src
COPY assets ./assets

RUN touch src/main.rs \
    && cargo build --release --locked

FROM debian:bookworm-slim AS runtime

ENV USER=scw
ENV UID=42069

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

# Install ca-certificates for HTTPS support
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/gitlab_scaleway /app/app

RUN chown -R "${USER}:${USER}" /app

# Use the unprivileged user
USER scw:scw
# Set entrypoint to run backend
ENTRYPOINT ["/app/app"]
