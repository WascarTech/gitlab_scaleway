
FROM rust:1.91-bullseye AS builder

WORKDIR /app

ARG service
ARG features

COPY . .

RUN cargo build --release

FROM debian:bullseye-slim AS runtime

ARG service


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
RUN apt-get update
RUN apt-get install -y ca-certificates
RUN rm -rf /var/lib/apt/lists/*

WORKDIR /app


COPY --from=builder /app/target/release/gitlab_scaleway /app/app

RUN chown -R "${USER}:${USER}" /app

# Use the unprivileged user
USER scw:scw
# Set entrypoint to run backend
ENTRYPOINT ["/app/app"]
