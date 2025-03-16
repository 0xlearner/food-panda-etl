# Builder stage
FROM rust:1.85-bookworm as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    perl \
    pkg-config \
    libclang-dev \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

# Create a new empty shell project
WORKDIR /usr/src/foodpanda_etl
COPY Cargo.toml Cargo.lock ./

# Build only the dependencies to cache them
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy the actual source code
COPY src ./src
COPY config ./config

# Build for release
RUN touch src/main.rs && \
    cargo build --release

# Final stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary and config
COPY --from=builder /usr/src/foodpanda_etl/target/release/foodpanda_etl /app/
COPY config /app/config/

# Create logs directory
RUN mkdir -p logs && \
    chmod 777 logs

RUN mkdir -p data && \
    chmod 777 data

# Set environment variables
ENV USER_LOGIN=docker_user
ENV OUTPUT_DIR=/app/data

# Run the binary
CMD ["/app/foodpanda_etl"]