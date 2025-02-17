# Multi-stage Dockerfile:
# The `builder` stage compiles the binary and gathers all dependencies in the `/export/` directory.
FROM debian:12 AS builder
RUN apt-get update && apt-get -y upgrade \
 && apt-get -y install wget curl build-essential gcc make libssl-dev pkg-config git

# Install the latest Rust build environment.
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install the `depres` utility for dependency resolution.
RUN cd /usr/local/src/ \
 && git clone https://github.com/rrauch/depres.git \
 && cd depres \
 && git checkout 717d0098751024c1282d42c2ee6973e6b53002dc \
 && cargo build --release \
 && cp target/release/depres /usr/local/bin/

COPY Cargo.* /usr/local/src/sia_vbd/
COPY build.rs /usr/local/src/sia_vbd/
COPY migrations /usr/local/src/sia_vbd/migrations/
COPY protos /usr/local/src/sia_vbd/protos/
COPY src /usr/local/src/sia_vbd/src/

# Build the `sia_vbd` binary.
RUN cd /usr/local/src/sia_vbd/ \
 && cargo build --release \
 && cp ./target/release/sia_vbd /usr/local/bin/

# Use `depres` to identify all required files for the final image.
RUN depres /bin/sh /bin/bash /bin/ls /usr/local/bin/sia_vbd \
    /etc/ssl/certs/ \
    /usr/share/ca-certificates/ \
    >> /tmp/export.list

# Copy all required files into the `/export/` directory.
RUN cat /tmp/export.list \
 # remove all duplicates
 && cat /tmp/export.list | sort -o /tmp/export.list -u - \
 && mkdir -p /export/ \
 && rm -rf /export/* \
 # copying all necessary files
 && cat /tmp/export.list | xargs cp -a --parents -t /export/ \
 && mkdir -p /export/tmp && chmod 0777 /export/tmp


# The final stage creates a minimal image with all necessary files.
FROM scratch
WORKDIR /

# Copy files from the `builder` stage.
COPY --from=builder /export/ /

VOLUME /data
ENV CONFIG="/data/sia_vbd.toml"

ENTRYPOINT ["/usr/local/bin/sia_vbd"]
