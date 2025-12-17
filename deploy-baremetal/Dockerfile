# Dockerfile for bare metal deployment
FROM ubuntu:noble

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    ffmpeg \
    libavutil-dev \
    libavformat-dev \
    libavfilter-dev \
    libavdevice-dev \
    libavcodec-dev \
    libswscale-dev \
    libswresample-dev \
    libblas-dev \
    liblapack-dev \
    unzip \
    wget

# Install Storj uplink client
RUN curl -L https://github.com/storj/storj/releases/latest/download/uplink_linux_amd64.zip -o uplink_linux_amd64.zip \
    && unzip -o uplink_linux_amd64.zip \
    && install uplink /usr/local/bin/uplink \
    && rm -f uplink_linux_amd64.zip uplink

# Copy the binary
COPY ./target/x86_64-unknown-linux-gnu/release/icp-off-chain-agent .

# Copy entrypoint script
COPY ./deploy-baremetal/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose gRPC port
EXPOSE 50051

ENTRYPOINT ["/entrypoint.sh"]
