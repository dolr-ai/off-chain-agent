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
    unzip

RUN curl -L https://github.com/storj/storj/releases/latest/download/uplink_linux_amd64.zip -o uplink_linux_amd64.zip
RUN unzip -o uplink_linux_amd64.zip
RUN install uplink /usr/local/bin/uplink

EXPOSE 50051

COPY ./target/x86_64-unknown-linux-gnu/release/icp-off-chain-agent .
CMD ["./icp-off-chain-agent"]
