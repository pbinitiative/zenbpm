FROM ubuntu:noble-20260410

RUN apt-get update \
    && apt-get install --only-upgrade -y libssl3t64 \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/zenbpm /usr/bin/
ENTRYPOINT ["/usr/bin/zenbpm"]
