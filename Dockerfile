FROM ubuntu:latest
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/zenbpm /usr/bin/
ENTRYPOINT ["/usr/bin/zenbpm"]
