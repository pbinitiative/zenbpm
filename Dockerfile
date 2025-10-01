FROM scratch
ARG TARGETPLATFORM
ENTRYPOINT ["/usr/bin/zenbpm"]
COPY $TARGETPLATFORM/zenbpm /usr/bin/
