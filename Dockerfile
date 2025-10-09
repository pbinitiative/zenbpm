FROM golang:1.25 AS builder
RUN apt-get update && apt-get install -y gcc make curl unzip
WORKDIR /app
COPY . .

RUN make generate
RUN CGO_ENABLED=1 go build -o /zenbpm cmd/zenbpm/main.go


FROM ubuntu:latest
COPY --from=builder /zenbpm /usr/bin/zenbpm


EXPOSE 8080 9090
ENTRYPOINT ["/usr/bin/zenbpm"]
