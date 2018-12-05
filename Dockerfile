FROM golang:1.11-alpine AS builder

RUN apk add --no-cache \
	  git
ADD . /rumour
WORKDIR /rumour
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN set -eux; \
	  go build -o bin/rumour cmd/rumour/main.go

# ---------------------------------------------------------------------

FROM iron/go
COPY --from=builder /rumour/bin/rumour /usr/bin/

EXPOSE 8080/tcp
ENV RUMOUR_CLUSTERS="default" RUMOUR_DEFAULT_BROKERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"
CMD ["/usr/bin/rumour"]