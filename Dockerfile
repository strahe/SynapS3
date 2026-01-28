# Build stage
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -trimpath -o /synaps3 ./cmd/synaps3

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /synaps3 /usr/local/bin/synaps3

RUN mkdir -p /var/lib/synaps3/cache

EXPOSE 8080

ENTRYPOINT ["synaps3"]
CMD ["--config", "/etc/synaps3/config.yaml"]
