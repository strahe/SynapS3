# Build stage
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=1 go build -trimpath \
    -ldflags "-X github.com/strahe/synaps3/internal/buildinfo.Version=${VERSION} \
              -X github.com/strahe/synaps3/internal/buildinfo.Commit=${COMMIT} \
              -X github.com/strahe/synaps3/internal/buildinfo.Date=${BUILD_DATE}" \
    -o /synaps3 ./cmd/synaps3

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /synaps3 /usr/local/bin/synaps3

RUN mkdir -p /var/lib/synaps3/cache

EXPOSE 8080

ENTRYPOINT ["synaps3"]
CMD ["--config", "/etc/synaps3/config.yaml"]
