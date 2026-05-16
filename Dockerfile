# Build stage - frontend
FROM node:26-alpine AS frontend

ENV PNPM_HOME=/pnpm
ENV PATH="$PNPM_HOME:$PATH"

WORKDIR /ui

RUN npm install -g pnpm@11

COPY ui/pnpm-lock.yaml ui/pnpm-workspace.yaml ./
RUN pnpm fetch --frozen-lockfile --config.store-dir=/pnpm/store --config.confirmModulesPurge=false

COPY ui/package.json ./
RUN pnpm install --frozen-lockfile --offline --config.store-dir=/pnpm/store --config.confirmModulesPurge=false

COPY ui/ .
RUN pnpm run build

# Build stage - Go binary
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY ui/embed.go ui/embed_dev.go ./ui/
COPY --from=frontend /ui/dist /src/ui/dist

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=1 go build -trimpath \
    -ldflags "-X github.com/strahe/synaps3/internal/buildinfo.Version=${VERSION} \
              -X github.com/strahe/synaps3/internal/buildinfo.Commit=${COMMIT} \
              -X github.com/strahe/synaps3/internal/buildinfo.Date=${BUILD_DATE}" \
    -o /synaps3 ./cmd/synaps3

# Runtime stage
FROM alpine:3

RUN apk add --no-cache ca-certificates tzdata wget

COPY --from=builder /synaps3 /usr/local/bin/synaps3
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && mkdir -p /var/lib/synaps3/cache

VOLUME ["/var/lib/synaps3"]

EXPOSE 8080 9090

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -q --spider http://localhost:9090/healthz || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
