# Cloud-agnostic image: a static binary on distroless. Runs on any container
# platform (ECS, Azure Container Apps, Cloud Run, Kubernetes, Docker).
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/sfzb ./cmd/zerobus

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sfzb /sfzb
# Admin port: /healthz (liveness), /readyz (readiness), /metrics, /debug/*
EXPOSE 9090
ENV SFZB_ADMIN_ADDR=:9090 SFZB_LOG_FORMAT=json
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s CMD ["/sfzb", "healthcheck"]
USER nonroot:nonroot
ENTRYPOINT ["/sfzb"]
CMD ["run"]
