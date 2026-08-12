ARG GO_IMAGE=docker.io/library/golang:1.25.11-bookworm@sha256:b96f24a8d7d010ea0acb9c3ba99064740f02b6b984612b28bd3c9c5ab9453e38
ARG RUNTIME_IMAGE=docker.io/library/alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS builder-base
ARG TARGETOS=linux
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

FROM builder-base AS server-builder
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/wukongim ./cmd/wukongim

FROM builder-base AS tools-builder
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/wkbench ./cmd/wkbench \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/wkanalysis ./cmd/wkanalysis \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/wkcloudsim ./cmd/wkcloudsim

FROM ${RUNTIME_IMAGE} AS runtime-base
ARG OCI_CREATED
ARG OCI_REVISION
ARG OCI_SOURCE
ARG OCI_VERSION
WORKDIR /app
COPY --from=server-builder /out/wukongim /usr/local/bin/wukongim

LABEL org.opencontainers.image.title="wukongim" \
      org.opencontainers.image.description="WuKongIM messaging server" \
      org.opencontainers.image.source="${OCI_SOURCE}" \
      org.opencontainers.image.revision="${OCI_REVISION}" \
      org.opencontainers.image.created="${OCI_CREATED}" \
      org.opencontainers.image.version="${OCI_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"

EXPOSE 5001 5100 5200 5301 7000
ENTRYPOINT ["/usr/local/bin/wukongim", "-config", "/etc/wukongim/wukongim.toml"]

FROM runtime-base AS dev-tools
COPY --from=tools-builder /out/wkbench /usr/local/bin/wkbench
COPY --from=tools-builder /out/wkanalysis /usr/local/bin/wkanalysis
COPY --from=tools-builder /out/wkcloudsim /usr/local/bin/wkcloudsim
EXPOSE 19092

FROM runtime-base AS runtime
