ARG GO_IMAGE=docker.io/library/golang:1.25.12-bookworm@sha256:c62765daa3fb92521a46cc8242797b81b03cf592b5aeffc36bae63d9abc1385c
ARG RUNTIME_IMAGE=docker.io/library/alpine:3.23.5@sha256:1beb0dc0a51de7ff38e3b5274078a2e0b81113ba5c7535e1a03d5913a5edbda3

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS builder-base
ARG TARGETOS=linux
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,id=wukongim-go-mod,target=/go/pkg/mod,sharing=locked \
    go mod download

COPY . .

FROM builder-base AS server-builder
RUN --mount=type=cache,id=wukongim-go-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=wukongim-go-build,target=/root/.cache/go-build,sharing=locked \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/wukongim ./cmd/wukongim

FROM builder-base AS tools-builder
RUN --mount=type=cache,id=wukongim-go-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=wukongim-go-build,target=/root/.cache/go-build,sharing=locked \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/wkbench ./cmd/wkbench \
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
