# syntax=docker/dockerfile:1
#
# Forge sandbox image for the CI speedup benchmark. Adds Go 1.23.4 to
# actions/setup-go's own tool cache so the unmodified `setup-go` step
# in forge-bench.yml finds a cache hit instead of downloading the
# toolchain into a fresh container on every job. This isolates the
# benchmark to compute time, matching a hosted runner's preinstalled
# toolchain. It does not change the workflow file.
FROM ghcr.io/actions/actions-runner:latest

ARG GO_VERSION=1.23.4
ARG TARGETARCH

USER root

# Go's release tarballs use "amd64"/"arm64"; setup-go's tool-cache
# keys arches as "x64"/"arm64". Populate the cache at the exact path
# and completion-marker format @actions/tool-cache writes when it
# downloads a version itself: <cache>/go/<version>/<arch>/ plus a
# sibling <arch>.complete marker.
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) GO_ARCH=amd64; TC_ARCH=x64 ;; \
      arm64) GO_ARCH=arm64; TC_ARCH=arm64 ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" -o /tmp/go.tar.gz; \
    mkdir -p "/opt/hostedtoolcache/go/${GO_VERSION}/${TC_ARCH}"; \
    tar -C "/opt/hostedtoolcache/go/${GO_VERSION}/${TC_ARCH}" --strip-components=1 -xzf /tmp/go.tar.gz; \
    rm /tmp/go.tar.gz; \
    touch "/opt/hostedtoolcache/go/${GO_VERSION}/${TC_ARCH}.complete"; \
    chmod -R a+rX /opt/hostedtoolcache

ENV RUNNER_TOOL_CACHE=/opt/hostedtoolcache

USER runner
