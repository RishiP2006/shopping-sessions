# Shopping Sessions: native C++ pipeline, static binary in an empty image.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
ARG TARGETARCH
COPY src/ /src/
RUN if [ "$TARGETARCH" = "arm64" ]; then ARCHFLAGS="-mcpu=native"; else ARCHFLAGS="-march=haswell -mtune=skylake-avx512"; fi && \
    g++ -O3 $ARCHFLAGS -std=c++17 -static -pthread -fno-plt -o /pipeline /src/pipeline.cpp && \
    strip /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
USER 1000:1000
CMD ["/pipeline"]
