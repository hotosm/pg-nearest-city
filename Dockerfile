# Copyright (c) Humanitarian OpenStreetMap Team
# This file is part of pg-nearest-city.
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with pg-nearest-city.  If not, see <https:#www.gnu.org/licenses/>.
#
ARG PYTHON_IMG_TAG=3.10
ARG UV_IMG_TAG=0.5.2
FROM ghcr.io/astral-sh/uv:${UV_IMG_TAG} AS uv


# Includes all labels and timezone info to extend from
FROM docker.io/python:${PYTHON_IMG_TAG}-slim-bookworm AS base
ARG COMMIT_REF
ARG PYTHON_IMG_TAG
ARG MAINTAINER=admin@hotosm.org
LABEL org.hotosm.pg-nearest-city.python-img-tag="${PYTHON_IMG_TAG}" \
      org.hotosm.pg-nearest-city.commit-ref="${COMMIT_REF}" \
      org.hotosm.pg-nearest-city.maintainer="${MAINTAINER}"
RUN apt-get update --quiet \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --quiet --no-install-recommends \
        "locales" "ca-certificates" "curl" \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates
# Set locale & env vars
# - Silence uv complaining about not being able to use hard links,
# - tell uv to byte-compile packages for faster application startups,
# - prevent uv from accidentally downloading isolated Python builds,
# - use a temp dir instead of cache during install,
# - select system python version,
# - declare `/opt/python` as the target for `uv sync` (i.e. instead of .venv).
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_NO_CACHE=1 \
    UV_PYTHON="python$PYTHON_IMG_TAG" \
    UV_PROJECT_ENVIRONMENT=/opt/python
STOPSIGNAL SIGINT



# Build the Python wheel for distribution
FROM base AS build-wheel
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /build
COPY . .
RUN uv build



# Build stage will all dependencies required to build Python wheels
FROM base AS build
RUN apt-get update --quiet \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --quiet --no-install-recommends \
        "build-essential" \
        "gcc" \
        "libpq-dev" \
    && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md /_lock/
# Ensure caching
RUN --mount=type=cache,target=/root/.cache <<EOT
    uv sync \
        --project /_lock \
        --locked \
        --no-install-project \
        --no-dev \
EOT
# Install this package from built wheel
COPY --from=build-wheel \
    "/build/dist/*-py3-none-any.whl" /build/
RUN whl_file=$(find /build -name '*-py3-none-any.whl' -type f) \
    && uv pip install \
      --python=$UV_PROJECT_ENVIRONMENT --no-deps \
      "${whl_file}"



# Run stage will minimal dependencies required to run Python libraries
FROM base AS runtime
ARG PYTHON_IMG_TAG
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PATH="/opt/python/bin:$PATH" \
    PYTHONPATH="/opt" \
    PYTHON_LIB="/opt/python/lib/python$PYTHON_IMG_TAG/site-packages" \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
RUN apt-get update --quiet \
    && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --quiet --no-install-recommends \
        "postgresql-client" \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*
# Copy Python deps from build to runtime
COPY --from=build /opt/python /opt/python
WORKDIR /data
# Add non-root user, permissions
RUN useradd -u 1001 -m -c "appuser" -d /home/appuser -s /bin/false appuser \
    && chown -R appuser:appuser /opt /home/appuser /data
# Change to non-root user
USER appuser



# Stage to use during local development + CI
FROM runtime AS ci
COPY --from=uv /uv /usr/local/bin/uv
COPY . /_lock/
RUN --mount=type=cache,target=/root/.cache <<EOT
    uv sync \
        --project /_lock \
        --locked \
        --no-install-project \
        --group test \
        --group docs
EOT
# Install this package from built wheel
COPY --from=build-wheel \
    "/build/dist/*-py3-none-any.whl" /build/
RUN whl_file=$(find /build -name '*-py3-none-any.whl' -type f) \
    && uv pip install \
      --python=$UV_PROJECT_ENVIRONMENT --no-deps \
      "${whl_file}"
CMD ["bash"]



FROM runtime as dist
# Bundle the built wheel for distribution
COPY --from=build-wheel \
    "/build/dist/*-py3-none-any.whl" /build/
