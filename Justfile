# Copyright (c) Humanitarian OpenStreetMap Team
# This file is part of pg-nearest-city.
#
#     pg-nearest-city is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     pg-nearest-city is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with pg-nearest-city.  If not, see <https:#www.gnu.org/licenses/>.
#

set dotenv-load

# List available commands
[private]
default:
  just help

# List available commands
help:
  just --justfile {{justfile()}} --list

# Build and publish to PyPI, requires PYPI_TOKEN
publish:
  docker run --rm -v "$PWD":/app:ro -w /app -e UV_PUBLISH_TOKEN="$PYPI_TOKEN" \
    ghcr.io/astral-sh/uv:0.10.12-python3.13-trixie-slim \
    sh -c 'uv build -o /tmp/dist && uv publish /tmp/dist/*'
