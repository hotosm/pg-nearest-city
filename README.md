# Simple PostGIS Reverse Geocoder

<!-- markdownlint-disable -->
<p align="center">
  <img src="https://raw.githubusercontent.com/hotosm/pg-nearest-city/refs/heads/main/docs/images/hot_logo.png" style="width: 200px;" alt="HOT"></a>
</p>
<p align="center">
  <em>Given a geopoint, find the nearest city using PostGIS (reverse geocode).</em>
</p>
<p align="center">
  <a href="https://github.com/hotosm/pg-nearest-city/actions/workflows/docs.yml" target="_blank">
      <img src="https://github.com/hotosm/pg-nearest-city/actions/workflows/docs.yml/badge.svg" alt="Publish Docs">
  </a>
  <a href="https://github.com/hotosm/pg-nearest-city/actions/workflows/publish.yml" target="_blank">
      <img src="https://github.com/hotosm/pg-nearest-city/actions/workflows/publish.yml/badge.svg" alt="Publish">
  </a>
  <a href="https://github.com/hotosm/pg-nearest-city/actions/workflows/pytest.yml" target="_blank">
      <img src="https://github.com/hotosm/pg-nearest-city/actions/workflows/pytest.yml/badge.svg?branch=main" alt="Test">
  </a>
  <a href="https://pypi.org/project/pg-nearest-city" target="_blank">
      <img src="https://img.shields.io/pypi/v/pg-nearest-city?color=%2334D058&label=pypi%20package" alt="Package version">
  </a>
  <a href="https://pypistats.org/packages/pg-nearest-city" target="_blank">
      <img src="https://img.shields.io/pypi/dm/pg-nearest-city.svg" alt="Downloads">
  </a>
  <a href="https://github.com/hotosm/pg-nearest-city/blob/main/LICENSE.md" target="_blank">
      <img src="https://img.shields.io/github/license/hotosm/pg-nearest-city.svg" alt="License">
  </a>
</p>

---

📖 **Documentation**: <a href="https://hotosm.github.io/pg-nearest-city/" target="_blank">https://hotosm.github.io/pg-nearest-city/</a>

🖥️ **Source Code**: <a href="https://github.com/hotosm/pg-nearest-city" target="_blank">https://github.com/hotosm/pg-nearest-city</a>

---

<!-- markdownlint-enable -->

## Why do we need this?

This package was developed primarily as a **basic** reverse geocoder for use within
web frameworks (APIs) that have an existing PostGIS connection to utilise.

- The reverse geocoding package in Python [here](https://github.com/thampiman/reverse-geocoder)
  is probably the original and canonincal implementation using K-D tree.
  - However, it's a bit outdated now, with numerous unattended pull
    requests and uses an unfavourable multiprocessing-based approach.
- The package [here](https://github.com/richardpenman/reverse_geocode) is an excellent
  revamp of the package above, an likely the best choice in many scenarios.

The K-D tree implementation in Python is performant (see [benchmarks](#benchmarks))
and an excellent choice for scripts.

However, it does leave a large memory footprint of approximately 160Mb to load the
K-D tree in memory (see [benchmarks](#benchmarks)).

Once computed, the K-D tree remains in memory! This is an unacceptable compromise
for a web server, for such a small amount of functionality, particularly if the
web server is run via a container orchestrator as replicas with minimal memory.

As we already have a Postgres database running alongside our webserver, an approach
to simply query via pre-loaded data via PostGIS is much more memory efficient (~2Mb)
and has an acceptable performance penalty (see [benchmarks](#benchmarks)).

> [!NOTE]
> We don't discuss web based geocoding services here, such as Nominatim, as simple
> offline reverse-geocoding has two purposes:
>
> - Reduced latency, when very precise locations are not required.
> - Reduced load on free services such as Nominatim (particularly when running
> in automated tests frequently).

### Priorities

- Lightweight package size.
- Minimal memory footprint.
- Reasonably good performance.

### How This Package Works

- geonames.org data.
- Voronoi polygons based on geopoints.
- Gzipped data bundled with package.
- Query the Voronois.

## Usage

### Install

Distributed as a pip package on PyPi:

```bash
pip install pg-nearest-city
# or use your dependency manager of choice
```

### Run The Code

#### Async

```python
from pg_nearest_city import AsyncNearestCity

# Existing code to get db connection, say from API endpoint
db = await get_db_connection()

async with AsyncNearestCity(db) as geocoder:
    location = await geocoder.query(40.7128, -74.0060)

print(location.city)
# "New York City"
print(location.country)
# "USA"
```

#### Sync

```python
from pg_nearest_city import NearestCity

# Existing code to get db connection, say from API endpoint
db = get_db_connection()

with NearestCity(db) as geocoder:
    location = geocoder.query(40.7128, -74.0060)

print(location.city)
# "New York City"
print(location.country)
# "USA"
```

#### Create A New DB Connection

- If your app upstream already has a psycopg connection, this can be
  passed through.
- If you require a new database connection, the connection parameters
  can be defined as DbConfig object variables:

```python
from pg_nearest_city import DbConfig, AsyncNearestCity

db_config = DbConfig(
    dbname="db1",
    user="user1",
    password="pass1",
    host="localhost",
    port="5432",
)

async with AsyncNearestCity(db_config) as geocoder:
    location = await geocoder.query(40.7128, -74.0060)
```

- Or alternatively as variables from your system environment:

```dotenv
PGNEAREST_DB_NAME=cities
PGNEAREST_DB_USER=cities
PGNEAREST_DB_PASSWORD=somepassword
PGNEAREST_DB_HOST=localhost
PGNEAREST_DB_PORT=5432
```

then

```python
from pg_nearest_city import AsyncNearestCity

async with AsyncNearestCity() as geocoder:
    location = await geocoder.query(40.7128, -74.0060)
```

## Benchmarks

- todo

## Testing

Run the tests with:

```bash
docker compose run --rm code pytest
```
