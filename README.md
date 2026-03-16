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
  <a href="https://results.pre-commit.ci/latest/github/hotosm/pg-nearest-city/main" target="_blank">
      <img src="https://results.pre-commit.ci/badge/github/hotosm/pg-nearest-city/main.svg" alt="Pre-Commit">
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

**td;dr**: a reduction in memory footprint by around 260MB, when compared
to in-memory approaches (an advantage on say, a web server).

This package was developed primarily as a **basic** reverse geocoder for use within
web frameworks (APIs) that **have an existing PostGIS connection to utilise**.

Simple alternatives:

- The [reverse geocoding](https://github.com/thampiman/reverse-geocoder) package
  in Python is probably the original and canonincal implementation using K-D tree.
  - However, it's a bit outdated now, with numerous unattended pull
    requests and uses an unfavourable multiprocessing-based approach.
  - It leaves a large memory footprint of approximately 260MB to load the
    K-D tree in memory (see [benchmarks](./benchmark-results.md)), which
    remains there: an unacceptable compromise for a web server for such a
    small amount of functionality.
- [This package](https://github.com/richardpenman/reverse_geocode) is an excellent
  revamp of the package above, and possibly the best choice in many scenarios,
  particularly if PostGIS is not available.

The pg-nearest-city approach:

- Uses PostgreSQL/PostGIS for reverse geocoding with a lightweight Python client.
- Keeps geocoding data in-database instead of loading a large in-memory KD-tree.
- Overall it has a much lower memory footprint, see [benchmarks](./benchmark-results.md).
- As for performance, as expected, it's slightly slow than the in-memory approach.
  A `272 ms` per lookup versus `168 ms` per lookup for the KD-tree baseline.
  However, note these figures are dominated by external overhead (network round-trips,
  asyncio wrappers, Python call overhead), not the query itself.
- Has a one-time initialization cost of about `39.0 s`, versus `2.6 s` for the
  KD-tree benchmark.

See [benchmarks](./benchmark-results.md) for more details.

> [!NOTE]
> We don't discuss web based geocoding services here, such as Nominatim, as simple
> offline reverse-geocoding has two purposes:
>
> - Reduced latency, when very precise locations are not required.
> - Reduced load on free services such as Nominatim (particularly when running
> in automated tests frequently).
> - Reduced reliance on externally managed tools.

### Priorities

- Minimal memory footprint.
- High performance.
- Keeping package size as small as possible.

### How This Package Works

- Ingest
  [GeoNames](https://download.geonames.org/export/dump/)
  data for cities over 500 population.
- Ingest [GADM 4.1](https://gadm.org/) country boundary polygons.
- Bundle both datasets with this package and load into PostGIS.
- Given a query point, find which country it falls within, then return
  the nearest city within that country by distance.

### Limitations

- The country-boundary approach is intentionally simple and does not guarantee
  perfect handling of all enclave and counter-enclave edge cases globally.
- For locations extremely close to complex borders, results may occasionally map
  to a neighboring country's nearest city.

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
# "US"
print(location.country_alpha3)
# "USA"
print(location.country_name)
# "United States of America"
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
# "US"
print(location.country_alpha3)
# "USA"
print(location.country_name)
# "United States of America"
```

### Country Fields Returned

- `country`: ISO 3166-1 alpha-2 code (e.g. `US`).
- `country_alpha3`: ISO 3166-1 alpha-3 code (e.g. `USA`).
- `country_name`: Official ISO country name (e.g. `United States of America`).

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

## Testing

Run the tests with:

```bash
docker compose run --rm test
```

## Benchmarks

Run the benchmarks with:

```bash
docker compose run --rm benchmark
```

Latest checked-in results are in [benchmark-results.md](./benchmark-results.md).

## Data Generator

### Overview

The pg-nearest-city package bundles two pre-generated data files:

- `cities_500_simple.txt.gz` - city points from GeoNames (pop > 500)
- `country_boundaries.wkb.gz` - country boundary polygons from GADM 4.1

These files are generated by the `data_generator` script.

### Prerequisites

- Docker (or a local PostGIS instance with the `gdal` Python package installed)
- ~1.5 GB free disk space for the GADM download and extraction

### Data Generation

The generator is containerized and can be run using Docker Compose:

```bash
docker compose run --rm generator
```

This will automatically:

1. Download city data from GeoNames (cities with population > 500)
2. Download the GADM 4.1 six-level GeoPackage (~500 MB) from UC Davis
3. Import both datasets into PostGIS
4. Export two output files into `/data/output` in the container
   (mounted to `pg_nearest_city/scripts/data/output/` on host):
   - `cities_500_simple.txt.gz` - simplified city data
   - `country_boundaries.wkb.gz` - country boundary geometries (WKB, keyed by ISO alpha-2)

Downloads and extracted source files are cached in `/data/input`
(mounted to `pg_nearest_city/scripts/data/input/` on host).

### After Generation

Once the files are generated, copy them into the package data directory:

```bash
cp /data/output/cities_500_simple.txt.gz pg_nearest_city/data/cities_500_simple.txt.gz
cp /data/output/country_boundaries.wkb.gz pg_nearest_city/data/country_boundaries.wkb.gz
```

### Configuration

The generator accepts the following options:

```
--country              Filter to a specific country code for testing (e.g. IT)
--no-compress          Disable gzip compression of output files
--input-dir            Directory where downloaded source files are stored
                       (default: /data/input)
--output-dir           Directory to write output files (default: /data/output)
--simplify-tolerance   ST_SimplifyPreserveTopology tolerance in degrees for country
                       boundary export (default: 0.01 ≈ 1km)
```

### Updating Data

The pre-generated data files are bundled with the package,
so regeneration is only necessary when:

- New GeoNames or GADM data is released and you want to update
- You need to filter for a specific geographic region for testing
