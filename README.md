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

- Is approximately ~450x more performant (45ms --> 0.1ms).
- Has a small ~8MB memory footprint, compared to ~260MB.
  - Depends on the selected baseline dataset and compression algorithm:
    GADM when compressed is ~25MB.
- However it has a one-time initialisation penalty of approximately 30s-4m
  to load the data into the database (which could be handled at
  web server startup).
  - As with the memory footprint, this depends on the baseline dataset
    and compression algorithm selected.

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
- High performance.

### How This Package Works

- Ingest [GeoNames](https://www.geonames.org/) cities500 data
  (cities with population > 500).
- Ingest country boundary polygons from either [GADM](https://gadm.org/) or
  [Natural Earth](https://www.naturalearthdata.com/).
- Apply [GeoBoundaries](https://www.geoboundaries.org/) corrections for
  inaccurate upstream geometry (e.g. disputed territories, overseas regions).
- Clean and normalise country data (spelling fixes, ISO 3166-1 alignment, etc.).
- Simplify geometry with PostGIS `ST_Subdivide` to shrink size and speed up queries.
- Query: use `ST_Covers` to identify the covering country polygon, then a KNN
  lateral join to find the nearest city within that country.

## Usage

### Install

Distributed as a pip package on PyPi:

```bash
pip install pg-nearest-city
# or use your dependency manager of choice
```

### Run The Code

> [!NOTE]
> Coordinates use **(lon, lat)** order throughout, matching the GIS / PostGIS
> convention where longitude is the X axis and latitude is Y.

#### Async

```python
from pg_nearest_city import AsyncNearestCity

# Existing code to get db connection, say from API endpoint
db = await get_db_connection()

async with AsyncNearestCity(db) as geocoder:
    location = await geocoder.query(-74.0060, 40.7128)

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
    location = geocoder.query(-74.0060, 40.7128)

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
    location = await geocoder.query(-74.0060, 40.7128)
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
    location = await geocoder.query(-74.0060, 40.7128)
```

## Testing

Via Docker:

```bash
docker compose run --rm code pytest
```

Or locally (requires a running PostgreSQL instance with PostGIS and loaded data):

```bash
PGNEAREST_DB_USER=myuser PGNEAREST_DB_NAME=postgres uv run pytest tests/ -v
```

## Data Pipeline

The `pgnearest-load` CLI command runs a multi-step pipeline that downloads source
data, imports it into PostGIS, applies corrections, simplifies geometry, and
exports compressed CSV files for distribution.

### Running the Pipeline

```bash
# Using Natural Earth boundaries
uv run pgnearest-load --boundary-source naturalearth \
    --db-name postgres --db-user myuser --output-dir ./output

# Using GADM boundaries
uv run pgnearest-load --boundary-source gadm \
    --db-name postgres --db-user myuser --output-dir ./output

# Full rebuild (drops all tables first)
uv run pgnearest-load ... --clean
```

### Key Flags

| Flag                                    | Description                      |
| --------------------------------------- | -------------------------------- |
| `--boundary-source {gadm,naturalearth}` | Which boundary dataset to use    |
| `--compression {auto,gzip,bz2,xz,zstd}` | Compression for exported files   |
| `--no-cache`                            | Download to temp dir, no persist |
| `--clean`                               | Drop all project tables first    |
| `--skip-steps` / `--only-steps`         | Step prefixes to skip or isolate |
| `--list-steps`                          | Print pipeline steps and exit    |
| `--country`                             | Filter to a country (e.g. `IT`)  |

### Output Files

The pipeline exports three compressed CSV files:

- `country.csv.<ext>` — subdivided country polygons (alpha2, alpha3, name, WKB geometry).
- `geocoding.csv.<ext>` — city-to-country mapping (city, country, lat, lon).
- `cities_500_simple.txt.<ext>` — simplified city data.

### When to Regenerate

Manual regeneration is only necessary when:

- New GeoNames data becomes available and you want to update.
- Upstream boundary datasets have been corrected.
- You need to filter for a specific geographic region.
