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

We wanted a basic reverse geocoder for web APIs that already have a PostGIS
connection: given a point, return the nearest city and country.

There are good alternatives:

- [reverse-geocoder](https://github.com/thampiman/reverse-geocoder) is the
  original Python package, using a K-D tree. It's a bit outdated now, and keeps
  around 260MB in memory, which is a lot for a web server.
- [reverse_geocode](https://github.com/richardpenman/reverse_geocode) is a great
  revamp of that package, and likely the best choice if you don't have PostGIS.

Compared to these, pg-nearest-city:

- Is around 450x faster (45ms down to 0.1ms).
- Uses around 8MB of memory, instead of 260MB.
- Has a one-time cost of 30s to 4 minutes to load the data into your database.
  You could do this at web server startup.

> [!NOTE]
> We don't cover web services like Nominatim here. An offline lookup has lower
> latency, avoids load on free services (particularly in automated tests), and
> means one less external dependency.

### How it works

- Cities come from [GeoNames](https://www.geonames.org/) cities500 (population
  over 500).
- Country boundaries come from
  [geoBoundaries CGAZ](https://www.geoboundaries.org/).
- Our own shapes replace CGAZ for territories and disputed areas.
- Geometry is simplified and subdivided, to keep the package small and queries
  fast.
- A query finds the nearest country polygon within 5km, then the nearest city
  in that country.

### Accuracy

This is a small offline lookup, so treat results as approximate:

- Simplified borders can shift by up to around 100m. A point right on a border
  may return the neighbouring country.
- You get the nearest city in the matched country. That may not be the place
  containing the point, and could be far away in sparsely populated areas.
- Points over 5km from any country return `None`. That covers open sea, plus
  some remote atolls and very small islands.
- Disputed areas use one set of boundaries, which you may see drawn differently
  elsewhere. Cities keep their GeoNames country, so the nearest city can be
  distant there. For example, a point in Crimea returns Russia, with the nearest
  Russian city across the Kerch Strait.

As a guide, we agree with Nominatim on around 90% of a 312 point test set. Many
of the differences are territories, where we return Hong Kong (`HK`) and
Nominatim returns China (`CN`).

## Usage

```bash
pip install pg-nearest-city
```

> [!NOTE]
> Coordinates are always **(lon, lat)**, matching PostGIS.

### Async

```python
from pg_nearest_city import AsyncNearestCity

# Existing code to get a db connection, say from an API endpoint
db = await get_db_connection()

async with AsyncNearestCity(db) as geocoder:
    location = await geocoder.query(-74.0060, 40.7128)

print(location.city)
# "New York City"
print(location.country)
# "US"
```

### Sync

```python
from pg_nearest_city import NearestCity

# Existing code to get a db connection, say from an API endpoint
db = get_db_connection()

with NearestCity(db) as geocoder:
    location = geocoder.query(-74.0060, 40.7128)

print(location.city)
# "New York City"
print(location.country)
# "US"
```

### Creating a new connection

If you don't already have a psycopg connection, pass a `DbConfig`:

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

Or set these environment variables, and use `AsyncNearestCity()` with no
arguments:

```dotenv
PGNEAREST_DB_NAME=cities
PGNEAREST_DB_USER=cities
PGNEAREST_DB_PASSWORD=somepassword
PGNEAREST_DB_HOST=localhost
PGNEAREST_DB_PORT=5432
```

## Testing

```bash
docker compose run --rm test pytest
```

Or locally, with a running PostGIS instance:

```bash
PGNEAREST_DB_USER=myuser PGNEAREST_DB_NAME=postgres uv run pytest tests/ -v
```

## Regenerating the data

You only need this for new GeoNames or CGAZ data, or to change our boundaries.

`pgnearest-load` downloads the sources, builds everything in PostGIS, and
exports compressed CSVs. It takes around 20 minutes.

```bash
uv run pgnearest-load --db-name postgres --db-user myuser --output-dir ./output

# Full rebuild, dropping all tables first
uv run pgnearest-load ... --clean
```

| Flag                                    | Description                      |
| --------------------------------------- | -------------------------------- |
| `--compression {auto,gzip,bz2,xz,zstd}` | Compression for exported files   |
| `--no-cache`                            | Download to temp dir, no persist |
| `--clean`                               | Drop all project tables first    |
| `--skip-steps` / `--only-steps`         | Step prefixes to skip or isolate |
| `--list-steps`                          | Print pipeline steps and exit    |
| `--country`                             | Filter to a country (e.g. `IT`)  |

The output files are:

- `country.csv.<ext>`: subdivided country polygons.
- `geocoding.csv.<ext>`: cities, with their country and coordinates.
- `cities_500_simple.txt.<ext>`: simplified city data.

### Changing boundaries

`pg_nearest_city/db/boundary_overrides.csv.xz` holds the shapes we use instead
of CGAZ: territories, small enclaves and disputed areas. Each row is an alpha-2
code and a WKB MultiPolygon. Edit the file and rebuild to change one.

Country names are in `pg_nearest_city/db/country_names.csv`.

## Attribution

The bundled data is built from:

- [geoBoundaries](https://www.geoboundaries.org/) (CC BY 4.0). Runfola D et al.
  (2020) geoBoundaries: A global database of political administrative boundaries.
  PLoS ONE 15(4): e0231866.
- [GeoNames](https://www.geonames.org/) (CC BY 4.0).
- [Natural Earth](https://www.naturalearthdata.com/) (public domain).
- © [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors (ODbL),
  via Overpass and [Overture Maps](https://overturemaps.org/).
