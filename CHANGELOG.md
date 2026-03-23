# Changelog

## 2.0.0 (2026-03-23)

### Feat

- adds package exports
- added single-country import with tests
- split testing pipeline; unit and integration
- added bundled outputs to package
- add --boundary-source and --compression CLI flags
- CSV bootstrap import and query(lon, lat) API change
- multi-source data import pipeline with Overpass and overlap resolution
- source-specific data corrections and Overpass boundary targets
- overhaul SQL queries for subdivision, NE support, and overlap resolution
- add compression framework and MRO fix for filter_items
- add boundary source types, Natural Earth datasets, and registry persistence
- bumped version to 0.3.0
- implement boundary corrections and HK/MO support in import pipeline
- add SQL queries for boundary corrections and ADM1 promotion
- add database performance settings for data loading
- add declarative data corrections framework
- add utility functions for subclass discovery and filtering
- add datasets package
- properly merges ADM_0 AND ADM_1 layers for HK and MO, but tests fail
- adds data cleanup post-import

### Fix

- cherry-picks code from PR #62
- addresses problem with MA and EH overlaps
- re-added Kosovo as a sovereign country
- added COMPRESSION.md
- changed warning about database readiness to a log entry
- improved caching
- added geom column

### Refactor

- subdivide country table into Polygons, decouple country_init
- update scripts and CLI for new datasets architecture
- update table definitions for boundary correction workflow


- remove benchmarks

## 1.0.0 (2026-03-15)

### Refactor

- country lookups near borders now use st_covers against corrected polygons instead of voronoi tessellation
- iso3166-1 used as definitive standard for country sovereignty
- new data import pipeline supporting gadm and natural earth boundary sources
- geoboundaries and overpass corrections for countries with inaccurate upstream geometry
- automatic overlap resolution using geocoding city presence as a heuristic
- declarative data corrections framework for spelling fixes and errata
- datasets package with url configs, caching, and a persistent registry

## 0.2.1 (2025-02-17)

### Fix

- replace deprecated importlib .path method with .files() API

## 0.2.0 (2025-02-11)

### Fix

- add context managers via __enter__ methods, update usage
- do not default use test db conn, error on missing vars

### Refactor

- use encode/httpcore unasync impl, restructure
- fallback to env vars for NearestCity.connect(), esp in tests
- export main classes in __init__.__all__ for pg_nearest_city pkg
- lint all, add extra pre-commit hooks, allow env var db initialisation

## 0.1.0 (2025-02-08)

### Feat

- re-added usage with context manager
- added sync code generation with unasync
- init status and logger
- async wrapper
- auto init when used with context manager
- initialization checks
- added support for external db connections and for closing internal ones
- delete voronoi file after init to lower disk usage
- gzipped files to lower disk usage
- first commit, add stub project, license

### Fix

- added pre-generated sync files
- return on invalid table structure
- changed test dbconfig to match compose file
- moved check for init files existance when they're actually needed

### Refactor

- moved shared logic into base class
