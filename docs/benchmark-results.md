<!-- markdownlint-disable -->

# Benchmarking Results

Latest checked-in benchmark run from:

```bash
docker compose run --rm benchmark
```

## Summary

| Implementation | Test Runs | Init Memory (MB) | Geocoder Memory (MB) | Memory Growth (MB) | Init Time (ms) | Warmup Time (ms) | Test Run Time (ms) | Avg Time Per Operation (ms) |
|----------------|----------:|-----------------:|---------------------:|-------------------:|---------------:|-----------------:|-------------------:|----------------------------:|
| KD-tree | 1000 | 45.71 | 315.12 | 269.42 | 2750.71 | 790.45 | 178317.55 | 178.32 |
| pg-nearest-city | 1000 | 46.21 | 54.71 | 8.50 | 12791.01 | 1926.87 | 318199.64 | 318.20 |

## KD-tree

Benchmark Summary: `kdtree_geocoding`

- Test runs: `1000`
- Initial memory: `45.71 MB`
- Geocoder initialized memory: `315.12 MB`
- Memory growth at initialization: `+269.42 MB`
- Initialization duration: `2750.71 ms`
- Warmup duration: `790.45 ms`
- Test run duration: `178317.55 ms`
- Final memory: `315.38 MB`

## pg-nearest-city

Benchmark Summary: `pgnearest_geocoding`

- Test runs: `1000`
- Initial memory: `46.21 MB`
- Geocoder initialized memory: `54.71 MB`
- Memory growth at initialization: `+8.50 MB`
- Initialization duration: `12791.01 ms`
- Warmup duration: `1926.87 ms`
- Test run duration: `318199.64 ms`
- Final memory: `52.91 MB`

## Raw Output

```text
Benchmark Summary: kdtree_geocoding

Test Runs: 1000
--------------------------------------------------

initial_state:
  Memory: 45.71 MB

geocoder_initialized:
  Memory: 315.12 MB
  Memory Δ: +269.42 MB
  Duration: 2750.71 ms
  Duration Δ: 2750.71 ms

warmup_start:
  Duration: 2752.80 ms
  Duration Δ: 2.09 ms

warmup_complete:
  Duration: 3543.25 ms
  Duration Δ: 790.45 ms

test_runs_start:
  Memory: 315.38 MB
  Memory Δ: +0.25 MB
  Duration: 3543.32 ms
  Duration Δ: 0.06 ms

test_runs_complete:
  Memory: 315.38 MB
  Memory Δ: +0.00 MB
  Duration: 181860.87 ms
  Duration Δ: 178317.55 ms

final_state:
  Memory: 315.38 MB
  Memory Δ: +0.00 MB

Benchmark Summary: pgnearest_geocoding

Test Runs: 1000
--------------------------------------------------

initial_state:
  Memory: 46.21 MB

geocoder_initialized:
  Memory: 54.71 MB
  Memory Δ: +8.50 MB
  Duration: 12791.01 ms
  Duration Δ: 12791.01 ms

warmup_start:
  Duration: 12791.52 ms
  Duration Δ: 0.51 ms

warmup_complete:
  Duration: 14718.39 ms
  Duration Δ: 1926.87 ms

test_runs_start:
  Memory: 54.71 MB
  Memory Δ: +0.00 MB
  Duration: 14718.41 ms
  Duration Δ: 0.02 ms

test_runs_complete:
  Memory: 54.83 MB
  Memory Δ: +0.12 MB
  Duration: 332918.05 ms
  Duration Δ: 318199.64 ms

final_state:
  Memory: 52.91 MB
  Memory Δ: -1.92 MB
```

## Direct psql

```bash
docker exec -it pg-nearest-city-db psql -U cities -d cities -c "
    EXPLAIN ANALYZE
    WITH query_point AS (
      SELECT ST_SetSRID(ST_MakePoint(-85.33073821848836, 34.99838480842798), 4326) AS geom
    )
    SELECT g.city, c.alpha2, c.alpha3, g.lon, g.lat, c.name
    FROM query_point qp
    JOIN country c ON c.geom && qp.geom AND ST_Contains(c.geom, qp.geom)
    JOIN geocoding g ON c.alpha2 = g.country
    ORDER BY g.geom <-> qp.geom
    LIMIT 1;"
```

Result:

```
 Limit  (cost=0.42..26.04 rows=1 width=53) (actual time=18.322..18.326 rows=1 loops=1)
   ->  Nested Loop  (cost=0.42..23619.18 rows=922 width=53) (actual time=18.319..18.321 rows=1 loops=1)
         Join Filter: (c.alpha2 = g.country)
         ->  Index Scan using geocoding_geom_idx on geocoding g  (cost=0.28..19563.50 rows=230584 width=62) (actual time=0.863..0.864 rows=1 loops=1)
               Order By: (geom <-> '0101000020E610000078FCA1D02A5555C0BD82CA12CB7F4140'::geometry)
         ->  Materialize  (cost=0.14..20.66 rows=1 width=18) (actual time=17.435..17.436 rows=1 loops=1)
               ->  Index Scan using country_geom_idx on country c  (cost=0.14..20.66 rows=1 width=18) (actual time=17.426..17.426 rows=1 loops=1)
                     Index Cond: ((geom && '0101000020E610000078FCA1D02A5555C0BD82CA12CB7F4140'::geometry) AND (geom ~ '0101000020E610000078FCA1D02A5555C0BD82CA12CB7F4140'::geometry))
                     Filter: st_contains(geom, '0101000020E610000078FCA1D02A5555C0BD82CA12CB7F4140'::geometry)
 Planning Time: 25.314 ms
 Execution Time: 19.717 ms
(11 rows)
```

## Summary

A pretty quick 20ms for direct `psql` queries means most of the latency above
is things like network overhead, asyncio wrappers, etc.

The KD-tree's similarly high per-query time (~168 ms) confirms how much
environmental overhead the benchmark captures.

The real cost of the DB approach versus in-memory is the database
round-trip, not the query execution.
