# Compression Benchmarks

All benchmarks were run on a MacBook Pro M4 Pro using Python 3.10.14's stdlib
compressors (`gzip`, `bz2`, `lzma`) and the `zstd` CLI tool (v1.5.6).
Each measurement is a single-pass, in-memory compression of the full file.

## GADM (country + geocoding CSVs)

Raw uncompressed size: **80.4 MB** (73 MB country geometry + 7.4 MB geocoding)

| Method | Profile | Level | Size (MB) | Ratio | Time |
|--------|---------|------:|----------:|------:|-----:|
| gzip   | fast    |     1 |     28.65 | 2.81x | 0.38s |
| gzip   | normal  |     6 |     24.88 | 3.23x | 1.75s |
| gzip   | high    |     9 |     24.62 | 3.27x | 3.90s |
| bz2    | fast    |     1 |     23.45 | 3.43x | 6.69s |
| bz2    | normal  |     5 |     22.22 | 3.62x | 7.19s |
| bz2    | high    |     9 |     21.99 | 3.66x | 7.42s |
| xz     | fast    |     1 |     20.86 | 3.85x | 5.92s |
| xz     | normal  |     6 |     16.53 | 4.86x | 52.66s |
| xz     | high    |     9 |     15.65 | 5.14x | 64.36s |
| zstd   | fast    |     1 |     24.60 | 3.27x | 0.06s |
| zstd   | normal  |     3 |     23.81 | 3.38x | 0.11s |
| zstd   | high    |    19 |     19.08 | 4.21x | 15.01s |


## NaturalEarth (country + geocoding CSVs)

Raw uncompressed size: **22.4 MB**

| Method | Profile | Level | Size | Ratio | Time |
|--------|---------|------:|-----:|------:|-----:|
| gzip   | fast    |     1 | 9,994,721 | 2.35x | 0.13s |
| gzip   | normal  |     6 | 8,644,532 | 2.71x | 0.59s |
| gzip   | high    |     9 | 8,577,519 | 2.73x | 1.02s |
| bz2    | fast    |     1 | 7,791,101 | 3.01x | 1.84s |
| bz2    | normal  |     5 | 7,773,140 | 3.02x | 1.94s |
| bz2    | high    |     9 | 7,800,440 | 3.01x | 1.98s |
| xz     | fast    |     1 | 8,053,780 | 2.91x | 2.14s |
| xz     | normal  |     6 | 6,304,488 | 3.72x | 14.20s |
| xz     | high    |     9 | 6,145,500 | 3.82x | 14.74s |
| zstd   | fast    |     1 | 8,731,752 | 2.69x | 0.12s |
| zstd   | normal  |     3 | 8,884,884 | 2.64x | 0.14s |
| zstd   | high    |    19 | 6,946,284 | 3.38x | 6.15s |


## Cities (cities_500_simple.txt)

Raw uncompressed size: **7.4 MB**

| Method | Profile | Level | Size | Ratio | Time |
|--------|---------|------:|-----:|------:|-----:|
| gzip   | fast    |     1 | 3,399,769 | 2.17x | 0.05s |
| gzip   | normal  |     6 | 3,022,465 | 2.44x | 0.19s |
| gzip   | high    |     9 | 3,011,400 | 2.45x | 0.36s |
| bz2    | fast    |     1 | 2,453,937 | 3.00x | 0.53s |
| bz2    | normal  |     5 | 2,473,301 | 2.98x | 0.54s |
| bz2    | high    |     9 | 2,488,339 | 2.96x | 0.55s |
| xz     | fast    |     1 | 2,899,496 | 2.54x | 0.72s |
| xz     | normal  |     6 | 2,471,472 | 2.98x | 3.78s |
| xz     | high    |     9 | 2,471,472 | 2.98x | 3.78s |
| zstd   | fast    |     1 | 3,375,739 | 2.18x | 0.01s |
| zstd   | normal  |     3 | 3,204,507 | 2.30x | 0.03s |
| zstd   | high    |    19 | 2,667,834 | 2.76x | 1.48s |

