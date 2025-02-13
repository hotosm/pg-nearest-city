# Benchmarking Results

### Test Run Performance (1000 geocoding operations)
| Implementation | Test Run Time (ms) | Std Dev (ms) | Min (ms) | Max (ms) | Avg Time Per Operation (ms) |
|----------------|------------------:|-------------:|---------:|---------:|---------------------------:|
| KD-tree        | 45,560.73        | 3,359.99     | 39,796.90| 47,936.96| 45.56                     |
| Voronoi        | 1,831.31         | 400.08       | 1,431.14 | 2,496.77 | 1.83                      |

### Memory Footprint After Initialization
| Implementation | Stable Memory (MB) | Memory Std Dev (MB) | Initial Memory (MB) | Memory Growth |
|----------------|------------------:|-------------------:|-------------------:|---------------:|
| KD-tree        | 336.29           | 0.08              | ~73                | +263 MB        |
| Voronoi        | 33.00            | 0.23              | ~25                | +8 MB          |

### Initialization Times
| Implementation | Init Time (ms) | Warmup Time (ms) | Total Startup (ms) |
|----------------|---------------:|----------------:|-------------------:|
| KD-tree        | ~1,380        | ~350            | ~1,730            |
| Voronoi        | ~16,200       | ~15             | ~16,215           |
