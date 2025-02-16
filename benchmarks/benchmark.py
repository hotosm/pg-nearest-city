"""Benchmark k-tree vs our voronoi geocoding approaches."""

from typing import List, Tuple
import asyncio
import random
from multiprocessing import Process, Queue

import reverse_geocoder

from benchmarker_lib import BenchmarkSession
from pg_nearest_city import AsyncNearestCity
from pg_nearest_city.base_nearest_city import Location


async def benchmark_voronoi(
    test_points: List[Tuple[float, float]],
    warmup_runs: int = 5,
    num_test_runs: int = 1000,
) -> Tuple[List[Location], BenchmarkSession]:
    """Run benchmark for Voronoi implementation."""
    session = BenchmarkSession("voronoi_geocoding", num_test_runs)
    results = []

    # Initial memory snapshot
    session.mark_memory("initial_state")

    # Initialize geocoder
    async with AsyncNearestCity() as geocoder:
        session.mark("geocoder_initialized")

        # Warmup runs - only care about time
        session.mark_time("warmup_start")
        for _ in range(warmup_runs):
            await geocoder.query(test_points[0][0], test_points[0][1])
        session.mark_time("warmup_complete")

        # Test runs - measure time for the batch, with periodic memory checks
        session.mark("test_runs_start")
        for i, (lat, lon) in enumerate(test_points[:num_test_runs]):
            result = await geocoder.query(lat, lon)
            results.append(result)
            # Check memory every 1000 points
            if i > 0 and i % 1000 == 0:
                session.mark_memory(f"progress_{i}")

        session.mark("test_runs_complete")

    # Final memory state
    session.mark_memory("final_state")

    return (results, session)


def benchmark_kdtree(
    test_points: List[Tuple[float, float]],
    warmup_runs: int = 5,
    num_test_runs: int = 1000,
) -> Tuple[List[Location], BenchmarkSession]:
    """Run benchmark for KDTree implementation."""
    session = BenchmarkSession("kdtree_geocoding", num_test_runs)
    results = []

    # Initial memory snapshot
    session.mark_memory("initial_state")

    # Initialize geocoder - measure both time and memory
    reverse_geocoder.RGeocoder(mode=2, verbose=False)
    session.mark("geocoder_initialized")

    # Warmup runs - only care about time
    session.mark_time("warmup_start")
    for _ in range(warmup_runs):
        reverse_geocoder.get(test_points[0])
    session.mark_time("warmup_complete")

    # Test runs - measure time for the batch, with periodic memory checks
    session.mark("test_runs_start")
    for i, (lat, lon) in enumerate(test_points[:num_test_runs]):
        result = reverse_geocoder.get((lat, lon))
        results.append(
            Location(lat=lat, lon=lon, city=result["name"], country=result["cc"])
        )

        # Check memory every 1000 points
        if i > 0 and i % 1000 == 0:
            session.mark_memory(f"progress_{i}")

    session.mark("test_runs_complete")

    # Final memory state
    session.mark_memory("final_state")

    return (results, session)


def generate_test_points(count: int = 10000) -> List[Tuple[float, float]]:
    """Generate a consistent set of test points."""
    random.seed(42)
    points = [
        (random.uniform(-90, 90), random.uniform(-180, 180)) for _ in range(count)
    ]
    random.seed()
    return points


async def main():
    """Wrap benchmarker functionality + output."""
    print("Running benchmarks. This may take 2-10 mins...")
    test_points = generate_test_points()

    def kdtree_process(queue):
        """Process kdtree benchmarks in separate system process."""
        results = benchmark_kdtree(test_points)
        queue.put(results)

    def voronoi_process(queue):
        """Process voronoi benchmarks in separate system process."""
        results = asyncio.run(benchmark_voronoi(test_points))
        queue.put(results)

    # NOTE we run both benchmarks in separate processes

    kdtree_queue = Queue()
    kdtree_p = Process(target=kdtree_process, args=(kdtree_queue,))
    kdtree_p.start()

    voronoi_queue = Queue()
    voronoi_p = Process(target=voronoi_process, args=(voronoi_queue,))
    voronoi_p.start()

    kdtree_p.join()
    voronoi_p.join()

    # Get results
    (_kdtree_results, kdtree_session) = kdtree_queue.get()
    (_voronoi_results, voronoi_session) = voronoi_queue.get()

    kdtree_session.print_summary()
    voronoi_session.print_summary()

    # Output to JSON
    kdtree_session.to_json()
    voronoi_session.to_json()

    # Output to final markdown
    # FIXME markdown file generated not fully automated yet
    # benchmark_file = "benchmarks/benchmark-results.md"
    # kdtree_session.append_to_markdown(file_path=str(benchmark_file))
    # voronoi_session.append_to_markdown(file_path=str(benchmark_file))


if __name__ == "__main__":
    """Run the async benchmarking to completion."""
    asyncio.run(main())
