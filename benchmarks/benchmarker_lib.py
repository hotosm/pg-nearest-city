"""General library for benchmarking a process.

Metrics:
- Performance.
- Memory usage.
- Initialisation time.
"""

from dataclasses import dataclass, asdict
from typing import Optional
import time
import psutil
import tracemalloc
from pathlib import Path
import json
from datetime import datetime


@dataclass
class BenchmarkPoint:
    """A single measurement point during benchmarking."""

    label: str
    timestamp: Optional[float | str]
    memory_mb: Optional[float] = None
    duration_ms: Optional[float] = None
    memory_delta_mb: Optional[float] = None
    duration_delta_ms: Optional[float] = None


@dataclass
class BenchmarkJson:
    """The final JSON output structure."""

    name: str
    timestamp: Optional[str]
    num_test_runs: int
    points: list[BenchmarkPoint]


class BenchmarkSession:
    """Handle a benchmarking session."""

    def __init__(self, name: str, num_test_runs: int):
        """Initialise benchmarker."""
        self.name = name
        self.num_test_runs = num_test_runs
        self.points: list[BenchmarkPoint] = []
        self.start_time = time.perf_counter()
        self.last_time = self.start_time
        self.last_memory = None
        tracemalloc.start()

    def _get_current_memory(self) -> float:
        """Get current memory usage in MB."""
        return psutil.Process().memory_info().rss / (1024 * 1024)

    def _get_time_metrics(self, current_time: float) -> tuple[float, float]:
        """Calculate total and delta time in milliseconds."""
        total_duration = (current_time - self.start_time) * 1000
        delta_duration = (current_time - self.last_time) * 1000
        self.last_time = current_time
        return total_duration, delta_duration

    def _get_memory_metrics(
        self, current_memory: float
    ) -> tuple[float, Optional[float]]:
        """Calculate memory and delta memory in MB."""
        memory_delta = None
        if self.last_memory is not None:
            memory_delta = current_memory - self.last_memory
        self.last_memory = current_memory
        return current_memory, memory_delta

    def mark_time(self, label: str) -> BenchmarkPoint:
        """Create a benchmark point measuring only time."""
        current_time = time.perf_counter()
        duration, duration_delta = self._get_time_metrics(current_time)

        point = BenchmarkPoint(
            timestamp=current_time,
            label=label,
            duration_ms=duration,
            duration_delta_ms=duration_delta,
        )
        self.points.append(point)
        return point

    def mark_memory(self, label: str) -> BenchmarkPoint:
        """Create a benchmark point measuring only memory."""
        current_time = time.perf_counter()
        current_memory = self._get_current_memory()
        memory, memory_delta = self._get_memory_metrics(current_memory)

        point = BenchmarkPoint(
            timestamp=current_time,
            label=label,
            memory_mb=memory,
            memory_delta_mb=memory_delta,
        )
        self.points.append(point)
        return point

    def mark(self, label: str) -> BenchmarkPoint:
        """Create a benchmark point measuring both time and memory."""
        current_time = time.perf_counter()
        current_memory = self._get_current_memory()

        duration, duration_delta = self._get_time_metrics(current_time)
        memory, memory_delta = self._get_memory_metrics(current_memory)

        point = BenchmarkPoint(
            timestamp=current_time,
            label=label,
            memory_mb=memory,
            duration_ms=duration,
            memory_delta_mb=memory_delta,
            duration_delta_ms=duration_delta,
        )
        self.points.append(point)
        return point

    def get_results_dict(self) -> dict:
        """Get results in a structured format dict."""
        return asdict(
            BenchmarkJson(
                name=self.name,
                timestamp=datetime.now().isoformat(),
                num_test_runs=self.num_test_runs,
                points=[
                    BenchmarkPoint(
                        label=p.label,
                        memory_mb=round(p.memory_mb, 2)
                        if p.memory_mb is not None
                        else None,
                        memory_delta_mb=round(p.memory_delta_mb, 2)
                        if p.memory_delta_mb is not None
                        else None,
                        duration_ms=round(p.duration_ms, 2)
                        if p.duration_ms is not None
                        else None,
                        duration_delta_ms=round(p.duration_delta_ms, 2)
                        if p.duration_delta_ms is not None
                        else None,
                        timestamp=None,
                    )
                    for p in self.points
                ],
            ),
            # Remove null values
            dict_factory=lambda x: {k: v for (k, v) in x if v is not None},
        )

    def to_json(self, directory: str = "benchmarks") -> str:
        """Save results to a JSON file."""
        Path(directory).mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.name}_{timestamp}.json"
        filepath = Path(directory) / filename

        with open(filepath, "w") as file:
            json.dump(
                self.get_results_dict(),
                file,
                indent=2,
            )
        filepath.chmod(mode=0o666)

        return str(filepath)

    # FIXME not fully implemented yet
    # def append_to_markdown(self, file_path: str):
    #     """Create or append to an existing markdown benchmark output."""

    #     def _extract_test_run_metrics():
    #         test_times = [
    #             r["duration_ms"]
    #             for r in data["points"]
    #             if r["label"] == "test_runs_complete"
    #         ]
    #         avg_time_per_op = [t / self.num_test_runs for t in test_times]
    #         return test_times, avg_time_per_op

    #     def _extract_memory_metrics():
    #         mem_final = [
    #             r["final_memory_mb"] for r in data["points"] if "final_memory_mb" in r
    #         ]
    #         mem_initial = [
    #             r["initial_memory_mb"]
    #             for r in data["points"]
    #             if "initial_memory_mb" in r
    #         ]
    #         mem_growth = [f - i for f, i in zip(mem_final, mem_initial, strict=False)]
    #         return mem_final, mem_initial, mem_growth

    #     def _extract_initialization_times():
    #         init_times = [
    #             r["init_time_ms"] for r in data["points"] if "init_time_ms" in r
    #         ]
    #         warmup_times = [
    #             r["warmup_time_ms"] for r in data["points"] if "warmup_time_ms" in r
    #         ]
    #         total_startup = [
    #             i + w for i, w in zip(init_times, warmup_times, strict=False)
    #         ]
    #         return init_times, warmup_times, total_startup

    #     def _format_stat_row(values):
    #         return (
    #             f"{statistics.mean(values):.2f} | {statistics.stdev(values):.2f} "
    #             f"| {min(values):.2f} | {max(values):.2f}"
    #         )

    #     def _format_memory_row():
    #         return (
    #             f"{statistics.mean(mem_final):.2f} "
    #             f"| {statistics.stdev(mem_final):.2f} "
    #             f"| ~{statistics.mean(mem_initial):.0f} "
    #             f"| +{statistics.mean(mem_growth):.0f} MB"
    #         )

    #     def _append_to_section(content: str, section_header: str, path: str):
    #         markdown_path = Path(path)
    #         if not markdown_path.exists():
    #             markdown_path.write_text(
    #                 f"# Benchmarking Results\n\n{section_header}\n\n{content}\n"
    #             )
    #             return

    #         existing = markdown_path.read_text()
    #         if section_header in existing:
    #             existing = existing.replace(
    #                 section_header, f"{section_header}\n\n{content}"
    #             )
    #         else:
    #             existing += f"\n{section_header}\n\n{content}\n"

    #         markdown_path.write_text(existing)

    #     # Re-create file if already exists
    #     benchmark_file = Path(file_path)
    #     benchmark_file.unlink(missing_ok=True)
    #     benchmark_file.touch(mode=0o666)

    #     data = self.get_results_dict()
    #     test_times, avg_time_per_op = _extract_test_run_metrics()
    #     mem_final, mem_initial, mem_growth = _extract_memory_metrics()
    #     init_times, warmup_times, total_startup = _extract_initialization_times()

    #     _append_to_section(
    #         (
    #             "| Implementation | Test Run Time (ms) | Std Dev (ms) "
    #             "| Min (ms) | Max (ms) | Avg Time Per Operation (ms) |\n"
    #             "|----------------|------------------:|-------------:"
    #             "|---------:|---------:|---------------------------:|\n"
    #             f"| {self.name} | {_format_stat_row(test_times)} "
    #             f"| {statistics.mean(avg_time_per_op):.2f} |\n"
    #         ),
    #         "### Test Run Performance (1000 geocoding operations)",
    #         file_path,
    #     )

    #     _append_to_section(
    #         (
    #             "| Implementation | Stable Memory (MB) | Memory Std Dev (MB) "
    #             "| Initial Memory (MB) | Memory Growth |\n"
    #             "|----------------|------------------:|-------------------:"
    #             "|-------------------:|---------------:|\n"
    #             f"| {self.name} | {_format_memory_row()} |\n"
    #         ),
    #         "### Memory Footprint After Initialization",
    #         file_path,
    #     )

    #     _append_to_section(
    #         (
    #             "| Implementation | Init Time (ms) | Warmup Time (ms) "
    #             "| Total Startup (ms) |\n"
    #             "|----------------|---------------:|----------------:"
    #             "|-------------------:|\n"
    #             f"| {self.name} | ~{statistics.mean(init_times):.0f} | "
    #             f"~{statistics.mean(warmup_times):.0f} "
    #             f"| ~{statistics.mean(total_startup):.0f} |\n"
    #         ),
    #         "### Initialization Times",
    #         file_path,
    #     )

    def print_summary(self):
        """Print a human-readable summary."""
        print(f"\nBenchmark Summary: {self.name}")
        print(f"\nTest Runs: {self.num_test_runs}")
        print("-" * 50)

        for point in self.points:
            print(f"\n{point.label}:")
            if point.memory_mb is not None:
                print(f"  Memory: {point.memory_mb:.2f} MB")
                if point.memory_delta_mb is not None:
                    print(f"  Memory Δ: {point.memory_delta_mb:+.2f} MB")
            if point.duration_ms is not None:
                print(f"  Duration: {point.duration_ms:.2f} ms")
                if point.duration_delta_ms is not None:
                    print(f"  Duration Δ: {point.duration_delta_ms:.2f} ms")
