from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Flag, auto
import time
import psutil
import tracemalloc
from pathlib import Path
import json
from datetime import datetime


@dataclass
class BenchmarkPoint:
    """A single measurement point during benchmarking"""

    timestamp: float
    label: str
    memory_mb: Optional[float] = None
    duration_ms: Optional[float] = None
    memory_delta_mb: Optional[float] = None
    duration_delta_ms: Optional[float] = None


class BenchmarkSession:
    def __init__(self, name: str, test_runs: int):
        self.name = name
        self.test_runs = test_runs
        self.points: List[BenchmarkPoint] = []
        self.start_time = time.perf_counter()
        self.last_time = self.start_time
        self.last_memory = None
        tracemalloc.start()

    def _get_current_memory(self) -> float:
        """Get current memory usage in MB"""
        return psutil.Process().memory_info().rss / (1024 * 1024)

    def _get_time_metrics(self, current_time: float) -> tuple[float, float]:
        """Calculate total and delta time in milliseconds"""
        total_duration = (current_time - self.start_time) * 1000
        delta_duration = (current_time - self.last_time) * 1000
        self.last_time = current_time
        return total_duration, delta_duration

    def _get_memory_metrics(
        self, current_memory: float
    ) -> tuple[float, Optional[float]]:
        """Calculate memory and delta memory in MB"""
        memory_delta = None
        if self.last_memory is not None:
            memory_delta = current_memory - self.last_memory
        self.last_memory = current_memory
        return current_memory, memory_delta

    def mark_time(self, label: str) -> BenchmarkPoint:
        """Create a benchmark point measuring only time"""
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
        """Create a benchmark point measuring only memory"""
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
        """Create a benchmark point measuring both time and memory"""
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

    def get_results(self) -> Dict:
        """Get results in a structured format"""
        return {
            "name": self.name,
            "timestamp": datetime.now().isoformat(),
            "test_runs": self.test_runs,
            "points": [
                {
                    "label": p.label,
                    "memory_mb": round(p.memory_mb, 2)
                    if p.memory_mb is not None
                    else None,
                    "memory_delta_mb": round(p.memory_delta_mb, 2)
                    if p.memory_delta_mb is not None
                    else None,
                    "duration_ms": round(p.duration_ms, 2)
                    if p.duration_ms is not None
                    else None,
                    "duration_delta_ms": round(p.duration_delta_ms, 2)
                    if p.duration_delta_ms is not None
                    else None,
                }
                for p in self.points
            ],
        }

    def save(self, directory: str = "benchmarks/benchmark_results") -> str:
        """Save results to a JSON file"""
        Path(directory).mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.name}_{timestamp}.json"
        filepath = Path(directory) / filename

        with open(filepath, "w") as f:
            json.dump(self.get_results(), f, indent=2)

        return str(filepath)

    def print_summary(self):
        """Print a human-readable summary"""
        print(f"\nBenchmark Summary: {self.name}")
        print(f"\nTest Runs: {self.test_runs}")
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
