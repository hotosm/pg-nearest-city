"""Convery async modules to sync equivalents using tokenisation."""

import subprocess
import sys
from pathlib import Path

import unasync

OUTPUT_FILES = ["pg_nearest_city/nearest_city.py", "tests/test_nearest_city.py"]


def build_sync():
    """Transform async code to sync versions with proper import handling."""
    source_files = [
        "pg_nearest_city/async_nearest_city.py",
        "tests/test_async_nearest_city.py",
    ]

    common_replacements = {
        # Class and type replacements
        "AsyncNearestCity": "NearestCity",
        "async_nearest_city": "nearest_city",
        "AsyncConnection": "Connection",
        "AsyncCursor": "Cursor",
        # Test-specific patterns (not working, but not required for now)
        "pytest_asyncio": "pytest",
        # "@pytest_asyncio": "@pytest",
        # "@pytest_asyncio.fixture(loop_scope=\"function\")": "None",
        # "@pytest.mark.asyncio(loop_scope=\"function\")": "None",
        # "@pytest.mark.asyncio": "",
    }

    try:
        unasync.unasync_files(
            source_files,
            rules=[
                unasync.Rule(
                    "async_nearest_city.py",
                    "nearest_city.py",
                    additional_replacements=common_replacements,
                ),
                unasync.Rule(
                    "test_async_nearest_city.py",
                    "test_nearest_city.py",
                    additional_replacements=common_replacements,
                ),
            ],
        )

        print("Transformation completed!")
        # Verify with special focus on import statements
        for output_file in OUTPUT_FILES:
            if Path(output_file).exists():
                print(f"\nSuccessfully created: {output_file}")
            else:
                print(f"Warning: Expected output file not found: {output_file}")

        # Check if the output files were modified
        result = subprocess.run(
            ["git", "diff", "--quiet", "--"] + OUTPUT_FILES, check=False
        )

        if result.returncode == 1:
            print("Files were modified by unasync.")
            sys.exit(0)  # Allow pre-commit to continue

        sys.exit(0)  # No changes, allow pre-commit to continue

    except Exception as e:
        print(f"Error during transformation: {type(e).__name__}: {str(e)}")
        import traceback

        traceback.print_exc()
        # Force failure of pre-commit hook
        sys.exit(1)


if __name__ == "__main__":
    build_sync()
