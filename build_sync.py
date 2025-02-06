import unasync
import os
import pathlib

def build_sync():
    """Transform async code to sync versions with proper import handling."""
    source_files = [
        "pg_nearest_city/async_nearest_city.py",
        "tests/test_async_nearest_city.py"
    ]
    
    common_replacements = {
        # Class and type replacements
        "AsyncNearestCity": "NearestCity",
        "async_nearest_city": "nearest_city",
        "AsyncConnection": "Connection",
        "AsyncCursor": "Cursor",
        
        # Test-specific patterns
        "import pytest_asyncio": "",
        "pytest_asyncio": "pytest",
        "asyncio": "sync",
    }
    
    try:
        unasync.unasync_files(
            source_files,
            rules=[
                unasync.Rule(
                    "async_nearest_city.py",
                    "nearest_city.py",
                    additional_replacements=common_replacements
                ),
                unasync.Rule(
                    "test_async_nearest_city.py",
                    "test_nearest_city.py",
                    additional_replacements=common_replacements
                )
            ]
        )
        
        print("Transformation completed!")
        # Verify with special focus on import statements
        for output_file in ["pg_nearest_city/nearest_city.py", "tests/test_nearest_city.py"]:
            if os.path.exists(output_file):
                print(f"\nSuccessfully created: {output_file}")
            else:
                print(f"Warning: Expected output file not found: {output_file}")
                
    except Exception as e:
        print(f"Error during transformation: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    build_sync()
