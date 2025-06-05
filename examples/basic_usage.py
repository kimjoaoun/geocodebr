"""
Basic usage examples for PyGeocodeBR.

This script demonstrates how to use the main functions of the library.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pygeocodebr as geo


def main():
    print("PyGeocodeBR - Basic Usage Examples")
    print("=" * 40)

    try:
        print("\n1. Cache Management")
        print("-" * 20)
        cache_dir = geo.get_cache_dir()
        print(f"Current cache directory: {cache_dir}")

        cached_files = geo.list_cached_data()
        print(f"Number of cached files: {len(cached_files)}")

        print("\n2. Download CNEFE Data")
        print("-" * 20)
        print("Note: This would download several GB of data in a real scenario")
        print("For demonstration, we'll skip the actual download")

        print("\n3. Search by CEP Example")
        print("-" * 20)
        sample_ceps = ["01310-100", "20040-020"]
        print(f"Would search for CEPs: {sample_ceps}")

        try:
            results = geo.search_by_cep(sample_ceps, verbose=True)
            print(f"Results shape: {results.shape}")
            print(results.head())
        except FileNotFoundError as e:
            print(f"Expected error (no data downloaded): {e}")

        print("\n4. Geocoding Example")
        print("-" * 20)
        sample_addresses = [
            "Rua da Consolação, 1000, São Paulo, SP",
            "Avenida Paulista, 500, São Paulo, SP",
        ]
        print(f"Would geocode addresses: {sample_addresses}")

        try:
            results = geo.geocode(sample_addresses, verbose=True)
            print(f"Results shape: {results.shape}")
            print(results.head())
        except FileNotFoundError as e:
            print(f"Expected error (no data downloaded): {e}")

        print("\n5. Reverse Geocoding Example")
        print("-" * 20)
        sample_points = [(-23.5505, -46.6333), (-22.9068, -43.1729)]
        print(f"Would reverse geocode points: {sample_points}")

        try:
            results = geo.reverse_geocode(sample_points, verbose=True)
            print(f"Results shape: {results.shape}")
            print(results.head())
        except FileNotFoundError as e:
            print(f"Expected error (no data downloaded): {e}")

        print("\nSuccessfully demonstrated PyGeocodeBR basic functionality!")
        print("To use with real data, run: geo.download_cnefe()")

    except Exception as e:
        print(f"Error running examples: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
