# PyGeocodeBR

Python port of the [geocodebr](https://github.com/ipea/geocodebr) R package - A geocoding library for Brazilian addresses using CNEFE data.

## Overview

PyGeocodeBR provides high-quality geocoding for Brazilian addresses using data from the National Address File for Statistical Purposes (CNEFE) maintained by IBGE. This Python implementation offers the same functionality as the original R package with optimized performance using DuckDB and Arrow.

## Features

- **Precise Geocoding**: Match Brazilian addresses to latitude/longitude coordinates
- **Reverse Geocoding**: Find addresses from coordinates
- **CEP Search**: Look up addresses by postal code (CEP)
- **Multiple Match Types**: Support for various geocoding precision levels
- **Fast Performance**: Powered by DuckDB and Arrow for efficient data processing
- **Caching**: Smart caching system for CNEFE data files

## Installation

```bash
pip install pygeocodebr
```

## Quick Start

```python
import pygeocodebr as geo

# Download CNEFE data (first time only)
geo.download_cnefe()

# Geocode addresses
addresses = [
    "Rua da Consolação, 1000, São Paulo, SP",
    "Avenida Paulista, 500, São Paulo, SP"
]

results = geo.geocode(addresses)
print(results)
```

## API Reference

### Main Functions

- `geocode(enderecos, ...)`: Geocode Brazilian addresses
- `reverse_geocode(points, ...)`: Reverse geocoding from coordinates
- `search_by_cep(ceps, ...)`: Search addresses by CEP
- `download_cnefe()`: Download CNEFE data files

### Cache Management

- `get_cache_dir()`: Get current cache directory
- `set_cache_dir(path)`: Set custom cache directory
- `delete_cache()`: Clear all cached data

## Requirements

- Python 3.9+
- Internet connection for initial data download

## License

MIT License - see LICENSE file for details.

## Contributing

This is a Python port of the original R package. Please refer to the main geocodebr repository for contribution guidelines. 