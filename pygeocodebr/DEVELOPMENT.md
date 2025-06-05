# PyGeocodeBR Development Guide

This document provides guidance for setting up the development environment and contributing to PyGeocodeBR.

## Development Setup

### Prerequisites

- Python 3.9 or higher
- Git
- [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager

### Installation for Development

1. Clone the repository:
```bash
git clone https://github.com/ipea/geocodebr.git
cd geocodebr/pygeocodebr
```

2. Install uv (if not already installed):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# or on Windows with PowerShell:
# powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

3. Install the package and dependencies with uv:
```bash
uv sync --dev
```

4. Activate the virtual environment:
```bash
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
# or use uv run for individual commands
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/pygeocodebr

# Run specific test file
uv run pytest tests/test_cache.py

# Run with verbose output
uv run pytest -v
```

### Code Quality

We use several tools to maintain code quality:

```bash
# Format code with black
uv run black src/ tests/

# Sort imports with isort
uv run isort src/ tests/

# Lint with flake8
uv run flake8 src/ tests/

# Type checking with mypy
uv run mypy src/

# Run all quality checks at once
uv run black src/ tests/ && uv run isort src/ tests/ && uv run flake8 src/ tests/ && uv run mypy src/
```

## Project Structure

```
pygeocodebr/
├── src/
│   └── pygeocodebr/
│       ├── __init__.py          # Package entry point
│       ├── api.py               # Main API functions
│       ├── cache.py             # Cache management
│       ├── config.py            # Configuration constants
│       ├── downloader.py        # Data downloading
│       ├── matching.py          # Geocoding matching logic (TODO)
│       ├── standardizer.py      # Address standardization (TODO)
│       ├── tie_breaker.py       # Tie-breaking logic (TODO)
│       └── utils.py             # Utility functions
├── tests/
│   ├── conftest.py              # Test configuration
│   ├── test_cache.py            # Cache tests
│   ├── test_utils.py            # Utility tests
│   └── fixtures/                # Test data
├── examples/
│   └── basic_usage.py           # Usage examples
├── pyproject.toml               # Project configuration
├── README.md                    # User documentation
└── DEVELOPMENT.md               # This file
```

## Implementation Status

### ✅ Completed Modules

- **Cache Management (`cache.py`)**: Full implementation with persistent configuration
- **Configuration (`config.py`)**: Constants and data release information
- **Download System (`downloader.py`)**: GitHub release downloading with progress bars
- **Utilities (`utils.py`)**: Distance calculations, database helpers
- **Basic API (`api.py`)**: Simplified implementations of main functions

### 🚧 Partially Implemented

- **API Functions**: Basic structure exists but needs full geocoding logic
- **Testing**: Basic tests for utilities and cache, needs more comprehensive coverage

### ❌ Todo (High Priority)

1. **Address Standardization (`standardizer.py`)**
   - Port `enderecobr` functionality
   - Implement Brazilian address parsing and standardization
   - Handle common address format variations

2. **Geocoding Matching (`matching.py`)**
   - Implement SQL-based matching logic from R version
   - Port all match types (dn01, dn02, etc.)
   - Optimize DuckDB queries

3. **Tie Breaking (`tie_breaker.py`)**
   - Handle multiple geocoding results
   - Implement distance-based ranking
   - Port tie-breaking logic from R

4. **Enhanced API Functions**
   - Complete `geocode()` function implementation
   - Improve `reverse_geocode()` with spatial queries
   - Add proper error handling and validation

### ❌ Todo (Lower Priority)

1. **Documentation**
   - API documentation with Sphinx
   - More usage examples
   - Performance benchmarks

2. **CI/CD**
   - GitHub Actions workflow
   - Automated testing
   - Package publishing

3. **Performance Optimization**
   - Parallel processing
   - Memory optimization for large datasets
   - Query optimization

## Architecture Decisions

### Database Backend
- **DuckDB**: Chosen for compatibility with R version and excellent Parquet support
- In-memory operations for performance
- SQL queries maintain compatibility with R implementation

### Data Processing
- **Polars**: Preferred over Pandas for better performance
- **PyArrow**: Direct integration with Parquet files
- **GeoPandas**: For spatial operations and GeoDataFrame output

### Cache Strategy
- **platformdirs**: Cross-platform cache directories
- Persistent configuration across sessions
- Versioned data releases

## Contributing

### Pull Request Process

1. Create a feature branch from `main`
2. Implement your changes with tests
3. Ensure all tests pass and code quality checks pass
4. Update documentation if necessary
5. Submit a pull request with a clear description

### Coding Standards

- Follow PEP 8 style guidelines
- Use type hints for all function signatures
- Write comprehensive docstrings with examples
- Maintain test coverage above 80%
- Keep functions focused and well-documented

### Testing Guidelines

- Write unit tests for all new functions
- Include integration tests for API functions
- Use descriptive test names
- Mock external dependencies (downloads, file I/O)
- Test both success and error cases

## Next Steps for Development

1. **Implement Address Standardization**
   - Study the `enderecobr` R package
   - Create regex-based standardization rules
   - Handle Brazilian address formats

2. **Port Geocoding Logic**
   - Translate SQL queries from R match_* functions
   - Implement iterative matching process
   - Add progress tracking

3. **Add Comprehensive Testing**
   - Create test fixtures with sample CNEFE data
   - Test all match types and edge cases
   - Add performance benchmarks

4. **Documentation and Examples**
   - Complete API documentation
   - Add Jupyter notebook examples
   - Create migration guide from R version

## Getting Help

- Review the original R package documentation
- Check existing issues on GitHub
- Run tests to understand expected behavior
- Use the examples for reference implementations 