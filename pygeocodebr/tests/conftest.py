"""
Pytest configuration and fixtures for PyGeocodeBR tests.
"""

import tempfile
from pathlib import Path
from typing import List

import pytest
import pandas as pd

from pygeocodebr.cache import set_cache_dir


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory for testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        original_cache = set_cache_dir(tmp_dir)
        yield Path(tmp_dir)
        set_cache_dir(None)


@pytest.fixture
def sample_addresses():
    """Sample Brazilian addresses for testing."""
    return [
        "Rua da Consolação, 1000, São Paulo, SP",
        "Avenida Paulista, 500, São Paulo, SP",
        "Rua XV de Novembro, 200, Centro, São Paulo, SP",
    ]


@pytest.fixture
def sample_ceps():
    """Sample CEPs for testing."""
    return ["01310-100", "01310-200", "20040-020"]


@pytest.fixture
def sample_coordinates():
    """Sample coordinates for reverse geocoding tests."""
    return [
        (-23.5505, -46.6333),  # São Paulo
        (-22.9068, -43.1729),  # Rio de Janeiro
    ]


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame with addresses for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "endereco": [
                "Rua da Consolação, 1000, São Paulo, SP",
                "Avenida Paulista, 500, São Paulo, SP",
                "Rua XV de Novembro, 200, Centro, São Paulo, SP",
            ],
            "municipio": ["São Paulo", "São Paulo", "São Paulo"],
            "estado": ["SP", "SP", "SP"],
        }
    )
