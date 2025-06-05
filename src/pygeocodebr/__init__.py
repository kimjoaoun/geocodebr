"""
PyGeocodeBR - Python port of geocodebr R package

A high-quality geocoding library for Brazilian addresses using CNEFE data.
"""

from .api import geocode, reverse_geocode, search_by_cep
from .cache import get_cache_dir, set_cache_dir, delete_cache, list_cached_data
from .downloader import download_cnefe

__version__ = "0.1.0"
__author__ = "IPEA"

__all__ = [
    "geocode",
    "reverse_geocode",
    "search_by_cep",
    "download_cnefe",
    "get_cache_dir",
    "set_cache_dir",
    "delete_cache",
    "list_cached_data",
]
