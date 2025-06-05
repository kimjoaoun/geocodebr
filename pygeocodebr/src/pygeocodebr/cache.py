"""
Cache management for PyGeocodeBR

Handles caching of CNEFE data files with persistent configuration.
"""

import os
import shutil
from pathlib import Path
from typing import List, Optional, Union

import platformdirs

from .config import DATA_RELEASE


def _get_default_cache_dir() -> Path:
    """Get the default cache directory using platformdirs."""
    base_dir = platformdirs.user_cache_dir("pygeocodebr", "ipea")
    return Path(base_dir) / f"data_release_{DATA_RELEASE}"


def _get_config_file_path() -> Path:
    """Get the path to the cache configuration file."""
    config_dir = platformdirs.user_config_dir("pygeocodebr", "ipea")
    return Path(config_dir) / "cache_dir"


def set_cache_dir(path: Optional[Union[str, Path]] = None) -> Path:
    """
    Set the cache directory for PyGeocodeBR data.

    This configuration persists across Python sessions.

    Args:
        path: Path to the directory for caching data. If None, uses the default
              versioned directory.

    Returns:
        Path to the cache directory.

    Examples:
        >>> import tempfile
        >>> set_cache_dir(tempfile.gettempdir())
        >>> set_cache_dir(None)  # Reset to default
    """
    if path is None:
        cache_dir = _get_default_cache_dir()
    else:
        cache_dir = Path(path).resolve()

    print(f"Cache directory set to: {cache_dir}")

    config_file = _get_config_file_path()
    config_file.parent.mkdir(parents=True, exist_ok=True)

    with open(config_file, "w") as f:
        f.write(str(cache_dir))

    return cache_dir


def get_cache_dir() -> Path:
    """
    Get the current cache directory.

    Returns the cache directory configured with set_cache_dir() in a previous
    session, or the default cache directory if none was configured.

    Returns:
        Path to the cache directory.

    Examples:
        >>> get_cache_dir()
    """
    config_file = _get_config_file_path()

    if config_file.exists():
        with open(config_file, "r") as f:
            cache_dir = Path(f.read().strip())
    else:
        cache_dir = _get_default_cache_dir()

    return cache_dir


def list_cached_data(print_tree: bool = False) -> List[Path]:
    """
    List cached data files.

    Args:
        print_tree: Whether to print the cache directory contents in tree format.

    Returns:
        List of paths to cached files.

    Examples:
        >>> list_cached_data()
        >>> list_cached_data(print_tree=True)
    """
    cache_dir = get_cache_dir()

    if not cache_dir.exists():
        return []

    cached_files = list(cache_dir.rglob("*"))
    cached_files = [f for f in cached_files if f.is_file()]

    if print_tree:
        _print_directory_tree(cache_dir)
        return cached_files

    return cached_files


def delete_cache() -> Path:
    """
    Delete all files from the cache directory.

    Returns:
        Path to the cache directory that was deleted.

    Examples:
        >>> delete_cache()  # doctest: +SKIP
    """
    cache_dir = get_cache_dir()

    if cache_dir.exists():
        shutil.rmtree(cache_dir)

    print(f"Deleted cache directory: {cache_dir}")

    return cache_dir


def _print_directory_tree(
    directory: Path, prefix: str = "", max_depth: int = 10, current_depth: int = 0
) -> None:
    """Print a directory tree structure."""
    if current_depth > max_depth:
        return

    if current_depth == 0:
        print(f"{directory.name}/")

    try:
        contents = list(directory.iterdir())
        contents.sort(key=lambda x: (x.is_file(), x.name))

        for i, item in enumerate(contents):
            is_last = i == len(contents) - 1
            current_prefix = "└── " if is_last else "├── "
            print(f"{prefix}{current_prefix}{item.name}{'/' if item.is_dir() else ''}")

            if item.is_dir() and current_depth < max_depth:
                next_prefix = prefix + ("    " if is_last else "│   ")
                _print_directory_tree(item, next_prefix, max_depth, current_depth + 1)

    except PermissionError:
        print(f"{prefix}[Permission Denied]")
