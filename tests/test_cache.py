"""
Tests for cache management functionality.
"""

import tempfile
from pathlib import Path

import pytest

from pygeocodebr.cache import (
    get_cache_dir,
    set_cache_dir,
    delete_cache,
    list_cached_data,
    _get_default_cache_dir,
    _get_config_file_path,
)


def test_default_cache_dir():
    """Test that default cache directory is created correctly."""
    default_dir = _get_default_cache_dir()
    assert "pygeocodebr" in str(default_dir)
    assert "data_release_v0.2.0" in str(default_dir)


def test_config_file_path():
    """Test config file path generation."""
    config_path = _get_config_file_path()
    assert "pygeocodebr" in str(config_path)
    assert config_path.name == "cache_dir"


def test_set_and_get_cache_dir():
    """Test setting and getting cache directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_path = Path(tmp_dir) / "test_cache"

        set_result = set_cache_dir(test_path)
        assert set_result == test_path

        get_result = get_cache_dir()
        assert get_result == test_path


def test_set_cache_dir_none():
    """Test setting cache directory to None (default)."""
    result = set_cache_dir(None)
    default_dir = _get_default_cache_dir()
    assert result == default_dir


def test_list_cached_data_empty():
    """Test listing cached data when directory is empty."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        set_cache_dir(tmp_dir)
        cached_files = list_cached_data()
        assert cached_files == []


def test_list_cached_data_with_files(temp_cache_dir):
    """Test listing cached data with some files present."""
    test_file = temp_cache_dir / "test_file.parquet"
    test_file.touch()

    cached_files = list_cached_data()
    assert len(cached_files) == 1
    assert test_file in cached_files


def test_delete_cache(temp_cache_dir):
    """Test deleting cache directory."""
    test_file = temp_cache_dir / "test_file.parquet"
    test_file.touch()

    assert test_file.exists()

    deleted_dir = delete_cache()
    assert deleted_dir == temp_cache_dir
    assert not temp_cache_dir.exists()


def test_list_cached_data_print_tree(temp_cache_dir, capsys):
    """Test printing directory tree."""
    test_file = temp_cache_dir / "test_file.parquet"
    test_file.touch()

    sub_dir = temp_cache_dir / "subdir"
    sub_dir.mkdir()
    sub_file = sub_dir / "sub_file.txt"
    sub_file.touch()

    cached_files = list_cached_data(print_tree=True)

    captured = capsys.readouterr()
    assert temp_cache_dir.name in captured.out
    assert "test_file.parquet" in captured.out
    assert "subdir/" in captured.out

    assert len(cached_files) == 2


def test_cache_persistence():
    """Test that cache configuration persists across function calls."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_path = Path(tmp_dir) / "persistent_cache"

        set_cache_dir(test_path)

        retrieved_path = get_cache_dir()
        assert retrieved_path == test_path

        set_cache_dir(None)
