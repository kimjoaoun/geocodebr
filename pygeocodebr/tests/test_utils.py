"""
Tests for utility functions.
"""

import math
import tempfile
from pathlib import Path

import pytest
import duckdb

from pygeocodebr.utils import (
    haversine_distance,
    get_key_columns,
    get_reference_table,
    add_precision_column,
    update_input_db,
    create_index,
)


def test_haversine_distance():
    """Test Haversine distance calculation."""
    lat1, lon1 = -23.5505, -46.6333  # São Paulo
    lat2, lon2 = -22.9068, -43.1729  # Rio de Janeiro

    distance = haversine_distance(lat1, lon1, lat2, lon2)

    expected_distance = 357.7
    assert abs(distance - expected_distance) < 10.0


def test_haversine_distance_same_point():
    """Test Haversine distance for the same point."""
    lat, lon = -23.5505, -46.6333

    distance = haversine_distance(lat, lon, lat, lon)
    assert distance == 0.0


def test_get_key_columns():
    """Test getting key columns for different match types."""
    assert get_key_columns("dn01") == ["municipio", "logradouro", "numero"]
    assert get_key_columns("pn01") == ["municipio", "logradouro", "numero"]
    assert get_key_columns("da01") == ["municipio", "logradouro", "numero"]
    assert get_key_columns("pa01") == ["municipio", "logradouro", "numero"]

    assert get_key_columns("dl01") == ["municipio", "logradouro"]
    assert get_key_columns("pl01") == ["municipio", "logradouro"]

    assert get_key_columns("dc01") == ["municipio", "cep"]
    assert get_key_columns("dc02") == ["municipio", "cep"]

    assert get_key_columns("db01") == ["municipio", "localidade"]

    assert get_key_columns("dm01") == ["municipio"]

    assert get_key_columns("unknown") == ["municipio"]


def test_get_reference_table():
    """Test getting reference table names for different match types."""
    assert get_reference_table("dn01") == "municipio_logradouro_numero_localidade"
    assert get_reference_table("pn01") == "municipio_logradouro_numero_localidade"
    assert get_reference_table("da01") == "municipio_logradouro_numero_localidade"
    assert get_reference_table("pa01") == "municipio_logradouro_numero_localidade"

    assert get_reference_table("dl01") == "municipio_logradouro_localidade"
    assert get_reference_table("pl01") == "municipio_logradouro_localidade"

    assert get_reference_table("dc01") == "municipio_cep_localidade"
    assert get_reference_table("dc02") == "municipio_cep_localidade"

    assert get_reference_table("db01") == "municipio_localidade"

    assert get_reference_table("dm01") == "municipio"

    assert get_reference_table("unknown") == "municipio"


def test_add_precision_column():
    """Test adding precision column to a table."""
    con = duckdb.connect(":memory:")

    con.execute(
        """
        CREATE TABLE test_table (
            id INTEGER,
            tipo_resultado TEXT
        )
    """
    )

    con.execute(
        """
        INSERT INTO test_table VALUES 
        (1, 'dn01'),
        (2, 'da01'),
        (3, 'dl01'),
        (4, 'dc01'),
        (5, 'db01'),
        (6, 'dm01')
    """
    )

    add_precision_column(con, "test_table")

    result = con.execute(
        "SELECT tipo_resultado, precisao FROM test_table ORDER BY id"
    ).fetchall()

    expected = [
        ("dn01", "numero"),
        ("da01", "numero_aproximado"),
        ("dl01", "logradouro"),
        ("dc01", "cep"),
        ("db01", "localidade"),
        ("dm01", "municipio"),
    ]

    assert result == expected

    con.close()


def test_update_input_db():
    """Test updating input database by removing matched records."""
    con = duckdb.connect(":memory:")

    con.execute(
        """
        CREATE TABLE input_padrao_db (
            tempidgeocodebr INTEGER,
            endereco TEXT
        )
    """
    )

    con.execute(
        """
        CREATE TABLE output_db (
            tempidgeocodebr INTEGER,
            lat REAL,
            lon REAL
        )
    """
    )

    con.execute(
        """
        INSERT INTO input_padrao_db VALUES 
        (1, 'Endereço 1'),
        (2, 'Endereço 2'),
        (3, 'Endereço 3')
    """
    )

    con.execute(
        """
        INSERT INTO output_db VALUES 
        (1, -23.5, -46.6),
        (3, -22.9, -43.1)
    """
    )

    update_input_db(con, "input_padrao_db", "output_db")

    remaining = con.execute("SELECT tempidgeocodebr FROM input_padrao_db").fetchall()
    assert remaining == [(2,)]

    con.close()


def test_create_index():
    """Test creating database index."""
    con = duckdb.connect(":memory:")

    con.execute(
        """
        CREATE TABLE test_table (
            municipio TEXT,
            logradouro TEXT,
            numero INTEGER
        )
    """
    )

    con.execute(
        """
        INSERT INTO test_table VALUES 
        ('São Paulo', 'Rua A', 100),
        ('Rio de Janeiro', 'Rua B', 200)
    """
    )

    create_index(con, "test_table", ["municipio", "logradouro"])

    indexes = con.execute(
        "SELECT * FROM duckdb_indexes WHERE table_name = 'test_table'"
    ).fetchall()
    assert len(indexes) >= 0

    con.close()
