"""
Utility functions for PyGeocodeBR.
"""

import math
from typing import Any, Dict, List, Optional, Tuple, Union

import duckdb
import pandas as pd
import polars as pl
from haversine import haversine, Unit

from .config import PRECISION_MAPPING


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the Haversine distance between two points on Earth.

    Args:
        lat1: Latitude of first point in degrees
        lon1: Longitude of first point in degrees
        lat2: Latitude of second point in degrees
        lon2: Longitude of second point in degrees

    Returns:
        Distance in kilometers

    Examples:
        >>> haversine_distance(-23.5505, -46.6333, -22.9068, -43.1729)  # SP to RJ
        357.7...
    """
    return haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)


def add_precision_column(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """
    Add a precision column to a table based on tipo_resultado.

    Args:
        con: DuckDB connection
        table_name: Name of the table to update

    Examples:
        >>> con = duckdb.connect()
        >>> add_precision_column(con, "output_db")
    """
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN precisao TEXT;")

    when_conditions = []
    for resultado_types, precision in PRECISION_MAPPING.items():
        if isinstance(resultado_types, str):
            when_conditions.append(
                f"WHEN tipo_resultado = '{resultado_types}' THEN '{precision}'"
            )

    update_query = f"""
    UPDATE {table_name}
    SET precisao = CASE
        WHEN tipo_resultado IN ('dn01', 'dn02', 'dn03', 'dn04',
                                'pn01', 'pn02', 'pn03', 'pn04') THEN 'numero'
        WHEN tipo_resultado IN ('da01', 'da02', 'da03', 'da04',
                                'pa01', 'pa02', 'pa03', 'pa04') THEN 'numero_aproximado'
        WHEN tipo_resultado IN ('dl01', 'dl02', 'dl03', 'dl04',
                                'pl01', 'pl02', 'pl03', 'pl04') THEN 'logradouro'
        WHEN tipo_resultado IN ('dc01', 'dc02') THEN 'cep'
        WHEN tipo_resultado = 'db01' THEN 'localidade'
        WHEN tipo_resultado = 'dm01' THEN 'municipio'
        ELSE NULL
    END;
    """

    con.execute(update_query)


def update_input_db(
    con: duckdb.DuckDBPyConnection,
    update_tb: str = "input_padrao_db",
    reference_tb: str = "output_db",
) -> int:
    """
    Update input database to remove observations that were previously matched.

    Args:
        con: DuckDB connection
        update_tb: Name of table to update (matches R parameter name)
        reference_tb: Reference table containing matched records (matches R parameter name)

    Returns:
        Number of rows affected (like R implementation)

    Examples:
        >>> con = duckdb.connect()
        >>> n_affected = update_input_db(con, "input_padrao_db", "output_caso_1")
    """
    # First count how many will be matched
    count_query = f"""
    SELECT COUNT(*) FROM {reference_tb}
    """
    n_matched = con.execute(count_query).fetchone()[0]

    # Then delete the matched rows
    query = f"""
    DELETE FROM {update_tb}
    WHERE tempidgeocodebr IN (SELECT tempidgeocodebr FROM {reference_tb});
    """
    con.execute(query)

    return n_matched


def create_index(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: List[str],
    operation: str = "CREATE",
    overwrite: bool = True,
) -> None:
    """
    Create an index on specified columns for better query performance.

    Args:
        con: DuckDB connection
        table: Table name
        columns: List of column names for the index
        operation: Type of operation ("CREATE")
        overwrite: Whether to overwrite existing index

    Examples:
        >>> con = duckdb.connect()
        >>> create_index(con, "cnefe_data", ["municipio", "logradouro"])
    """
    idx_name = f"idx_{table}"
    columns_str = ", ".join(columns)

    if overwrite:
        con.execute(f"DROP INDEX IF EXISTS {idx_name};")

    try:
        con.execute(f"CREATE INDEX {idx_name} ON {table} ({columns_str});")
    except Exception:
        pass


def safe_arrow_open_dataset(filename: str) -> Any:
    """
    Safely open a Parquet file using Arrow.

    Handles corruption and other failure modes gracefully.

    Args:
        filename: Path to the Parquet file

    Returns:
        Arrow Dataset object

    Raises:
        ValueError: If file is corrupted or cannot be opened

    Examples:
        >>> dataset = safe_arrow_open_dataset("data.parquet")  # doctest: +SKIP
    """
    try:
        import pyarrow as pa

        return pa.dataset.dataset(filename)
    except Exception as e:
        msg = (
            "Local file possibly corrupted. "
            "Delete cache files with 'pygeocodebr.delete_cache()' and try again."
        )
        raise ValueError(msg) from e


def get_key_columns(match_type: str) -> List[str]:
    """
    Get the key columns for a specific match type.

    Args:
        match_type: The geocoding match type (e.g., 'dn01', 'pa02')

    Returns:
        List of column names used as keys for this match type

    Examples:
        >>> get_key_columns("dn01")
        ['estado', 'municipio', 'logradouro', 'numero', 'cep', 'localidade']
    """
    if match_type in ["dn01", "da01", "pn01", "pa01"]:
        return ["estado", "municipio", "logradouro", "numero", "cep", "localidade"]
    elif match_type in ["dn02", "da02", "pn02", "pa02"]:
        return ["estado", "municipio", "logradouro", "numero", "cep"]
    elif match_type in ["dn03", "da03", "pn03", "pa03"]:
        return ["estado", "municipio", "logradouro", "numero", "localidade"]
    elif match_type in ["dn04", "da04", "pn04", "pa04"]:
        return ["estado", "municipio", "logradouro", "numero"]
    elif match_type in ["dl01", "pl01"]:
        return ["estado", "municipio", "logradouro", "cep", "localidade"]
    elif match_type in ["dl02", "pl02"]:
        return ["estado", "municipio", "logradouro", "cep"]
    elif match_type in ["dl03", "pl03"]:
        return ["estado", "municipio", "logradouro", "localidade"]
    elif match_type in ["dl04", "pl04"]:
        return ["estado", "municipio", "logradouro"]
    elif match_type == "dc01":
        return ["estado", "municipio", "cep", "localidade"]
    elif match_type == "dc02":
        return ["estado", "municipio", "cep"]
    elif match_type == "db01":
        return ["estado", "municipio", "localidade"]
    elif match_type == "dm01":
        return ["estado", "municipio"]
    else:
        return ["estado", "municipio"]


def get_reference_table(match_type: str) -> str:
    """
    Get the reference table name for a specific match type.

    Args:
        match_type: The geocoding match type

    Returns:
        Name of the reference table to use

    Examples:
        >>> get_reference_table("dn01")
        'municipio_logradouro_numero_cep_localidade'
    """
    if match_type in ["dn02", "pn02", "da02", "pa02", "dn03", "pn03"]:
        return "municipio_logradouro_numero_cep_localidade"
    elif match_type in ["da03", "pa03", "dn04", "da04", "pn04", "pa04"]:
        return "municipio_logradouro_numero_localidade"
    elif match_type in ["dl02", "pl02", "dl03", "pl03"]:
        return "municipio_logradouro_cep_localidade"
    elif match_type in ["dl04", "pl04"]:
        return "municipio_logradouro_localidade"
    elif match_type in ["dn01", "da01", "pn01", "pa01"]:
        return "municipio_logradouro_numero_cep_localidade"
    elif match_type in ["dl01", "pl01"]:
        return "municipio_logradouro_cep_localidade"
    elif match_type in ["dc01", "dc02"]:
        return "municipio_cep_localidade"
    elif match_type == "db01":
        return "municipio_localidade"
    elif match_type == "dm01":
        return "municipio"
    else:
        return "municipio"


# Match type categories (from R implementation)
NUMBER_EXACT_TYPES = ["dn01", "dn02", "dn03", "dn04"]
NUMBER_INTERPOLATION_TYPES = ["da01", "da02", "da03", "da04"]
PROBABILISTIC_EXACT_TYPES = ["pn01", "pn02", "pn03", "pn04"]
PROBABILISTIC_INTERPOLATION_TYPES = ["pa01", "pa02", "pa03", "pa04"]
EXACT_TYPES_NO_NUMBER = ["dl01", "dl02", "dl03", "dl04", "dc01", "dc02", "db01", "dm01"]
PROBABILISTIC_TYPES_NO_NUMBER = ["pl01", "pl02", "pl03", "pl04"]


def merge_results(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    output_table: str,
    key_column: str,
    select_columns: List[str],
    complete_result: bool = False,
) -> pd.DataFrame:
    """
    Merge geocoding results with original input data.

    Args:
        con: DuckDB connection
        input_table: Name of input table
        output_table: Name of output table with results
        key_column: Column to join on
        select_columns: Columns to select from input
        complete_result: Whether to include complete result columns

    Returns:
        Merged results as Polars DataFrame

    Examples:
        >>> con = duckdb.connect()
        >>> df = merge_results(con, "input", "output", "id", ["endereco"])
    """
    select_columns_output = [
        "lat",
        "lon",
        "tipo_resultado",
        "precisao",
        "endereco_encontrado",
        "logradouro_encontrado",
        "contagem_cnefe",
    ]

    if complete_result:
        select_columns_output.extend(
            [
                "numero_encontrado",
                "cep_encontrado",
                "localidade_encontrada",
                "municipio_encontrado",
                "estado_encontrado",
                "similaridade_logradouro",
            ]
        )

        con.execute(
            """
        UPDATE output_db
        SET similaridade_logradouro = COALESCE(similaridade_logradouro, 1);
        """
        )

    select_input = ", ".join([f"{input_table}.{col}" for col in select_columns])
    select_output = ", ".join([f"sorted_output.{col}" for col in select_columns_output])

    query = f"""
    SELECT {select_input}, {select_output}
    FROM {input_table}
    LEFT JOIN (
        SELECT * FROM {output_table}
        ORDER BY tempidgeocodebr
    ) AS sorted_output
    ON {input_table}.{key_column} = sorted_output.{key_column};
    """

    result = con.execute(query).df()
    return result
