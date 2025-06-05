"""
Utility functions for PyGeocodeBR.
"""

import math
from typing import Any, Dict, List, Optional, Tuple, Union

import duckdb
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
    update_table: str = "input_padrao_db",
    reference_table: str = "output_db",
) -> None:
    """
    Update input database to remove observations that were previously matched.

    Args:
        con: DuckDB connection
        update_table: Name of table to update
        reference_table: Reference table containing matched records

    Examples:
        >>> con = duckdb.connect()
        >>> update_input_db(con, "input_padrao_db", "output_caso_1")
    """
    query = f"""
    DELETE FROM {update_table}
    WHERE tempidgeocodebr IN (SELECT tempidgeocodebr FROM {reference_table});
    """
    con.execute(query)


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
        ['municipio', 'logradouro', 'numero']
    """
    if match_type.startswith("dn") or match_type.startswith("pn"):
        return ["municipio", "logradouro", "numero"]
    elif match_type.startswith("da") or match_type.startswith("pa"):
        return ["municipio", "logradouro", "numero"]
    elif match_type.startswith("dl") or match_type.startswith("pl"):
        return ["municipio", "logradouro"]
    elif match_type.startswith("dc"):
        return ["municipio", "cep"]
    elif match_type.startswith("db"):
        return ["municipio", "localidade"]
    elif match_type.startswith("dm"):
        return ["municipio"]
    else:
        return ["municipio"]


def get_reference_table(match_type: str) -> str:
    """
    Get the reference table name for a specific match type.

    Args:
        match_type: The geocoding match type

    Returns:
        Name of the reference table to use

    Examples:
        >>> get_reference_table("dn01")
        'municipio_logradouro_numero_localidade'
    """
    if match_type.startswith("dn") or match_type.startswith("pn"):
        return "municipio_logradouro_numero_localidade"
    elif match_type.startswith("da") or match_type.startswith("pa"):
        return "municipio_logradouro_numero_localidade"
    elif match_type.startswith("dl") or match_type.startswith("pl"):
        return "municipio_logradouro_localidade"
    elif match_type.startswith("dc"):
        return "municipio_cep_localidade"
    elif match_type.startswith("db"):
        return "municipio_localidade"
    elif match_type.startswith("dm"):
        return "municipio"
    else:
        return "municipio"


def merge_results(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    output_table: str,
    key_column: str,
    select_columns: List[str],
    complete_result: bool = False,
) -> pl.DataFrame:
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

    result = con.execute(query).pl()
    return result
