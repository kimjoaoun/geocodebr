"""
Main API functions for PyGeocodeBR.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

import duckdb
import pandas as pd
import polars as pl
import geopandas as gpd

from .cache import get_cache_dir
from .config import (
    ALL_POSSIBLE_MATCH_TYPES,
    COMPLETE_OUTPUT_COLUMNS,
    DEFAULT_OUTPUT_COLUMNS,
)
from .utils import (
    add_precision_column,
    get_key_columns,
    get_reference_table,
    merge_results,
)


def search_by_cep(
    ceps: Union[str, List[str]], resultado_sf: bool = False, verbose: bool = True
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Search addresses by CEP (postal code).

    Args:
        ceps: CEP(s) to search for. Can be a single string or list of strings.
        resultado_sf: Whether to return results as a GeoDataFrame with spatial features.
        verbose: Whether to show progress messages.

    Returns:
        DataFrame or GeoDataFrame with address information for the given CEPs.

    Examples:
        >>> df = search_by_cep("01310-100")  # doctest: +SKIP
        >>> df = search_by_cep(["01310-100", "20040-020"])  # doctest: +SKIP
    """
    if isinstance(ceps, str):
        ceps = [ceps]

    cache_dir = get_cache_dir()
    cep_file = cache_dir / "municipio_cep_localidade.parquet"

    if not cep_file.exists():
        raise FileNotFoundError(
            f"CEP data file not found at {cep_file}. "
            "Please run pygeocodebr.download_cnefe() first."
        )

    con = duckdb.connect(":memory:")

    try:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW cep_data AS 
            SELECT * FROM read_parquet('{cep_file}')
        """
        )

        ceps_quoted = "', '".join(ceps)
        query = f"""
            SELECT 
                cep,
                endereco_completo,
                lat, 
                lon
            FROM cep_data 
            WHERE cep IN ('{ceps_quoted}')
        """

        if verbose:
            print(f"Searching for {len(ceps)} CEP(s)...")

        result_df = con.execute(query).df()

        if resultado_sf and not result_df.empty:
            geometry = gpd.points_from_xy(result_df["lon"], result_df["lat"])
            result_gdf = gpd.GeoDataFrame(result_df, geometry=geometry, crs="EPSG:4326")
            return result_gdf

        return result_df

    finally:
        con.close()


def geocode(
    enderecos: Union[pd.DataFrame, List[str]],
    campos_endereco: Optional[Dict[str, Optional[str]]] = None,
    resultado_completo: bool = False,
    resultado_sf: bool = False,
    resolver_empates: bool = False,
    verbose: bool = True,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Geocode Brazilian addresses using CNEFE data.

    Args:
        enderecos: Addresses to geocode. Can be a DataFrame with address columns
                  or a list of address strings.
        campos_endereco: Field mapping dictionary from define_fields(). If None,
                        assumes enderecos is a list of address strings.
        resultado_completo: Whether to return complete result with all columns.
        resultado_sf: Whether to return results as a GeoDataFrame.
        resolver_empates: Whether to handle tie-breaking for multiple matches.
        verbose: Whether to show progress messages.

    Returns:
        DataFrame or GeoDataFrame with geocoded coordinates and metadata.

    Examples:
        >>> addresses = ["Rua da Consolação, 1000, São Paulo, SP"]
        >>> df = geocode(addresses)  # doctest: +SKIP
    """
    from .standardizer import standardize_addresses, define_fields
    from .matching import select_match_function

    if verbose:
        print("Starting geocoding process...")

    # Check if CNEFE data is available
    cache_dir = get_cache_dir()
    required_files = [
        f"{table}.parquet"
        for table in [
            "municipio_logradouro_numero_cep_localidade",
            "municipio_logradouro_numero_localidade",
            "municipio_logradouro_cep_localidade",
            "municipio_logradouro_localidade",
            "municipio_cep_localidade",
            "municipio_localidade",
            "municipio",
        ]
    ]

    missing_files = [f for f in required_files if not (cache_dir / f).exists()]
    if missing_files:
        raise FileNotFoundError(
            f"Required data files not found: {missing_files[:3]}... "
            "Please run pygeocodebr.download_cnefe() first."
        )

    # Handle input data format
    if isinstance(enderecos, str):
        # Single string - convert to list
        enderecos = [enderecos]

    if isinstance(enderecos, list):
        # Simple address strings - create basic structure
        enderecos_df = pd.DataFrame({"endereco": enderecos})
        if campos_endereco is None:
            # For simple strings, assume they need to be parsed
            campos_endereco = define_fields(
                estado="endereco",  # Will be parsed from endereco column
                municipio="endereco",
                logradouro="endereco",
                numero="endereco",
                cep="endereco",
                localidade="endereco",
            )
            # Will be parsed by standardizer
    else:
        enderecos_df = enderecos.copy()
        if campos_endereco is None:
            raise ValueError(
                "campos_endereco must be provided when enderecos is a DataFrame"
            )

    # Standardize addresses
    if verbose:
        print("Standardizing addresses...")

    input_padrao = standardize_addresses(enderecos_df, campos_endereco)

    # Create DuckDB connection
    con = duckdb.connect(":memory:")

    try:
        # Register standardized input data
        con.execute("CREATE TABLE input_padrao_db AS SELECT * FROM input_padrao")

        # Create empty output table with proper schema
        output_schema_cols = [
            "tempidgeocodebr INTEGER",
            "lat REAL",
            "lon REAL",
            "endereco_encontrado TEXT",
            "logradouro_encontrado TEXT",
            "tipo_resultado TEXT",
            "contagem_cnefe INTEGER",
        ]

        if resultado_completo:
            output_schema_cols.extend(
                [
                    "numero_encontrado INTEGER",
                    "localidade_encontrada TEXT",
                    "cep_encontrado TEXT",
                    "municipio_encontrado TEXT",
                    "estado_encontrado TEXT",
                    "similaridade_logradouro REAL",
                ]
            )

        output_schema = ", ".join(output_schema_cols)
        con.execute(f"CREATE TABLE output_db ({output_schema})")

        # Start matching process
        if verbose:
            print(f"Geocoding {len(input_padrao)} addresses...")
            print("Looking for matches...")

        n_rows = len(input_padrao)
        matched_rows = 0

        # Iterate through all possible match types
        for match_type in ALL_POSSIBLE_MATCH_TYPES:
            key_cols = get_key_columns(match_type)

            if verbose:
                print(f"-> Trying match type: {match_type}")

            # Check if all required columns exist in input
            if all(col in input_padrao.columns for col in key_cols):
                # Select appropriate match function
                match_function = select_match_function(match_type)

                # Execute matching
                n_rows_affected = match_function(
                    con=con,
                    match_type=match_type,
                    key_cols=key_cols,
                    resultado_completo=resultado_completo,
                    verbose=verbose,
                )

                matched_rows += n_rows_affected

                # Stop early if all addresses are matched
                if matched_rows >= n_rows:
                    break
            else:
                if verbose:
                    missing_cols = [
                        col for col in key_cols if col not in input_padrao.columns
                    ]
                    print(f"  - Skipping {match_type}: missing columns {missing_cols}")

        if verbose:
            print(f"Matched {matched_rows} out of {n_rows} addresses")
            print("Preparing output...")

        # Add precision column
        add_precision_column(con, "output_db")

        # Prepare original input with tempidgeocodebr for merging
        enderecos_with_id = enderecos_df.copy()
        enderecos_with_id["tempidgeocodebr"] = range(len(enderecos_with_id))

        # Register original input for merging
        con.register("enderecos_with_id", enderecos_with_id)
        con.execute("CREATE TABLE input_db AS SELECT * FROM enderecos_with_id")

        # Merge results with original input
        output_df = merge_results(
            con=con,
            input_table="input_db",
            output_table="output_db",
            key_column="tempidgeocodebr",
            select_columns=list(enderecos_df.columns),
            complete_result=resultado_completo,
        )

        # Handle ties/empates if needed
        if len(output_df) > n_rows and resolver_empates:
            if verbose:
                print("Resolving ties...")
            # Placeholder for tie resolution logic
            # For now, just take the first match for each address
            output_df = output_df.groupby("tempidgeocodebr").first().reset_index()

        # Remove temporary ID column
        if "tempidgeocodebr" in output_df.columns:
            output_df = output_df.drop("tempidgeocodebr", axis=1)

        # Remove logradouro_encontrado if not complete result
        if not resultado_completo and "logradouro_encontrado" in output_df.columns:
            output_df = output_df.drop("logradouro_encontrado", axis=1)

        # Convert to spatial format if requested
        if resultado_sf and not output_df.empty:
            valid_coords = output_df[["lat", "lon"]].notna().all(axis=1)
            if valid_coords.any():
                geometry = gpd.points_from_xy(
                    output_df.loc[valid_coords, "lon"],
                    output_df.loc[valid_coords, "lat"],
                )
                result_gdf = gpd.GeoDataFrame(output_df, geometry=None, crs="EPSG:4674")
                result_gdf.loc[valid_coords, "geometry"] = geometry
                return result_gdf

        return output_df

    finally:
        con.close()


def reverse_geocode(
    points: Union[pd.DataFrame, List[tuple]],
    buffer_radius: float = 100.0,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Reverse geocode coordinates to find nearby addresses.

    Args:
        points: Points to reverse geocode. Can be a DataFrame with lat/lon columns
               or a list of (lat, lon) tuples.
        buffer_radius: Search radius in meters.
        verbose: Whether to show progress messages.

    Returns:
        DataFrame with nearest addresses for each point.

    Examples:
        >>> points = [(-23.5505, -46.6333)]  # São Paulo coordinates
        >>> df = reverse_geocode(points)  # doctest: +SKIP
    """
    if isinstance(points, list):
        import pandas as pd

        points_df = pd.DataFrame(points, columns=["lat", "lon"])
        points_df["id"] = range(len(points_df))
    else:
        points_df = points.copy()

    if "id" not in points_df.columns:
        points_df["id"] = range(len(points_df))

    if verbose:
        print(f"Reverse geocoding {len(points_df)} points...")

    cache_dir = get_cache_dir()
    address_file = cache_dir / "municipio_logradouro_numero_localidade.parquet"

    if not address_file.exists():
        raise FileNotFoundError(
            f"Address data file not found at {address_file}. "
            "Please run pygeocodebr.download_cnefe() first."
        )

    con = duckdb.connect(":memory:")

    try:
        con.register("points_data", points_df)

        con.execute(
            f"""
            CREATE OR REPLACE VIEW address_data AS 
            SELECT * FROM read_parquet('{address_file}')
        """
        )

        result_df = con.execute("SELECT * FROM points_data").df()

        return result_df

    finally:
        con.close()
