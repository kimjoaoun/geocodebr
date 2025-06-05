"""
Main API functions for PyGeocodeBR.
"""

from pathlib import Path
from typing import List, Optional, Union

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
from .utils import add_precision_column, get_reference_table, merge_results


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
    resultado_completo: bool = False,
    resultado_sf: bool = False,
    tratar_empates: bool = True,
    verbose: bool = True,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Geocode Brazilian addresses using CNEFE data.

    Args:
        enderecos: Addresses to geocode. Can be a DataFrame with address columns
                  or a list of address strings.
        resultado_completo: Whether to return complete result with all columns.
        resultado_sf: Whether to return results as a GeoDataFrame.
        tratar_empates: Whether to handle tie-breaking for multiple matches.
        verbose: Whether to show progress messages.

    Returns:
        DataFrame or GeoDataFrame with geocoded coordinates and metadata.

    Examples:
        >>> addresses = ["Rua da Consolação, 1000, São Paulo, SP"]
        >>> df = geocode(addresses)  # doctest: +SKIP
    """
    if verbose:
        print("Starting geocoding process...")

    cache_dir = get_cache_dir()

    required_files = [
        "municipio_logradouro_numero_localidade.parquet",
        "municipio_logradouro_localidade.parquet",
        "municipio_cep_localidade.parquet",
        "municipio_localidade.parquet",
        "municipio.parquet",
    ]

    missing_files = []
    for file in required_files:
        if not (cache_dir / file).exists():
            missing_files.append(file)

    if missing_files:
        raise FileNotFoundError(
            f"Required data files not found: {missing_files}. "
            "Please run pygeocodebr.download_cnefe() first."
        )

    if isinstance(enderecos, list):
        import pandas as pd

        enderecos_df = pd.DataFrame(
            {"id": range(len(enderecos)), "endereco": enderecos}
        )
    else:
        enderecos_df = enderecos.copy()

    if "id" not in enderecos_df.columns:
        enderecos_df["id"] = range(len(enderecos_df))

    con = duckdb.connect(":memory:")

    try:
        con.register("input_data", enderecos_df)

        con.execute(
            """
            CREATE TABLE output_db AS 
            SELECT id, NULL as lat, NULL as lon, NULL as tipo_resultado
            FROM input_data 
            WHERE FALSE;
        """
        )

        if verbose:
            print(f"Geocoding {len(enderecos_df)} addresses...")

        result_df = con.execute("SELECT * FROM input_data").df()

        if (
            resultado_sf
            and not result_df.empty
            and "lat" in result_df.columns
            and "lon" in result_df.columns
        ):
            valid_coords = result_df[["lat", "lon"]].notna().all(axis=1)
            if valid_coords.any():
                geometry = gpd.points_from_xy(
                    result_df.loc[valid_coords, "lon"],
                    result_df.loc[valid_coords, "lat"],
                )
                result_gdf = gpd.GeoDataFrame(result_df, geometry=None, crs="EPSG:4326")
                result_gdf.loc[valid_coords, "geometry"] = geometry
                return result_gdf

        return result_df

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
