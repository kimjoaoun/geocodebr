"""
Download CNEFE data files from GitHub releases.
"""

import os
from pathlib import Path
from typing import List, Optional, Union
from urllib.parse import urljoin

import requests
from tqdm import tqdm

from .cache import get_cache_dir
from .config import ALL_FILES, GITHUB_RELEASE_URL


class DownloadError(Exception):
    """Raised when download fails."""

    pass


def download_cnefe(
    tabela: str = "todas",
    verbose: bool = True,
    cache: bool = True,
    force_download: bool = False,
) -> Path:
    """
    Download preprocessed and enriched CNEFE data.

    Downloads a preprocessed version of CNEFE (National Address File for
    Statistical Purposes) that has been created for use with this package.

    Args:
        tabela: Name of the table to download. Defaults to "todas" (all tables).
        verbose: Whether to show download progress and messages.
        cache: Whether to use cached data if available.
        force_download: Whether to force re-download even if files exist.

    Returns:
        Path to the directory where data was saved.

    Raises:
        DownloadError: If download fails for one or more files.
        ValueError: If invalid table name is provided.

    Examples:
        >>> download_cnefe(verbose=False)  # doctest: +SKIP
        >>> download_cnefe(tabela="municipio")  # doctest: +SKIP
    """
    all_files = ALL_FILES.copy()
    all_files_basename = [Path(f).stem for f in all_files]

    if tabela != "todas":
        if tabela not in all_files_basename:
            raise ValueError(f"Table must be one of: {', '.join(all_files_basename)}")

        all_files = [f for f in all_files if Path(f).stem == tabela]

    data_urls = [urljoin(GITHUB_RELEASE_URL + "/", filename) for filename in all_files]

    if not cache:
        import tempfile

        data_dir = Path(tempfile.mkdtemp(prefix="standardized_cnefe_"))
    else:
        data_dir = get_cache_dir()

    data_dir.mkdir(parents=True, exist_ok=True)

    existing_files = [f.name for f in data_dir.iterdir() if f.is_file()]

    if force_download:
        files_to_download = all_files
        urls_to_download = data_urls
    else:
        files_to_download = [f for f in all_files if f not in existing_files]
        urls_to_download = [
            url
            for url, filename in zip(data_urls, all_files)
            if filename in files_to_download
        ]

    if not files_to_download:
        if verbose:
            print("Using locally cached CNEFE data.")
        return data_dir

    downloaded_files = _download_files(
        data_dir, urls_to_download, files_to_download, verbose
    )

    return data_dir


def _download_files(
    data_dir: Path,
    urls_to_download: List[str],
    files_to_download: List[str],
    verbose: bool,
) -> List[Path]:
    """Download files with progress tracking."""
    if verbose:
        print("Downloading CNEFE data...")

    downloaded_files = []

    for url, filename in zip(urls_to_download, files_to_download):
        dest_path = data_dir / filename

        try:
            _download_single_file(url, dest_path, verbose)
            downloaded_files.append(dest_path)
        except Exception as e:
            raise DownloadError(f"Failed to download {filename}: {e}")

    return downloaded_files


def _download_single_file(url: str, dest_path: Path, verbose: bool) -> None:
    """Download a single file with progress bar."""
    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))

    with open(dest_path, "wb") as f:
        if verbose and total_size > 0:
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=dest_path.name,
                leave=False,
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        else:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def _verify_download(file_path: Path, expected_size: Optional[int] = None) -> bool:
    """Verify that a downloaded file is valid."""
    if not file_path.exists():
        return False

    if expected_size is not None and file_path.stat().st_size != expected_size:
        return False

    try:
        import pyarrow.parquet as pq

        pq.read_metadata(file_path)
        return True
    except Exception:
        return False
