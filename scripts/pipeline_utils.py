"""Small helpers shared by the numbered pipeline scripts."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb


# -----------------------------------------------------------------------------
# Default paths
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


# -----------------------------------------------------------------------------
# Command-line arguments
# -----------------------------------------------------------------------------

def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the data, CPU, memory, and overwrite options used by every script."""
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Folder containing the raw data and generated Parquet files.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=max(1, (os.cpu_count() or 2) - 1),
        help="Number of CPU threads DuckDB may use.",
    )
    parser.add_argument(
        "--memory-limit",
        default="8GB",
        help="Maximum memory DuckDB may use, for example 4GB or 12GB.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace output files that already exist.",
    )


# -----------------------------------------------------------------------------
# DuckDB setup
# -----------------------------------------------------------------------------

def sql_string(value: str | Path) -> str:
    """Turn plain text into a safely quoted DuckDB string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def open_duckdb(data_dir: Path, threads: int, memory_limit: str) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection and give it a local spill folder."""
    work_dir = data_dir.resolve() / "work"
    temp_dir = work_dir / "duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    connection.execute(f"SET threads = {max(1, threads)}")
    connection.execute(f"SET memory_limit = {sql_string(memory_limit)}")
    connection.execute(f"SET temp_directory = {sql_string(temp_dir)}")
    connection.execute("SET preserve_insertion_order = false")
    return connection


# -----------------------------------------------------------------------------
# Safe output helpers
# -----------------------------------------------------------------------------

def require_files(paths: list[Path]) -> None:
    """Stop with one readable error if any required input file is missing."""
    missing = [path for path in paths if not path.is_file()]
    if missing:
        formatted = "\n".join(f"  - {path}" for path in missing)
        raise FileNotFoundError(f"Required input files are missing:\n{formatted}")


def should_build(path: Path, force: bool) -> bool:
    """Say whether an output should be built, and create its parent folder."""
    path.parent.mkdir(parents=True, exist_ok=True)
    return force or not path.exists()


def copy_query_to_parquet(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    output_path: Path,
) -> None:
    """Run a query and atomically replace one compressed Parquet file with its result."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(output_path.name + ".tmp")
    temporary_path.unlink(missing_ok=True)

    connection.execute(
        f"""
        COPY ({query})
        TO {sql_string(temporary_path)} (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 3,
            ROW_GROUP_SIZE 122880
        )
        """
    )
    temporary_path.replace(output_path)


def parquet_scan(path: Path) -> str:
    """Return the SQL needed to read one Parquet file."""
    return f"read_parquet({sql_string(path.resolve())})"
