"""Read the raw FriendFeed files, clean their types, and write Parquet files."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm

from pipeline_utils import (
    add_common_arguments,
    copy_query_to_parquet,
    open_duckdb,
    parquet_scan,
    require_files,
    should_build,
    sql_string,
)


# -----------------------------------------------------------------------------
# Raw table definitions
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class TableSpec:
    """Describe where one raw table lives and how DuckDB should read it."""

    name: str
    filenames: tuple[str, ...]
    delimiter: str
    columns: tuple[str, ...]
    select_sql: str
    timestamp_flag: str | None = None


TABLES = (
    TableSpec(
        name="entries",
        filenames=("entries1.csv", "entries2.csv", "entries3.csv"),
        delimiter="\t",
        columns=(
            "post_id_raw", "author_id_raw", "source_name_raw", "source_url_raw",
            "geo_x_raw", "geo_y_raw", "posted_at_raw", "text_raw",
            "image_count_raw", "image_urls_raw", "video_count_raw", "video_urls_raw",
        ),
        select_sql="""
            WITH typed AS (
                SELECT
                    NULLIF(TRIM(post_id_raw), '') AS post_id,
                    NULLIF(TRIM(author_id_raw), '') AS author_id,
                    NULLIF(TRIM(source_name_raw), '') AS source_name,
                    NULLIF(TRIM(source_url_raw), '') AS source_url,
                    TRY_CAST(geo_x_raw AS DOUBLE) AS geo_x,
                    TRY_CAST(geo_y_raw AS DOUBLE) AS geo_y,
                    TRY_CAST(posted_at_raw AS TIMESTAMP) AS posted_at,
                    posted_at_raw,
                    text_raw AS post_text,
                    TRY_CAST(image_count_raw AS INTEGER) AS image_count,
                    NULLIF(TRIM(image_urls_raw), '') AS image_urls,
                    TRY_CAST(video_count_raw AS INTEGER) AS video_count,
                    NULLIF(TRIM(video_urls_raw), '') AS video_urls,
                    filename AS source_file
                FROM raw_data
            )
            SELECT
                *,
                post_id IS NOT NULL AND author_id IS NOT NULL AS identifier_is_valid,
                posted_at IS NOT NULL AND posted_at >= TIMESTAMP '2010-08-01'
                    AND posted_at < TIMESTAMP '2010-10-01' AS timestamp_in_study_window
            FROM typed
        """,
        timestamp_flag="timestamp_in_study_window",
    ),
    TableSpec(
        name="comments",
        filenames=("commentAugSept.csv",),
        delimiter="\t",
        columns=(
            "comment_id_raw", "entry_id_raw", "author_id_raw", "source_name_raw",
            "source_url_raw", "geo_x_raw", "geo_y_raw", "commented_at_raw",
            "text_raw",
        ),
        select_sql="""
            WITH typed AS (
                SELECT
                    NULLIF(TRIM(comment_id_raw), '') AS comment_id,
                    NULLIF(TRIM(entry_id_raw), '') AS entry_id,
                    NULLIF(TRIM(author_id_raw), '') AS author_id,
                    NULLIF(TRIM(source_name_raw), '') AS source_name,
                    NULLIF(TRIM(source_url_raw), '') AS source_url,
                    TRY_CAST(geo_x_raw AS DOUBLE) AS geo_x,
                    TRY_CAST(geo_y_raw AS DOUBLE) AS geo_y,
                    TRY_CAST(commented_at_raw AS TIMESTAMP) AS commented_at,
                    commented_at_raw,
                    text_raw AS comment_text,
                    filename AS source_file
                FROM raw_data
            )
            SELECT
                *,
                comment_id IS NOT NULL AND entry_id IS NOT NULL AS identifier_is_valid,
                commented_at IS NOT NULL AND commented_at >= TIMESTAMP '2010-08-01'
                    AND commented_at < TIMESTAMP '2010-10-01' AS timestamp_in_study_window
            FROM typed
        """,
        timestamp_flag="timestamp_in_study_window",
    ),
    TableSpec(
        name="likes",
        filenames=("likes.csv",),
        delimiter="\t",
        columns=("user_id_raw", "post_id_raw", "liked_at_raw"),
        select_sql="""
            WITH typed AS (
                SELECT
                    NULLIF(TRIM(user_id_raw), '') AS user_id,
                    NULLIF(TRIM(post_id_raw), '') AS post_id,
                    TRY_CAST(liked_at_raw AS TIMESTAMP) AS liked_at,
                    liked_at_raw,
                    filename AS source_file
                FROM raw_data
            )
            SELECT
                *,
                user_id IS NOT NULL AND post_id IS NOT NULL AS identifier_is_valid,
                liked_at IS NOT NULL AND liked_at >= TIMESTAMP '2008-01-01'
                    AND liked_at < TIMESTAMP '2010-10-17' AS timestamp_is_plausible
            FROM typed
        """,
        timestamp_flag="timestamp_is_plausible",
    ),
    TableSpec(
        name="following_events",
        filenames=("followingAugSept.csv",),
        delimiter="\t",
        columns=("followed_id_raw", "follower_id_raw", "observed_at_raw"),
        select_sql="""
            WITH typed AS (
                SELECT
                    NULLIF(TRIM(followed_id_raw), '') AS followed_id,
                    NULLIF(TRIM(follower_id_raw), '') AS follower_id,
                    TRY_CAST(observed_at_raw AS TIMESTAMP) AS observed_at,
                    observed_at_raw,
                    filename AS source_file
                FROM raw_data
            )
            SELECT
                *,
                followed_id IS NOT NULL AND follower_id IS NOT NULL AS identifier_is_valid,
                observed_at IS NOT NULL AND observed_at >= TIMESTAMP '2010-08-01'
                    AND observed_at < TIMESTAMP '2010-10-01' AS timestamp_in_study_window
            FROM typed
        """,
        timestamp_flag="timestamp_in_study_window",
    ),
    TableSpec(
        name="subscriptions",
        filenames=("subscriptions.csv",),
        delimiter=",",
        columns=("subscriber_id_raw", "subscribed_to_id_raw"),
        select_sql="""
            WITH typed AS (
                SELECT
                    NULLIF(TRIM(subscriber_id_raw), '') AS subscriber_id,
                    NULLIF(TRIM(subscribed_to_id_raw), '') AS subscribed_to_id,
                    filename AS source_file
                FROM raw_data
            )
            SELECT
                *,
                subscriber_id IS NOT NULL AND subscribed_to_id IS NOT NULL
                    AS identifier_is_valid
            FROM typed
        """,
    ),
    TableSpec(
        name="services",
        filenames=("services.csv",),
        delimiter="|",
        columns=(
            "user_id_raw", "service_id_raw", "service_name_raw", "service_url_raw",
            "username_on_service_raw", "user_url_on_service_raw",
        ),
        select_sql="""
            WITH typed AS (
                SELECT
                    NULLIF(TRIM(user_id_raw), '') AS user_id,
                    NULLIF(TRIM(service_id_raw), '') AS service_id,
                    NULLIF(TRIM(service_name_raw), '') AS service_name,
                    NULLIF(TRIM(service_url_raw), '') AS service_url,
                    NULLIF(TRIM(username_on_service_raw), '') AS username_on_service,
                    NULLIF(TRIM(user_url_on_service_raw), '') AS user_url_on_service,
                    filename AS source_file
                FROM raw_data
            )
            SELECT *, user_id IS NOT NULL AS identifier_is_valid
            FROM typed
        """,
    ),
    TableSpec(
        name="users",
        filenames=("users.csv",),
        delimiter="|",
        columns=("user_id_raw", "user_type_raw", "display_name_raw", "reserved_raw", "description_raw"),
        select_sql="""
            WITH typed AS (
                SELECT
                    NULLIF(TRIM(user_id_raw), '') AS user_id,
                    NULLIF(TRIM(user_type_raw), '') AS user_type,
                    NULLIF(TRIM(display_name_raw), '') AS display_name,
                    NULLIF(TRIM(reserved_raw), '') AS reserved_value,
                    NULLIF(TRIM(description_raw), '') AS user_description,
                    filename AS source_file
                FROM raw_data
            )
            SELECT *, user_id IS NOT NULL AS identifier_is_valid
            FROM typed
        """,
    ),
)


# -----------------------------------------------------------------------------
# Input discovery
# -----------------------------------------------------------------------------

def find_raw_directory(data_dir: Path) -> Path:
    """Use data/raw when it exists; otherwise use the current flat data folder."""
    raw_dir = data_dir / "raw"
    if raw_dir.is_dir():
        return raw_dir
    return data_dir


def raw_reader_sql(spec: TableSpec, raw_dir: Path) -> str:
    """Build one explicit DuckDB CSV reader instead of guessing the file format."""
    paths = [raw_dir / filename for filename in spec.filenames]
    path_sql = (
        sql_string(paths[0].resolve())
        if len(paths) == 1
        else "[" + ", ".join(sql_string(path.resolve()) for path in paths) + "]"
    )
    columns_sql = ", ".join(f"{sql_string(name)}: 'VARCHAR'" for name in spec.columns)
    reject_table = f"{spec.name}_reject_errors"
    reject_scan = f"{spec.name}_reject_scans"

    return f"""
        read_csv(
            {path_sql},
            auto_detect = false,
            header = false,
            delim = {sql_string(spec.delimiter)},
            quote = '',
            escape = '',
            columns = {{{columns_sql}}},
            nullstr = ['', '\\N', 'null', 'NULL'],
            store_rejects = true,
            rejects_table = {sql_string(reject_table)},
            rejects_scan = {sql_string(reject_scan)},
            rejects_limit = 10000,
            filename = true
        )
    """


# -----------------------------------------------------------------------------
# Table conversion
# -----------------------------------------------------------------------------

def count_saved_rejected_rows(reject_path: Path) -> int | None:
    """Count unique bad source lines in a reject report from an earlier run."""
    if not reject_path.is_file():
        return None
    with reject_path.open("r", encoding="utf-8", newline="") as handle:
        return len(
            {
                (row["file_id"], row["line"])
                for row in csv.DictReader(handle)
            }
        )


def convert_table(connection, spec: TableSpec, raw_dir: Path, output_dir: Path, force: bool) -> dict[str, object]:
    """Read one raw table, add quality flags, and save it as compressed Parquet."""
    output_path = output_dir / f"{spec.name}.parquet"
    reject_path = output_dir.parent.parent / "quality" / "csv_rejects" / f"{spec.name}.csv"
    rejected_csv_rows = None
    if not should_build(output_path, force):
        print(f"Skipping {spec.name}: {output_path.name} already exists")
        rejected_csv_rows = count_saved_rejected_rows(reject_path)
    else:
        source_sql = raw_reader_sql(spec, raw_dir)
        query = spec.select_sql.replace("FROM raw_data", f"FROM {source_sql} AS raw_data")
        copy_query_to_parquet(connection, query, output_path)

        reject_table = f"{spec.name}_reject_errors"
        rejected_csv_rows = connection.execute(
            f"SELECT count(DISTINCT (file_id, line)) FROM {reject_table}"
        ).fetchone()[0]
        reject_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_reject_path = reject_path.with_name(reject_path.name + ".tmp")
        temporary_reject_path.unlink(missing_ok=True)
        connection.execute(
            f"""
            COPY (SELECT * FROM {reject_table})
            TO {sql_string(temporary_reject_path)} (FORMAT CSV, HEADER true)
            """
        )
        temporary_reject_path.replace(reject_path)

    scan = parquet_scan(output_path)
    timestamp_sql = (
        f", count_if(NOT {spec.timestamp_flag}) AS invalid_timestamp_rows"
        if spec.timestamp_flag
        else ", NULL::BIGINT AS invalid_timestamp_rows"
    )
    values = connection.execute(
        f"""
        SELECT
            count(*) AS row_count,
            count_if(NOT identifier_is_valid) AS invalid_identifier_rows
            {timestamp_sql}
        FROM {scan}
        """
    ).fetchone()

    return {
        "table": spec.name,
        "row_count": values[0],
        "invalid_identifier_rows": values[1],
        "invalid_timestamp_rows": values[2],
        "rejected_csv_rows": rejected_csv_rows,
        "parquet_file": str(output_path.relative_to(output_dir.parent.parent)),
        "parquet_bytes": output_path.stat().st_size,
    }


# -----------------------------------------------------------------------------
# Quality report
# -----------------------------------------------------------------------------

def write_quality_report(rows: list[dict[str, object]], data_dir: Path) -> None:
    """Write a small CSV that shows how many records passed the basic checks."""
    report_path = data_dir / "quality" / "preprocessing_summary.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Quality summary: {report_path}")


# -----------------------------------------------------------------------------
# Main program
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Read command-line options for this script."""
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    return parser.parse_args()


def main() -> None:
    """Convert every source table in a predictable order."""
    args = parse_args()
    data_dir = args.data_dir.resolve()
    raw_dir = find_raw_directory(data_dir)
    clean_dir = data_dir / "parquet" / "clean"

    required = [raw_dir / filename for spec in TABLES for filename in spec.filenames]
    require_files(required)

    connection = open_duckdb(data_dir, args.threads, args.memory_limit)
    try:
        summaries = []
        for spec in tqdm(TABLES, desc="Preparing raw tables", unit="table"):
            summaries.append(convert_table(connection, spec, raw_dir, clean_dir, args.force))
        write_quality_report(summaries, data_dir)
    finally:
        connection.close()

    print("Preprocessing finished.")


if __name__ == "__main__":
    main()
