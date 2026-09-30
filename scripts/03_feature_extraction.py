"""Join the prepared tables and create one analysis-ready row per post."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from tqdm import tqdm

from pipeline_utils import (
    add_common_arguments,
    copy_query_to_parquet,
    open_duckdb,
    parquet_scan,
    require_files,
    should_build,
)


# -----------------------------------------------------------------------------
# Input paths
# -----------------------------------------------------------------------------

def input_paths(data_dir: Path) -> dict[str, Path]:
    """Return the three Parquet files that are joined into the feature table."""
    return {
        "entries": data_dir / "parquet" / "clean" / "entries.parquet",
        "engagement": data_dir / "parquet" / "aggregated" / "post_engagement.parquet",
        "authors": data_dir / "parquet" / "aggregated" / "author_statistics.parquet",
    }


# -----------------------------------------------------------------------------
# Feature table SQL
# -----------------------------------------------------------------------------

def base_feature_query(paths: dict[str, Path], top_sources: int) -> str:
    """Create text, time, content, source, and author features without using the target."""
    entries = parquet_scan(paths["entries"])
    engagement = parquet_scan(paths["engagement"])
    authors = parquet_scan(paths["authors"])

    return f"""
        WITH unique_entries AS (
            SELECT * EXCLUDE (duplicate_number)
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY post_id
                        ORDER BY posted_at NULLS LAST, source_file
                    ) AS duplicate_number
                FROM {entries}
                WHERE identifier_is_valid
            )
            WHERE duplicate_number = 1
        ),
        popular_sources AS (
            SELECT coalesce(NULLIF(TRIM(source_name), ''), 'FriendFeed') AS source_name
            FROM unique_entries
            GROUP BY 1
            ORDER BY count(*) DESC, source_name
            LIMIT {max(1, top_sources)}
        ),
        joined AS (
            SELECT
                entries.*,
                engagement.has_full_7d_window,
                engagement.comment_count_recorded,
                engagement.like_count_recorded,
                engagement.comment_count_7d,
                engagement.like_count_7d,
                engagement.engagement_count_7d,
                authors.user_type,
                authors.has_profile_description,
                authors.post_count AS author_post_count,
                authors.comments_written AS author_comments_written,
                authors.likes_given AS author_likes_given,
                authors.connected_service_count,
                authors.follower_count,
                authors.following_count,
                authors.new_follower_count,
                authors.network_snapshot_available,
                regexp_replace(coalesce(entries.post_text, ''), '<[^>]+>', ' ', 'g')
                    AS visible_text,
                coalesce(NULLIF(TRIM(entries.source_name), ''), 'FriendFeed')
                    AS normalized_source
            FROM unique_entries AS entries
            INNER JOIN {engagement} AS engagement USING (post_id)
            LEFT JOIN {authors} AS authors ON entries.author_id = authors.author_id
        ),
        numeric_features AS (
            SELECT
                *,
                length(visible_text) AS character_count,
                list_count(regexp_extract_all(visible_text, '[[:alnum:]_]+')) AS word_count,
                list_count(regexp_extract_all(visible_text, '#[[:alnum:]_]+')) AS hashtag_count,
                list_count(regexp_extract_all(visible_text, 'https?://')) AS url_count,
                coalesce(image_count, 0) > 0 AS has_image,
                coalesce(video_count, 0) > 0 AS has_video,
                extract(hour FROM posted_at)::INTEGER AS post_hour,
                extract(isodow FROM posted_at)::INTEGER AS weekday_number,
                strftime(posted_at, '%A') AS weekday_name,
                extract(isodow FROM posted_at) IN (6, 7) AS is_weekend,
                CASE
                    WHEN normalized_source IN (SELECT source_name FROM popular_sources)
                        THEN normalized_source
                    ELSE 'Other'
                END AS source_group
            FROM joined
        )
        SELECT
            post_id,
            author_id,
            posted_at,
            normalized_source AS source_name,
            source_group,
            coalesce(image_count, 0) AS image_count,
            coalesce(video_count, 0) AS video_count,
            character_count,
            word_count,
            hashtag_count,
            url_count,
            has_image,
            has_video,
            post_hour,
            weekday_number,
            weekday_name,
            is_weekend,
            user_type,
            coalesce(has_profile_description, false) AS has_profile_description,
            coalesce(author_post_count, 0) AS author_post_count,
            coalesce(author_comments_written, 0) AS author_comments_written,
            coalesce(author_likes_given, 0) AS author_likes_given,
            coalesce(connected_service_count, 0) AS connected_service_count,
            follower_count,
            following_count,
            coalesce(new_follower_count, 0) AS new_follower_count,
            coalesce(network_snapshot_available, false) AS network_snapshot_available,
            comment_count_recorded,
            like_count_recorded,
            comment_count_7d,
            like_count_7d,
            engagement_count_7d,
            has_full_7d_window,
            CASE
                WHEN character_count = 0 THEN 'empty'
                WHEN character_count < 80 THEN 'short'
                WHEN character_count < 200 THEN 'medium'
                WHEN character_count < 500 THEN 'long'
                ELSE 'very_long'
            END AS post_length_band,
            CASE
                WHEN post_hour < 6 THEN 'night'
                WHEN post_hour < 12 THEN 'morning'
                WHEN post_hour < 18 THEN 'afternoon'
                ELSE 'evening'
            END AS time_of_day,
            CASE
                WHEN follower_count IS NULL THEN 'unknown'
                WHEN follower_count = 0 THEN '0'
                WHEN follower_count < 10 THEN '1-9'
                WHEN follower_count < 100 THEN '10-99'
                WHEN follower_count < 1000 THEN '100-999'
                ELSE '1000+'
            END AS follower_band,
            CASE
                WHEN author_post_count < 10 THEN '1-9'
                WHEN author_post_count < 100 THEN '10-99'
                WHEN author_post_count < 1000 THEN '100-999'
                ELSE '1000+'
            END AS author_activity_band,
            CASE
                WHEN follower_count IS NULL THEN NULL
                ELSE engagement_count_7d::DOUBLE / (follower_count + 1)
            END AS reach_adjusted_engagement,
            timestamp_in_study_window AND has_full_7d_window
                AND post_id IS NOT NULL AND author_id IS NOT NULL
                AS analysis_eligible
        FROM numeric_features
    """


def labelled_feature_query(base_query: str, raw_threshold: int, reach_threshold: float) -> str:
    """Add the two high-engagement labels after their thresholds have been measured."""
    return f"""
        SELECT
            *,
            analysis_eligible
                AND engagement_count_7d > 0
                AND engagement_count_7d >= {int(raw_threshold)} AS high_engagement,
            analysis_eligible
                AND reach_adjusted_engagement IS NOT NULL
                AND reach_adjusted_engagement > 0
                AND reach_adjusted_engagement >= {float(reach_threshold)}
                AS high_reach_adjusted_engagement
        FROM ({base_query}) AS features
    """


# -----------------------------------------------------------------------------
# Threshold selection
# -----------------------------------------------------------------------------

def choose_thresholds(connection, base_query: str, quantile: float) -> tuple[int, float]:
    """Find percentile cutoffs and prevent a zero-engagement post from being called high."""
    raw_value, reach_value = connection.execute(
        f"""
        SELECT
            quantile_disc(engagement_count_7d, {quantile}),
            quantile_cont(reach_adjusted_engagement, {quantile})
                FILTER (WHERE reach_adjusted_engagement > 0)
        FROM ({base_query}) AS features
        WHERE analysis_eligible
        """
    ).fetchone()

    raw_threshold = max(1, int(raw_value or 0))
    reach_threshold = max(0.0, float(reach_value or 0.0))

    return raw_threshold, reach_threshold


def write_metadata(
    data_dir: Path,
    quantile: float,
    raw_threshold: int,
    reach_threshold: float,
    top_sources: int,
) -> None:
    """Save the choices that affect how the final feature table was produced."""
    metadata_path = data_dir / "quality" / "feature_metadata.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "high_engagement_quantile": quantile,
        "high_engagement_minimum_count": raw_threshold,
        "high_reach_adjusted_engagement_minimum": reach_threshold,
        "reach_adjusted_quantile_population": "posts with positive engagement",
        "top_source_categories": top_sources,
        "engagement_window_days": 7,
        "last_full_window_post_time": "2010-09-24T00:00:00",
    }
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Feature metadata: {metadata_path}")


# -----------------------------------------------------------------------------
# Main program
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Read feature settings and the shared pipeline options."""
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    parser.add_argument(
        "--high-quantile",
        type=float,
        default=0.90,
        help="Percentile used to define high engagement (default: 0.90).",
    )
    parser.add_argument(
        "--top-sources",
        type=int,
        default=20,
        help="Keep this many source services; combine the rest as Other.",
    )
    args = parser.parse_args()
    if not 0.5 <= args.high_quantile < 1.0:
        parser.error("--high-quantile must be at least 0.5 and below 1.0")
    return args


def main() -> None:
    """Build and save the complete post-level feature table."""
    args = parse_args()
    data_dir = args.data_dir.resolve()
    paths = input_paths(data_dir)
    require_files(list(paths.values()))

    output_path = data_dir / "parquet" / "features" / "post_features.parquet"
    if not should_build(output_path, args.force):
        print(f"Skipping feature extraction: {output_path.name} already exists")
        return

    connection = open_duckdb(data_dir, args.threads, args.memory_limit)
    try:
        base_query = base_feature_query(paths, args.top_sources)
        with tqdm(total=2, desc="Extracting post features", unit="stage") as progress:
            raw_threshold, reach_threshold = choose_thresholds(
                connection,
                base_query,
                args.high_quantile,
            )
            progress.update(1)
            final_query = labelled_feature_query(base_query, raw_threshold, reach_threshold)
            copy_query_to_parquet(connection, final_query, output_path)
            progress.update(1)
    finally:
        connection.close()

    write_metadata(
        data_dir,
        args.high_quantile,
        raw_threshold,
        reach_threshold,
        args.top_sources,
    )
    print(f"Feature table: {output_path}")


if __name__ == "__main__":
    main()
