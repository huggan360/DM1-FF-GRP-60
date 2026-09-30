"""Aggregate post engagement and author/network statistics."""

from __future__ import annotations

import argparse
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
# Input and output paths
# -----------------------------------------------------------------------------

def clean_paths(data_dir: Path) -> dict[str, Path]:
    """Return the cleaned Parquet path for every table needed here."""
    clean_dir = data_dir / "parquet" / "clean"
    return {
        name: clean_dir / f"{name}.parquet"
        for name in (
            "entries", "comments", "likes", "following_events",
            "subscriptions", "services", "users",
        )
    }


# -----------------------------------------------------------------------------
# Engagement aggregation
# -----------------------------------------------------------------------------

def engagement_query(paths: dict[str, Path]) -> str:
    """Count likes and comments for each post, including a fair seven-day window."""
    entries = parquet_scan(paths["entries"])
    comments = parquet_scan(paths["comments"])
    likes = parquet_scan(paths["likes"])

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
        comment_counts AS (
            SELECT
                comments.entry_id AS post_id,
                count(*) AS comment_count_recorded,
                count_if(
                    entries.posted_at IS NOT NULL
                    AND comments.commented_at >= entries.posted_at
                    AND comments.commented_at < entries.posted_at + INTERVAL '7 days'
                ) AS comment_count_7d
            FROM {comments} AS comments
            INNER JOIN unique_entries AS entries
                ON comments.entry_id = entries.post_id
            WHERE comments.identifier_is_valid
            GROUP BY comments.entry_id
        ),
        like_counts AS (
            SELECT
                likes.post_id,
                count(*) AS like_count_recorded,
                count_if(
                    entries.posted_at IS NOT NULL
                    AND likes.liked_at >= entries.posted_at
                    AND likes.liked_at < entries.posted_at + INTERVAL '7 days'
                ) AS like_count_7d
            FROM {likes} AS likes
            INNER JOIN unique_entries AS entries
                ON likes.post_id = entries.post_id
            WHERE likes.identifier_is_valid
            GROUP BY likes.post_id
        )
        SELECT
            entries.post_id,
            entries.author_id,
            entries.posted_at,
            entries.timestamp_in_study_window,
            entries.posted_at IS NOT NULL
                AND entries.posted_at >= TIMESTAMP '2010-08-01'
                AND entries.posted_at < TIMESTAMP '2010-09-24'
                AS has_full_7d_window,
            coalesce(comments.comment_count_recorded, 0) AS comment_count_recorded,
            coalesce(likes.like_count_recorded, 0) AS like_count_recorded,
            coalesce(comments.comment_count_7d, 0) AS comment_count_7d,
            coalesce(likes.like_count_7d, 0) AS like_count_7d,
            coalesce(comments.comment_count_7d, 0)
                + coalesce(likes.like_count_7d, 0) AS engagement_count_7d
        FROM unique_entries AS entries
        LEFT JOIN comment_counts AS comments USING (post_id)
        LEFT JOIN like_counts AS likes USING (post_id)
    """


# -----------------------------------------------------------------------------
# Author aggregation
# -----------------------------------------------------------------------------

def author_query(paths: dict[str, Path]) -> str:
    """Build one row per posting author with activity, network, and profile counts."""
    entries = parquet_scan(paths["entries"])
    comments = parquet_scan(paths["comments"])
    likes = parquet_scan(paths["likes"])
    following = parquet_scan(paths["following_events"])
    subscriptions = parquet_scan(paths["subscriptions"])
    services = parquet_scan(paths["services"])
    users = parquet_scan(paths["users"])

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
        posting_authors AS (
            SELECT DISTINCT author_id
            FROM unique_entries
            WHERE author_id IS NOT NULL
        ),
        post_activity AS (
            SELECT
                author_id,
                count(*) AS post_count,
                min(posted_at) AS first_post_at,
                max(posted_at) AS last_post_at
            FROM unique_entries
            GROUP BY author_id
        ),
        comment_activity AS (
            SELECT author_id, count(*) AS comments_written
            FROM {comments}
            WHERE identifier_is_valid AND author_id IS NOT NULL
            GROUP BY author_id
        ),
        like_activity AS (
            SELECT user_id AS author_id, count(*) AS likes_given
            FROM {likes}
            WHERE identifier_is_valid
            GROUP BY user_id
        ),
        service_activity AS (
            SELECT
                user_id AS author_id,
                count(DISTINCT service_id) AS connected_service_count
            FROM {services}
            WHERE identifier_is_valid
            GROUP BY user_id
        ),
        follower_counts AS (
            SELECT
                subscribed_to_id AS author_id,
                count(DISTINCT subscriber_id) AS follower_count
            FROM {subscriptions}
            WHERE identifier_is_valid
            GROUP BY subscribed_to_id
        ),
        following_counts AS (
            SELECT
                subscriber_id AS author_id,
                count(DISTINCT subscribed_to_id) AS following_count
            FROM {subscriptions}
            WHERE identifier_is_valid
            GROUP BY subscriber_id
        ),
        new_follower_counts AS (
            SELECT
                followed_id AS author_id,
                count(DISTINCT follower_id) AS new_follower_count
            FROM {following}
            WHERE identifier_is_valid AND timestamp_in_study_window
            GROUP BY followed_id
        ),
        unique_users AS (
            SELECT * EXCLUDE (duplicate_number)
            FROM (
                SELECT
                    *,
                    row_number() OVER (PARTITION BY user_id ORDER BY source_file) AS duplicate_number
                FROM {users}
                WHERE identifier_is_valid
            )
            WHERE duplicate_number = 1
        )
        SELECT
            authors.author_id,
            users.user_type,
            users.user_description IS NOT NULL AS has_profile_description,
            coalesce(posts.post_count, 0) AS post_count,
            posts.first_post_at,
            posts.last_post_at,
            coalesce(comments.comments_written, 0) AS comments_written,
            coalesce(likes.likes_given, 0) AS likes_given,
            coalesce(services.connected_service_count, 0) AS connected_service_count,
            followers.follower_count,
            following_counts.following_count,
            coalesce(new_followers.new_follower_count, 0) AS new_follower_count,
            followers.follower_count IS NOT NULL AS network_snapshot_available
        FROM posting_authors AS authors
        LEFT JOIN post_activity AS posts USING (author_id)
        LEFT JOIN comment_activity AS comments USING (author_id)
        LEFT JOIN like_activity AS likes USING (author_id)
        LEFT JOIN service_activity AS services USING (author_id)
        LEFT JOIN follower_counts AS followers USING (author_id)
        LEFT JOIN following_counts USING (author_id)
        LEFT JOIN new_follower_counts AS new_followers USING (author_id)
        LEFT JOIN unique_users AS users ON authors.author_id = users.user_id
    """


# -----------------------------------------------------------------------------
# Main program
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Read command-line options for this script."""
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    return parser.parse_args()


def main() -> None:
    """Create the post and author aggregate files in a fixed order."""
    args = parse_args()
    data_dir = args.data_dir.resolve()
    paths = clean_paths(data_dir)
    require_files(list(paths.values()))

    output_dir = data_dir / "parquet" / "aggregated"
    jobs = (
        ("post engagement", output_dir / "post_engagement.parquet", engagement_query(paths)),
        ("author statistics", output_dir / "author_statistics.parquet", author_query(paths)),
    )

    connection = open_duckdb(data_dir, args.threads, args.memory_limit)
    try:
        for label, output_path, query in tqdm(jobs, desc="Aggregating tables", unit="table"):
            if should_build(output_path, args.force):
                copy_query_to_parquet(connection, query, output_path)
            else:
                print(f"Skipping {label}: {output_path.name} already exists")
    finally:
        connection.close()

    print("Aggregation finished.")


if __name__ == "__main__":
    main()
