"""Create the first data summaries and figures for the preliminary report."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm

from pipeline_utils import add_common_arguments, open_duckdb, parquet_scan, require_files


# -----------------------------------------------------------------------------
# Plot styling
# -----------------------------------------------------------------------------

def set_plot_style() -> None:
    """Give every generated chart the same plain, readable style."""
    plt.style.use("seaborn-v0_8-whitegrid")
    matplotlib.rcParams.update(
        {
            "figure.figsize": (9, 5),
            "axes.titlesize": 14,
            "axes.labelsize": 11,
            "figure.dpi": 130,
        }
    )


# -----------------------------------------------------------------------------
# Data summaries
# -----------------------------------------------------------------------------

def write_summary_tables(connection, feature_sql: str, output_dir: Path) -> None:
    """Write small CSV summaries so reported numbers can be checked later."""
    overview = connection.execute(
        f"""
        SELECT
            count(*) AS total_posts,
            count_if(analysis_eligible) AS eligible_posts,
            count_if(analysis_eligible AND engagement_count_7d = 0) AS zero_engagement_posts,
            count_if(high_engagement) AS high_engagement_posts,
            avg(engagement_count_7d) FILTER (WHERE analysis_eligible) AS mean_engagement_7d,
            quantile_disc(engagement_count_7d, [0.5, 0.75, 0.9, 0.95, 0.99])
                FILTER (WHERE analysis_eligible) AS engagement_quantiles,
            count_if(analysis_eligible AND follower_count IS NULL) AS missing_follower_posts
        FROM {feature_sql}
        """
    ).df()
    overview.to_csv(output_dir / "dataset_overview.csv", index=False)

    column_summary = connection.execute(
        f"SUMMARIZE SELECT * FROM {feature_sql}"
    ).df()
    column_summary.to_csv(output_dir / "column_summary.csv", index=False)


# -----------------------------------------------------------------------------
# Individual figures
# -----------------------------------------------------------------------------

def plot_engagement_distribution(connection, feature_sql: str, output_dir: Path) -> None:
    """Plot engagement counts with a log scale so the long tail remains visible."""
    frame = connection.execute(
        f"""
        SELECT least(engagement_count_7d, 50) AS engagement, count(*) AS posts
        FROM {feature_sql}
        WHERE analysis_eligible
        GROUP BY 1
        ORDER BY 1
        """
    ).df()
    fig, axis = plt.subplots()
    axis.bar(frame["engagement"], frame["posts"], width=0.9, color="#4c78a8")
    axis.set_yscale("log")
    axis.set_xlabel("Likes + comments in the first 7 days (50 means 50+)")
    axis.set_ylabel("Number of posts (log scale)")
    axis.set_title("FriendFeed engagement is strongly right-skewed")
    fig.tight_layout()
    fig.savefig(output_dir / "engagement_distribution.png")
    plt.close(fig)


def plot_engagement_by_source(connection, feature_sql: str, output_dir: Path) -> None:
    """Compare common source services without loading individual posts into Python."""
    frame = connection.execute(
        f"""
        SELECT
            source_group,
            count(*) AS posts,
            avg(engagement_count_7d) AS mean_engagement,
            avg(high_engagement::INTEGER) AS high_engagement_share
        FROM {feature_sql}
        WHERE analysis_eligible
        GROUP BY source_group
        HAVING count(*) >= 100
        ORDER BY posts DESC
        LIMIT 15
        """
    ).df().sort_values("high_engagement_share")
    fig, axis = plt.subplots()
    axis.barh(frame["source_group"], frame["high_engagement_share"], color="#f58518")
    axis.set_xlabel("Share of posts labelled high engagement")
    axis.set_ylabel("Source")
    axis.set_title("High engagement by common source service")
    fig.tight_layout()
    fig.savefig(output_dir / "high_engagement_by_source.png")
    plt.close(fig)


def plot_engagement_by_hour(connection, feature_sql: str, output_dir: Path) -> None:
    """Show whether posting hour is associated with average engagement."""
    frame = connection.execute(
        f"""
        SELECT
            post_hour,
            count(*) AS posts,
            avg(engagement_count_7d) AS mean_engagement,
            avg(high_engagement::INTEGER) AS high_engagement_share
        FROM {feature_sql}
        WHERE analysis_eligible
        GROUP BY post_hour
        ORDER BY post_hour
        """
    ).df()
    fig, axis = plt.subplots()
    axis.plot(frame["post_hour"], frame["high_engagement_share"], marker="o", color="#54a24b")
    axis.set_xticks(range(0, 24, 2))
    axis.set_xlabel("Posting hour (dataset time zone, GMT+1)")
    axis.set_ylabel("Share labelled high engagement")
    axis.set_title("High engagement by posting hour")
    fig.tight_layout()
    fig.savefig(output_dir / "high_engagement_by_hour.png")
    plt.close(fig)


def plot_engagement_by_followers(connection, feature_sql: str, output_dir: Path) -> None:
    """Show the popularity confounder by comparing follower-count bands."""
    frame = connection.execute(
        f"""
        SELECT
            follower_band,
            count(*) AS posts,
            avg(engagement_count_7d) AS mean_engagement,
            avg(high_engagement::INTEGER) AS high_engagement_share
        FROM {feature_sql}
        WHERE analysis_eligible
        GROUP BY follower_band
        """
    ).df()
    order = ["unknown", "0", "1-9", "10-99", "100-999", "1000+"]
    frame["follower_band"] = pd.Categorical(frame["follower_band"], order, ordered=True)
    frame = frame.sort_values("follower_band")
    fig, axis = plt.subplots()
    axis.bar(frame["follower_band"].astype(str), frame["high_engagement_share"], color="#e45756")
    axis.set_xlabel("Approximate follower count")
    axis.set_ylabel("Share labelled high engagement")
    axis.set_title("Author reach must be considered when interpreting engagement")
    fig.tight_layout()
    fig.savefig(output_dir / "high_engagement_by_followers.png")
    plt.close(fig)


# -----------------------------------------------------------------------------
# Main program
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Read command-line options for this script."""
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    return parser.parse_args()


def main() -> None:
    """Generate all EDA tables and plots in one repeatable run."""
    args = parse_args()
    data_dir = args.data_dir.resolve()
    feature_path = data_dir / "parquet" / "features" / "post_features.parquet"
    require_files([feature_path])

    output_dir = Path(__file__).resolve().parents[1] / "reports" / "generated" / "eda"
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_sql = parquet_scan(feature_path)
    set_plot_style()

    jobs = (
        ("summary tables", write_summary_tables),
        ("engagement distribution", plot_engagement_distribution),
        ("source comparison", plot_engagement_by_source),
        ("hour comparison", plot_engagement_by_hour),
        ("follower comparison", plot_engagement_by_followers),
    )

    connection = open_duckdb(data_dir, args.threads, args.memory_limit)
    try:
        for _, job in tqdm(jobs, desc="Creating EDA output", unit="item"):
            job(connection, feature_sql, output_dir)
    finally:
        connection.close()

    print(f"EDA output: {output_dir}")


if __name__ == "__main__":
    main()
