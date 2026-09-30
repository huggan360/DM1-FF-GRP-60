"""Find interpretable feature combinations associated with high engagement."""

from __future__ import annotations

import argparse
import csv
from itertools import combinations
from pathlib import Path

from tqdm import tqdm

from pipeline_utils import add_common_arguments, open_duckdb, parquet_scan, require_files


# -----------------------------------------------------------------------------
# Allowed rule items
# -----------------------------------------------------------------------------

FEATURE_COLUMNS = (
    "post_length_band",
    "time_of_day",
    "weekday_name",
    "source_group",
    "follower_band",
    "author_activity_band",
    "has_image",
    "has_video",
    "is_weekend",
)

TARGET_COLUMNS = (
    "high_engagement",
    "high_reach_adjusted_engagement",
)


# -----------------------------------------------------------------------------
# Rule counting
# -----------------------------------------------------------------------------

def format_item(column: str, value: object) -> str:
    """Turn a column/value pair into one readable association-rule item."""
    if isinstance(value, bool):
        text = "yes" if value else "no"
    else:
        text = "missing" if value is None else str(value)
    return f"{column}={text}"


def count_rules(
    connection,
    feature_sql: str,
    target: str,
    max_antecedent: int,
    min_support: float,
    min_confidence: float,
    min_lift: float,
    min_count: int,
) -> list[dict[str, object]]:
    """Count candidate rules on all eligible posts and keep only supported rules."""
    total_rows, target_rows = connection.execute(
        f"""
        SELECT count(*), coalesce(count_if({target}), 0)
        FROM {feature_sql}
        WHERE analysis_eligible
        """
    ).fetchone()
    if not total_rows or not target_rows:
        raise RuntimeError(f"No usable rows were found for target {target!r}.")

    target_support = target_rows / total_rows
    feature_sets = [
        columns
        for size in range(1, max_antecedent + 1)
        for columns in combinations(FEATURE_COLUMNS, size)
    ]
    rules: list[dict[str, object]] = []

    for columns in tqdm(feature_sets, desc="Counting rule candidates", unit="group"):
        selected = ", ".join(columns)
        grouped_rows = connection.execute(
            f"""
            SELECT
                {selected},
                count(*) AS antecedent_count,
                coalesce(count_if({target}), 0) AS joint_count
            FROM {feature_sql}
            WHERE analysis_eligible
            GROUP BY {selected}
            HAVING count(*) >= {int(min_count)}
            """
        ).fetchall()

        for row in grouped_rows:
            values = row[: len(columns)]
            antecedent_count = int(row[-2])
            joint_count = int(row[-1])
            support = joint_count / total_rows
            confidence = joint_count / antecedent_count
            lift = confidence / target_support

            if support < min_support or confidence < min_confidence or lift < min_lift:
                continue

            rules.append(
                {
                    "antecedent": " AND ".join(
                        format_item(column, value)
                        for column, value in zip(columns, values, strict=True)
                    ),
                    "consequent": f"{target}=yes",
                    "antecedent_size": len(columns),
                    "antecedent_count": antecedent_count,
                    "joint_count": joint_count,
                    "support": support,
                    "confidence": confidence,
                    "lift": lift,
                    "target_baseline": target_support,
                    "eligible_posts": total_rows,
                }
            )

    return sorted(
        rules,
        key=lambda rule: (rule["lift"], rule["support"], rule["confidence"]),
        reverse=True,
    )


# -----------------------------------------------------------------------------
# Result writing
# -----------------------------------------------------------------------------

def write_rules(rules: list[dict[str, object]], output_path: Path) -> None:
    """Write rules as a normal CSV that can be inspected or used in slides."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = (
        list(rules[0])
        if rules
        else [
            "antecedent", "consequent", "antecedent_size", "antecedent_count",
            "joint_count", "support", "confidence", "lift", "target_baseline",
            "eligible_posts",
        ]
    )
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rules)


# -----------------------------------------------------------------------------
# Main program
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Read thresholds for support, confidence, lift, and rule length."""
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    parser.add_argument(
        "--target",
        choices=("both", *TARGET_COLUMNS),
        default="both",
        help="Mine raw, reach-adjusted, or both engagement targets.",
    )
    parser.add_argument("--max-antecedent", type=int, choices=(1, 2, 3), default=2)
    parser.add_argument("--min-support", type=float, default=0.001)
    parser.add_argument("--min-confidence", type=float, default=0.10)
    parser.add_argument("--min-lift", type=float, default=1.05)
    parser.add_argument("--min-count", type=int, default=500)
    return parser.parse_args()


def main() -> None:
    """Mine rules against the chosen high-engagement target."""
    args = parse_args()
    data_dir = args.data_dir.resolve()
    feature_path = data_dir / "parquet" / "features" / "post_features.parquet"
    require_files([feature_path])

    targets = TARGET_COLUMNS if args.target == "both" else (args.target,)
    output_dir = (
        Path(__file__).resolve().parents[1]
        / "reports"
        / "generated"
        / "association_rules"
    )

    connection = open_duckdb(data_dir, args.threads, args.memory_limit)
    try:
        for target in targets:
            rules = count_rules(
                connection=connection,
                feature_sql=parquet_scan(feature_path),
                target=target,
                max_antecedent=args.max_antecedent,
                min_support=args.min_support,
                min_confidence=args.min_confidence,
                min_lift=args.min_lift,
                min_count=args.min_count,
            )
            output_path = output_dir / f"rules_{target}.csv"
            write_rules(rules, output_path)
            print(f"Saved {len(rules):,} rules to {output_path}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
