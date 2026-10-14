"""Sensitivity analysis for association-rule thresholds (support, confidence, lift).

Counts every candidate rule ONCE with the most permissive threshold, then
re-filters the counts in memory for a grid of settings. This is much cheaper
than re-running 05_association_rules.py for every combination.

Outputs (reports/generated/association_rules/):
  sensitivity_grid_<target>.csv        rules passing each (support, confidence) cell
  sensitivity_rules_<target>.csv       all candidates above the loosest thresholds
  rules_without_source_<target>.csv    candidates that do not use source_group
"""

from __future__ import annotations

import argparse
import csv
from itertools import combinations
from pathlib import Path

from tqdm import tqdm

from pipeline_utils import add_common_arguments, open_duckdb, parquet_scan, require_files

FEATURE_COLUMNS = (
    "post_length_band", "time_of_day", "weekday_name", "source_group",
    "follower_band", "author_activity_band", "has_image", "has_video", "is_weekend",
)
TARGET_COLUMNS = ("high_engagement", "high_reach_adjusted_engagement")

SUPPORT_GRID = (0.0001, 0.0005, 0.001, 0.002)
CONFIDENCE_GRID = (0.02, 0.05, 0.10, 0.20)


def collect_candidates(connection, feature_sql, target, max_antecedent, loosest_support):
    total, hits = connection.execute(
        f"SELECT count(*), coalesce(count_if({target}), 0) FROM {feature_sql} WHERE analysis_eligible"
    ).fetchone()
    baseline = hits / total
    min_joint = max(1, int(loosest_support * total))
    feature_sets = [
        c for n in range(1, max_antecedent + 1) for c in combinations(FEATURE_COLUMNS, n)
    ]
    candidates = []
    for columns in tqdm(feature_sets, desc=f"Counting ({target})", unit="group"):
        sel = ", ".join(columns)
        rows = connection.execute(
            f"""
            SELECT {sel}, count(*) AS n_x, coalesce(count_if({target}), 0) AS n_xh
            FROM {feature_sql} WHERE analysis_eligible
            GROUP BY {sel}
            HAVING coalesce(count_if({target}), 0) >= {min_joint}
            """
        ).fetchall()
        for row in rows:
            n_x, n_xh = int(row[-2]), int(row[-1])
            conf = n_xh / n_x
            antecedent = " AND ".join(
                f"{c}={'missing' if v is None else v}" for c, v in zip(columns, row[: len(columns)])
            )
            candidates.append({
                "antecedent": antecedent,
                "antecedent_size": len(columns),
                "uses_source": "source_group" in columns,
                "antecedent_count": n_x,
                "joint_count": n_xh,
                "support": n_xh / total,
                "confidence": conf,
                "lift": conf / baseline,
            })
    return candidates, total, baseline


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def summarise_grid(candidates, baseline):
    grid = []
    for s in SUPPORT_GRID:
        for c in CONFIDENCE_GRID:
            kept = [r for r in candidates if r["support"] >= s and r["confidence"] >= c]
            no_src = [r for r in kept if not r["uses_source"]]
            top = max(kept, key=lambda r: r["lift"], default=None)
            grid.append({
                "min_support": s,
                "min_confidence": c,
                "implied_min_lift": round(c / baseline, 2),
                "rules": len(kept),
                "rules_without_source": len(no_src),
                "min_joint_posts": int(min((r["joint_count"] for r in kept), default=0)),
                "top_rule": top["antecedent"] if top else "",
                "top_lift": round(top["lift"], 2) if top else "",
            })
    return grid


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(p)
    p.add_argument("--target", choices=("both", *TARGET_COLUMNS), default="both")
    p.add_argument("--max-antecedent", type=int, choices=(1, 2, 3), default=2)
    return p.parse_args()


def main():
    args = parse_args()
    data_dir = args.data_dir.resolve()
    feature_path = data_dir / "parquet" / "features" / "post_features.parquet"
    require_files([feature_path])
    out = Path(__file__).resolve().parents[1] / "reports" / "generated" / "association_rules"
    targets = TARGET_COLUMNS if args.target == "both" else (args.target,)

    con = open_duckdb(data_dir, args.threads, args.memory_limit)
    try:
        for target in targets:
            cands, total, base = collect_candidates(
                con, parquet_scan(feature_path), target,
                args.max_antecedent, min(SUPPORT_GRID),
            )
            cands.sort(key=lambda r: r["lift"], reverse=True)
            grid = summarise_grid(cands, base)
            write_csv(out / f"sensitivity_grid_{target}.csv", grid)
            write_csv(out / f"sensitivity_rules_{target}.csv", cands)
            loose = [r for r in cands if not r["uses_source"]
                     and r["support"] >= min(SUPPORT_GRID) and r["confidence"] >= min(CONFIDENCE_GRID)]
            write_csv(out / f"rules_without_source_{target}.csv", loose)
            print(f"\n{target}: {total:,} posts, baseline {base:.4%}")
            print(f"{'support':>8} {'conf':>6} {'rules':>6} {'no-source':>10}  top rule")
            for g in grid:
                print(f"{g['min_support']:>8} {g['min_confidence']:>6} {g['rules']:>6} "
                      f"{g['rules_without_source']:>10}  {g['top_rule']} (lift {g['top_lift']})")
    finally:
        con.close()


if __name__ == "__main__":
    main()
