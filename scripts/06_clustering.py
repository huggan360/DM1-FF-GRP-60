"""Cluster a reproducible post sample without using engagement as an input."""

from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.compose import ColumnTransformer
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from tqdm import tqdm

from pipeline_utils import add_common_arguments, open_duckdb, parquet_scan, require_files


# -----------------------------------------------------------------------------
# Features used for clustering
# -----------------------------------------------------------------------------

NUMERIC_FEATURES = (
    "log_character_count",
    "log_word_count",
    "log_hashtag_count",
    "log_url_count",
    "log_follower_count",
    "log_author_post_count",
    "connected_service_count",
    "hour_sine",
    "hour_cosine",
)

CATEGORICAL_FEATURES = (
    "source_group",
    "weekday_name",
    "has_image",
    "has_video",
)


# -----------------------------------------------------------------------------
# Sampling and preprocessing
# -----------------------------------------------------------------------------

def load_post_sample(connection, feature_sql: str, sample_size: int, seed: int) -> pd.DataFrame:
    """Load a deterministic hash sample and keep engagement only for interpretation."""
    return connection.execute(
        f"""
        SELECT
            post_id,
            ln(character_count + 1) AS log_character_count,
            ln(word_count + 1) AS log_word_count,
            ln(hashtag_count + 1) AS log_hashtag_count,
            ln(url_count + 1) AS log_url_count,
            ln(coalesce(follower_count, 0) + 1) AS log_follower_count,
            ln(author_post_count + 1) AS log_author_post_count,
            connected_service_count,
            sin(2 * pi() * post_hour / 24.0) AS hour_sine,
            cos(2 * pi() * post_hour / 24.0) AS hour_cosine,
            source_group,
            weekday_name,
            has_image,
            has_video,
            engagement_count_7d,
            high_engagement
        FROM (
            SELECT *
            FROM {feature_sql}
            WHERE analysis_eligible
        ) AS eligible_posts
        ORDER BY hash(post_id || ':' || CAST({seed} AS VARCHAR))
        LIMIT {max(1, sample_size)}
        """
    ).df()


def make_preprocessor() -> ColumnTransformer:
    """Scale quantities and one-hot encode categories before Euclidean clustering."""
    return ColumnTransformer(
        transformers=(
            ("numeric", StandardScaler(), list(NUMERIC_FEATURES)),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                list(CATEGORICAL_FEATURES),
            ),
        ),
        remainder="drop",
    )


# -----------------------------------------------------------------------------
# Model selection
# -----------------------------------------------------------------------------

def fit_candidate_models(
    transformed: np.ndarray,
    minimum_k: int,
    maximum_k: int,
    seed: int,
    silhouette_sample: int,
) -> tuple[MiniBatchKMeans, pd.DataFrame]:
    """Try several cluster counts and keep the model with the best silhouette score."""
    rows = []
    best_model = None
    best_score = -np.inf

    for cluster_count in tqdm(
        range(minimum_k, maximum_k + 1),
        desc="Testing cluster counts",
        unit="model",
    ):
        model = MiniBatchKMeans(
            n_clusters=cluster_count,
            random_state=seed,
            batch_size=4096,
            n_init=10,
        )
        labels = model.fit_predict(transformed)
        score = silhouette_score(
            transformed,
            labels,
            sample_size=min(silhouette_sample, len(transformed)),
            random_state=seed,
        )
        rows.append(
            {
                "clusters": cluster_count,
                "silhouette": score,
                "inertia": model.inertia_,
            }
        )
        if score > best_score:
            best_model = model
            best_score = score

    if best_model is None:
        raise RuntimeError("No clustering model could be fitted.")
    return best_model, pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# Results and plots
# -----------------------------------------------------------------------------

def write_cluster_results(
    frame: pd.DataFrame,
    scores: pd.DataFrame,
    preprocessor: ColumnTransformer,
    model: MiniBatchKMeans,
    output_dir: Path,
    model_dir: Path,
) -> None:
    """Save model-selection scores, cluster profiles, assignments, and the fitted model."""
    output_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)

    frame = frame.copy()
    transformed = preprocessor.transform(frame)
    frame["cluster"] = model.predict(transformed)

    scores.to_csv(output_dir / "cluster_model_selection.csv", index=False)
    frame[["post_id", "cluster", "engagement_count_7d", "high_engagement"]].to_csv(
        output_dir / "cluster_sample_assignments.csv",
        index=False,
    )

    profile_columns = list(NUMERIC_FEATURES) + ["engagement_count_7d", "high_engagement"]
    profiles = frame.groupby("cluster", observed=True)[profile_columns].mean()
    profiles.insert(0, "sample_posts", frame.groupby("cluster", observed=True).size())
    profiles.to_csv(output_dir / "cluster_profiles.csv")

    categorical_profiles = (
        frame.groupby("cluster", observed=True)[list(CATEGORICAL_FEATURES)]
        .agg(lambda values: values.mode().iloc[0] if not values.mode().empty else None)
    )
    categorical_profiles.to_csv(output_dir / "cluster_categorical_modes.csv")

    joblib.dump(
        {"preprocessor": preprocessor, "model": model},
        model_dir / "post_cluster_model.joblib",
    )

    fig, axis = plt.subplots(figsize=(8, 5))
    axis.plot(scores["clusters"], scores["silhouette"], marker="o")
    axis.set_xlabel("Number of clusters")
    axis.set_ylabel("Silhouette score")
    axis.set_title("Cluster-count selection")
    axis.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_dir / "cluster_silhouette.png", dpi=130)
    plt.close(fig)


# -----------------------------------------------------------------------------
# Main program
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Read sample size, random seed, and cluster-count options."""
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    parser.add_argument("--sample-size", type=int, default=100_000)
    parser.add_argument("--min-clusters", type=int, default=2)
    parser.add_argument("--max-clusters", type=int, default=8)
    parser.add_argument("--silhouette-sample", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=60)
    args = parser.parse_args()
    if args.min_clusters < 2 or args.max_clusters < args.min_clusters:
        parser.error("Cluster range must start at 2 and end at or above the start.")
    return args


def main() -> None:
    """Sample posts, fit candidate models, and save the most coherent clustering."""
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]
    data_dir = args.data_dir.resolve()
    feature_path = data_dir / "parquet" / "features" / "post_features.parquet"
    require_files([feature_path])

    connection = open_duckdb(data_dir, args.threads, args.memory_limit)
    try:
        frame = load_post_sample(
            connection,
            parquet_scan(feature_path),
            args.sample_size,
            args.seed,
        )
    finally:
        connection.close()

    if len(frame) <= args.max_clusters:
        raise RuntimeError("The eligible sample is too small for the requested cluster range.")

    preprocessor = make_preprocessor()
    transformed = preprocessor.fit_transform(frame)
    model, scores = fit_candidate_models(
        transformed,
        args.min_clusters,
        args.max_clusters,
        args.seed,
        args.silhouette_sample,
    )
    write_cluster_results(
        frame=frame,
        scores=scores,
        preprocessor=preprocessor,
        model=model,
        output_dir=project_root / "reports" / "generated" / "clustering",
        model_dir=data_dir / "models",
    )
    print(f"Selected {model.n_clusters} clusters.")


if __name__ == "__main__":
    main()
