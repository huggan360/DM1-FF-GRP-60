# DM1-FF-GRP-60

FriendFeed data-mining project for Group 60.

Research question:

> Which combinations of post format, posting time, source, and author
> characteristics are associated with unusually high engagement on FriendFeed,
> and do these patterns remain after accounting for audience size?

The raw dataset and all generated Parquet files stay inside `data/`, which is
ignored by Git. Only reproducible code, documentation, and report source files
belong in the repository.

## Setup

Python 3.11 or newer is recommended.

```bash
cd ~/Documents/DM1-FF-GRP-60
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The scripts accept either of these raw-data layouts:

```text
data/entries1.csv
data/entries2.csv
...
```

or:

```text
data/raw/entries1.csv
data/raw/entries2.csv
...
```

## Run order

Run one step at a time while developing:

```bash
python scripts/01_preprocessing.py
python scripts/02_aggregation.py
python scripts/03_feature_extraction.py
python scripts/04_exploratory_analysis.py
python scripts/05_association_rules.py
python scripts/06_clustering.py
```

Or run the complete pipeline:

```bash
python scripts/run_pipeline.py
```

Useful runner examples:

```bash
# Re-run only feature extraction through association rules.
python scripts/run_pipeline.py --from-step 3 --to-step 5 --force

# Limit DuckDB when running on a smaller computer.
python scripts/run_pipeline.py --threads 4 --memory-limit 6GB
```

`--force` replaces an existing output. Without it, the expensive preparation
stages skip files that have already been built.

## What each script does

1. `01_preprocessing.py`
   - Reads every source file with explicit names, delimiters, and raw string types.
   - Converts timestamps and numeric columns safely.
   - Adds identifier and timestamp quality flags.
   - Writes Zstandard-compressed Parquet files.
   - Writes `data/quality/preprocessing_summary.csv`.

2. `02_aggregation.py`
   - Deduplicates post IDs before joining.
   - Counts recorded and seven-day likes/comments per post.
   - Builds author activity, service, follower, and following statistics.
   - Uses `subscriptions.csv` as the static network snapshot: the first column is
     treated as the subscriber and the second as the account being followed.

3. `03_feature_extraction.py`
   - Produces one analysis-ready row per post.
   - Extracts text length, word, hashtag, URL, image, video, time, source, and
     author features.
   - Gives posts a common seven-day engagement window.
   - Defines high engagement using the configurable 99th percentile by default.

4. `04_exploratory_analysis.py`
   - Creates the initial data-quality and descriptive CSV files.
   - Produces engagement, source, time, and follower plots for the preliminary
     presentation.

5. `05_association_rules.py`
   - Finds supported combinations pointing to high engagement.
   - Reports support, confidence, lift, and record counts.
   - Counts candidates on the full eligible post table rather than a sample.
   - Mines both raw and reach-adjusted engagement targets by default. Use
     `--target high_engagement` to run only one.

6. `06_clustering.py`
   - Draws a reproducible post sample.
   - Scales quantities and one-hot encodes categories.
   - Tests several MiniBatch K-means cluster counts with silhouette scores.
   - Does not use engagement as a clustering input; engagement is examined only
     after clusters have been formed.

## Main outputs

```text
data/parquet/clean/          typed source tables
data/parquet/aggregated/     engagement and author aggregates
data/parquet/features/       final post-level feature table
data/quality/                preprocessing summaries and thresholds
data/models/                 fitted clustering model
reports/generated/eda/       preliminary tables and plots
reports/generated/association_rules/
reports/generated/clustering/
```

Generated data and figures are ignored so that running the pipeline does not
accidentally add user content or multi-gigabyte files to Git.

## Important interpretation notes

- Results describe associations, not causal effects.
- Engagement is counted during the first seven days after a post. Posts from
  September 24 onward are excluded from analyses requiring a complete window.
- The follower graph is a later static snapshot, so follower counts are an
  approximation of audience size at posting time.
- `high_engagement` uses raw likes plus comments. A second label,
  `high_reach_adjusted_engagement`, is included as a sensitivity analysis.
- Text is multilingual and contains HTML. The pipeline extracts simple structural
  features but does not infer sentiment or language.

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for the table assumptions and
generated fields.

## LaTeX report

The Overleaf-ready project report is in `latex/main.tex`. Its required figures
are self-contained under `latex/plots/`, and `latex/main.pdf` is a compiled
six-page preview.
