# DM1-FF-GRP-60

Group 60's FriendFeed project. The pipeline prepares the raw files, explores the data and finds association rules for high engagement.

## Setup

```fish
cd ~/Documents/DM1-FF-GRP-60
python -m venv .venv
source .venv/bin/activate.fish
python -m pip install -r requirements.txt
```

Put the raw CSV files in `data/` or `data/raw/`.

## Run

```fish
python scripts/run.py
```

Run the steps separately:

```fish
python scripts/01_preprocessing.py
python scripts/02_aggregation.py
python scripts/03_feature_extraction.py
python scripts/04_exploratory_analysis.py
python scripts/05_association_rules.py
```

Useful options:

```fish
python scripts/run.py --force
python scripts/run.py --from-step 3 --to-step 5
python scripts/run.py --threads 4 --memory-limit 6GB
```

See [Data description](DATA_DESCRIPTION.md) for the table and column descriptions.
