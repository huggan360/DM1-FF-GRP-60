"""Run the numbered FriendFeed pipeline scripts in order."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from tqdm import tqdm


# -----------------------------------------------------------------------------
# Pipeline order
# -----------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE = (
    "01_preprocessing.py",
    "02_aggregation.py",
    "03_feature_extraction.py",
    "04_exploratory_analysis.py",
    "05_association_rules.py",
    "06_clustering.py",
)


# -----------------------------------------------------------------------------
# Command construction
# -----------------------------------------------------------------------------

def command_for_script(script: str, args: argparse.Namespace) -> list[str]:
    """Build the exact Python command used for one numbered step."""
    command = [
        sys.executable,
        str(SCRIPT_DIR / script),
        "--data-dir",
        str(args.data_dir),
        "--threads",
        str(args.threads),
        "--memory-limit",
        args.memory_limit,
    ]
    if args.force:
        command.append("--force")
    return command


# -----------------------------------------------------------------------------
# Main program
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Read shared runner options and optional start/end step numbers."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=SCRIPT_DIR.parent / "data",
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--from-step", type=int, choices=range(1, 7), default=1)
    parser.add_argument("--to-step", type=int, choices=range(1, 7), default=6)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.from_step > args.to_step:
        parser.error("--from-step cannot be larger than --to-step")
    return args


def main() -> None:
    """Run each selected script and stop immediately if one step fails."""
    args = parse_args()
    selected = PIPELINE[args.from_step - 1 : args.to_step]
    for script in tqdm(selected, desc="Running pipeline", unit="step"):
        print(f"\nRunning {script}", flush=True)
        subprocess.run(command_for_script(script, args), check=True)
    print("Pipeline finished.")


if __name__ == "__main__":
    main()
