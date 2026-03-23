import os
import shutil
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd


PIPELINE_VERSION = "2026-03-23-a"


def ensure_parent_dir(filepath: str) -> None:
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(dirpath: str) -> None:
    Path(dirpath).mkdir(parents=True, exist_ok=True)


def get_run_metadata() -> tuple[str, str]:
    """
    Returns:
        run_id: stable identifier for this run
        snapshot_time: ISO UTC timestamp
    """
    now = datetime.now(UTC).replace(microsecond=0)
    snapshot_time = now.isoformat()
    run_id = snapshot_time
    return run_id, snapshot_time


def safe_timestamp_for_filename(timestamp: str) -> str:
    return (
        timestamp.replace(":", "-")
        .replace("+00:00", "Z")
    )


def load_csv_if_exists(filepath: str) -> pd.DataFrame:
    if not os.path.exists(filepath):
        return pd.DataFrame()
    return pd.read_csv(filepath)


def append_dataframe(df: pd.DataFrame, output_csv: str) -> int:
    """
    Appends DataFrame rows to a CSV, creating the file if needed.
    Returns number of rows written.
    """
    ensure_parent_dir(output_csv)

    if df.empty:
        if not os.path.exists(output_csv):
            # create an empty file with no rows only if columns exist
            if len(df.columns) > 0:
                df.to_csv(output_csv, index=False)
        return 0

    write_header = not os.path.exists(output_csv)
    df.to_csv(output_csv, mode="a", header=write_header, index=False)
    return len(df)


def append_snapshot_from_csv(
    source_csv: str,
    history_csv: str,
    run_id: str,
    snapshot_time: str,
    extra_cols: dict | None = None,
) -> int:
    """
    Reads a current-state CSV, adds metadata columns, and appends to a history CSV.
    """
    if not os.path.exists(source_csv):
        print(f"[history] Source file missing, skipping snapshot: {source_csv}")
        return 0

    df = pd.read_csv(source_csv)

    df["run_id"] = run_id
    df["snapshot_time"] = snapshot_time
    df["pipeline_version"] = PIPELINE_VERSION

    if extra_cols:
        for key, value in extra_cols.items():
            df[key] = value

    rows_written = append_dataframe(df, history_csv)
    print(f"[history] Appended {rows_written} rows to {history_csv}")
    return rows_written


def archive_file_copy(source_csv: str, archive_dir: str, prefix: str, snapshot_time: str) -> str | None:
    """
    Copies the raw output CSV into an archive folder with a timestamped filename.
    """
    if not os.path.exists(source_csv):
        print(f"[history] Source file missing, skipping archive: {source_csv}")
        return None

    ensure_dir(archive_dir)
    safe_ts = safe_timestamp_for_filename(snapshot_time)
    archive_path = os.path.join(archive_dir, f"{prefix}_{safe_ts}.csv")
    shutil.copy2(source_csv, archive_path)
    print(f"[history] Archived {source_csv} -> {archive_path}")
    return archive_path


def write_run_log(run_log_csv: str, row: dict) -> None:
    df = pd.DataFrame([row])
    append_dataframe(df, run_log_csv)
    print(f"[history] Wrote run log row to {run_log_csv}")


def build_stage_status_dict(stage_results: dict[str, dict]) -> dict:
    """
    Flattens stage results into run-log-friendly columns.
    """
    out = {}
    for stage_name, info in stage_results.items():
        out[f"{stage_name}_status"] = info.get("status", "unknown")
        out[f"{stage_name}_message"] = info.get("message", "")
        out[f"{stage_name}_duration_sec"] = info.get("duration_sec", None)
    return out