import os
import time
import sys
import argparse
from pathlib import Path

import pandas as pd

# Ensure we can import from the same directory (src)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from apis.kalshi_api import main as run_kalshi_api
from apis.polymarket_api import main as run_polymarket_api
from matching.matching import main as run_matching
from matching.semantic_matching import rescore_existing_matches
from apis.orderbook import run_batch_fetch as run_fetcher
from arbitrage_calculator import calculate_arbitrage as run_calculator
from liquidity_analyzer import analyze_liquidity_and_efficiency as run_liquidity_check
from history.plot_arbitrage_history import plot_arbitrage_history
from history.history_writer import (
    get_run_metadata,
    append_snapshot_from_csv,
    archive_file_copy,
    write_run_log,
    build_stage_status_dict,
)

# =========================
# Configuration
# =========================

KALSHI_CSV = "Data/kalshi_markets.csv"
POLYMARKET_CSV = "Data/polymarket_markets.csv"
MATCHES_CSV = "Data/predicted_equivalent_markets_with_close_times.csv"
ARBITRAGE_CSV = "Data/arbitrage_opportunities.csv"
LIQUIDITY_VERIFIED_CSV = "Data/liquidity_verified_arbitrage.csv"
HISTORY_DIR = "Data/history"

LIQUIDITY_VERIFIED_HISTORY_CSV = os.path.join(HISTORY_DIR, "liquidity_verified_arbitrage_snapshots.csv")
MAX_AGE_SECONDS = 36000  # 10 hours

# Historical outputs
ARCHIVE_DIR = "Data/archive"

KALSHI_HISTORY_CSV = os.path.join(HISTORY_DIR, "kalshi_market_snapshots.csv")
POLYMARKET_HISTORY_CSV = os.path.join(HISTORY_DIR, "polymarket_market_snapshots.csv")
MATCH_HISTORY_CSV = os.path.join(HISTORY_DIR, "match_snapshots.csv")
ARBITRAGE_HISTORY_CSV = os.path.join(HISTORY_DIR, "arbitrage_snapshots.csv")
RUN_LOG_CSV = os.path.join(HISTORY_DIR, "run_log.csv")

LOCK_FILE = "Data/orchestrator.lock"


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def is_file_updated(filepath, max_age):
    """Checks if a file exists and is newer than max_age (in seconds)."""
    if not os.path.exists(filepath):
        print(f"File {filepath} does not exist.")
        return False

    file_age = time.time() - os.path.getmtime(filepath)
    is_updated = file_age < max_age

    if is_updated:
        print(f"File {filepath} is up to date ({(file_age / 60):.2f}m old).")
    else:
        print(f"File {filepath} is outdated ({(file_age / 60):.2f}m old).")

    return is_updated


def acquire_lock(lock_file: str) -> bool:
    ensure_dir(os.path.dirname(lock_file))
    if os.path.exists(lock_file):
        print(f"Lock file exists: {lock_file}")
        print("Another run may still be active. Exiting.")
        return False

    with open(lock_file, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))
    return True


def release_lock(lock_file: str) -> None:
    if os.path.exists(lock_file):
        os.remove(lock_file)


def timed_stage(stage_name: str, fn, *args, **kwargs) -> dict:
    """
    Runs a stage, times it, and captures status.
    Returns a dict with status, message, and duration.
    """
    print(f"\n--- RUNNING {stage_name.upper()} ---")
    start = time.time()

    try:
        fn(*args, **kwargs)
        duration = round(time.time() - start, 3)
        print(f"{stage_name} completed in {duration:.3f}s")
        return {
            "status": "success",
            "message": "",
            "duration_sec": duration,
        }
    except Exception as e:
        duration = round(time.time() - start, 3)
        print(f"Error running {stage_name}: {e}")
        return {
            "status": "failed",
            "message": str(e),
            "duration_sec": duration,
        }


def get_row_count(filepath: str) -> int:
    if not os.path.exists(filepath):
        return 0
    try:
        return len(pd.read_csv(filepath))
    except Exception:
        return 0


def save_historical_outputs(run_id: str, snapshot_time: str) -> dict:
    """
    Appends current outputs into historical append-only files
    and archives full copies.
    Returns counts written.
    """
    print("\n--- SAVING HISTORICAL SNAPSHOTS ---")

    counts = {
        "kalshi_snapshot_rows": append_snapshot_from_csv(
            source_csv=KALSHI_CSV,
            history_csv=KALSHI_HISTORY_CSV,
            run_id=run_id,
            snapshot_time=snapshot_time,
            extra_cols={"platform": "kalshi"},
        ),
        "polymarket_snapshot_rows": append_snapshot_from_csv(
            source_csv=POLYMARKET_CSV,
            history_csv=POLYMARKET_HISTORY_CSV,
            run_id=run_id,
            snapshot_time=snapshot_time,
            extra_cols={"platform": "polymarket"},
        ),
        "match_snapshot_rows": append_snapshot_from_csv(
            source_csv=MATCHES_CSV,
            history_csv=MATCH_HISTORY_CSV,
            run_id=run_id,
            snapshot_time=snapshot_time,
        ),
        "arbitrage_snapshot_rows": append_snapshot_from_csv(
            source_csv=ARBITRAGE_CSV,
            history_csv=ARBITRAGE_HISTORY_CSV,
            run_id=run_id,
            snapshot_time=snapshot_time,
        ),
        "liquidity_verified_snapshot_rows": append_snapshot_from_csv(
            source_csv=LIQUIDITY_VERIFIED_CSV,
            history_csv=LIQUIDITY_VERIFIED_HISTORY_CSV,
            run_id=run_id,
            snapshot_time=snapshot_time,
        ),
    }

    archive_file_copy(
        source_csv=KALSHI_CSV,
        archive_dir=os.path.join(ARCHIVE_DIR, "kalshi"),
        prefix="kalshi",
        snapshot_time=snapshot_time,
    )
    archive_file_copy(
        source_csv=POLYMARKET_CSV,
        archive_dir=os.path.join(ARCHIVE_DIR, "polymarket"),
        prefix="polymarket",
        snapshot_time=snapshot_time,
    )
    archive_file_copy(
        source_csv=MATCHES_CSV,
        archive_dir=os.path.join(ARCHIVE_DIR, "matches"),
        prefix="matches",
        snapshot_time=snapshot_time,
    )
    archive_file_copy(
        source_csv=ARBITRAGE_CSV,
        archive_dir=os.path.join(ARCHIVE_DIR, "arbitrage"),
        prefix="arbitrage",
        snapshot_time=snapshot_time,
    )
    archive_file_copy(
        source_csv=LIQUIDITY_VERIFIED_CSV,
        archive_dir=os.path.join(ARCHIVE_DIR, "liquidity_verified_arbitrage"),
        prefix="liquidity_verified_arbitrage",
        snapshot_time=snapshot_time,
    )

    return counts


def run_once(args):
    if not acquire_lock(LOCK_FILE):
        return

    run_id, snapshot_time = get_run_metadata()
    run_start = time.time()

    stage_results = {}
    overall_status = "success"
    overall_message = ""

    try:
        print("=== STARTING ORCHESTRATOR ===")
        print(f"run_id: {run_id}")

        kalshi_ok = False
        poly_ok = False

        if not args.skip_api:
            kalshi_ok = is_file_updated(KALSHI_CSV, MAX_AGE_SECONDS) and not args.force
            poly_ok = is_file_updated(POLYMARKET_CSV, MAX_AGE_SECONDS) and not args.force

            if not kalshi_ok:
                stage_results["kalshi_api"] = timed_stage("kalshi_api", run_kalshi_api)
            else:
                print("\n--- SKIPPING KALSHI API (fresh file exists) ---")
                stage_results["kalshi_api"] = {
                    "status": "skipped",
                    "message": "Fresh file exists",
                    "duration_sec": 0.0,
                }

            if not poly_ok:
                stage_results["polymarket_api"] = timed_stage("polymarket_api", run_polymarket_api)
            else:
                print("\n--- SKIPPING POLYMARKET API (fresh file exists) ---")
                stage_results["polymarket_api"] = {
                    "status": "skipped",
                    "message": "Fresh file exists",
                    "duration_sec": 0.0,
                }
        else:
            print("\n--- SKIPPING API FETCH ---")
            stage_results["kalshi_api"] = {
                "status": "skipped",
                "message": "--skip-api used",
                "duration_sec": 0.0,
            }
            stage_results["polymarket_api"] = {
                "status": "skipped",
                "message": "--skip-api used",
                "duration_sec": 0.0,
            }

        stage_results["matching"] = timed_stage("matching", run_matching)

        if stage_results["matching"]["status"] != "success":
            overall_status = "failed"
            overall_message = "Matching stage failed"
            print("\nMatching failed; skipping downstream stages.")
        else:
            if args.semantic:
                print("\n--- RUNNING SEMANTIC RESCORING ---")
                semantic_start = time.time()
                try:
                    if os.path.exists(MATCHES_CSV):
                        matches_df = pd.read_csv(MATCHES_CSV)
                        rescored_df = rescore_existing_matches(
                            matches_df,
                            threshold=args.semantic_threshold,
                        )
                        rescored_df.to_csv(MATCHES_CSV, index=False)
                        duration = round(time.time() - semantic_start, 3)
                        print(f"Rescored {len(rescored_df)} matches and saved to {MATCHES_CSV}")
                        stage_results["semantic_rescoring"] = {
                            "status": "success",
                            "message": "",
                            "duration_sec": duration,
                        }
                    else:
                        duration = round(time.time() - semantic_start, 3)
                        msg = f"Matches file {MATCHES_CSV} not found. Skipping semantic rescoring."
                        print(msg)
                        stage_results["semantic_rescoring"] = {
                            "status": "skipped",
                            "message": msg,
                            "duration_sec": duration,
                        }
                except Exception as e:
                    duration = round(time.time() - semantic_start, 3)
                    print(f"Error during semantic rescoring: {e}")
                    stage_results["semantic_rescoring"] = {
                        "status": "failed",
                        "message": str(e),
                        "duration_sec": duration,
                    }
            else:
                stage_results["semantic_rescoring"] = {
                    "status": "skipped",
                    "message": "Semantic rescoring not enabled",
                    "duration_sec": 0.0,
                }

            stage_results["orderbook_fetcher"] = timed_stage("orderbook_fetcher", run_fetcher)
            stage_results["arbitrage_calculator"] = timed_stage("arbitrage_calculator", run_calculator)
            stage_results["liquidity_check"] = timed_stage("liquidity_check", run_liquidity_check)

        history_counts = save_historical_outputs(run_id=run_id, snapshot_time=snapshot_time)

        if args.plot:
            print("\n--- GENERATING ARBITRAGE HISTORY PLOT ---")
            plot_start = time.time()
            try:
                plot_arbitrage_history(
                    input_csv=LIQUIDITY_VERIFIED_HISTORY_CSV,
                    output_png=os.path.join(HISTORY_DIR, "liquidity_verified_arbitrage_history_plot.png"),
                    top_n=args.plot_top_n,
                    min_profit=args.plot_min_profit,
                )
                stage_results["plot_arbitrage_history"] = {
                    "status": "success",
                    "message": "",
                    "duration_sec": round(time.time() - plot_start, 3),
                }
            except Exception as e:
                print(f"Error generating arbitrage history plot: {e}")
                stage_results["plot_arbitrage_history"] = {
                    "status": "failed",
                    "message": str(e),
                    "duration_sec": round(time.time() - plot_start, 3),
                }
        else:
            stage_results["plot_arbitrage_history"] = {
                "status": "skipped",
                "message": "Plotting not enabled",
                "duration_sec": 0.0,
            }

        total_duration = round(time.time() - run_start, 3)

        run_log_row = {
            "run_id": run_id,
            "snapshot_time": snapshot_time,
            "overall_status": overall_status,
            "overall_message": overall_message,
            "total_duration_sec": total_duration,
            "kalshi_current_rows": get_row_count(KALSHI_CSV),
            "polymarket_current_rows": get_row_count(POLYMARKET_CSV),
            "match_current_rows": get_row_count(MATCHES_CSV),
            "arbitrage_current_rows": get_row_count(ARBITRAGE_CSV),
            **history_counts,
            **build_stage_status_dict(stage_results),
        }

        write_run_log(RUN_LOG_CSV, run_log_row)
        print("\n=== ORCHESTRATOR DONE ===")

    except Exception as e:
        total_duration = round(time.time() - run_start, 3)
        print(f"\nFATAL ORCHESTRATOR ERROR: {e}")

        run_log_row = {
            "run_id": run_id,
            "snapshot_time": snapshot_time,
            "overall_status": "failed",
            "overall_message": str(e),
            "total_duration_sec": total_duration,
            **build_stage_status_dict(stage_results),
        }

        try:
            write_run_log(RUN_LOG_CSV, run_log_row)
        except Exception as log_error:
            print(f"Failed to write run log after fatal error: {log_error}")

    finally:
        release_lock(LOCK_FILE)
def main():
    parser = argparse.ArgumentParser(description="PolyKalshi Orchestrator")
    parser.add_argument("--force", action="store_true", help="Force refresh of all data")
    parser.add_argument("--skip-api", action="store_true", help="Skip fetching data from APIs")
    parser.add_argument("--semantic", action="store_true", help="Use semantic matching for rescoring")
    parser.add_argument("--semantic-threshold", type=float, default=0.40, help="Threshold for semantic matching")
    parser.add_argument("--plot", action="store_true", help="Generate arbitrage history plot after run")
    parser.add_argument("--plot-top-n", type=int, default=15, help="Top N arbitrages to plot")
    parser.add_argument("--plot-min-profit", type=float, default=None, help="Minimum profit_pct to include in plot")
    parser.add_argument("--loop", action="store_true", help="Run continuously on a fixed interval")
    parser.add_argument("--interval-minutes", type=int, default=30, help="Minutes between runs in loop mode")
    args = parser.parse_args()

    if not args.loop:
        run_once(args)
        return

    interval_seconds = max(args.interval_minutes, 1) * 60
    iteration = 1

    print(f"=== STARTING CONTINUOUS MODE: every {args.interval_minutes} minutes ===")

    while True:
        cycle_start = time.time()
        print(f"\n\n########## LOOP ITERATION {iteration} ##########")
        run_once(args)

        elapsed = time.time() - cycle_start
        sleep_seconds = max(interval_seconds - elapsed, 0)

        print(f"\nIteration {iteration} finished in {elapsed:.1f}s")
        print(f"Sleeping for {sleep_seconds / 60:.2f} minutes... Press Ctrl+C to stop.")

        try:
            time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            print("\nContinuous mode stopped by user.")
            break

        iteration += 1
if __name__ == "__main__":
    main()