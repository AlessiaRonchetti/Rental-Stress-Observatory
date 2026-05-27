import subprocess
import time
import csv
from datetime import datetime
from pathlib import Path


# ============================================================
# Runtime Measurement Script
# Rental Stress Observatory
# ============================================================
# This script measures the execution time of each main pipeline step:
# 1. Scraper: downloads raw datasets
# 2. Ingestion: uploads raw files to MinIO Bronze
# 3. Processing: Spark processing Bronze -> Silver -> Gold
# 4. Serving: loads Gold data into PostgreSQL
#
# Output:
# - runtime_results.csv
# ============================================================

OUTPUT_PATH = Path("/app/runtime_results.csv")


STEPS = [
    {
        "name": "Bronze ingestion",
        "script": "/app/pipeline/ingestion.py",
        "description": "Uploads raw files to MinIO Bronze layer",
        "timeout_seconds": 600
    },
    {
        "name": "Spark processing",
        "script": "/app/pipeline/processing.py",
        "description": "Processes Bronze data into Silver and Gold layers",
        "timeout_seconds": 1800
    },
    {
        "name": "PostgreSQL serving",
        "script": "/app/pipeline/serving.py",
        "description": "Loads Gold data into PostgreSQL tables",
        "timeout_seconds": 900
    }
]


def format_time(seconds):
    """
    Converts seconds into a readable format.
    """
    minutes = int(seconds // 60)
    remaining_seconds = round(seconds % 60, 2)

    if minutes == 0:
        return f"{remaining_seconds} sec"

    return f"{minutes} min {remaining_seconds} sec"


def run_step(step):
    """
    Runs a single pipeline step and measures execution time.
    If the process does not terminate within the timeout, it is stopped
    and marked as completed with shutdown issue.
    """

    print("\n" + "=" * 70)
    print(f"Running step: {step['name']}")
    print(f"Description: {step['description']}")
    print("=" * 70)

    command = f"/opt/conda/bin/python {step['script']}"

    start_time = time.time()

    try:
        result = subprocess.run(
            command,
            shell=True,
            text=True,
            timeout=step["timeout_seconds"]
        )

        elapsed_seconds = round(time.time() - start_time, 2)

        if result.returncode == 0:
            status = "success"
            notes = "Step completed successfully"
        else:
            status = "failed"
            notes = f"Step failed with return code {result.returncode}"

    except subprocess.TimeoutExpired:
        elapsed_seconds = round(time.time() - start_time, 2)

        if step["name"] == "PostgreSQL serving":
            status = "completed_with_shutdown_issue"
            notes = (
                "Serving process did not terminate cleanly. "
                "PostgreSQL output must be validated manually."
            )
        else:
            status = "timeout"
            notes = "Step exceeded maximum allowed runtime"

    runtime_readable = format_time(elapsed_seconds)

    print(f"\nStep completed: {step['name']}")
    print(f"Status: {status}")
    print(f"Runtime: {runtime_readable}")
    print(f"Notes: {notes}")

    return {
        "step": step["name"],
        "description": step["description"],
        "runtime_seconds": elapsed_seconds,
        "runtime_minutes": round(elapsed_seconds / 60, 2),
        "runtime_readable": runtime_readable,
        "status": status,
        "notes": notes,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def save_results(results):
    """
    Saves runtime results to CSV.
    """

    fieldnames = [
        "step",
        "description",
        "runtime_seconds",
        "runtime_minutes",
        "runtime_readable",
        "status",
        "notes",
        "timestamp"
    ]

    with open(OUTPUT_PATH, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("\n" + "=" * 70)
    print(f"Runtime results saved to: {OUTPUT_PATH}")
    print("=" * 70)


def print_summary(results):
    """
    Prints a readable summary for PowerPoint.
    """

    measured_results = [
        row for row in results
        if row["status"] == "success"
    ]

    total_seconds = sum(row["runtime_seconds"] for row in measured_results)

    print("\n" + "=" * 70)
    print("PIPELINE RUNTIME SUMMARY")
    print("=" * 70)

    for row in results:
        print(
            f"{row['step']}: {row['runtime_readable']} "
            f"[{row['status']}]"
        )

    print("-" * 70)

    if measured_results:
        print(f"Measured total runtime: {format_time(total_seconds)}")

        bottleneck = max(
            measured_results,
            key=lambda x: x["runtime_seconds"]
        )

        print(f"Main bottleneck: {bottleneck['step']}")
    else:
        print("No successful measured step available.")

    print("=" * 70)

    print("\nPowerPoint note:")
    print(
        "The scraper was excluded from the runtime benchmark because it "
        "runs as a scheduled background service and checks for dataset "
        "updates every 24 hours."
    )

    print(
        "PostgreSQL serving should be validated through database output "
        "because Spark/Py4J may not shut down cleanly after table creation."
    )


def main():
    print("\nStarting runtime measurement for Rental Stress Observatory pipeline")
    print("Scraper excluded: scheduled background service checking every 24h")

    all_results = []

    for step in STEPS:
        result = run_step(step)
        all_results.append(result)

        if result["status"] in ["failed", "timeout"]:
            print(f"\nPipeline benchmark stopped at: {result['step']}")
            break

    save_results(all_results)
    print_summary(all_results)


if __name__ == "__main__":
    main()